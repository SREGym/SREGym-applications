"""Run against a dedicated MySQL database, never an in-memory storage replacement."""

import os
import secrets
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from codehub.config import Settings
from codehub.domain import Conflict, DomainError, Forbidden, Operation
from codehub.storage.mysql import MySQLStorage

pytestmark = pytest.mark.integration


@pytest.fixture
def storage():
    if not os.environ.get("CODEHUB_TEST_MYSQL_DATABASE"):
        pytest.skip("Requires explicitly selected dedicated MySQL test database")
    settings = Settings.load()
    settings = replace(settings, mysql_database=os.environ["CODEHUB_TEST_MYSQL_DATABASE"], mysql_read_host=None)
    store = MySQLStorage(settings)
    store.migrate(Path(__file__).parents[1] / "migrations/mysql/001_initial.sql")
    return store


def setup_project(storage):
    owner = str(uuid4())
    credential = secrets.token_urlsafe(32)
    storage.register_user(owner, f"morgan-{owner[:8]}", credential)
    assert storage.authenticate(credential) == owner
    tenant, project = str(uuid4()), str(uuid4())
    storage.accept(
        Operation(
            str(uuid4()),
            tenant,
            tenant,
            None,
            1,
            "organization.create",
            {"slug": f"harbor-{tenant[:8]}", "name": "Harbor Systems"},
        ),
        owner,
    )
    storage.accept(
        Operation(
            str(uuid4()),
            tenant,
            project,
            project,
            1,
            "project.create",
            {"slug": "request-router", "name": "Request Router", "default_ref": "refs/heads/main"},
        ),
        owner,
    )
    return owner, tenant, project


def test_acknowledgement_is_atomic_and_conflicts_do_not_change_history(storage):
    owner, tenant, project = setup_project(storage)
    issue = str(uuid4())
    original = Operation(
        str(uuid4()),
        tenant,
        issue,
        project,
        1,
        "issue.create",
        {"title": "Request timeout", "body": "Track timeout configuration", "state": "open"},
    )
    receipt = storage.accept(original, owner)
    assert receipt["replayed"] is False
    assert storage.accept(original, owner)["replayed"] is True
    with pytest.raises(Conflict):
        storage.accept(replace(original, payload={**original.payload, "title": "Changed identity"}), owner)
    newer = replace(
        original,
        event_id=str(uuid4()),
        client_revision=3,
        kind="issue.update",
        payload={"title": "Bounded request timeout", "body": "Maximum timeout is documented", "state": "closed"},
    )
    earlier = replace(
        newer,
        event_id=str(uuid4()),
        client_revision=2,
        payload={"title": "Timeout investigation", "body": "Check slow reads", "state": "open"},
    )
    storage.accept(newer, owner)
    storage.accept(earlier, owner)
    assert storage.get_entity(tenant, issue, owner)["revision"] == 3
    assert storage.get_entity(tenant, issue, owner)["document"]["title"] == newer.payload["title"]
    with storage.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS n FROM journal WHERE entity_id=%s", (issue,))
        assert cursor.fetchone()["n"] == 3
        cursor.execute(
            "SELECT COUNT(*) AS n FROM outbox WHERE event_id IN (%s,%s,%s)",
            (original.event_id, newer.event_id, earlier.event_id),
        )
        assert cursor.fetchone()["n"] == 3


def test_tenant_authorization_and_fk_rejections_leave_no_accepted_event(storage):
    owner, tenant, project = setup_project(storage)
    outsider = str(uuid4())
    storage.register_user(outsider, f"jamie-{outsider[:8]}", secrets.token_urlsafe(32))
    op = Operation(
        str(uuid4()),
        tenant,
        str(uuid4()),
        project,
        1,
        "comment.create",
        {"issue_id": str(uuid4()), "body": "Please add telemetry"},
    )
    with pytest.raises(Forbidden):
        storage.accept(op, outsider)
    with pytest.raises(DomainError):
        storage.accept(op, owner)
    with storage.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS n FROM journal WHERE event_id=%s", (op.event_id,))
        assert cursor.fetchone()["n"] == 0
        cursor.execute("SELECT COUNT(*) AS n FROM outbox WHERE event_id=%s", (op.event_id,))
        assert cursor.fetchone()["n"] == 0


def test_secondary_unique_identity_and_implicit_owner_are_not_overwritten(storage):
    owner, tenant, project = setup_project(storage)
    duplicate = str(uuid4())
    with pytest.raises(Conflict):
        storage.accept(
            Operation(
                str(uuid4()),
                tenant,
                duplicate,
                duplicate,
                1,
                "project.create",
                {"slug": "request-router", "name": "Conflicting project", "default_ref": "refs/heads/main"},
            ),
            owner,
        )
    assert storage.get_entity(tenant, project, owner)["document"]["name"] == "Request Router"
    with pytest.raises(Conflict):
        storage.accept(
            Operation(str(uuid4()), tenant, owner, None, 1, "membership.set", {"user_id": owner, "role": "viewer"}),
            owner,
        )


def test_bounded_entity_lookup_preserves_real_sql_authorization_and_single_read_shape(storage):
    owner, tenant, project = setup_project(storage)
    outsider, other_tenant, other_project = setup_project(storage)
    issue = str(uuid4())
    storage.accept(
        Operation(
            str(uuid4()),
            tenant,
            issue,
            project,
            1,
            "issue.create",
            {"title": "Connection budget", "body": "Bound request duration", "state": "open"},
        ),
        owner,
    )
    ids = [issue, other_project, project, *(str(uuid4()) for _ in range(125))]
    rows = storage.get_entities(tenant, ids, owner)
    assert rows == sorted(
        [storage.get_entity(tenant, issue, owner), storage.get_entity(tenant, project, owner)],
        key=lambda item: item["id"],
    )
    assert storage.get_entities(tenant, [str(uuid4())], owner) == []
    with pytest.raises(Forbidden):
        storage.get_entities(tenant, [project], outsider)
    with pytest.raises(Forbidden):
        storage.get_entities(other_tenant, [other_project], owner)
    with pytest.raises(DomainError):
        storage.get_entities(tenant, [issue, issue], owner)
