from dataclasses import replace
from uuid import uuid4

import pytest

from codehub.changes import review_document
from codehub.domain import DomainError, Forbidden, Operation, canonical, effects_for
from codehub.identity import membership_document
from codehub.issues import comment_document
from codehub.projects import ref_name


def operation(kind="issue.create", payload=None, revision=1):
    return Operation(
        str(uuid4()),
        str(uuid4()),
        str(uuid4()),
        None if kind.split(".")[0] in {"membership", "organization"} else str(uuid4()),
        revision,
        kind,
        payload or {"title": "Connection pooling", "body": "Bound retry concurrency", "state": "open"},
    )


@pytest.mark.parametrize("revision", [True, False, 0, -1, "1", 1.0])
def test_revision_is_a_client_integer(revision):
    with pytest.raises(DomainError):
        operation(revision=revision)


def test_payload_has_no_opaque_extra_fields_or_nonfinite_values():
    with pytest.raises(DomainError):
        operation(payload={"title": "Cache", "state": "open", "hidden": "value"})
    with pytest.raises(DomainError):
        canonical({"value": float("nan")})


def test_comments_and_reviews_cannot_cross_project_boundaries():
    op = operation("comment.create", {"issue_id": str(uuid4()), "body": "Add tests"})
    with pytest.raises(DomainError):
        comment_document(op, str(uuid4()), lambda _table, _id: {"project_id": str(uuid4())})
    reviewer = str(uuid4())
    change_id = str(uuid4())
    head = "a" * 40
    review = operation(
        "review.create", {"change_id": change_id, "head_sha": head, "verdict": "approve", "body": "Looks good"}
    )
    change = {"project_id": review.project_id, "state": "open", "head_sha": head, "author_id": reviewer}
    with pytest.raises(Forbidden):
        review_document(review, reviewer, lambda _table, _id: change)
    change["author_id"] = str(uuid4())
    change["head_sha"] = "b" * 40
    with pytest.raises(DomainError):
        review_document(review, reviewer, lambda _table, _id: change)


def test_only_an_owner_can_create_another_owner():
    member = str(uuid4())
    op = replace(operation("membership.set", {"user_id": member, "role": "owner"}), entity_id=member, project_id=None)
    with pytest.raises(Forbidden):
        membership_document(
            op, str(uuid4()), lambda table, _id: "admin" if table == "memberships_role" else {"id": member}
        )


def test_effect_map_only_builds_repository_changes_and_applies_subscriptions():
    op = operation("comment.create", {"issue_id": str(uuid4()), "body": "Metrics look healthy"})
    subscription = {
        "id": str(uuid4()),
        "url": "https://notifications.example/events",
        "enabled": True,
        "events": ["comment.create"],
    }
    effects = effects_for(op, [subscription])
    assert [effect.kind for effect in effects] == ["search", "delivery"]
    assert effects[0].identity(op.event_id) != effects[1].identity(op.event_id)
    push = operation("repository.push", {"ref": "refs/heads/main", "commit_sha": "a" * 40})
    assert [effect.kind for effect in effects_for(push, [])] == ["search", "build"]


@pytest.mark.parametrize(
    "value",
    ["HEAD", "refs/heads/../../secret", "refs/heads/a.lock", "refs/heads/a b", "refs/heads/a^", "refs/heads/a\\b"],
)
def test_unsafe_git_ref_names_are_rejected(value):
    with pytest.raises(DomainError):
        ref_name({"ref": value}, "ref")
