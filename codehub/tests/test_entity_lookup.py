"""Bounded authenticated entity lookups and real SQLite projection reads."""

import json
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from codehub.api import make_app
from codehub.config import Settings
from codehub.domain import DomainError, Forbidden
from codehub.search import SearchIndex, make_app as make_search_app
from codehub.storage.base import MAX_ENTITY_DOCUMENT_BYTES
from codehub.storage.mysql import MySQLStorage
from codehub.storage.routing import StorageRouter


def uid(value):
    return str(UUID(int=value))


@pytest.fixture
def customer():
    store = Mock()
    store.authenticate.side_effect = lambda token: uid(10) if token == "customer-token" else reject_credential()
    store.get_entities.return_value = [
        {
            "tenant_id": uid(100),
            "id": uid(1),
            "project_id": uid(200),
            "entity_type": "issue",
            "revision": 2,
            "document": {"title": "Connection budget", "body": "Bound request duration", "state": "open"},
        }
    ]
    router = Mock()
    router.for_tenant.return_value = store
    with TestClient(make_app(settings=SimpleNamespace(region="region-a"), router=router, client=Mock())) as client:
        yield client, router, store


def reject_credential():
    raise Forbidden("Valid user credentials are required")


def test_customer_lookup_uses_authenticated_tenant_and_returns_only_items(customer):
    client, router, store = customer
    ids = [uid(1), uid(2)]
    response = client.post(
        f"/v1/tenants/{uid(100)}/entities/lookup", json={"ids": ids}, headers={"Authorization": "Bearer customer-token"}
    )
    assert response.status_code == 200
    assert response.json() == {"items": store.get_entities.return_value}
    router.for_tenant.assert_called_once_with(uid(100))
    store.authenticate.assert_called_once_with("customer-token")
    store.get_entities.assert_called_once_with(uid(100), tuple(ids), uid(10))
    store.get_entity.assert_not_called()


@pytest.mark.parametrize("header", [None, "Bearer invalid", "Basic customer-token"])
def test_customer_lookup_requires_customer_credentials(customer, header):
    client, _router, store = customer
    response = client.post(
        f"/v1/tenants/{uid(100)}/entities/lookup",
        json={"ids": [uid(1)]},
        headers={"Authorization": header} if header else {},
    )
    assert response.status_code == 403
    store.get_entities.assert_not_called()


def test_customer_lookup_retains_membership_rejection(customer):
    client, _router, store = customer
    store.get_entities.side_effect = Forbidden("Membership does not permit this operation")
    response = client.post(
        f"/v1/tenants/{uid(100)}/entities/lookup",
        json={"ids": [uid(1)]},
        headers={"Authorization": "Bearer customer-token"},
    )
    assert response.status_code == 403


def test_waiting_customer_lookup_does_not_block_other_http_requests(customer):
    client, _router, store = customer
    started, release = threading.Event(), threading.Event()
    lookup_responses, health_responses = [], []

    def read(*_args):
        started.set()
        if not release.wait(5):
            raise RuntimeError("Test database gate was not released")
        return []

    store.get_entities.side_effect = read
    lookup = threading.Thread(
        target=lambda: lookup_responses.append(
            client.post(
                f"/v1/tenants/{uid(100)}/entities/lookup",
                json={"ids": [uid(1)]},
                headers={"Authorization": "Bearer customer-token"},
            )
        )
    )
    health = threading.Thread(target=lambda: health_responses.append(client.get("/healthz")))
    try:
        lookup.start()
        assert started.wait(3)
        health.start()
        health.join(3)
        assert not health.is_alive(), "A waiting database lookup blocked the HTTP event loop"
        assert health_responses[0].status_code == 200
        assert lookup.is_alive()
    finally:
        release.set()
        lookup.join(3)
        if health.ident is not None:
            health.join(3)
    assert lookup_responses[0].status_code == 200


INVALID_LOOKUPS = [
    {},
    {"ids": []},
    {"ids": None},
    {"ids": uid(1)},
    {"ids": [True]},
    {"ids": ["not-a-uuid"]},
    {"ids": [uid(0xABCDEF).upper()]},
    {"ids": [uid(1), uid(1)]},
    {"ids": [uid(value) for value in range(129)]},
    {"ids": [uid(1)], "limit": 2},
]


@pytest.mark.parametrize("body", INVALID_LOOKUPS)
def test_customer_lookup_rejects_unbounded_or_ambiguous_ids_before_storage(customer, body):
    client, router, _store = customer
    response = client.post(
        f"/v1/tenants/{uid(100)}/entities/lookup", json=body, headers={"Authorization": "Bearer customer-token"}
    )
    assert response.status_code == 422
    router.for_tenant.assert_not_called()


@pytest.mark.parametrize("content", [b"not JSON", b"[]", json.dumps({"ids": [uid(1)]}).encode() + b" " * 8192])
def test_customer_lookup_enforces_json_object_and_request_byte_bound(customer, content):
    client, router, _store = customer
    response = client.post(
        f"/v1/tenants/{uid(100)}/entities/lookup", content=content, headers={"Authorization": "Bearer customer-token"}
    )
    assert response.status_code == 422
    router.for_tenant.assert_not_called()


def test_customer_lookup_accepts_the_full_128_id_batch_and_rejects_bad_tenant(customer):
    client, router, store = customer
    response = client.post(
        f"/v1/tenants/{uid(100)}/entities/lookup",
        json={"ids": [uid(value) for value in range(128)]},
        headers={"Authorization": "Bearer customer-token"},
    )
    assert response.status_code == 200
    assert len(store.get_entities.call_args.args[1]) == 128
    router.reset_mock()
    assert client.post("/v1/tenants/not-a-uuid/entities/lookup", json={"ids": [uid(1)]}).status_code == 422
    router.for_tenant.assert_not_called()


def mysql_observation(monkeypatch, *, role="viewer", document=None):
    store = MySQLStorage(SimpleNamespace())
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"role": role} if role is not None else None
    cursor.fetchall.return_value = [
        {
            "tenant_id": uid(100),
            "id": uid(1),
            "project_id": uid(200),
            "entity_type": "issue",
            "revision": 1,
            "document": document if document is not None else '{"title":"Connection budget"}',
        }
    ]
    modes = []

    @contextmanager
    def connect(*, read=False):
        modes.append(read)
        yield connection

    monkeypatch.setattr(store, "connection", connect)
    monkeypatch.setattr(store, "get_entity", Mock(side_effect=AssertionError("Lookup must not perform per-ID reads")))
    return store, cursor, modes


@pytest.mark.parametrize("role", ["owner", "admin", "developer", "viewer"])
def test_mysql_batch_uses_one_authorization_one_parameterized_select_and_reader(monkeypatch, role):
    store, cursor, modes = mysql_observation(monkeypatch, role=role)
    rows = store.get_entities(uid(100), [uid(2), uid(1)], uid(10))
    assert rows[0]["document"] == {"title": "Connection budget"}
    assert modes == [True]
    assert cursor.execute.call_count == 2
    membership, entities = [call.args for call in cursor.execute.call_args_list]
    assert "FROM memberships" in membership[0] and membership[1] == (uid(100), uid(10))
    assert "FROM entities WHERE tenant_id=%s AND id IN (%s,%s) ORDER BY id" in entities[0]
    assert entities[1] == (MAX_ENTITY_DOCUMENT_BYTES, uid(100), uid(2), uid(1))
    assert uid(1) not in entities[0]


@pytest.mark.parametrize("role", [None, "suspended"])
def test_mysql_batch_denies_membership_before_reading_entities(monkeypatch, role):
    store, cursor, _modes = mysql_observation(monkeypatch, role=role)
    with pytest.raises(Forbidden):
        store.get_entities(uid(100), [uid(1)], uid(10))
    assert cursor.execute.call_count == 1
    cursor.fetchall.assert_not_called()


def test_mysql_batch_rejects_server_guarded_oversized_document(monkeypatch):
    store, cursor, _modes = mysql_observation(monkeypatch)
    cursor.fetchall.return_value[0]["document"] = None
    with pytest.raises(DomainError, match="document exceeds"):
        store.get_entities(uid(100), [uid(1)], uid(10))
    assert "OCTET_LENGTH(document)" in cursor.execute.call_args.args[0]


def test_tenant_batch_selects_its_group_and_actual_reader_port(tmp_path, monkeypatch):
    routes = tmp_path / "database-routes.json"
    routes.write_text(
        json.dumps(
            {
                uid(100): {
                    "group": "group-1",
                    "writer_host": "writer-b",
                    "reader_host": "reader-b",
                    "port": 3307,
                    "read_port": 3308,
                    "database": "tenant_group_b",
                }
            }
        )
    )
    settings = Settings(
        "writer-a",
        3306,
        "codehub",
        "user",
        "private",
        "region-a",
        "",
        (),
        "",
        tmp_path,
        mysql_read_host="reader-a",
        routes_file=routes,
    )
    store = StorageRouter(settings).for_tenant(uid(100))
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"role": "viewer"}
    cursor.fetchall.return_value = []
    connect = Mock(return_value=connection)
    monkeypatch.setattr("codehub.storage.mysql.pymysql.connect", connect)
    assert store.get_entities(uid(100), [uid(1)], uid(10)) == []
    assert connect.call_args.kwargs["host"] == "reader-b"
    assert connect.call_args.kwargs["port"] == 3308
    assert connect.call_args.kwargs["database"] == "tenant_group_b"


@pytest.fixture
def projection(tmp_path):
    index = SearchIndex(tmp_path / "search.sqlite")
    for sequence, (tenant, entity) in enumerate([(uid(100), uid(1)), (uid(100), uid(2)), (uid(101), uid(3))], 1):
        index.index(
            f"{sequence:064x}",
            {
                "event_id": entity,
                "tenant_id": tenant,
                "entity_id": entity,
                "project_id": uid(200),
                "client_revision": 1,
                "kind": "issue.create",
                "payload": {"title": "Connection budget", "state": "open"},
            },
        )
    return index


def test_sqlite_batch_is_one_real_query_preserves_shape_and_excludes_other_tenants(projection, monkeypatch):
    expected = [projection.document(uid(100), uid(value)) for value in (1, 2)]
    statements = []
    original = projection.connect

    @contextmanager
    def traced():
        with original() as connection:
            connection.set_trace_callback(statements.append)
            yield connection

    monkeypatch.setattr(projection, "connect", traced)
    monkeypatch.setattr(projection, "document", Mock(side_effect=AssertionError("Lookup must use one query")))
    assert projection.documents(uid(100), [uid(3), uid(2), uid(9), uid(1)]) == expected
    assert len([sql for sql in statements if "FROM documents WHERE" in sql]) == 1
    assert projection.documents(uid(100), [uid(9)]) == []


def test_sqlite_batch_bounds_encoded_document_bytes_without_silent_omission(projection):
    with projection.connect() as connection:
        connection.execute(
            "UPDATE documents SET document=? WHERE tenant_id=? AND id=?",
            (json.dumps({"body": "x" * MAX_ENTITY_DOCUMENT_BYTES}), uid(100), uid(1)),
        )
    with pytest.raises(DomainError, match="document exceeds"):
        projection.documents(uid(100), [uid(1), uid(2)])


@pytest.fixture
def service(projection, monkeypatch):
    monkeypatch.setenv("CODEHUB_SERVICE_TOKEN", "service-token")
    monkeypatch.delenv("CODEHUB_SERVICE_TOKEN_FILE", raising=False)
    with TestClient(make_search_app(settings=SimpleNamespace(region="region-a"), index=projection)) as client:
        yield client, projection


def test_search_lookup_requires_service_credential_and_returns_only_matching_documents(service):
    client, index = service
    path = f"/internal/tenants/{uid(100)}/entities/lookup"
    assert client.post(path, json={"ids": [uid(1)]}).status_code == 403
    assert (
        client.post(path, json={"ids": [uid(1)]}, headers={"Authorization": "Bearer customer-token"}).status_code == 403
    )
    response = client.post(
        path, json={"ids": [uid(3), uid(1), uid(9)]}, headers={"Authorization": "Bearer service-token"}
    )
    assert response.status_code == 200
    assert response.json() == {"items": [index.document(uid(100), uid(1))]}


@pytest.mark.parametrize("body", INVALID_LOOKUPS)
def test_search_lookup_shares_uuid_and_count_bounds(service, body):
    client, _index = service
    assert (
        client.post(
            f"/internal/tenants/{uid(100)}/entities/lookup",
            json=body,
            headers={"Authorization": "Bearer service-token"},
        ).status_code
        == 422
    )


def test_search_lookup_full_batch_and_request_byte_bounds(service):
    client, _index = service
    path = f"/internal/tenants/{uid(100)}/entities/lookup"
    headers = {"Authorization": "Bearer service-token"}
    response = client.post(path, json={"ids": [uid(value) for value in range(128)]}, headers=headers)
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [uid(1), uid(2)]
    assert (
        client.post(path, content=json.dumps({"ids": [uid(1)]}).encode() + b" " * 8192, headers=headers).status_code
        == 422
    )
