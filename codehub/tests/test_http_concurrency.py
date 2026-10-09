"""Write/provisioning requests preserve semantics while database IO waits."""

import threading
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import httpx
import pymysql
import pytest
from fastapi.testclient import TestClient

from codehub.api import make_app
from codehub.domain import Conflict, DomainError, Forbidden, Operation, digest


def uid(value):
    return str(UUID(int=value))


def operation_body():
    return {
        "event_id": uid(900),
        "tenant_id": uid(100),
        "entity_id": uid(300),
        "project_id": uid(200),
        "client_revision": 1,
        "kind": "issue.create",
        "payload": {"title": "Connection limits", "body": "Keep requests responsive", "state": "open"},
    }


def receipt(body):
    operation = Operation(**body)
    return {
        **body,
        "record_key": operation.record_key,
        "actor_id": uid(10),
        "operation_sha256": digest(body | {"actor_id": uid(10)}),
        "region": "region-a",
        "replayed": False,
    }


@pytest.fixture
def endpoint(monkeypatch):
    monkeypatch.setenv("CODEHUB_BOOTSTRAP_TOKEN", "bootstrap-token")
    monkeypatch.delenv("CODEHUB_BOOTSTRAP_TOKEN_FILE", raising=False)
    monkeypatch.delenv("CODEHUB_READ_ONLY", raising=False)
    first, second = Mock(), Mock()
    first.authenticate.return_value = uid(10)
    first.accept.return_value = receipt(operation_body())
    user = {"user_id": uid(10), "username": "morgan", "api_token": "c" * 32}
    first.register_user.return_value = second.register_user.return_value = {
        "id": user["user_id"],
        "username": user["username"],
    }
    router = Mock()
    router.for_tenant.return_value = first
    router.groups.return_value = {"group-a": first, "group-b": second}
    requests = []

    def git_response(request):
        requests.append(request)
        return httpx.Response(200, json={"refs": {"refs/heads/main": "ab" * 20}})

    with httpx.Client(transport=httpx.MockTransport(git_response)) as git:
        app = make_app(
            settings=SimpleNamespace(region="region-a", repository_url="http://repository"), router=router, client=git
        )
        with TestClient(app) as client:
            yield SimpleNamespace(client=client, router=router, first=first, second=second, user=user, git=requests)


@pytest.mark.parametrize("path", ["/v1/operations", "/v1/identity/users"])
def test_waiting_sql_write_or_registration_keeps_other_http_requests_responsive(endpoint, path):
    body = operation_body() if path == "/v1/operations" else endpoint.user
    token = "customer-token" if path == "/v1/operations" else "bootstrap-token"
    method = endpoint.first.accept if path == "/v1/operations" else endpoint.first.register_user
    expected = method.return_value
    started, release = threading.Event(), threading.Event()
    responses, health = [], []

    def wait_for_database(*_args):
        started.set()
        if not release.wait(5):
            raise RuntimeError("Test database gate was not released")
        return expected

    method.side_effect = wait_for_database
    request = threading.Thread(
        target=lambda: responses.append(
            endpoint.client.post(path, json=body, headers={"Authorization": f"Bearer {token}"})
        ),
        daemon=True,
    )
    health_request = threading.Thread(target=lambda: health.append(endpoint.client.get("/healthz")), daemon=True)
    try:
        request.start()
        assert started.wait(3)
        health_request.start()
        health_request.join(3)
        assert not health_request.is_alive(), "SQL write/provisioning blocked the HTTP event loop"
        assert health[0].status_code == 200
        assert request.is_alive()
    finally:
        release.set()
        request.join(3)
        if health_request.ident is not None:
            health_request.join(3)
    assert not request.is_alive() and not health_request.is_alive()
    assert responses[0].status_code == 200
    assert responses[0].json() == expected


def test_write_keeps_exact_actor_tenant_operation_and_project_authorization(endpoint):
    body = operation_body()
    response = endpoint.client.post("/v1/operations", json=body, headers={"Authorization": "Bearer customer-token"})
    assert response.status_code == 200
    assert response.json() == receipt(body)
    endpoint.router.for_tenant.assert_called_once_with(body["tenant_id"])
    endpoint.first.authenticate.assert_called_once_with("customer-token")
    endpoint.first.project.assert_called_once_with(body["project_id"], uid(10), write=True)
    operation, actor = endpoint.first.accept.call_args.args
    assert isinstance(operation, Operation) and operation.as_dict() == body and actor == uid(10)
    assert endpoint.git == []


def test_write_still_validates_actual_git_object_and_ref_before_acceptance(endpoint):
    body = {
        **operation_body(),
        "kind": "repository.push",
        "payload": {"ref": "refs/heads/main", "commit_sha": "ab" * 20},
    }
    endpoint.first.accept.return_value = receipt(body)
    response = endpoint.client.post("/v1/operations", json=body, headers={"Authorization": "Bearer customer-token"})
    assert response.status_code == 200 and response.json() == receipt(body)
    assert [request.url.path for request in endpoint.git] == [
        f"/internal/projects/{uid(200)}/commits/{'ab' * 20}",
        f"/internal/projects/{uid(200)}/refs",
    ]
    assert endpoint.first.accept.call_args.args == (Operation(**body), uid(10))


@pytest.mark.parametrize("gate", ["authenticate", "project", "maintenance", "missing_bearer"])
def test_denied_requests_cannot_write_or_contact_git(endpoint, monkeypatch, gate):
    body = {
        **operation_body(),
        "kind": "repository.push",
        "payload": {"ref": "refs/heads/main", "commit_sha": "ab" * 20},
    }
    if gate in {"authenticate", "project"}:
        getattr(endpoint.first, gate).side_effect = Forbidden("Membership does not permit this operation")
    elif gate == "maintenance":
        monkeypatch.setenv("CODEHUB_READ_ONLY", "1")
    response = endpoint.client.post(
        "/v1/operations",
        json=body,
        headers={} if gate == "missing_bearer" else {"Authorization": "Bearer customer-token"},
    )
    assert response.status_code == 403
    endpoint.first.accept.assert_not_called()
    assert endpoint.git == []
    if gate in {"maintenance", "missing_bearer"}:
        endpoint.router.for_tenant.assert_not_called()


@pytest.mark.parametrize(
    "failure,status",
    [
        (Conflict("Conflicting identity"), 409),
        (DomainError("Invalid relationship"), 422),
        (pymysql.err.OperationalError(2003, "Database unavailable"), 503),
    ],
)
def test_offloaded_write_preserves_exception_status(endpoint, failure, status):
    endpoint.first.accept.side_effect = failure
    response = endpoint.client.post(
        "/v1/operations", json=operation_body(), headers={"Authorization": "Bearer customer-token"}
    )
    assert response.status_code == status


def test_failed_git_validation_prevents_sql_acceptance(endpoint):
    body = {
        **operation_body(),
        "kind": "repository.push",
        "payload": {"ref": "refs/heads/feature", "commit_sha": "ab" * 20},
    }
    response = endpoint.client.post("/v1/operations", json=body, headers={"Authorization": "Bearer customer-token"})
    assert response.status_code == 422
    endpoint.first.accept.assert_not_called()
    assert len(endpoint.git) == 2


def test_registration_keeps_bootstrap_auth_and_every_group_arguments(endpoint):
    response = endpoint.client.post(
        "/v1/identity/users", json=endpoint.user, headers={"Authorization": "Bearer bootstrap-token"}
    )
    assert response.status_code == 200
    assert response.json() == {"id": endpoint.user["user_id"], "username": endpoint.user["username"]}
    for storage in (endpoint.first, endpoint.second):
        storage.register_user.assert_called_once_with(
            endpoint.user["user_id"], endpoint.user["username"], endpoint.user["api_token"]
        )
    endpoint.router.for_tenant.assert_not_called()
    assert endpoint.git == []


@pytest.mark.parametrize("header", [None, "Bearer invalid", "Basic bootstrap-token"])
def test_denied_registration_does_not_provision_any_group(endpoint, header):
    response = endpoint.client.post(
        "/v1/identity/users", json=endpoint.user, headers={"Authorization": header} if header else {}
    )
    assert response.status_code == 403
    endpoint.router.groups.assert_not_called()
    endpoint.first.register_user.assert_not_called()
    endpoint.second.register_user.assert_not_called()


def test_partial_registration_failure_is_not_acknowledged_and_identical_retry_reaches_every_group(endpoint):
    endpoint.second.register_user.side_effect = pymysql.err.OperationalError(2003, "Database unavailable")
    headers = {"Authorization": "Bearer bootstrap-token"}
    assert endpoint.client.post("/v1/identity/users", json=endpoint.user, headers=headers).status_code == 503
    assert endpoint.first.register_user.call_count == endpoint.second.register_user.call_count == 1
    endpoint.second.register_user.side_effect = None
    response = endpoint.client.post("/v1/identity/users", json=endpoint.user, headers=headers)
    assert response.status_code == 200
    assert endpoint.first.register_user.call_count == endpoint.second.register_user.call_count == 2


@pytest.mark.parametrize(
    "path,body",
    [
        ("/v1/operations", {"unexpected": "field"}),
        ("/v1/identity/users", {"user_id": uid(10), "username": "morgan", "api_token": "c" * 32, "role": "owner"}),
    ],
)
def test_malformed_envelopes_never_reach_storage(endpoint, path, body):
    response = endpoint.client.post(path, json=body, headers={"Authorization": "Bearer bootstrap-token"})
    assert response.status_code == 422
    endpoint.router.for_tenant.assert_not_called()
    endpoint.router.groups.assert_not_called()
