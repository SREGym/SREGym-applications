"""Write/provisioning requests preserve semantics while database IO waits."""

import asyncio
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


@pytest.fixture
def git_endpoint(monkeypatch, tmp_path):
    from fastapi import Response

    from codehub import repository as module

    monkeypatch.setenv("CODEHUB_SERVICE_TOKEN", "replication-token")
    monkeypatch.delenv("CODEHUB_SERVICE_TOKEN_FILE", raising=False)
    monkeypatch.delenv("CODEHUB_REPOSITORY_PEERS", raising=False)
    store, router, repository = Mock(), Mock(), Mock()
    router.authenticated_project.return_value = (store, "customer")
    router.for_project.return_value = store
    store.project.return_value = {"default_ref": "refs/heads/main"}
    repository.backend.return_value = Response(b"git-packet", media_type="application/x-git-upload-pack-result")
    monkeypatch.setattr(module, "StorageRouter", lambda _settings: router)
    settings = SimpleNamespace(region="region-a", data_dir=tmp_path)
    with TestClient(module.make_app(settings=settings, storage=store, repository=repository)) as client:
        yield SimpleNamespace(client=client, router=router, store=store, repository=repository)


@pytest.mark.parametrize("gate", ["authenticate", "project", "backend"])
def test_git_sql_or_subprocess_wait_does_not_block_health_or_other_repository_requests(git_endpoint, gate):
    endpoint = git_endpoint
    method = {
        "authenticate": endpoint.router.authenticated_project,
        "project": endpoint.store.project,
        "backend": endpoint.repository.backend,
    }[gate]
    expected = method.return_value
    started, release = threading.Event(), threading.Event()
    responses, concurrent = [], []

    def wait_for_io(*_args, **_kwargs):
        started.set()
        if not release.wait(5):
            raise RuntimeError("Test repository IO gate was not released")
        return expected

    method.side_effect = wait_for_io
    request = threading.Thread(
        target=lambda: responses.append(
            endpoint.client.post(
                f"/git/{uid(200)}/git-upload-pack",
                content=b"upload-packet",
                headers={
                    "Authorization": "Bearer customer-token",
                    "Content-Type": "application/x-git-upload-pack-request",
                },
            )
        ),
        daemon=True,
    )

    def other_requests():
        concurrent.append(endpoint.client.get("/healthz"))
        concurrent.append(
            endpoint.client.get(
                f"/internal/projects/{uid(200)}/commits/{'ab' * 20}",
                headers={"Authorization": "Bearer replication-token"},
            )
        )

    endpoint.repository.commit.return_value = {"commit_sha": "ab" * 20, "parents": []}
    observer = threading.Thread(target=other_requests, daemon=True)
    try:
        request.start()
        assert started.wait(3)
        observer.start()
        observer.join(2)
        assert not observer.is_alive(), "Git/SQL IO blocked the repository HTTP event loop"
        assert [response.status_code for response in concurrent] == [200, 200]
        assert request.is_alive()
    finally:
        release.set()
        request.join(3)
        if observer.ident is not None:
            observer.join(3)
    assert not request.is_alive() and not observer.is_alive()
    assert responses[0].status_code == 200 and responses[0].content == b"git-packet"
    endpoint.router.authenticated_project.assert_called_once_with(uid(200), "customer-token", write=False)
    endpoint.store.project.assert_called_once_with(uid(200), None)
    endpoint.repository.backend.assert_called_once_with(
        uid(200),
        "git-upload-pack",
        method="POST",
        query="",
        content_type="application/x-git-upload-pack-request",
        actor_id="customer",
        body=b"upload-packet",
        default_ref="refs/heads/main",
    )


@pytest.mark.parametrize("service", [False, True])
@pytest.mark.parametrize("write", [False, True])
def test_git_offloading_preserves_customer_and_replication_authority(git_endpoint, service, write):
    endpoint = git_endpoint
    token = "replication-token" if service else "customer-token"
    verb = "git-receive-pack" if write else "git-upload-pack"
    response = endpoint.client.get(
        f"/git/{uid(200)}/info/refs?service={verb}", headers={"Authorization": f"Bearer {token}"}
    )
    if service and write:
        assert response.status_code == 403
        endpoint.router.for_project.assert_not_called()
        endpoint.store.project.assert_not_called()
        endpoint.repository.backend.assert_not_called()
    else:
        assert response.status_code == 200 and response.content == b"git-packet"
        endpoint.store.project.assert_called_once_with(uid(200), None)
        if service:
            endpoint.router.for_project.assert_called_once_with(uid(200), None)
        else:
            endpoint.router.authenticated_project.assert_called_once_with(uid(200), token, write=write)
        assert endpoint.repository.backend.call_args.kwargs["actor_id"] == (
            "repository-service" if service else "customer"
        )


@pytest.mark.parametrize("gate", ["authenticate", "project", "backend"])
def test_git_offloaded_domain_failures_preserve_status_and_prevent_backend_when_denied(git_endpoint, gate):
    endpoint = git_endpoint
    method = {
        "authenticate": endpoint.router.authenticated_project,
        "project": endpoint.store.project,
        "backend": endpoint.repository.backend,
    }[gate]
    method.side_effect = Forbidden("Repository access denied")
    response = endpoint.client.get(
        f"/git/{uid(200)}/info/refs?service=git-receive-pack", headers={"Authorization": "Bearer customer-token"}
    )
    assert response.status_code == 403 and response.json() == {"detail": "Repository access denied"}
    assert response.headers["www-authenticate"] == 'Basic realm="CodeHub"'
    if gate != "backend":
        endpoint.repository.backend.assert_not_called()


@pytest.mark.parametrize("gate", ["authenticate", "project", "replication_write"])
def test_denied_git_post_does_not_read_the_upload_body(git_endpoint, gate):
    from starlette.requests import Request

    endpoint = git_endpoint
    token = "replication-token" if gate == "replication_write" else "customer-token"
    if gate == "authenticate":
        endpoint.router.authenticated_project.side_effect = Forbidden("Access denied")
    elif gate == "project":
        endpoint.store.project.side_effect = Forbidden("Access denied")
    consumed = []

    async def receive():
        consumed.append(True)
        raise AssertionError("Denied Git handler consumed the upload body")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": f"/git/{uid(200)}/git-receive-pack",
            "query_string": b"",
            "headers": [(b"authorization", f"Bearer {token}".encode())],
        },
        receive=receive,
    )
    route = next(route for route in endpoint.client.app.routes if route.path == "/git/{project_id}/{tail:path}")
    with pytest.raises(Forbidden):
        asyncio.run(route.endpoint(uid(200), "git-receive-pack", request))
    assert consumed == []
    endpoint.repository.backend.assert_not_called()


@pytest.mark.parametrize("overflow", [False, True])
def test_chunked_git_input_keeps_actual_service_capacity_and_never_executes_on_overflow(git_endpoint, overflow):
    from codehub.repository import MAX_GIT_BYTES

    endpoint = git_endpoint

    async def send():
        async def chunks():
            chunk = b"g" * (1024 * 1024)
            for _ in range(MAX_GIT_BYTES // len(chunk)):
                yield chunk
            if overflow:
                yield b"!"

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=endpoint.client.app), base_url="http://repository"
        ) as client:
            return await client.post(
                f"/git/{uid(200)}/git-upload-pack", content=chunks(), headers={"Authorization": "Bearer customer-token"}
            )

    response = asyncio.run(send())
    if overflow:
        assert response.status_code == 422 and response.json() == {"detail": "Git input exceeds service capacity"}
        endpoint.repository.backend.assert_not_called()
    else:
        assert response.status_code == 200 and response.content == b"git-packet"
        body = endpoint.repository.backend.call_args.kwargs["body"]
        assert len(body) == MAX_GIT_BYTES and body[:1] == body[-1:] == b"g"
