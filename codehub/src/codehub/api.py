"""Authenticated collaboration API with SQL-atomic operation acceptance."""

import hmac
import json
import os

import httpx
import pymysql
from fastapi import FastAPI, Request, Response
from starlette.concurrency import run_in_threadpool

from codehub.config import Settings, secret
from codehub.domain import DomainError, Forbidden, Operation, identifier
from codehub.http_support import bearer, serve, service_headers
from codehub.storage.base import lookup_ids
from codehub.storage.routing import StorageRouter
from codehub.telemetry import Telemetry


async def bounded_json(request, maximum=256 * 1024):
    data = bytearray()
    async for part in request.stream():
        if len(data) + len(part) > maximum:
            raise DomainError("Request exceeds API capacity")
        data.extend(part)
    try:
        value = json.loads(data)
    except (ValueError, UnicodeError) as exc:
        raise DomainError("Request must contain JSON") from exc
    if not isinstance(value, dict):
        raise DomainError("Request body must be an object")
    return value


def validate_git(operation, settings, client):
    op = operation
    if op.kind not in {"repository.push", "change.create", "change.update"}:
        return
    if op.kind == "repository.push":
        shas = [op.payload.get("commit_sha")]
    else:
        shas = [op.payload.get("head_sha"), op.payload.get("base_sha")]
    for commit_sha in shas:
        from codehub.domain import sha

        sha({"value": commit_sha}, "value")
        response = client.get(f"{settings.repository_url}/internal/projects/{op.project_id}/commits/{commit_sha}")
        if response.status_code == 422:
            raise DomainError("Commit does not exist in the project repository")
        response.raise_for_status()
    response = client.get(f"{settings.repository_url}/internal/projects/{op.project_id}/refs")
    response.raise_for_status()
    refs = response.json()["refs"]
    if op.kind == "repository.push":
        if refs.get(op.payload.get("ref")) != op.payload["commit_sha"]:
            raise DomainError("Push receipt must match the actual repository branch")
    elif op.payload.get("state") == "merged":
        if refs.get(op.payload.get("base_ref")) != op.payload["base_sha"]:
            raise DomainError("Merged change must match the actual base branch")
        response = client.get(
            f"{settings.repository_url}/internal/projects/{op.project_id}/ancestor",
            params={"head": op.payload["head_sha"], "base": op.payload["base_sha"]},
        )
        if response.status_code == 422:
            raise DomainError("Merged branch must contain the change head")
        response.raise_for_status()
    elif (
        refs.get(op.payload.get("head_ref")) != op.payload["head_sha"]
        or refs.get(op.payload.get("base_ref")) != op.payload["base_sha"]
    ):
        raise DomainError("Change must identify the actual head and base branch objects")


def make_app(settings=None, router=None, client=None):
    settings = settings or Settings.load()
    router = router or StorageRouter(settings)
    client = client or httpx.Client(timeout=30, headers=service_headers())
    app = FastAPI(title="CodeHub", version="0.1.0")
    telemetry = Telemetry(settings.region)
    app.middleware("http")(telemetry.middleware)

    @app.exception_handler(DomainError)
    async def error(_request, exc):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    @app.exception_handler(pymysql.MySQLError)
    async def database_error(_request, _exc):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "Database operation unavailable"}, status_code=503)

    @app.exception_handler(httpx.HTTPError)
    async def upstream_error(_request, _exc):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "Dependent service unavailable"}, status_code=503)

    @app.get("/healthz")
    def health():
        return {"status": "ok", "region": settings.region}

    @app.get("/readyz")
    def ready():
        return {
            "region": settings.region,
            "groups": {name: store.readiness() for name, store in router.groups().items()},
        }

    @app.get("/metrics")
    def metrics():
        return telemetry.metrics()

    @app.post("/v1/identity/users")
    async def register(request: Request):
        if not hmac.compare_digest(bearer(request), secret("CODEHUB_BOOTSTRAP_TOKEN")):
            raise Forbidden("Identity provisioning credentials are required")
        body = await bounded_json(request)
        if set(body) != {"user_id", "username", "api_token"}:
            raise DomainError("Registration requires user_id, username and api_token")

        def provision():
            result = None
            # Credential provisioning uses the same identities in each configured database group.
            # A partial failure is retried idempotently and never reported as successful.
            for storage in router.groups().values():
                result = storage.register_user(body["user_id"], body["username"], body["api_token"])
            return result

        return await run_in_threadpool(provision)

    @app.post("/v1/operations")
    async def accept(request: Request):
        if os.environ.get("CODEHUB_READ_ONLY", "0") == "1":
            raise Forbidden("This API instance is in maintenance read-only mode")
        body = await bounded_json(request)
        try:
            operation = Operation(**body)
        except TypeError as exc:
            raise DomainError("Operation envelope has missing or unsupported fields") from exc
        token = bearer(request)

        def write():
            storage = router.for_tenant(operation.tenant_id)
            actor = storage.authenticate(token)
            if operation.project_id and operation.entity_type != "project":
                storage.project(operation.project_id, actor, write=True)
            validate_git(operation, settings, client)
            return storage.accept(operation, actor)

        return await run_in_threadpool(write)

    @app.get("/v1/tenants/{tenant_id}/entities/{entity_id}")
    def entity(tenant_id: str, entity_id: str, request: Request):
        identifier(tenant_id, "tenant_id")
        identifier(entity_id, "entity_id")
        storage = router.for_tenant(tenant_id)
        return storage.get_entity(tenant_id, entity_id, storage.authenticate(bearer(request)))

    @app.post("/v1/tenants/{tenant_id}/entities/lookup")
    async def lookup(tenant_id: str, request: Request):
        identifier(tenant_id, "tenant_id")
        ids = lookup_ids(await bounded_json(request, maximum=8 * 1024))
        token = bearer(request)

        def read():
            storage = router.for_tenant(tenant_id)
            actor = storage.authenticate(token)
            return {"items": storage.get_entities(tenant_id, ids, actor)}

        return await run_in_threadpool(read)

    @app.get("/v1/tenants/{tenant_id}/entities")
    def entities(
        tenant_id: str,
        request: Request,
        project_id: str | None = None,
        entity_type: str | None = None,
        after: str = "",
        limit: int = 100,
    ):
        identifier(tenant_id, "tenant_id")
        storage = router.for_tenant(tenant_id)
        actor = storage.authenticate(bearer(request))
        return {
            "items": storage.list_entities(
                tenant_id,
                actor,
                project_id=project_id,
                entity_type=entity_type,
                after=after,
                limit=max(1, min(limit, 1000)),
            )
        }

    @app.get("/v1/tenants/{tenant_id}/events")
    def events(tenant_id: str, request: Request, after: str = "", limit: int = 100):
        identifier(tenant_id, "tenant_id")
        storage = router.for_tenant(tenant_id)
        return {
            "items": storage.events(
                tenant_id, storage.authenticate(bearer(request)), after=after, limit=max(1, min(limit, 1000))
            )
        }

    @app.get("/v1/tenants/{tenant_id}/search")
    def search(tenant_id: str, q: str, request: Request):
        identifier(tenant_id, "tenant_id")
        storage = router.for_tenant(tenant_id)
        actor = storage.authenticate(bearer(request))
        with storage.connection(read=True) as connection, connection.cursor() as cursor:
            storage.authorize(cursor, tenant_id, actor)
        if not settings.search_urls:
            raise DomainError("Search endpoint is not configured")
        response = client.get(f"{settings.search_urls[0]}/internal/search", params={"tenant_id": tenant_id, "q": q})
        response.raise_for_status()
        return response.json()

    @app.get("/v1/projects/{project_id}/builds")
    def builds(project_id: str, request: Request):
        token = bearer(request)
        for storage in router.groups().values():
            actor = storage.authenticate(token)
            from codehub.domain import NotFound

            try:
                return {"items": storage.builds(project_id, actor)}
            except NotFound:
                continue
        raise NotFound("Project does not exist")

    @app.get("/v1/projects/{project_id}/artifacts/{checksum}")
    def artifact(project_id: str, checksum: str, request: Request):
        token = bearer(request)
        from codehub.domain import NotFound

        for storage in router.groups().values():
            actor = storage.authenticate(token)
            try:
                storage.project(project_id, actor)
            except NotFound:
                continue
            with storage.connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT sha256 FROM artifacts WHERE sha256=%s AND project_id=%s", (checksum, project_id))
                if not cursor.fetchone():
                    raise NotFound("Artifact does not belong to this project")
            response = client.get(f"{settings.delivery_url}/internal/artifacts/{checksum}")
            response.raise_for_status()
            return Response(response.content, media_type="application/zip", headers={"ETag": f'"{checksum}"'})
        raise NotFound("Project does not exist")

    return app


if __name__ == "__main__":
    serve(make_app())
