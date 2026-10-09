"""Durable eventual-consistency projection with real SQLite FTS queries."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from starlette.concurrency import run_in_threadpool

from codehub.config import Settings
from codehub.domain import Conflict, DomainError, Operation, canonical, digest, identifier
from codehub.http_support import require_service, serve
from codehub.storage.base import MAX_ENTITY_DOCUMENT_BYTES, entity_ids, lookup_ids
from codehub.telemetry import Telemetry


class SearchIndex:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connect() as connection:
            connection.executescript(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"
                "CREATE TABLE IF NOT EXISTS processed(effect_id TEXT PRIMARY KEY, payload_sha256 TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS documents(tenant_id TEXT NOT NULL,id TEXT NOT NULL,project_id TEXT,"
                "revision INTEGER NOT NULL,kind TEXT NOT NULL,document TEXT NOT NULL,PRIMARY KEY(tenant_id,id));"
                "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(tenant_id UNINDEXED,id UNINDEXED,text);"
            )

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def index(self, effect_id: str, operation: dict):
        op = Operation(**operation)
        operation_digest = digest(operation)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            previous = connection.execute(
                "SELECT payload_sha256 FROM processed WHERE effect_id=?", (effect_id,)
            ).fetchone()
            if previous:
                if previous["payload_sha256"] != operation_digest:
                    raise Conflict("A projection identity cannot describe different content")
                return {"effect_id": effect_id, "operation_sha256": operation_digest}
            current = connection.execute(
                "SELECT revision,document FROM documents WHERE tenant_id=? AND id=?", (op.tenant_id, op.entity_id)
            ).fetchone()
            if current and current["revision"] == op.client_revision and current["document"] != canonical(op.payload):
                raise Conflict("A projection revision cannot describe different content")
            if not current or op.client_revision > current["revision"]:
                connection.execute(
                    "INSERT INTO documents(tenant_id,id,project_id,revision,kind,document) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(tenant_id,id) DO UPDATE SET revision=excluded.revision,kind=excluded.kind,"
                    "document=excluded.document",
                    (op.tenant_id, op.entity_id, op.project_id, op.client_revision, op.kind, canonical(op.payload)),
                )
                connection.execute("DELETE FROM documents_fts WHERE tenant_id=? AND id=?", (op.tenant_id, op.entity_id))
                searchable = " ".join(
                    value
                    for key, value in op.payload.items()
                    if key in {"title", "body", "name", "slug", "ref"} and isinstance(value, str)
                )
                connection.execute(
                    "INSERT INTO documents_fts(tenant_id,id,text) VALUES(?,?,?)",
                    (op.tenant_id, op.entity_id, searchable),
                )
            connection.execute(
                "INSERT INTO processed(effect_id,payload_sha256) VALUES(?,?)", (effect_id, operation_digest)
            )
            return {"effect_id": effect_id, "operation_sha256": operation_digest}

    def search(self, tenant_id, query, *, limit=50):
        if not isinstance(query, str) or not query.strip() or len(query) > 200:
            raise DomainError("Search query must contain 1..200 characters")
        # Quote user terms so a product search string cannot become FTS query syntax.
        expression = " AND ".join('"' + word.replace('"', '""') + '"' for word in query.split()[:20])
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT d.* FROM documents_fts f JOIN documents d ON d.tenant_id=f.tenant_id AND d.id=f.id "
                "WHERE documents_fts MATCH ? AND d.tenant_id=? ORDER BY bm25(documents_fts),d.id LIMIT ?",
                (expression, tenant_id, limit),
            ).fetchall()
            return [{**dict(row), "document": json.loads(row["document"])} for row in rows]

    def document(self, tenant_id, entity_id):
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM documents WHERE tenant_id=? AND id=?", (tenant_id, entity_id)
            ).fetchone()
            return {**dict(row), "document": json.loads(row["document"])} if row else None

    def documents(self, tenant_id, ids):
        identifier(tenant_id, "tenant_id")
        ids = entity_ids(ids)
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT tenant_id,id,project_id,revision,kind,"
                "CASE WHEN length(CAST(document AS BLOB))<=? THEN document ELSE NULL END AS document "
                f"FROM documents WHERE tenant_id=? AND id IN ({placeholders}) ORDER BY id",
                (MAX_ENTITY_DOCUMENT_BYTES, tenant_id, *ids),
            ).fetchall()
            result = []
            for row in rows:
                if row["document"] is None:
                    raise DomainError("Entity document exceeds API capacity")
                result.append({**dict(row), "document": json.loads(row["document"])})
            return result


def make_app(settings=None, index=None):
    settings = settings or Settings.load()
    index = index or SearchIndex(settings.data_dir / "search.sqlite")
    app = FastAPI(title="CodeHub Search")
    telemetry = Telemetry(settings.region)
    app.middleware("http")(telemetry.middleware)

    @app.get("/metrics")
    def metrics():
        return telemetry.metrics()

    @app.exception_handler(DomainError)
    async def error(_request, exc):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.get("/readyz")
    def ready():
        with index.connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return {"status": "ok"}

    @app.post("/internal/index")
    async def apply(request: Request):
        require_service(request)
        body = await request.json()
        return index.index(body["effect_id"], body["operation"])

    @app.get("/internal/search")
    def search(tenant_id: str, q: str, request: Request, limit: int = 50):
        require_service(request)
        return {"items": index.search(tenant_id, q, limit=max(1, min(limit, 100)))}

    @app.get("/internal/tenants/{tenant_id}/entities/{entity_id}")
    def document(tenant_id: str, entity_id: str, request: Request):
        require_service(request)
        row = index.document(tenant_id, entity_id)
        if not row:
            from codehub.domain import NotFound

            raise NotFound("Projection does not exist")
        return row

    @app.post("/internal/tenants/{tenant_id}/entities/lookup")
    async def lookup(tenant_id: str, request: Request):
        from codehub.api import bounded_json

        require_service(request)
        identifier(tenant_id, "tenant_id")
        ids = lookup_ids(await bounded_json(request, maximum=8 * 1024))
        return {"items": await run_in_threadpool(index.documents, tenant_id, ids)}

    return app


if __name__ == "__main__":
    serve(make_app())
