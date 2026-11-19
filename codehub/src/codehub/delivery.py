"""Durable artifact hosting and retry-safe webhook delivery attempts."""

import hashlib
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request, Response

from codehub.artifacts import ArtifactStore
from codehub.config import Settings
from codehub.domain import Conflict, DomainError, canonical, digest, identifier
from codehub.http_support import require_service, serve
from codehub.telemetry import Telemetry


class DeliveryStore:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"
                "CREATE TABLE IF NOT EXISTS deliveries(effect_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,"
                "destination TEXT NOT NULL,payload TEXT NOT NULL,payload_sha256 TEXT NOT NULL,"
                "state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,status_code INTEGER,response_sha256 TEXT);"
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

    def deliver(self, effect_id, event_id, destination, payload, *, sender=None):
        identifier(event_id, "event_id")
        parsed = urlsplit(destination)
        allowed = {host.strip() for host in os.environ.get("CODEHUB_WEBHOOK_HOSTS", "").split(",") if host.strip()}
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname not in allowed
            or parsed.username
            or parsed.password
        ):
            raise DomainError("Webhook destination is not in the configured host allowlist")
        body = {"effect_id": effect_id, "event_id": event_id, "operation": payload}
        checksum = digest(body)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            previous = connection.execute("SELECT * FROM deliveries WHERE effect_id=?", (effect_id,)).fetchone()
            if previous:
                if previous["payload_sha256"] != checksum or previous["destination"] != destination:
                    raise Conflict("Delivery identity cannot describe different content")
                if previous["state"] == "delivered":
                    return dict(previous)
            else:
                connection.execute(
                    "INSERT INTO deliveries(effect_id,event_id,destination,payload,payload_sha256,state) "
                    "VALUES(?,?,?,?,?,'pending')",
                    (effect_id, event_id, destination, canonical(body), checksum),
                )
            connection.execute("UPDATE deliveries SET attempts=attempts+1 WHERE effect_id=?", (effect_id,))
        # A crash after receiving 2xx can retry this request. The receiver must honor Idempotency-Key.
        sender = sender or httpx.post
        response = sender(
            destination,
            content=canonical(body).encode(),
            headers={
                "Content-Type": "application/json",
                "Idempotency-Key": effect_id,
                "X-CodeHub-Event": event_id,
            },
            timeout=10,
            follow_redirects=False,
        )
        response.raise_for_status()
        with self.connect() as connection:
            connection.execute(
                "UPDATE deliveries SET state='delivered',status_code=?,response_sha256=? WHERE effect_id=?",
                (response.status_code, hashlib.sha256(response.content).hexdigest(), effect_id),
            )
            return dict(connection.execute("SELECT * FROM deliveries WHERE effect_id=?", (effect_id,)).fetchone())


def make_app(settings=None, deliveries=None, artifacts=None):
    settings = settings or Settings.load()
    deliveries = deliveries or DeliveryStore(settings.data_dir / "delivery.sqlite")
    artifacts = artifacts or ArtifactStore(settings.data_dir / "artifacts")
    app = FastAPI(title="CodeHub Delivery")
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
        with deliveries.connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return {"status": "ok"}

    @app.post("/internal/deliveries")
    async def deliver(request: Request):
        require_service(request)
        body = await request.json()
        return deliveries.deliver(body["effect_id"], body["event_id"], body["destination"], body["operation"])

    @app.put("/internal/artifacts/{checksum}")
    async def put(checksum: str, request: Request):
        require_service(request)
        content = bytearray()
        async for part in request.stream():
            content.extend(part)
            if len(content) > 32 * 1024 * 1024:
                raise DomainError("Artifact exceeds capacity")
        artifacts.put(checksum, bytes(content))
        return {"sha256": checksum, "size_bytes": len(content)}

    @app.get("/internal/artifacts/{checksum}")
    def get(checksum: str, request: Request):
        require_service(request)
        return Response(artifacts.get(checksum), media_type="application/zip")

    return app


if __name__ == "__main__":
    serve(make_app())
