"""Authenticated smart-HTTP Git and bounded commit/object operations."""

import os
import hmac
import re
import signal
import sqlite3
import subprocess
import threading
from contextlib import asynccontextmanager, closing
from pathlib import Path
from tempfile import TemporaryFile

from fastapi import FastAPI, Request, Response
from starlette.concurrency import run_in_threadpool

from codehub.config import Settings, secret
from codehub.domain import DomainError, Forbidden, identifier
from codehub.http_support import git_credential, require_service, serve
from codehub.storage.mysql import MySQLStorage
from codehub.storage.routing import StorageRouter
from codehub.projects import ref_name
from codehub.repository_sync import RepositorySynchronizer
from codehub.telemetry import Telemetry

MAX_GIT_BYTES = 64 * 1024 * 1024


class Repository:
    def __init__(self, root: Path):
        self.root = root
        self.initialization_lock = threading.Lock()
        self.reference_lock = threading.Lock()
        root.mkdir(parents=True, exist_ok=True)
        self.publication_state = root / "reference-publications.sqlite"
        with closing(sqlite3.connect(self.publication_state)) as database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS publications(project TEXT,ref TEXT,entity TEXT,revision INTEGER,sha TEXT,PRIMARY KEY(project,ref))"
            )
            database.execute(
                "CREATE TABLE IF NOT EXISTS publication_intents(project TEXT,ref TEXT,entity TEXT,revision INTEGER,sha TEXT,expected_sha TEXT,PRIMARY KEY(project,ref))"
            )
            database.commit()

    def path(self, project_id: str) -> Path:
        identifier(project_id, "project_id")
        return self.root / f"{project_id}.git"

    def ensure(self, project_id: str) -> Path:
        path = self.path(project_id)
        with self.initialization_lock:
            if not (path / "HEAD").exists():
                subprocess.run(
                    ["git", "init", "--bare", "--initial-branch=main", str(path)],
                    check=True,
                    capture_output=True,
                    timeout=15,
                )
                subprocess.run(
                    ["git", "-C", str(path), "config", "receive.maxInputSize", str(MAX_GIT_BYTES)],
                    check=True,
                    capture_output=True,
                    timeout=10,
                )
                subprocess.run(
                    ["git", "-C", str(path), "config", "http.receivepack", "true"],
                    check=True,
                    capture_output=True,
                    timeout=10,
                )
        return path

    def git(self, project_id: str, *args: str, maximum=MAX_GIT_BYTES) -> bytes:
        path = self.path(project_id)
        if not path.exists():
            raise DomainError("Repository does not exist")
        with TemporaryFile() as output:
            result = subprocess.run(
                ["git", "-C", str(path), *args],
                stdout=output,
                stderr=subprocess.PIPE,
                timeout=30,
            )
            if result.returncode:
                raise DomainError("Git operation failed")
            if output.tell() > maximum:
                raise DomainError("Git output exceeds service capacity")
            output.seek(0)
            return output.read()

    def commit(self, project_id: str, commit_sha: str) -> dict:
        if not re.fullmatch(r"[0-9a-f]{40}", commit_sha):
            raise DomainError("Commit must be a full Git object identifier")
        self.git(project_id, "cat-file", "-e", f"{commit_sha}^{{commit}}")
        parents = self.git(project_id, "rev-list", "--parents", "-n", "1", commit_sha, maximum=4096).decode().split()
        return {"commit_sha": parents[0], "parents": parents[1:]}

    def publish_reference(self, project_id, reference):
        """Publish a new SQL revision without overwriting an unacknowledged push."""
        ref = ref_name({"ref": reference["ref"]}, "ref")
        identifier(reference["id"], "repository_id")
        revision, desired = reference["revision"], reference["commit_sha"]
        if type(revision) is not int or revision < 1:
            raise DomainError("Reference publication requires an accepted positive revision")
        self.commit(project_id, desired)
        with self.reference_lock, closing(sqlite3.connect(self.publication_state)) as database:
            database.execute("PRAGMA synchronous=FULL")
            actual = self._reference_value(project_id, ref)
            pending = database.execute(
                "SELECT entity,revision,sha,expected_sha FROM publication_intents WHERE project=? AND ref=?",
                (project_id, ref),
            ).fetchone()
            if pending:
                if actual == pending[3]:
                    self.git(project_id, "update-ref", ref, pending[2], actual or "0" * 40, maximum=4096)
                    actual = pending[2]
                if actual == pending[2]:
                    self._record_publication(database, project_id, ref, *pending[:3])
                elif actual != desired:
                    return  # Preserve a concurrent local upload and its durable intent.
            previous = database.execute(
                "SELECT entity,revision,sha FROM publications WHERE project=? AND ref=?", (project_id, ref)
            ).fetchone()
            if previous and previous[0] == reference["id"]:
                if revision < previous[1]:
                    return
                if revision == previous[1]:
                    if desired != previous[2]:
                        raise DomainError("Accepted reference revision has conflicting content")
            if actual != desired:
                # A local Git upload precedes its API receipt. Only replace the
                # last published object (or an absent ref), never a newer local
                # upload. Git's compare-and-swap also guards a concurrent push.
                if (previous and actual == previous[2]) or actual is None:
                    # SQLite commits the accepted transition before Git changes.
                    # On restart the intent identifies a completed CAS even if
                    # authoritative SQL advanced while this process was down.
                    database.execute(
                        "INSERT INTO publication_intents VALUES(?,?,?,?,?,?) "
                        "ON CONFLICT(project,ref) DO UPDATE SET entity=excluded.entity,revision=excluded.revision,"
                        "sha=excluded.sha,expected_sha=excluded.expected_sha",
                        (project_id, ref, reference["id"], revision, desired, actual),
                    )
                    database.commit()
                    self.git(project_id, "update-ref", ref, desired, actual or "0" * 40, maximum=4096)
                elif previous:
                    return
                # On first observation preserve the local ref while recording
                # the SQL revision; its later acknowledged revision can publish.
            self._record_publication(database, project_id, ref, reference["id"], revision, desired)

    def _reference_value(self, project_id, ref):
        for line in (
            self.git(project_id, "for-each-ref", "--format=%(refname) %(objectname)", ref, maximum=65536)
            .decode()
            .splitlines()
        ):
            name, commit = line.split()
            if name == ref:
                return commit
        return None

    @staticmethod
    def _record_publication(database, project_id, ref, entity, revision, sha):
        database.execute(
            "INSERT INTO publications VALUES(?,?,?,?,?) ON CONFLICT(project,ref) DO UPDATE "
            "SET entity=excluded.entity,revision=excluded.revision,sha=excluded.sha",
            (project_id, ref, entity, revision, sha),
        )
        database.execute("DELETE FROM publication_intents WHERE project=? AND ref=?", (project_id, ref))
        database.commit()

    def ref(self, project_id: str, ref: str) -> str:
        if not ref.startswith("refs/heads/"):
            raise DomainError("A branch ref is required")
        return self.git(project_id, "rev-parse", "--verify", ref, maximum=4096).decode().strip()

    def archive(self, project_id: str, commit_sha: str) -> bytes:
        self.commit(project_id, commit_sha)
        return self.git(project_id, "archive", "--format=tar", commit_sha, maximum=32 * 1024 * 1024)

    def fsck(self, project_id: str) -> dict:
        self.git(project_id, "fsck", "--full", "--no-dangling", maximum=1024 * 1024)
        refs = self.git(project_id, "show-ref", maximum=1024 * 1024).decode().splitlines()
        return {"refs": {line.split()[1]: line.split()[0] for line in refs}}

    def backend(self, project_id, tail, *, method, query, content_type, actor_id, body, default_ref="refs/heads/main"):
        if tail not in {"info/refs", "git-upload-pack", "git-receive-pack"}:
            raise DomainError("Only Git smart HTTP endpoints are supported")
        path = self.ensure(project_id)
        with self.initialization_lock:
            if (path / "HEAD").read_text().strip() != f"ref: {default_ref}":
                self.git(project_id, "symbolic-ref", "HEAD", default_ref, maximum=4096)
        env = {
            **os.environ,
            "GIT_PROJECT_ROOT": str(self.root.resolve()),
            "GIT_HTTP_EXPORT_ALL": "1",
            "PATH_INFO": f"/{project_id}.git/{tail}",
            "REQUEST_METHOD": method,
            "QUERY_STRING": query,
            "CONTENT_TYPE": content_type,
            "CONTENT_LENGTH": str(len(body)),
            "REMOTE_USER": actor_id,
        }
        process = subprocess.Popen(
            ["git", "http-backend"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            start_new_session=os.name == "posix",
        )

        def kill_operation():
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass

        timer = threading.Timer(60, kill_operation)
        timer.start()

        def write_body():
            try:
                process.stdin.write(body)
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass

        writer = threading.Thread(target=write_body, daemon=True)
        writer.start()
        output = bytearray()
        try:
            while chunk := process.stdout.read(65536):
                output.extend(chunk)
                if len(output) > MAX_GIT_BYTES:
                    kill_operation()
                    raise DomainError("Git HTTP response exceeds service capacity")
            if process.wait(timeout=5):
                raise DomainError("Git HTTP request failed or exceeded its time limit")
        finally:
            timer.cancel()
            if process.poll() is None:
                kill_operation()
            process.wait(timeout=5)
            writer.join(timeout=5)
            process.stdout.close()
        output = bytes(output)
        head, separator, payload = output.partition(b"\r\n\r\n")
        if not separator:
            head, separator, payload = output.partition(b"\n\n")
        if not separator:
            raise DomainError("Invalid Git HTTP response")
        headers = {}
        status = 200
        for line in head.decode("latin-1").splitlines():
            name, _, value = line.partition(":")
            if name.lower() == "status":
                status = int(value.strip().split()[0])
            elif name:
                headers[name] = value.strip()
        return Response(payload, status_code=status, headers=headers)


def make_app(settings=None, storage=None, repository=None):
    settings = settings or Settings.load()
    storage = storage or MySQLStorage(settings)
    router = StorageRouter(settings)
    repository = repository or Repository(settings.data_dir / "repositories")
    synchronizer = RepositorySynchronizer(router, repository, os.environ.get("CODEHUB_REPOSITORY_PEERS", "").split(","))

    @asynccontextmanager
    async def lifespan(_app):
        synchronizer.start()
        yield
        synchronizer.close()

    app = FastAPI(title="CodeHub Repository", lifespan=lifespan)
    telemetry = Telemetry(settings.region)
    app.middleware("http")(telemetry.middleware)

    @app.get("/metrics")
    def metrics():
        return telemetry.metrics()

    @app.exception_handler(DomainError)
    async def error(_request, exc):
        from fastapi.responses import JSONResponse

        headers = {"WWW-Authenticate": 'Basic realm="CodeHub"'} if isinstance(exc, Forbidden) else None
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code, headers=headers)

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.get("/readyz")
    def ready():
        return {"groups": {group: store.readiness() for group, store in router.groups().items()}}

    @app.api_route("/git/{project_id}/{tail:path}", methods=["GET", "POST"])
    async def git_http(project_id: str, tail: str, request: Request):
        service = request.query_params.get("service", "")
        write = tail == "git-receive-pack" or service == "git-receive-pack"
        credential = git_credential(request)

        def authorize():
            if hmac.compare_digest(credential, secret("CODEHUB_SERVICE_TOKEN")):
                if write:
                    raise Forbidden("Replication service credentials permit reads only")
                _storage = router.for_project(project_id, None)
                actor = "repository-service"
            else:
                _storage, actor = router.authenticated_project(project_id, credential, write=write)
            return actor, _storage.project(project_id, None)

        actor, project = await run_in_threadpool(authorize)
        chunks = bytearray()
        async for chunk in request.stream():
            if len(chunks) + len(chunk) > MAX_GIT_BYTES:
                raise DomainError("Git input exceeds service capacity")
            chunks.extend(chunk)
        return await run_in_threadpool(
            repository.backend,
            project_id,
            tail,
            method=request.method,
            query=request.url.query,
            content_type=request.headers.get("content-type", ""),
            actor_id=actor,
            body=bytes(chunks),
            default_ref=project["default_ref"],
        )

    @app.get("/internal/projects/{project_id}/commits/{commit_sha}")
    def commit(project_id: str, commit_sha: str, request: Request):
        require_service(request)
        router.for_project(project_id, None)
        return repository.commit(project_id, commit_sha)

    @app.get("/internal/projects/{project_id}/refs")
    def refs(project_id: str, request: Request):
        require_service(request)
        router.for_project(project_id, None)
        return repository.fsck(project_id)

    @app.get("/internal/projects/{project_id}/archive/{commit_sha}")
    def archive(project_id: str, commit_sha: str, request: Request):
        require_service(request)
        router.for_project(project_id, None)
        return Response(repository.archive(project_id, commit_sha), media_type="application/x-tar")

    @app.get("/internal/projects/{project_id}/ancestor")
    def ancestor(project_id: str, head: str, base: str, request: Request):
        require_service(request)
        router.for_project(project_id, None)
        repository.commit(project_id, head)
        repository.commit(project_id, base)
        repository.git(project_id, "merge-base", "--is-ancestor", head, base, maximum=4096)
        return {"ancestor": True}

    return app


if __name__ == "__main__":
    serve(make_app())
