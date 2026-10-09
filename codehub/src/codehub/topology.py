"""MySQL reachability, bounded promotion and regional writer routing.

This is a minimal operator with local durability and ordinary configuration.
It does not supply consensus, a distributed lease or remote-writer fencing.
"""

import asyncio
import hmac
import json
import logging
import os
import ssl
import threading
import time
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path

import pymysql
import uvicorn
from fastapi import FastAPI, Header, HTTPException

logger = logging.getLogger("codehub.topology")


class TopologyManager:
    def __init__(self, configuration: dict, *, connect=None, route=None):
        self.configuration = configuration
        self._connect = connect or self._mysql_connect
        self._route = route or self._patch_route
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.state_path = Path(os.getenv("CODEHUB_DATA_DIR", "/var/lib/codehub")) / "topology-state.json"
        self.state = {
            "writer_reachable": None,
            "candidate_reachable": None,
            "consecutive_failures": 0,
            "promoted": False,
            "writer_observed_healthy": False,
            "observed_writer_host": None,
        }
        self.last_valid_lag = None
        self.last_valid_lag_at = 0.0
        if self.state_path.exists():
            self.state.update(json.loads(self.state_path.read_text()))
        if self.state["observed_writer_host"] != self.configuration["writer_host"]:
            self.state["writer_observed_healthy"] = False

    def _mysql_connect(self, host: str):
        password = Path(os.environ["CODEHUB_MYSQL_ADMIN_PASSWORD_FILE"]).read_text().strip()
        return pymysql.connect(
            host=host,
            port=3306,
            user="root",
            password=password,
            connect_timeout=1,
            read_timeout=2,
            write_timeout=2,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )

    @staticmethod
    def _patch_route(host: str, service: str):
        account = Path("/var/run/secrets/kubernetes.io/serviceaccount")
        namespace = account.joinpath("namespace").read_text().strip()
        token = account.joinpath("token").read_text().strip()
        endpoint = f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/services/{service}"
        request = urllib.request.Request(
            endpoint,
            data=json.dumps({"spec": {"type": "ExternalName", "externalName": host}}).encode(),
            method="PATCH",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/merge-patch+json"},
        )
        with urllib.request.urlopen(
            request, context=ssl.create_default_context(cafile=str(account / "ca.crt")), timeout=5
        ):
            pass

    def _persist(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.state, sort_keys=True))
        temporary.replace(self.state_path)

    def inspect(self, host: str, *, replica: bool = False) -> dict:
        with self._connect(host) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT @@GLOBAL.server_uuid AS server_uuid, @@GLOBAL.read_only AS read_only")
            result = dict(cursor.fetchone())
            if replica:
                cursor.execute("SHOW REPLICA STATUS")
                status = cursor.fetchone()
                if status:
                    result["lag_seconds"] = status["Seconds_Behind_Source"]
                    result["replication_error"] = status["Last_SQL_Error"]
                else:
                    result["lag_seconds"] = None
                    result["replication_error"] = "No replication channel"
            return result

    def promote(self):
        """Promote the configured candidate and atomically redirect this region."""
        with self.lock:
            if self.state["promoted"]:
                return dict(self.state)
            host = self.configuration["candidate_host"]
            with self._connect(host) as connection, connection.cursor() as cursor:
                cursor.execute("STOP REPLICA")
                cursor.execute("SET GLOBAL super_read_only=OFF")
                cursor.execute("SET GLOBAL read_only=OFF")
            self._route(host, self.configuration["routing_service"])
            self.state["promoted"] = True
            self.state["writer_host"] = host
            self.state["promoted_at"] = time.time()
            self._persist()
            logger.info("Writer promoted and regional routing updated", extra={"writer_host": host})
            return dict(self.state)

    def probe_once(self):
        try:
            self.inspect(self.configuration["writer_host"])
            reachable = True
        except (OSError, pymysql.Error):
            reachable = False
        candidate_safe = False
        try:
            candidate = self.inspect(self.configuration["candidate_host"], replica=not self.state["promoted"])
            candidate_reachable = True
            candidate_safe = not candidate.get("replication_error")
            lag = candidate.get("lag_seconds")
            if lag is not None and not candidate.get("replication_error"):
                self.last_valid_lag = lag
                self.last_valid_lag_at = time.monotonic()
        except (OSError, pymysql.Error):
            candidate_reachable = False
        with self.lock:
            self.state["writer_reachable"] = reachable
            if reachable:
                self.state["writer_observed_healthy"] = True
                self.state["observed_writer_host"] = self.configuration["writer_host"]
            self.state["candidate_reachable"] = candidate_reachable
            self.state["consecutive_failures"] = 0 if reachable else self.state["consecutive_failures"] + 1
            should_promote = (
                self.configuration["automatic_promotion"]
                and self.state["writer_observed_healthy"]
                and self.state["observed_writer_host"] == self.configuration["writer_host"]
                and not self.state["promoted"]
                and candidate_reachable
                and candidate_safe
                and self.state["consecutive_failures"] >= self.configuration["failure_threshold"]
                and self.last_valid_lag is not None
                and self.last_valid_lag <= self.configuration["maximum_lag_seconds"]
                and time.monotonic() - self.last_valid_lag_at <= self.configuration["maximum_lag_seconds"]
            )
            self._persist()
        if should_promote:
            self.promote()

    def run(self):
        while not self.stop.is_set():
            try:
                self.probe_once()
            except Exception:
                logger.exception("Topology probe or routing update failed")
            self.stop.wait(self.configuration["probe_interval_seconds"])


def create_app(manager: TopologyManager) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_):
        thread = threading.Thread(target=manager.run, name="database-topology", daemon=True)
        thread.start()
        try:
            yield
        finally:
            manager.stop.set()
            await asyncio.to_thread(thread.join, 10)
            if thread.is_alive():
                raise RuntimeError("Topology probes did not stop")

    application = FastAPI(lifespan=lifespan)

    def authorize(header):
        token = Path(os.environ["CODEHUB_SERVICE_TOKEN_FILE"]).read_text().strip()
        if not header or not hmac.compare_digest(header, f"Bearer {token}"):
            raise HTTPException(401, "Authentication required")

    @application.get("/healthz")
    def healthz():
        return {"status": "running"}

    @application.get("/readyz")
    def readyz():
        return {"status": "running"}

    @application.get("/v1/topology")
    def topology(authorization: str | None = Header(None)):
        authorize(authorization)
        with manager.lock:
            return dict(manager.state)

    @application.post("/v1/promotions")
    def promote(authorization: str | None = Header(None)):
        authorize(authorization)
        return manager.promote()

    return application


def main():
    logging.basicConfig(level=logging.INFO)
    configuration = json.loads(Path(os.environ["CODEHUB_TOPOLOGY_CONFIG_FILE"]).read_text())
    for name in ("failure_threshold", "probe_interval_seconds", "maximum_lag_seconds"):
        if type(configuration.get(name)) is not int or configuration[name] <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if type(configuration.get("automatic_promotion")) is not bool:
        raise ValueError("automatic_promotion must be a boolean")
    uvicorn.run(create_app(TopologyManager(configuration)), host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
