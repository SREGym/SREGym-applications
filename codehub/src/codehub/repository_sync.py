"""Explicit asynchronous regional Git object/ref replication."""

import logging
import os
import subprocess
import threading
from urllib.parse import urlsplit

from codehub.config import secret


class RepositorySynchronizer:
    def __init__(self, router, repository, peers):
        self.router = router
        self.repository = repository
        self.peers = tuple(peer.rstrip("/") for peer in peers if peer)
        for peer in self.peers:
            parsed = urlsplit(peer)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
                raise ValueError("Repository peer must be an HTTP(S) endpoint without embedded credentials")
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True, name="repository-sync")

    def synchronize(self):
        for storage in self.router.groups().values():
            with storage.connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT id FROM projects ORDER BY id")
                projects = cursor.fetchall()
            for project in projects:
                project_id = project["id"]
                path = self.repository.ensure(project_id)
                for index, peer in enumerate(self.peers):
                    env = {
                        **os.environ,
                        "GIT_TERMINAL_PROMPT": "0",
                        "GIT_CONFIG_COUNT": "1",
                        "GIT_CONFIG_KEY_0": "http.extraHeader",
                        "GIT_CONFIG_VALUE_0": f"Authorization: Bearer {secret('CODEHUB_SERVICE_TOKEN')}",
                    }
                    result = subprocess.run(
                        [
                            "git",
                            "-C",
                            str(path),
                            "fetch",
                            "--no-write-fetch-head",
                            f"{peer}/git/{project_id}",
                            f"+refs/heads/*:refs/remotes/peer-{index}/*",
                        ],
                        env=env,
                        capture_output=True,
                        timeout=30,
                    )
                    if result.returncode:
                        logging.warning("Repository peer %s fetch unavailable for project %s", index, project_id)
                with storage.connection() as connection, connection.cursor() as cursor:
                    cursor.execute("SELECT ref,commit_sha FROM repository_refs WHERE project_id=%s", (project_id,))
                    refs = cursor.fetchall()
                for ref in refs:
                    # Ref publication follows authoritative metadata, not peer wall-clock timestamps.
                    self.repository.commit(project_id, ref["commit_sha"])
                    self.repository.git(project_id, "update-ref", ref["ref"], ref["commit_sha"], maximum=4096)

    def run(self):
        while not self.stop.is_set():
            try:
                self.synchronize()
            except Exception as exc:
                logging.warning("Repository synchronization delayed: %s", type(exc).__name__)
            self.stop.wait(5)

    def start(self):
        if self.peers:
            self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread.is_alive():
            self.thread.join(timeout=35)
