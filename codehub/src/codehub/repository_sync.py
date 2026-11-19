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
        for group, storage in self.router.groups().items():
            if self.stop.is_set():
                return
            try:
                with storage.connection() as connection, connection.cursor() as cursor:
                    cursor.execute("SELECT id FROM projects ORDER BY id")
                    projects = cursor.fetchall()
            except Exception as error:
                logging.warning("Repository group %s unavailable: %s", group, type(error).__name__)
                continue
            for project in projects:
                if self.stop.is_set():
                    return
                try:
                    self.synchronize_project(storage, project["id"])
                except Exception as error:
                    logging.warning("Repository project %s delayed: %s", project["id"], type(error).__name__)

    def synchronize_project(self, storage, project_id):
        path = self.repository.ensure(project_id)
        for index, peer in enumerate(self.peers):
            if self.stop.is_set():
                return
            env = {
                **os.environ,
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "http.extraHeader",
                "GIT_CONFIG_VALUE_0": f"Authorization: Bearer {secret('CODEHUB_SERVICE_TOKEN')}",
            }
            try:
                result = subprocess.run(
                    ["git", "-C", str(path), "fetch", "--no-write-fetch-head", f"{peer}/git/{project_id}",
                     f"+refs/heads/*:refs/remotes/peer-{index}/*"],
                    env=env, capture_output=True, timeout=30,
                )
                if result.returncode:
                    logging.warning("Repository peer %s fetch unavailable for project %s", index, project_id)
            except subprocess.TimeoutExpired:
                logging.warning("Repository peer %s fetch timed out for project %s", index, project_id)
        if self.stop.is_set():
            return
        with storage.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT r.id,r.ref,r.commit_sha,e.revision FROM repository_refs r "
                "JOIN entities e ON e.tenant_id=r.tenant_id AND e.id=r.id WHERE r.project_id=%s",
                (project_id,),
            )
            refs = cursor.fetchall()
        for ref in refs:
            if self.stop.is_set():
                return
            # Ref publication follows authoritative metadata, not peer wall-clock timestamps.
            self.repository.publish_reference(project_id, ref)

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
