"""Confirmed publishing and manual-ack durable effect consumption."""

import hashlib
import json
import logging
import os
import signal
import threading

import httpx
import pika

from codehub.builds import build_bundle
from codehub.config import Settings, secret
from codehub.domain import DomainError
from codehub.http_support import service_headers
from codehub.storage.mysql import MySQLStorage
from codehub.storage.routing import StorageRouter


class EffectProcessor:
    def __init__(self, storage, settings, *, client=None):
        self.storage = storage
        self.settings = settings
        self.client = client or httpx.Client(timeout=30, headers=service_headers())

    def process(self, effect_id):
        effect = self.storage.effect(effect_id)
        if not effect:
            raise DomainError("Effect is absent from the routed database")
        if effect["state"] == "done":
            return
        payload = effect["payload"]
        if effect["effect_kind"] == "search":
            for destination in self.settings.search_urls:
                response = self.client.post(
                    f"{destination}/internal/index", json={"effect_id": effect_id, "operation": payload}
                )
                response.raise_for_status()
        elif effect["effect_kind"] == "build":
            project_id, commit_sha = payload["project_id"], payload["commit_sha"]
            response = self.client.get(
                f"{self.settings.repository_url}/internal/projects/{project_id}/archive/{commit_sha}"
            )
            response.raise_for_status()
            content, manifest = build_bundle(response.content, project_id=project_id, commit_sha=commit_sha)
            checksum = hashlib.sha256(content).hexdigest()
            for destination in self.settings.delivery_urls or (self.settings.delivery_url,):
                response = self.client.put(f"{destination}/internal/artifacts/{checksum}", content=content)
                response.raise_for_status()
            self.storage.record_build(
                effect_id, effect["event_id"], project_id, commit_sha, checksum, len(content), len(manifest["files"])
            )
        elif effect["effect_kind"] == "delivery":
            response = self.client.post(
                f"{self.settings.delivery_url}/internal/deliveries",
                json={
                    "effect_id": effect_id,
                    "event_id": effect["event_id"],
                    "destination": payload["url"],
                    "operation": payload["operation"],
                },
            )
            response.raise_for_status()
        else:
            raise DomainError("Unknown durable effect kind")
        self.storage.complete(effect_id)


class Worker:
    def __init__(self, settings, storage=None):
        self.settings = settings
        self.storage = storage or MySQLStorage(settings)
        self.router = StorageRouter(settings)
        self.storage_override = storage
        self.stop = threading.Event()
        self.queue = os.environ.get("CODEHUB_QUEUE", "work")
        self.heartbeat = settings.data_dir / "worker-heartbeat"
        settings.data_dir.mkdir(parents=True, exist_ok=True)

    def broker(self):
        params = pika.URLParameters(secret("CODEHUB_BROKER_URL"))
        params.heartbeat = 30
        params.blocked_connection_timeout = 15
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue, durable=True, arguments={"x-queue-type": "quorum"})
        channel.basic_qos(prefetch_count=8)
        return connection, channel

    def publish_loop(self):
        while not self.stop.is_set():
            try:
                connection, channel = self.broker()
                channel.confirm_delivery()
                try:
                    while not self.stop.is_set():
                        groups = {"group-0": self.storage_override} if self.storage_override else self.router.groups()
                        for group, storage in groups.items():
                            for effect_id in storage.pending(limit=100):
                                body = json.dumps(
                                    {"group": group, "effect_id": effect_id}, separators=(",", ":")
                                ).encode()
                                channel.basic_publish(
                                    exchange="",
                                    routing_key=self.queue,
                                    body=body,
                                    mandatory=True,
                                    properties=pika.BasicProperties(
                                        delivery_mode=2, message_id=effect_id, content_type="application/json"
                                    ),
                                )
                                storage.publish_confirmed(effect_id)
                        connection.process_data_events(time_limit=1)
                        self.stop.wait(0.25)
                finally:
                    if connection.is_open:
                        connection.close()
            except Exception as exc:
                logging.warning("Outbox publisher unavailable: %s", type(exc).__name__)
                self.stop.wait(2)

    def run(self):
        publisher = threading.Thread(target=self.publish_loop, name="outbox-publisher", daemon=True)
        publisher.start()
        client = httpx.Client(timeout=30, headers=service_headers())
        while not self.stop.is_set():
            try:
                connection, channel = self.broker()
                try:
                    for method, _properties, body in channel.consume(self.queue, inactivity_timeout=2, auto_ack=False):
                        self.heartbeat.touch()
                        if self.stop.is_set():
                            break
                        if method is None:
                            continue
                        try:
                            message = json.loads(body)
                            group, effect_id = message["group"], message["effect_id"]
                            groups = (
                                {"group-0": self.storage_override} if self.storage_override else self.router.groups()
                            )
                            storage = groups.get(group)
                            if storage is None:
                                raise DomainError("Message references an unconfigured database group")
                            if len(effect_id) != 64 or any(c not in "0123456789abcdef" for c in effect_id):
                                raise DomainError("Invalid effect identifier")
                            try:
                                processor = EffectProcessor(storage, self.settings, client=client)
                                processor.process(effect_id)
                            except Exception as exc:
                                # Durable SQL retry precedes ack; no work is discarded on transient failures.
                                retained = storage.retry(effect_id, type(exc).__name__)
                                if not retained and not storage.effect(effect_id):
                                    raise DomainError("Unavailable effect has no retained SQL work") from exc
                                logging.warning("Effect %s retry: %s", effect_id, type(exc).__name__)
                            channel.basic_ack(method.delivery_tag)
                        except Exception as exc:
                            # Invalid queue payload is retained, rather than silently deleting backlog.
                            logging.warning("Durable message could not be processed: %s", type(exc).__name__)
                            channel.basic_nack(method.delivery_tag, requeue=True)
                            connection.process_data_events(time_limit=1)
                    channel.cancel()
                finally:
                    if connection.is_open:
                        connection.close()
            except Exception as exc:
                logging.warning("Effect consumer unavailable: %s", type(exc).__name__)
                self.stop.wait(2)
        publisher.join(timeout=30)
        client.close()


if __name__ == "__main__":
    worker = Worker(Settings.load())
    signal.signal(signal.SIGTERM, lambda *_: worker.stop.set())
    signal.signal(signal.SIGINT, lambda *_: worker.stop.set())
    worker.run()
