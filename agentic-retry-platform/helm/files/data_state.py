"""State and synchronization management for Data API.

Maintains concurrency limits, thread bounds, active query registry,
and cluster-wide fault synchronization via Redis.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any

logger = logging.getLogger("data-api.state")


class RedisClient:
    """Minimal TCP Redis client for shared cluster coordination."""

    def __init__(self, host: str, port: int, timeout: float = 1.0):
        self.host = host
        self.port = port
        self.timeout = timeout

    def execute(self, *args) -> Any:
        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
                req = f"*{len(args)}\r\n"
                for arg in args:
                    s = str(arg)
                    req += f"${len(s.encode('utf-8'))}\r\n{s}\r\n"
                sock.sendall(req.encode("utf-8"))

                f = sock.makefile("rb")
                line = f.readline()
                if not line:
                    return None
                prefix = line[:1]
                if prefix in (b"+", b"-"):
                    return line[1:-2].decode("utf-8")
                elif prefix == b":":
                    return int(line[1:-2])
                elif prefix == b"$":
                    length = int(line[1:-2])
                    if length == -1:
                        return None
                    data = f.read(length)
                    f.read(2)
                    return data.decode("utf-8")
                return None
        except Exception:
            return None

    def get(self, key: str) -> str | None:
        res = self.execute("GET", key)
        return str(res) if res is not None else None

    def set(self, key: str, val: str, ex: int | None = None) -> bool:
        if ex:
            return self.execute("SET", key, val, "EX", ex) is not None
        return self.execute("SET", key, val) is not None

    def delete(self, key: str) -> bool:
        return self.execute("DEL", key) is not None


class DataAPIState:
    """Encapsulates all runtime state, metrics counters, and synchronization primitives."""

    def __init__(
        self,
        concurrency_limit: int = 25,
        queue_capacity: int = 40,
        max_request_handlers: int = 64,
        normal_latency: float = 0.10,
        fault_latency: float = 1.50,
        redis_host: str = "redis",
        redis_port: int = 6379,
    ):
        self.lock = threading.Lock()
        self.concurrency_limit = concurrency_limit
        self.queue_capacity = queue_capacity
        self.max_request_handlers = max_request_handlers
        self.normal_latency = normal_latency
        self.fault_latency = fault_latency
        self.local_fault_active = False

        self.semaphore = threading.BoundedSemaphore(self.concurrency_limit)
        self.active_requests = 0       # Currently executing in database
        self.queued_requests = 0       # Waiting for database worker semaphore
        self.active_handlers = 0       # Active HTTP query handlers on data port
        self.requests_shed = 0         # Requests immediately rejected with 503 due to handler limit

        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_latency_seconds = 0.0

        # Query tracking: physical_attempt_id -> info
        self.active_queries: dict[str, dict] = {}
        self.cancellation_requests = 0
        self.cancellation_completed = 0

        self.redis = RedisClient(redis_host, redis_port)

    def get_effective_latency(self) -> tuple[float, bool]:
        """Resolves latency: checks shared Redis cluster state before local state."""
        redis_fault = self.redis.get("fault:data-api:latency_ms")
        if redis_fault:
            try:
                lat = float(redis_fault) / 1000.0
                return lat, True
            except ValueError:
                pass

        with self.lock:
            return (self.fault_latency, True) if self.local_fault_active else (self.normal_latency, False)

    def inject_fault(self, latency_ms: float = 1500.0, duration_seconds: float = 10.0):
        """Sets distributed fault in Redis and marks local state."""
        self.redis.set("fault:data-api:latency_ms", str(latency_ms), ex=int(duration_seconds) + 1)
        self.redis.set("fault:data-api:active", "1", ex=int(duration_seconds) + 1)

        with self.lock:
            self.fault_latency = latency_ms / 1000.0
            self.local_fault_active = True
        logger.warning(f"Injected cluster fault: {latency_ms}ms for {duration_seconds}s (synchronized via Redis)")

    def recover_fault(self):
        """Clears distributed fault in Redis and restores normal local state."""
        self.redis.delete("fault:data-api:latency_ms")
        self.redis.delete("fault:data-api:active")
        with self.lock:
            self.local_fault_active = False
        logger.info("Recovered fault: restored normal latency across cluster")

    def register_query(self, att_id: str, op_id: str, wf_id: str) -> threading.Event:
        event = threading.Event()
        with self.lock:
            self.active_queries[att_id] = {
                "op_id": op_id,
                "wf_id": wf_id,
                "cancel_event": event,
                "start_time": time.time(),
                "cancelled": False,
            }
        return event

    def unregister_query(self, att_id: str):
        with self.lock:
            self.active_queries.pop(att_id, None)

    def cancel_query(self, op_id: str, att_id: str = "") -> int:
        cancelled_count = 0
        with self.lock:
            self.cancellation_requests += 1
            for q_id, q_info in list(self.active_queries.items()):
                if (att_id and q_id == att_id) or (op_id and q_info.get("op_id") == op_id):
                    q_info["cancelled"] = True
                    q_info["cancel_event"].set()
                    cancelled_count += 1
            self.cancellation_completed += cancelled_count
        if cancelled_count > 0:
            logger.info(f"Cancelled {cancelled_count} queries for op_id={op_id} (att={att_id})")
        return cancelled_count
