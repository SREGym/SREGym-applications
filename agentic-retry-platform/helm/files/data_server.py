"""Bounded data plane server for Data API (:8002).

Enforces strict concurrency limits:
1. Thread pool bound (max_request_handlers <= 64).
2. Immediate 503 shedding when handler capacity is reached without thread creation.
3. Queue capacity bound (queue_capacity <= 40).
4. Worker pool execution (concurrency_limit = 25) targeting PgBouncer.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

from data_state import DataAPIState
from pg_wire import execute_pg_query

logger = logging.getLogger("data-api.data")


class ThreadPoolHTTPServer(HTTPServer):
    """HTTPServer backed by a bounded ThreadPoolExecutor to prevent thread exhaustion."""

    def __init__(self, server_address, RequestHandlerClass, state: DataAPIState, max_workers: int = 64):
        super().__init__(server_address, RequestHandlerClass)
        self.state = state
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="data-worker",
        )

    def process_request(self, request, client_address):
        with self.state.lock:
            if self.state.active_handlers >= self.state.max_request_handlers:
                self.state.requests_shed += 1
                self.state.failed_requests += 1
                try:
                    body = b'{"error":"Data API request handler capacity saturated"}\n'
                    resp = (
                        b"HTTP/1.1 503 Service Unavailable\r\n"
                        b"Content-Type: application/json\r\n"
                        b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
                        b"Connection: close\r\n\r\n" + body
                    )
                    request.sendall(resp)
                except Exception:
                    pass
                finally:
                    self.shutdown_request(request)
                return

            self.state.active_handlers += 1

        try:
            self.executor.submit(self._process_request_thread, request, client_address)
        except RuntimeError:
            with self.state.lock:
                self.state.active_handlers = max(0, self.state.active_handlers - 1)
            self.shutdown_request(request)

    def _process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            with self.state.lock:
                self.state.active_handlers = max(0, self.state.active_handlers - 1)
            self.shutdown_request(request)

    def shutdown(self):
        super().shutdown()
        self.executor.shutdown(wait=False, cancel_futures=True)


class DataAPIHandler(BaseHTTPRequestHandler):
    state: DataAPIState
    pgbouncer_host: str
    pgbouncer_port: int

    def _send_json(self, status_code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/healthz", "/", "/readyz"):
            self._send_json(200, {"status": "ready", "plane": "data", "service": "data-api"})
            return
        self._send_json(
            404,
            {
                "error": "Data plane only serves /data/query; observability and admin endpoints are on control port 8003"
            },
        )

    def do_POST(self):
        if self.path not in ("/data/query", "/query", "/execute"):
            self._send_json(404, {"error": "Not Found"})
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = {}
        if content_length > 0:
            try:
                body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            except Exception:
                body = {}

        req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", "req-unknown"))
        wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", "wf-unknown"))
        op_id = self.headers.get("X-Operation-ID", body.get("operation_id", f"{wf_id}-op"))
        att_id = self.headers.get("X-Physical-Attempt-ID", body.get("physical_attempt_id", f"{op_id}-att-0"))

        with self.state.lock:
            self.state.total_requests += 1
            if self.state.queued_requests >= self.state.queue_capacity:
                self.state.requests_shed += 1
                self.state.failed_requests += 1
                self._send_json(503, {"error": "Backend connection backlog exhausted"})
                return
            self.state.queued_requests += 1

        cancel_event = self.state.register_query(att_id, op_id, wf_id)
        start_wait = time.time()

        # Acquire connection pool slot (concurrency limit = 25)
        acquired = self.state.semaphore.acquire(timeout=5.0)

        with self.state.lock:
            self.state.queued_requests = max(0, self.state.queued_requests - 1)
            if not acquired:
                self.state.failed_requests += 1
                self.state.unregister_query(att_id)
                self._send_json(504, {"error": "Connection acquisition timeout from pool"})
                return
            self.state.active_requests += 1

        try:
            if cancel_event.is_set():
                self._send_json(499, {"error": "Client cancelled before execution"})
                return

            latency, _ = self.state.get_effective_latency()
            ok, msg = execute_pg_query(
                self.pgbouncer_host,
                self.pgbouncer_port,
                latency,
                cancel_event,
            )

            duration = time.time() - start_wait
            with self.state.lock:
                if ok:
                    self.state.successful_requests += 1
                    self.state.total_latency_seconds += duration
                else:
                    self.state.failed_requests += 1

            if ok:
                self._send_json(
                    200,
                    {
                        "status": "success",
                        "operation_id": op_id,
                        "physical_attempt_id": att_id,
                        "execution_time_seconds": duration,
                        "result": [{"id": "item-1", "key": "agentic-state", "value": "healthy"}],
                    },
                )
            else:
                self._send_json(499 if "cancelled" in msg else 500, {"error": msg})
        finally:
            self.state.unregister_query(att_id)
            with self.state.lock:
                self.state.active_requests = max(0, self.state.active_requests - 1)
            self.state.semaphore.release()

    def log_message(self, format, *args):
        pass


def create_data_server(
    host: str,
    port: int,
    state: DataAPIState,
    pgbouncer_host: str,
    pgbouncer_port: int,
    max_request_handlers: int = 64,
) -> ThreadPoolHTTPServer:
    """Creates thread-bounded data plane HTTP server."""
    handler_class = type(
        "ConfiguredDataHandler",
        (DataAPIHandler,),
        {
            "state": state,
            "pgbouncer_host": pgbouncer_host,
            "pgbouncer_port": pgbouncer_port,
        },
    )
    return ThreadPoolHTTPServer((host, port), handler_class, state=state, max_workers=max_request_handlers)
