"""Control plane server for Data API (:8003).

Provides isolated endpoints for metrics collection, readiness probes,
fault administration, and out-of-band cancellation so observability remains
100% available even during extreme data-plane overload.
"""

from __future__ import annotations

import json
import logging
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from data_state import DataAPIState

logger = logging.getLogger("data-api.control")


def _tcp_dependency_ready(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


class ControlAPIHandler(BaseHTTPRequestHandler):
    state: DataAPIState
    pgbouncer_host: str
    pgbouncer_port: int
    redis_host: str
    redis_port: int

    def _send_json(self, status_code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status_code: int, text: str, content_type: str = "text/plain"):
        body = text.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/healthz", "/"):
            self._send_json(200, {"status": "ok", "plane": "control", "service": "data-api"})
            return

        if self.path == "/readyz":
            dependencies_ready = all(
                _tcp_dependency_ready(host, port)
                for host, port in (
                    (self.redis_host, self.redis_port),
                    (self.pgbouncer_host, self.pgbouncer_port),
                )
            )
            self._send_json(
                200 if dependencies_ready else 503,
                {
                    "status": "ready" if dependencies_ready else "dependencies_unavailable",
                    "service": "data-api",
                },
            )
            return

        if self.path == "/metrics":
            with self.state.lock:
                avg_lat = self.state.total_latency_seconds / max(1, self.state.successful_requests)
                now = time.time()
                # Queries still running exceeding transport timeout (0.6s)
                orphaned_count = sum(
                    1
                    for q in self.state.active_queries.values()
                    if not q["cancelled"] and (now - q["start_time"]) > 0.6
                )
                metrics_text = (
                    f"# HELP data_api_http_handlers_active Active HTTP request handlers on data plane\n"
                    f"# TYPE data_api_http_handlers_active gauge\n"
                    f"data_api_http_handlers_active {self.state.active_handlers}\n"
                    f"# HELP data_api_requests_shed_total Total requests shed due to handler saturation\n"
                    f"# TYPE data_api_requests_shed_total counter\n"
                    f"data_api_requests_shed_total {self.state.requests_shed}\n"
                    f"# HELP backend_requests_total Total number of backend query attempts\n"
                    f"# TYPE backend_requests_total counter\n"
                    f"backend_requests_total {self.state.total_requests}\n"
                    f"# HELP backend_requests_success_total Successful backend queries\n"
                    f"# TYPE backend_requests_success_total counter\n"
                    f"backend_requests_success_total {self.state.successful_requests}\n"
                    f"# HELP backend_requests_failed_total Failed backend queries\n"
                    f"# TYPE backend_requests_failed_total counter\n"
                    f"backend_requests_failed_total {self.state.failed_requests}\n"
                    f"# HELP backend_active_requests Currently executing database requests\n"
                    f"# TYPE backend_active_requests gauge\n"
                    f"backend_active_requests {self.state.active_requests}\n"
                    f"# HELP backend_waiting_requests Currently queued requests awaiting database worker\n"
                    f"# TYPE backend_waiting_requests gauge\n"
                    f"backend_waiting_requests {self.state.queued_requests}\n"
                    f"# HELP data_api_active_requests Requests executing in Data API worker pool\n"
                    f"# TYPE data_api_active_requests gauge\n"
                    f"data_api_active_requests {self.state.active_requests}\n"
                    f"# HELP data_api_waiting_requests Requests waiting for Data API worker pool\n"
                    f"# TYPE data_api_waiting_requests gauge\n"
                    f"data_api_waiting_requests {self.state.queued_requests}\n"
                    f"# HELP orphaned_operations_active In-flight operations continuing after caller timeout\n"
                    f"# TYPE orphaned_operations_active gauge\n"
                    f"orphaned_operations_active {orphaned_count}\n"
                    f"# HELP cancellation_requests_total Cancellation requests received from upstream\n"
                    f"# TYPE cancellation_requests_total counter\n"
                    f"cancellation_requests_total {self.state.cancellation_requests}\n"
                    f"# HELP cancellation_completed_total Successfully cancelled queries\n"
                    f"# TYPE cancellation_completed_total counter\n"
                    f"cancellation_completed_total {self.state.cancellation_completed}\n"
                    f"# HELP backend_request_duration_seconds Average latency in seconds\n"
                    f"# TYPE backend_request_duration_seconds gauge\n"
                    f"backend_request_duration_seconds {avg_lat:.4f}\n"
                )
            self._send_text(200, metrics_text, "text/plain; version=0.0.4")
            return

        if self.path == "/status":
            lat, is_fault = self.state.get_effective_latency()
            with self.state.lock:
                self._send_json(
                    200,
                    {
                        "active_requests": self.state.active_requests,
                        "queued_requests": self.state.queued_requests,
                        "active_handlers": self.state.active_handlers,
                        "requests_shed": self.state.requests_shed,
                        "concurrency_limit": self.state.concurrency_limit,
                        "max_request_handlers": self.state.max_request_handlers,
                        "total_requests": self.state.total_requests,
                        "successful_requests": self.state.successful_requests,
                        "failed_requests": self.state.failed_requests,
                        "fault_active": is_fault,
                        "effective_latency": lat,
                        "cancellation_completed": self.state.cancellation_completed,
                    },
                )
            return

        self._send_json(404, {"error": "Not Found"})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = {}
        if content_length > 0:
            try:
                body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            except Exception:
                body = {}

        if self.path == "/admin/fault":
            latency_ms = float(body.get("latency_ms", 1500.0))
            duration_seconds = float(body.get("duration_seconds", body.get("duration", 10.0)))
            self.state.inject_fault(latency_ms, duration_seconds)
            self._send_json(
                200,
                {
                    "status": "fault_injected",
                    "latency_ms": latency_ms,
                    "duration_seconds": duration_seconds,
                },
            )
            return

        if self.path == "/admin/recover":
            self.state.recover_fault()
            self._send_json(200, {"status": "recovered"})
            return

        if self.path in ("/data/cancel", "/cancel"):
            op_id = self.headers.get("X-Operation-ID", body.get("operation_id", ""))
            att_id = self.headers.get("X-Physical-Attempt-ID", body.get("physical_attempt_id", ""))
            cancelled = self.state.cancel_query(op_id, att_id)
            self._send_json(200, {"status": "cancelled", "cancelled_count": cancelled})
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def create_control_server(
    host: str,
    port: int,
    state: DataAPIState,
    pgbouncer_host: str,
    pgbouncer_port: int,
    redis_host: str,
    redis_port: int,
) -> ThreadingHTTPServer:
    """Creates control plane HTTP server configured with handler dependencies."""
    handler_class = type(
        "ConfiguredControlHandler",
        (ControlAPIHandler,),
        {
            "state": state,
            "pgbouncer_host": pgbouncer_host,
            "pgbouncer_port": pgbouncer_port,
            "redis_host": redis_host,
            "redis_port": redis_port,
        },
    )
    return ThreadingHTTPServer((host, port), handler_class)
