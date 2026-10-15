#!/usr/bin/env python3
"""Tool Gateway Service for Agentic Retry Platform.

Executes tool operations against Data API. Implements:
1. Two-layer nested retries (tool retry and HTTP transport retry) with proper max_attempts semantics.
2. Atomic global retry budget enforcement via Redis (DECR workflow:{wf_id}:retry_budget).
3. Propagation of detailed execution context (req_id, wf_id, op_id, gen, physical_attempt_id).
4. Cancellation forwarding to Data API to terminate backend executions.
5. Prometheus metrics exporter.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"tool-gateway","msg":"%(message)s"}',
)
logger = logging.getLogger("tool-gateway")

PORT = int(os.environ.get("PORT", "8001"))
DATA_API_URL = os.environ.get("DATA_API_URL", "http://data-api:8002/data/query")
DATA_API_CANCEL_URL = os.environ.get("DATA_API_CANCEL_URL", "http://data-api:8003/data/cancel")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
POLICY_PATH = os.environ.get("POLICY_PATH", "/etc/agent-policy/policy.json")


class RedisClient:
    def __init__(self, host: str, port: int, timeout: float = 1.0):
        self.host = host
        self.port = port
        self.timeout = timeout

    def decr(self, key: str) -> int | None:
        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
                req = f"*2\r\n$4\r\nDECR\r\n${len(key.encode('utf-8'))}\r\n{key}\r\n"
                sock.sendall(req.encode("utf-8"))
                f = sock.makefile("rb")
                line = f.readline()
                if line and line[:1] == b":":
                    return int(line[1:-2])
        except Exception:
            pass
        return None


redis_client = RedisClient(REDIS_HOST, REDIS_PORT)


def load_policy() -> dict:
    if os.path.exists(POLICY_PATH):
        try:
            with open(POLICY_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read policy from {POLICY_PATH}: {e}")
    return {
        "tool": {
            "timeout_ms": 1000,
            "max_attempts": 2,
        },
        "transport": {
            "timeout_ms": 600,
            "max_attempts": 2,
        },
        "retry_budget": {
            "enabled": False,
            "max_physical_attempts_per_workflow": 4,
        },
    }


class GatewayMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.operations_started = 0
        self.operations_completed = 0
        self.tool_retries = 0
        self.http_attempts = 0
        self.http_timeouts = 0
        self.cancellations_forwarded = 0
        self.budget_exhausted_total = 0
        self.unique_operations = set()


metrics = GatewayMetrics()


def forward_cancellation(op_id: str, physical_attempt_id: str = ""):
    """Dispatches cancellation signal to Data API."""
    try:
        payload = json.dumps({"operation_id": op_id, "physical_attempt_id": physical_attempt_id}).encode("utf-8")
        req = urllib.request.Request(DATA_API_CANCEL_URL, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Operation-ID", op_id)
        if physical_attempt_id:
            req.add_header("X-Physical-Attempt-ID", physical_attempt_id)
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            with metrics.lock:
                metrics.cancellations_forwarded += 1
            logger.info(f"Forwarded cancellation for op_id={op_id} (att={physical_attempt_id})")
    except Exception as e:
        logger.warning(f"Failed forwarding cancellation for op_id={op_id}: {e}")


def execute_http_call(
    req_id: str,
    wf_id: str,
    op_id: str,
    gen: int,
    physical_attempt_id: str,
    layer: str,
    deadline: float,
) -> dict:
    policy = load_policy()
    transport_timeout = policy.get("transport", {}).get("timeout_ms", 600) / 1000.0
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return {"status": "error", "error": "Tool deadline exhausted", "is_timeout": True, "latency": 0.0}

    payload = json.dumps({
        "logical_request_id": req_id,
        "workflow_id": wf_id,
        "operation_id": op_id,
        "generation": gen,
        "physical_attempt_id": physical_attempt_id,
        "layer": layer,
    }).encode("utf-8")

    req = urllib.request.Request(DATA_API_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Logical-Request-ID", req_id)
    req.add_header("X-Workflow-ID", wf_id)
    req.add_header("X-Operation-ID", op_id)
    req.add_header("X-Generation", str(gen))
    req.add_header("X-Physical-Attempt-ID", physical_attempt_id)
    req.add_header("X-Retry-Layer", layer)

    start = time.time()
    with metrics.lock:
        metrics.http_attempts += 1

    try:
        with urllib.request.urlopen(req, timeout=min(transport_timeout, remaining)) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return {"status": "ok", "latency": time.time() - start, "data": data}
    except urllib.error.HTTPError as e:
        return {"status": "error", "error": f"HTTP {e.code}", "latency": time.time() - start}
    except Exception as e:
        is_to = "timed out" in str(e).lower()
        if is_to:
            with metrics.lock:
                metrics.http_timeouts += 1
        return {"status": "error", "error": str(e), "is_timeout": is_to, "latency": time.time() - start}


def execute_tool_operation(req_id: str, wf_id: str, op_id: str, gen: int) -> dict:
    policy = load_policy()
    tool_attempts = policy.get("tool", {}).get("max_attempts", 2)
    tool_timeout = policy.get("tool", {}).get("timeout_ms", 1000) / 1000.0
    http_attempts = policy.get("transport", {}).get("max_attempts", 2)
    budget_enabled = policy.get("retry_budget", {}).get("enabled", False)

    with metrics.lock:
        metrics.operations_started += 1
        metrics.unique_operations.add(op_id)

    last_error = None
    deadline = time.monotonic() + tool_timeout

    for tool_try in range(tool_attempts):
        if tool_try > 0:
            with metrics.lock:
                metrics.tool_retries += 1

        layer = "tool" if tool_try > 0 else "initial"

        for http_try in range(http_attempts):
            if time.monotonic() >= deadline:
                return {"status": "error", "operation_id": op_id, "error": "Tool deadline exhausted"}
            if budget_enabled:
                rem = redis_client.decr(f"workflow:{wf_id}:retry_budget")
                if rem is not None and rem < 0:
                    with metrics.lock:
                        metrics.budget_exhausted_total += 1
                    logger.warning(f"Global retry budget exhausted for wf={wf_id} op={op_id}")
                    return {
                        "status": "error",
                        "error": "Global retry budget exhausted",
                        "budget_exhausted": True,
                    }

            http_layer = "transport" if http_try > 0 else layer
            physical_attempt_id = f"{op_id}-t{tool_try}-h{http_try}"
            res = execute_http_call(req_id, wf_id, op_id, gen, physical_attempt_id, http_layer, deadline)

            if res["status"] == "ok":
                with metrics.lock:
                    metrics.operations_completed += 1
                return {
                    "status": "success",
                    "operation_id": op_id,
                    "physical_attempt_id": physical_attempt_id,
                    "tool_attempt": tool_try + 1,
                    "http_attempt": http_try + 1,
                    "data": res["data"],
                }

            last_error = res["error"]

    return {
        "status": "error",
        "operation_id": op_id,
        "error": f"Tool operations exhausted: {last_error}",
    }


class ToolGatewayHandler(BaseHTTPRequestHandler):
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
            self._send_json(200, {"status": "ok", "service": "tool-gateway"})
            return

        if self.path == "/readyz":
            data_api_ready_url = os.environ.get(
                "DATA_API_READY_URL",
                DATA_API_CANCEL_URL.rsplit("/data/", 1)[0] + "/readyz",
            )
            try:
                with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=0.5), urllib.request.urlopen(data_api_ready_url, timeout=0.5):
                    pass
                self._send_json(200, {"status": "ready", "service": "tool-gateway"})
            except (OSError, urllib.error.URLError):
                self._send_json(503, {"status": "dependencies_unavailable", "service": "tool-gateway"})
            return

        if self.path == "/metrics":
            with metrics.lock:
                metrics_text = (
                    f"# HELP tool_operations_started_total Total tool operations started\n"
                    f"# TYPE tool_operations_started_total counter\n"
                    f"tool_operations_started_total {metrics.operations_started}\n"
                    f"# HELP tool_operations_completed_total Successfully completed tool operations\n"
                    f"# TYPE tool_operations_completed_total counter\n"
                    f"tool_operations_completed_total {metrics.operations_completed}\n"
                    f"# HELP unique_operation_ids_total Unique operation IDs handled\n"
                    f"# TYPE unique_operation_ids_total counter\n"
                    f"unique_operation_ids_total {len(metrics.unique_operations)}\n"
                    f"# HELP tool_retries_total Retries executed at tool level\n"
                    f"# TYPE tool_retries_total counter\n"
                    f"tool_retries_total {metrics.tool_retries}\n"
                    f"# HELP tool_http_attempts_total Total HTTP calls to Data API\n"
                    f"# TYPE tool_http_attempts_total counter\n"
                    f"tool_http_attempts_total {metrics.http_attempts}\n"
                    f"# HELP tool_cancellations_forwarded Cancellations forwarded to Data API\n"
                    f"# TYPE tool_cancellations_forwarded counter\n"
                    f"tool_cancellations_forwarded {metrics.cancellations_forwarded}\n"
                    f"# HELP retry_budget_exhausted_total Invocations blocked by retry budget\n"
                    f"# TYPE retry_budget_exhausted_total counter\n"
                    f"retry_budget_exhausted_total {metrics.budget_exhausted_total}\n"
                )
            self._send_text(200, metrics_text, "text/plain; version=0.0.4")
            return

        if self.path == "/config":
            self._send_json(200, load_policy())
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

        if self.path in ("/tools/cancel", "/cancel"):
            op_id = self.headers.get("X-Operation-ID", body.get("operation_id", ""))
            att_id = self.headers.get("X-Physical-Attempt-ID", body.get("physical_attempt_id", ""))
            threading.Thread(target=forward_cancellation, args=(op_id, att_id), daemon=True).start()
            self._send_json(200, {"status": "cancellation_dispatched", "operation_id": op_id, "physical_attempt_id": att_id})
            return

        if self.path in ("/tools/execute", "/execute"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", "req-unknown"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", "wf-unknown"))
            op_id = self.headers.get("X-Operation-ID", body.get("operation_id", f"{wf_id}-op"))
            gen = int(self.headers.get("X-Generation", body.get("generation", 0)))

            result = execute_tool_operation(req_id, wf_id, op_id, gen)
            if result["status"] == "success":
                self._send_json(200, result)
            else:
                self._send_json(504, result)
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, ToolGatewayHandler)
    logger.info(f"Starting Tool Gateway on 0.0.0.0:{PORT} (data_api={DATA_API_URL}, redis={REDIS_HOST}:{REDIS_PORT})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Tool Gateway...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
