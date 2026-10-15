#!/usr/bin/env python3
"""Agent Orchestrator Service for Agentic Retry Platform.

Coordinates multi-stage tool execution, manages workflow generations,
handles speculative replanning upon planner timeout, implements uncancelled
orphaned work mechanics vs cancellation propagation, connects to Redis for durable
workflow tracking, and tracks retry budgets.
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
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"agent-orchestrator","msg":"%(message)s"}',
)
logger = logging.getLogger("agent-orchestrator")

PORT = int(os.environ.get("PORT", "8000"))
TOOL_GATEWAY_URL = os.environ.get("TOOL_GATEWAY_URL", "http://tool-gateway:8001/tools/execute")
TOOL_GATEWAY_CANCEL_URL = os.environ.get("TOOL_GATEWAY_CANCEL_URL", "http://tool-gateway:8001/tools/cancel")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
POLICY_PATH = os.environ.get("POLICY_PATH", "/etc/agent-policy/policy.json")


class RedisClient:
    """Pure-Python RESP client with zero external dependencies."""

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

    def decr(self, key: str) -> int | None:
        res = self.execute("DECR", key)
        return int(res) if isinstance(res, int) else None

    def rpush(self, key: str, val: str) -> bool:
        return self.execute("RPUSH", key, val) is not None

    def hset(self, key: str, mapping: dict) -> bool:
        args = ["HSET", key]
        for k, v in mapping.items():
            args.extend([k, str(v)])
        return self.execute(*args) is not None


redis_client = RedisClient(REDIS_HOST, REDIS_PORT)


def load_policy() -> dict:
    if os.path.exists(POLICY_PATH):
        try:
            with open(POLICY_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read policy from {POLICY_PATH}: {e}")
    return {
        "workflow": {
            "timeout_ms": 1500,
            "max_attempts": 3,
            "max_inflight": 100,
            "cancel_children_on_timeout": False,
        },
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


class OrchestratorState:
    def __init__(self):
        self.lock = threading.Lock()
        self.logical_requests = 0
        self.goodput_requests = 0
        self.failed_workflows = 0
        self.workflow_generations = 0
        self.active_workflows = 0
        self.total_duration_seconds = 0.0
        self.orphaned_operations = 0

    def inc_active(self):
        with self.lock:
            self.active_workflows += 1

    def dec_active(self, success: bool, duration: float):
        with self.lock:
            self.active_workflows = max(0, self.active_workflows - 1)
            if success:
                self.goodput_requests += 1
            else:
                self.failed_workflows += 1
            self.total_duration_seconds += duration


state = OrchestratorState()


def dispatch_tool_cancellation(op_id: str):
    """Sends cancellation signal to Tool Gateway to kill child operation."""
    try:
        payload = json.dumps({"operation_id": op_id}).encode("utf-8")
        req = urllib.request.Request(TOOL_GATEWAY_CANCEL_URL, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Operation-ID", op_id)
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            logger.info(f"Dispatched cancellation for op_id={op_id}: {resp.status}")
    except Exception as e:
        logger.warning(f"Failed to dispatch cancellation for op_id={op_id}: {e}")


def execute_workflow(req_id: str, wf_id: str) -> dict:
    """Executes workflow with speculative replanning and generation tracking."""
    policy = load_policy()
    wf_cfg = policy.get("workflow", {})
    planner_timeout = wf_cfg.get("timeout_ms", 1500) / 1000.0
    max_generations = wf_cfg.get("max_attempts", 3)
    cancel_children = wf_cfg.get("cancel_children_on_timeout", False)
    max_inflight = wf_cfg.get("max_inflight", 100)

    budget_cfg = policy.get("retry_budget", {})
    budget_enabled = budget_cfg.get("enabled", False)
    initial_budget = budget_cfg.get("max_physical_attempts_per_workflow", 4)

    with state.lock:
        if state.active_workflows >= max_inflight:
            return {
                "status": "rejected",
                "code": 503,
                "error": "Max active workflows exceeded (admission control)",
            }
        state.logical_requests += 1

    state.inc_active()
    start_time = time.time()

    # Durable Redis state tracking
    redis_client.hset(
        f"workflow:{wf_id}",
        {
            "logical_request_id": req_id,
            "status": "active",
            "created_at": time.time(),
        },
    )
    redis_client.rpush("queue:pending", wf_id)
    if budget_enabled:
        redis_client.set(f"workflow:{wf_id}:retry_budget", str(initial_budget), ex=300)

    active_ops: list[str] = []
    last_error = None

    for gen in range(max_generations):
        with state.lock:
            state.workflow_generations += 1

        op_id = f"{wf_id}-op-gen{gen}"
        active_ops.append(op_id)
        redis_client.hset(f"workflow:{wf_id}", {"generation": gen, "active_op": op_id})

        result_holder: dict = {}
        done_event = threading.Event()

        def _call_gateway(target_op: str, current_gen: int):
            payload = json.dumps({
                "logical_request_id": req_id,
                "workflow_id": wf_id,
                "operation_id": target_op,
                "generation": current_gen,
                "retry_budget": initial_budget,
            }).encode("utf-8")
            req = urllib.request.Request(TOOL_GATEWAY_URL, data=payload, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("X-Logical-Request-ID", req_id)
            req.add_header("X-Workflow-ID", wf_id)
            req.add_header("X-Operation-ID", target_op)
            req.add_header("X-Generation", str(current_gen))
            req.add_header("X-Retry-Budget", str(initial_budget))

            try:
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    result_holder["response"] = json.loads(resp.read().decode("utf-8"))
                    result_holder["status"] = "ok"
            except Exception as e:
                result_holder["status"] = "error"
                result_holder["error"] = str(e)
            finally:
                done_event.set()

        t = threading.Thread(target=_call_gateway, args=(op_id, gen), daemon=True)
        t.start()

        completed_in_time = done_event.wait(timeout=planner_timeout)

        if completed_in_time and result_holder.get("status") == "ok":
            duration = time.time() - start_time
            state.dec_active(success=True, duration=duration)
            redis_client.hset(f"workflow:{wf_id}", {"status": "succeeded", "duration": duration})

            # Clean up earlier generations if cancellation enabled
            if cancel_children:
                for past_op in active_ops:
                    if past_op != op_id:
                        dispatch_tool_cancellation(past_op)

            return {
                "status": "success",
                "logical_request_id": req_id,
                "workflow_id": wf_id,
                "generations_attempted": gen + 1,
                "duration_seconds": duration,
                "result": result_holder["response"],
            }

        # Planner deadline expired
        if cancel_children:
            logger.info(f"Planner timeout expired for gen={gen}; cancelling op_id={op_id}")
            dispatch_tool_cancellation(op_id)
        else:
            with state.lock:
                state.orphaned_operations += 1
            logger.warning(
                f"Planner timeout expired for gen={gen} ({planner_timeout}s); "
                f"speculatively launching gen={gen+1} without cancelling orphaned op_id={op_id}"
            )

        last_error = result_holder.get("error", "Planner deadline timeout")

    duration = time.time() - start_time
    state.dec_active(success=False, duration=duration)
    redis_client.hset(f"workflow:{wf_id}", {"status": "failed", "error": str(last_error)})
    return {
        "status": "error",
        "logical_request_id": req_id,
        "workflow_id": wf_id,
        "generations_attempted": max_generations,
        "duration_seconds": duration,
        "error": f"Planner exhausted all {max_generations} speculative replans: {last_error}",
    }


class OrchestratorHandler(BaseHTTPRequestHandler):
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
            self._send_json(200, {"status": "ok", "service": "agent-orchestrator"})
            return

        if self.path == "/readyz":
            gateway_ready_url = TOOL_GATEWAY_URL.rsplit("/tools/", 1)[0] + "/readyz"
            try:
                with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=0.5), urllib.request.urlopen(gateway_ready_url, timeout=0.5):
                    pass
                self._send_json(200, {"status": "ready", "service": "agent-orchestrator"})
            except (OSError, urllib.error.URLError):
                self._send_json(503, {"status": "dependencies_unavailable", "service": "agent-orchestrator"})
            return

        if self.path == "/metrics":
            with state.lock:
                avg_dur = state.total_duration_seconds / max(1, state.goodput_requests)
                metrics_text = (
                    f"# HELP logical_workflows_total Total unique logical workflows submitted\n"
                    f"# TYPE logical_workflows_total counter\n"
                    f"logical_workflows_total {state.logical_requests}\n"
                    f"# HELP goodput_requests_total Successful unique logical requests completed\n"
                    f"# TYPE goodput_requests_total counter\n"
                    f"goodput_requests_total {state.goodput_requests}\n"
                    f"# HELP workflow_generation_total Total speculative replanning branches created\n"
                    f"# TYPE workflow_generation_total counter\n"
                    f"workflow_generation_total {state.workflow_generations}\n"
                    f"# HELP orphaned_operations_created Operations left running after planner timeout\n"
                    f"# TYPE orphaned_operations_created counter\n"
                    f"orphaned_operations_created {state.orphaned_operations}\n"
                    f"# HELP workflow_active_count Currently active workflows\n"
                    f"# TYPE workflow_active_count gauge\n"
                    f"workflow_active_count {state.active_workflows}\n"
                    f"# HELP workflow_duration_seconds Average workflow completion duration\n"
                    f"# TYPE workflow_duration_seconds gauge\n"
                    f"workflow_duration_seconds {avg_dur:.4f}\n"
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

        if self.path in ("/agent/workflow", "/query", "/workflow"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", f"req-{time.time()}"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", f"wf-{time.time()}"))

            result = execute_workflow(req_id, wf_id)
            code = result.get("code", 200 if result["status"] == "success" else 504)
            self._send_json(code, result)
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, OrchestratorHandler)
    logger.info(f"Starting Agent Orchestrator on 0.0.0.0:{PORT} (tools={TOOL_GATEWAY_URL}, redis={REDIS_HOST}:{REDIS_PORT})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Agent Orchestrator...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
