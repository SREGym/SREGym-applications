#!/usr/bin/env python3
"""Company provider gateway: a Nova 2023.1 API subset over the company's QEMU guests.

  GET  /v2.1/servers/detail?limit=N&marker=ID    paginated inventory
  GET  /v2.1/servers/{id}                         one server
  POST /v2.1/servers/{id}/action                  os-stop | os-start | rescue | unrescue | rebuild
  GET  /v2.1/templates/default, POST to publish   the instance initialization template
  POST /v2.1/instances, /v2.1/instances/{id}/delete   the recovery instance
  GET  /v2.1/limits, /v2.1/stats                  quota and request counts
Other operations return 501.

Every request, from any caller (on-call tools and company automation),
draws from one token bucket: 10 requests/s, burst 20. An empty
bucket returns 429 with Retry-After, as the company gateway documents. The
X-Client header is used only for accounting. One task at a time per server
(409 while busy). Statuses follow Nova: ACTIVE, SHUTOFF, RESCUE, with
task_state powering-off, powering-on, rescuing, unrescuing.

  python3 provider.py [--port 28301] [--rate 10] [--burst 20]
"""

import argparse
import json
import math
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import company_instances
import lab

company_instances.load()

LOG = lab.ARTIFACTS / (
    (lab.world_policy.config()["artifact_subdir"] + "/provider.jsonl") if lab.WORLD_ACTIVE else "provider.jsonl"
)


def world_now():
    p = lab.ROOT / "world-clock.json"
    if lab.WORLD_ACTIVE and p.exists():
        c = json.loads(p.read_text())
        return c["guest_epoch"] + time.time() - c["host_epoch"]
    return time.time()


class Bucket:
    def __init__(self, rate, burst):
        self.rate, self.burst = rate, burst
        self.tokens, self.updated = float(burst), time.monotonic()
        self.lock = threading.Lock()

    def take(self):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return 0.0
            return (1 - self.tokens) / self.rate


class Provider:
    def __init__(self, rate, burst):
        self.bucket = Bucket(rate, burst)
        self.tasks = {}  # server -> task_state
        self.rescued = set()
        self.lock = threading.Lock()
        self.counts = {}
        if os.environ.get("SMART_METER_PROVIDER_RESTORE"):
            with open(os.environ["SMART_METER_PROVIDER_RESTORE"]) as restore:
                state = json.load(restore)
            self.bucket.tokens = state["tokens"]
            self.rescued = set(state["rescued"])
            self.counts = state["counts"]

    def account(self, client, outcome):
        with self.lock:
            key = f"{client}:{outcome}"
            self.counts[key] = self.counts.get(key, 0) + 1

    def status(self, name):
        if name in self.rescued:
            return "RESCUE"
        return "ACTIVE" if lab.pid_of(lab.VM / f"{name}.pid") else "SHUTOFF"

    def server(self, name):
        spec = lab.VMS[name]
        with self.lock:
            task = self.tasks.get(name)
            rescue = name in self.rescued or task in ("rescuing", "unrescuing")
            target = lab.rescue_name(name) if rescue else name
            company_names = (lab.world_policy.config() or {}).get("memory_mib", {})
            methods = ["exec", "stream", "upload"] if not rescue or name in company_names else []
            # Ready means setup completed and the target process is present, not a transport probe.
            ready = bool(methods) and not task and bool(lab.pid_of(lab.VM / f"{target}.pid"))
            status = self.status(name)
            if not rescue and name in company_names:
                methods.append("console")
        return {
            "id": name,
            "name": name,
            "status": status,
            "access": {"server": target, "ready": ready, "methods": methods},
            "OS-EXT-STS:task_state": task,
            "OS-EXT-STS:power_state": 1 if status in ("ACTIVE", "RESCUE") else 4,
            "flavor": {"ram": spec["memory"], "vcpus": spec["cpus"]},
            "addresses": {"fabric": [{"addr": spec["ip"]}]},
        }

    def act(self, name, action, options=None):
        options = options or {}

        def run(task, function):
            started = time.monotonic()
            outcome = None
            error = None
            try:
                outcome = function()
            except Exception as exception:
                error = repr(exception)
            finally:
                record(
                    {
                        "event": "task_done",
                        "server": name,
                        "task": task,
                        "status": self.status(name),
                        "outcome": outcome,
                        "error": error,
                        "elapsed_s": round(time.monotonic() - started, 3),
                    }
                )
                with self.lock:
                    self.tasks.pop(name, None)

        status = self.status(name)
        plan = {
            "os-stop": ("powering-off", lambda: lab.acpi_stop(name, timeout=60), status == "ACTIVE"),
            "os-start": ("powering-on", lambda: lab.start([name]), status == "SHUTOFF"),
            "rebuild": ("rebuilding", lambda: company_instances.rebuild(name), status == "SHUTOFF"),
            "rescue": ("rescuing", lambda: (lab.rescue_start(name), self.rescued.add(name)), status == "SHUTOFF"),
            "unrescue": (
                "unrescuing",
                lambda: (
                    lab.rescue_stop(name),
                    self.rescued.discard(name),
                    lab.start([name]) if options.get("start", True) else None,
                ),
                status == "RESCUE",
            ),
        }
        if action not in plan:
            return 501, {"badRequest": {"message": f"{action} is not supported by this provider subset"}}
        task, function, allowed = plan[action]
        with self.lock:
            if name in self.tasks:
                return 409, {"conflictingRequest": {"message": f"server is {self.tasks[name]}"}}
            if not allowed:
                return 409, {"conflictingRequest": {"message": f"cannot {action} while {status}"}}
            self.tasks[name] = task
        record({"event": "task_start", "server": name, "task": task})
        threading.Thread(target=run, args=(task, function), daemon=True).start()
        return 202, None


def record(entry):
    with LOG.open("a") as output:
        output.write(json.dumps({"time": world_now(), **entry}) + "\n")


def handler(provider):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def reply(self, code, body=None, headers=None):
            data = json.dumps(body).encode() if body is not None else b""
            self.send_response(code)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def admit(self):
            client = self.headers.get("X-Client", "unknown")
            wait = provider.bucket.take()
            if wait:
                provider.account(client, "429")
                self.reply(
                    429,
                    {"overLimit": {"message": "company gateway rate limit", "retryAfter": wait}},
                    {"Retry-After": str(max(1, math.ceil(wait)))},
                )
                return None
            provider.account(client, "ok")
            return client

        def do_GET(self):
            if self.admit() is None:
                return
            url = urlparse(self.path)
            parts = url.path.strip("/").split("/")
            world = lab.world_policy.config()
            names = sorted(
                list(world["memory_mib"]) + list(company_instances.load())
                if world
                else (name for name in lab.VMS if not lab.VMS[name].get("rescue_of"))
            )
            if parts[:3] == ["v2.1", "servers", "detail"]:
                query = parse_qs(url.query)
                limit = int(query.get("limit", ["1000"])[0])
                marker = query.get("marker", [None])[0]
                start = names.index(marker) + 1 if marker in names else 0
                page = names[start : start + limit]
                body = {"servers": [provider.server(n) for n in page]}
                if start + limit < len(names):
                    body["servers_links"] = [
                        {"rel": "next", "href": f"/v2.1/servers/detail?limit={limit}&marker={page[-1]}"}
                    ]
                return self.reply(200, body)
            if parts[:2] == ["v2.1", "servers"] and len(parts) == 3 and parts[2] in names:
                return self.reply(200, {"server": provider.server(parts[2])})
            if parts == ["v2.1", "templates", "default"]:
                return self.reply(200, company_instances.template())
            if parts == ["v2.1", "limits"]:
                return self.reply(
                    200,
                    {
                        "temporary_worker_slots": 1,
                        "shared_by": ["recovery-01", "us-work-04", "eu-work-04", "eu-work-05"],
                        "includes_rescue": True,
                        "guest_allocation_plus_reserve_mib": 30720,
                        "world_limit_gib": 30,
                    },
                )
            if parts == ["v2.1", "stats"]:
                return self.reply(200, provider.counts)
            return self.reply(404, {"itemNotFound": {"message": "not found"}})

        def do_POST(self):
            import world_run_guard

            try:
                world_run_guard.require_open()
            except RuntimeError as e:
                return self.reply(409, {"error": str(e)})
            if self.admit() is None:
                return
            parts = urlparse(self.path).path.strip("/").split("/")
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            world = lab.world_policy.config()
            allowed = list(world["memory_mib"]) + list(company_instances.load()) if world else lab.VMS
            if parts == ["v2.1", "templates", "default"]:
                try:
                    return self.reply(200, company_instances.publish(body))
                except Exception as e:
                    return self.reply(400, {"error": str(e)})
            if parts == ["v2.1", "instances"]:
                try:
                    return self.reply(201, company_instances.create(body["name"]))
                except Exception as e:
                    return self.reply(409, {"error": str(e)})
            if len(parts) == 4 and parts[:2] == ["v2.1", "instances"] and parts[3] == "delete":
                try:
                    return self.reply(200, company_instances.delete(parts[2]))
                except Exception as e:
                    return self.reply(409, {"error": str(e)})
            if len(parts) == 4 and parts[:2] == ["v2.1", "servers"] and parts[3] == "action" and parts[2] in allowed:
                code, payload = provider.act(parts[2], next(iter(body), ""), next(iter(body.values()), None))
                return self.reply(code, payload)
            return self.reply(501, {"badRequest": {"message": "not supported by this provider subset"}})

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=28301)
    parser.add_argument("--rate", type=float, default=10.0)
    parser.add_argument("--burst", type=int, default=20)
    args = parser.parse_args()
    provider = Provider(args.rate, args.burst)
    if lab.WORLD_ACTIVE:

        def checkpoint(*_):
            with provider.lock, provider.bucket.lock:
                if provider.tasks:
                    state = dict(error="Provider tasks active", tasks=provider.tasks)
                else:
                    state = dict(
                        tokens=provider.bucket.tokens,
                        rescued=sorted(provider.rescued),
                        counts=provider.counts,
                        world_time=world_now(),
                        host_epoch=time.time(),
                    )
                (
                    lab.ARTIFACTS / lab.world_policy.config()["artifact_subdir"] / "provider-state-native.json"
                ).write_text(json.dumps(state, indent=2) + "\n")

        signal.signal(signal.SIGUSR1, checkpoint)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler(provider))
    server.daemon_threads = True
    record({"event": "provider_start", "rate": args.rate, "burst": args.burst})
    server.serve_forever()


if __name__ == "__main__":
    main()
