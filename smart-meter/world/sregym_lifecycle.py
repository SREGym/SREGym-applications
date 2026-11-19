"""Host-side lifecycle of a revived world for SREGym's Smart Meter application; never run by an agent.

SREGym owns the run. This helper readies, freezes and tears down the world with
the world's own QMP and process-family implementation.

  python3 sregym_lifecycle.py ready|freeze|teardown --project SMART_METER_DIR --run RUN_DIR
"""

from __future__ import annotations

import argparse
import concurrent.futures
import importlib
import json
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

# Customer endpoints whose service the grader judges at the freeze.
HEALTH_ENDPOINTS = {
    "eu-health": "http://127.0.0.1:28310/health",
    "us-health": "http://127.0.0.1:28320/health",
    "eu-live": "http://127.0.0.1:28310/api/live",
    "us-live": "http://127.0.0.1:28320/api/live",
}

# Company endpoints on the agent's tools page.
AGENT_PORTS = (28310, 28320, 28311, 28330, 28390, 28391, 28392)


def write(path, value):
    path = Path(path)
    temporary = path.with_suffix(".publishing")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def modules(project):
    sys.path.insert(0, str(Path(project) / "world"))
    return tuple(importlib.import_module(name) for name in ("lab", "revive", "world_policy", "world_run_guard"))


def require_owner(run, policy):
    config = policy.config() or {}
    if Path(config.get("probe_run", "/")) != Path(run):
        raise RuntimeError("Company world ownership changed; refusing to operate on another run")
    return config


def ready(project, run):
    lab, revive, policy, _guard = modules(project)
    config = require_owner(run, policy)
    inventory = subprocess.check_output([str(project / "world/companyctl"), "inventory"], text=True, timeout=30)
    rows = json.loads(inventory)["servers"]
    names = {row["name"] for row in rows}
    expected = {
        f"{region}-{role}" for region in ("eu", "us") for role in ("parent-01", "work-01", "work-02", "work-03")
    }
    if not expected <= names:
        raise RuntimeError("Company inventory is incomplete: " + ", ".join(sorted(expected - names)))
    # The agent's endpoints listen on host loopback and on the agent address revive was given.
    for address in ["127.0.0.1"] + ([config["agent_address"]] if config.get("agent_address") else []):
        for port in AGENT_PORTS:
            with socket.create_connection((address, port), timeout=5):
                pass
    saved = json.loads((run / "prepared.json").read_text())
    for name in saved["live"]:
        status = revive.result(name, "query-status")
        if not status["running"]:
            raise RuntimeError(f"Saved-live guest {name} is not running")
    start_resource_guard(lab, run)
    write(run / "readiness.json", {"inventory": sorted(names), "saved_live": saved["live"], "ready": True})


def resource_path(lab):
    config = lab.ROOT / "world-config.json"
    subdir = json.loads(config.read_text()).get("artifact_subdir", "runs") if config.exists() else "runs"
    return lab.ARTIFACTS / subdir / "resource-current.json"


def start_resource_guard(lab, run):
    """Arm the bundle's resource watcher after revive and before T0."""
    started = time.time()
    lab.start_background(
        "sregym-world-guard",
        lab.VM / "world-guard-sregym.pid",
        [sys.executable, str(lab.ROOT / "world_guard.py")],
        "sregym-world-guard.log",
    )
    end = time.monotonic() + 10
    resource = resource_path(lab)
    while time.monotonic() < end:
        pid = lab.pid_of(lab.VM / "world-guard-sregym.pid")
        if not pid or not lab.owned(pid, "world_guard.py"):
            raise RuntimeError("Host resource watcher did not remain running")
        try:
            row = json.loads(resource.read_text())
            if row["epoch"] >= started:
                if row.get("stop_reasons"):
                    raise RuntimeError("Host resource preflight failed: " + ", ".join(row["stop_reasons"]))
                if not row.get("CPU_PSI_and_load_are_telemetry_only"):
                    raise RuntimeError("Resource watcher must treat CPU pressure and load as telemetry")
                write(run / "resource-guard.json", {"pid": pid, "started_host_epoch": started, "initial_sample": row})
                return
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        time.sleep(0.05)
    raise TimeoutError("Host resource watcher produced no fresh initial sample")


def resource_problem(lab):
    pid = lab.pid_of(lab.VM / "world-guard-sregym.pid")
    if not pid or not lab.owned(pid, "world_guard.py"):
        return "Host resource watcher exited"
    path = resource_path(lab)
    try:
        row = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        # A sample may be mid-rewrite, but it must not stay missing or unreadable.
        try:
            age = time.time() - path.stat().st_mtime
        except FileNotFoundError:
            age = float("inf")
        return "Host resource watcher stopped reporting" if age > 10 else None
    if row.get("stop_reasons"):
        return "Host resource limit: " + ", ".join(row["stop_reasons"])
    if time.time() - row["epoch"] > 10:
        return "Host resource watcher stopped reporting"
    return None


def freeze(project, run):
    lab, revive, policy, guard = modules(project)
    cfg = require_owner(run, policy)
    issue = resource_problem(lab)
    if issue:
        # A host out of memory or disk says nothing about the agent; the oracle reports a harness failure.
        write(run / "abort.json", {"reason": issue, "host_epoch": time.time()})
        return
    guard.close()  # the world's tools accept no new action from here on
    probes = start_endpoint_probes()
    end = time.monotonic() + 6
    while len(probes["endpoints"]) < len(HEALTH_ENDPOINTS) and time.monotonic() < end:
        time.sleep(0.05)
    record_endpoint_probes(run, probes)
    record_guest_clock(lab, run)
    target = time.monotonic()
    cutoff = {"target_monotonic": target, "cutoff_begin_monotonic": target, "host_epoch": time.time(), "guests": {}}
    families = {}
    errors = []
    for label, expected in (("world-tools", "world_tools.py"), ("provider", "provider.py")):
        pid = lab.pid_of(lab.VM / (label + ".pid"))
        if pid and lab.owned(pid, expected):
            try:
                families[label] = guard.freeze_family(pid, expected)
            except Exception as exc:
                errors.append(f"{label}: {exc}")
    names = [p.stem for p in lab.VM.glob("*.pid") if (pid := lab.pid_of(p)) and lab.owned(pid, "qemu-system-x86_64")]

    def pause(name):
        result = revive.result(name, "stop")
        status = revive.result(name, "query-status")
        if status["status"] != "paused":
            raise RuntimeError(f"{name} did not pause: {status}")
        return name, {"at_monotonic": time.monotonic(), "result": result, "status": status}

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(pause, name): name for name in names}
        for future, name in futures.items():
            try:
                key, value = future.result()
                cutoff["guests"][key] = value
            except Exception as exc:
                errors.append(f"{name}: {exc}")
    cutoff["freeze_completed_monotonic"] = time.monotonic()
    cutoff["host_processes_stopped"] = families
    cutoff["errors"] = errors
    write(run / "native-cutoff.json", cutoff)
    if errors:
        raise RuntimeError("Incomplete freeze: " + "; ".join(errors))
    capsule = run / "end-state"
    capsule.mkdir(mode=0o700)
    (capsule / "ram").mkdir()
    (capsule / "disks").mkdir()
    # A paused QEMU exports native RAM/device state without advancing guests.
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda n: revive.export_native(n, capsule / "ram" / (n + ".gz")), names))
    for name in names:
        lab.stop_pid(lab.VM / (name + ".pid"), "qemu-system-x86_64", 2)
    public = json.loads((lab.ROOT / "public-instances.json").read_text())
    disks = {**cfg["fronts"], "ops-helper-01": cfg["private_client_disk"], **{n: v["disk"] for n, v in public.items()}}
    for name in names:
        if name.endswith("-" + lab.RESCUE_MARK):
            disks[name] = str(lab.VM / cfg["disk_subdir"] / (name + ".qcow2"))
    retained = {}
    for name, source in disks.items():
        source, target = Path(source), capsule / "disks" / (name + ".qcow2")
        if not source.exists():
            continue  # Agent-deleted instances may no longer have a disk.
        if source.is_relative_to(run):
            source.rename(target)
        else:
            subprocess.run(["cp", "--reflink=auto", "--sparse=always", str(source), str(target)], check=True)
        target.chmod(0o400)
        retained[name] = str(target)
    manifest = {
        "live": names,
        "allnames": list(retained),
        "disks": retained,
        "ram": {n: str(capsule / "ram" / (n + ".gz")) for n in names},
        "cutoff": cutoff,
        "world_config": cfg,
        "public_instances": public,
        "console_pending": {p.name: p.read_text() for p in lab.VM.glob("*.console-pending")},
        "run_identity": run.name,
        "no_post_deadline_actions_credited": True,
    }
    write(capsule / "config.json", cfg)
    write(capsule / "manifest.json", manifest)
    write(run / "native-capture-complete.json", {"capture": str(capsule), "completed_monotonic": time.monotonic()})


def teardown(project, run):
    lab, revive, policy, guard = modules(project)
    require_owner(run, policy)
    lab.stop_pid(lab.VM / "world-guard-sregym.pid", "world_guard.py", 2)
    cutoff = run / "native-cutoff.json"
    if cutoff.exists():
        for family in json.loads(cutoff.read_text()).get("host_processes_stopped", {}).values():
            guard.stop_frozen_family(family)
    revive.stop_owned()
    # These belong to the world infrastructure started by revive. Never stop
    # services in a shared slice or use name-wide pkill/systemctl commands.
    # The seed server runs as company_seed_http.py or http.server, whichever revive started.
    for label, expected in (("switch", "switch.py"), ("seed", "company_seed_http.py"), ("seed", "http.server")):
        pid = lab.pid_of(lab.VM / (label + ".pid"))
        if pid and lab.owned(pid, expected):
            lab.stop_pid(lab.VM / (label + ".pid"), expected, 2)
    # Everything above works from the pid files, so a world whose run folder was deleted
    # (to free disk) is still stopped. Only the record needs the folder.
    if run.is_dir():
        write(run / "teardown.json", {"completed_monotonic": time.monotonic(), "saved_capture_retained": True})


def probe_endpoint(label, url, answers):
    began = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=4) as response:
            body = response.read(400).decode("utf-8", "replace")
            answers[label] = {"status": response.status, "seconds": time.monotonic() - began, "body": body}
    except Exception as exc:
        answers[label] = {
            "status": getattr(exc, "code", None),
            "seconds": time.monotonic() - began,
            "error": repr(exc)[:300],
        }


def start_endpoint_probes():
    """Probe every customer endpoint concurrently; answers arrive in the returned record."""
    record = {"begin_host_epoch": time.time(), "begin_monotonic": time.monotonic(), "endpoints": {}}
    for label, url in HEALTH_ENDPOINTS.items():
        threading.Thread(target=probe_endpoint, args=(label, url, record["endpoints"]), daemon=True).start()
    return record


def record_endpoint_probes(run, record):
    """Write the answers received by now; an endpoint still silent at the freeze is not serving."""
    endpoints = dict(record["endpoints"])
    for label in HEALTH_ENDPOINTS:
        endpoints.setdefault(label, {"status": None, "error": "No response before the freeze"})
    write(run / "service-health-at-D.json", {**record, "endpoints": endpoints, "completed_monotonic": time.monotonic()})


def capture(project, run):
    """Freeze and capture the world; a failure is recorded with the run."""
    try:
        freeze(project, run)
    except Exception as exc:
        write(run / "capture-error.json", {"error": f"{type(exc).__name__}: {exc}"})
        raise


def record_guest_clock(lab, run):
    """Read the private guest clock once, bracketed by host monotonic readings."""
    try:
        before = time.monotonic_ns()
        native = int(lab.ssh("ops-helper-01", "date +%s%N", timeout=10).strip())
        after = time.monotonic_ns()
    except Exception:
        return  # Missing clock bounds remain ungraded in the grader.
    row = {
        "rows": [
            {
                "private_clock": [{"native_epoch_ns": native}],
                "host_before": {"host_monotonic_ns": before},
                "host_after": {"host_monotonic_ns": after},
            }
        ]
    }
    with (run / "guest-health.jsonl").open("a") as stream:
        stream.write(json.dumps(row) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("ready", "freeze", "teardown"))
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--run", type=Path, required=True)
    args = parser.parse_args()
    {"ready": ready, "freeze": capture, "teardown": teardown}[args.action](args.project, args.run)
