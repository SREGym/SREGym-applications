#!/usr/bin/env python3
"""Fleet connectivity repair with regional budgets, a mass-failure hold and retained-node cooldowns."""

import json
import os
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path

TOOLS = "http://10.72.0.30:28880"
STATE = Path("/var/lib/company/fleet-state.json")
# Wait before the one retained-worker retry; site policy can shorten it.
RETRY_COOLDOWN = float(os.environ.get("FLEET_RETRY_COOLDOWN_S", "21600"))
# Mass-failure guard: while this many workers of a region are already being repaired,
# further unreachable workers are held as they are for operator review, not stopped.
MAX_UNHEALTHY = int(os.environ.get("FLEET_MAX_UNHEALTHY", "1"))
# A held worker is released only after this many successful probes in a row,
# so one stray answer does not return it to automatic repair.
RELEASE_PROBES = int(os.environ.get("FLEET_RELEASE_PROBES", "3"))
NODES = {
    f"{r}-{role}": ip
    for r, ips in [
        ("us", ["10.72.0.10", "10.72.0.11", "10.72.0.12", "10.72.0.13"]),
        ("eu", ["10.72.0.20", "10.72.0.21", "10.72.0.22", "10.72.0.23"]),
    ]
    for role, ip in zip(["parent-01", "work-01", "work-02", "work-03"], ips, strict=False)
}


def api(path, body=None):
    req = urllib.request.Request(
        TOOLS + "/provider" + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-Client": "fleet-repair"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read()
        return json.loads(body) if body else {}


def reach(ip):
    try:
        s = socket.create_connection((ip, 22), 1)
        s.close()
        return True
    except OSError:
        return False


def majority():
    good = 0
    for i, ip in enumerate(["10.72.0.11", "10.72.0.12", "10.72.0.13"], 1):
        try:
            with socket.create_connection((ip, 32180 + i), 1) as s:
                s.settimeout(1)
                s.sendall(b"srvr")
                data = b""
                while len(data) < 16384:
                    part = s.recv(min(4096, 16384 - len(data)))
                    if not part:
                        break
                    data += part
                    if b"Mode: leader" in data or b"Mode: follower" in data:
                        break
            good += b"Mode: leader" in data or b"Mode: follower" in data
        except OSError:
            pass
    return good >= 2


state = (
    json.loads(STATE.read_text())
    if STATE.exists()
    else dict(nodes={}, eu_reservation=None, us_no_majority_since=None, retained_retry_used=False, enabled=True)
)


def save():
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.replace(STATE)


def act(n, action):
    print(json.dumps(dict(time=time.time(), server=n, action=action)), flush=True)
    api("/v2.1/servers/" + n + "/action", {action: None})


while True:
    try:
        now = time.time()
        mono = time.monotonic()
        inventory = {r["id"]: r for r in api("/v2.1/servers/detail")["servers"]}
        if not state.get("enabled", True):
            time.sleep(5)
            continue
        for n, row in state["nodes"].items():
            # A terminal provider failure is not a successful retained-node retry.
            if (
                row.get("restart_requested")
                and not row.get("recovered_at")
                and inventory[n]["status"] == "SHUTOFF"
                and not inventory[n].get("OS-EXT-STS:task_state")
                and mono >= row.get("boot_grace_until_mono", float("inf"))
            ):
                row.update(restart_requested=False, quarantined=True, failed_start_observed_at=now)
                state["retained_retry_used"] = False
                print(json.dumps(dict(time=now, server=n, event="retained-start-did-not-become-active")), flush=True)
        for n, ip in NODES.items():
            server = inventory[n]
            row = state["nodes"].setdefault(n, {})
            # Record only settled states: a restore can pass through SHUTOFF between two
            # polls, so every settled move into ACTIVE starts a fresh boot grace.
            prior = row.get("last_status")
            if not server.get("OS-EXT-STS:task_state"):
                row["last_status"] = server["status"]
            if (
                server["status"] == "ACTIVE"
                and not server.get("OS-EXT-STS:task_state")
                and prior not in (None, "ACTIVE")
            ):
                row["boot_grace_until_mono"] = mono + 120
                row.pop("unreachable_since", None)
                row.pop("unreachable_since_mono", None)
            if server["status"] == "ACTIVE" and reach(ip):
                if row.get("restart_requested"):
                    row.update(recovered_at=now, restart_requested=False)
                if row.get("held"):
                    row["held_reachable_probes"] = row.get("held_reachable_probes", 0) + 1
                    if row["held_reachable_probes"] >= RELEASE_PROBES:
                        row.pop("held")
                        row.pop("held_reachable_probes")
                row.pop("unreachable_since", None)
                row.pop("unreachable_since_mono", None)
                continue
            row.pop("held_reachable_probes", None)
            if server.get("OS-EXT-STS:task_state"):
                continue
            if server["status"] == "ACTIVE":
                if mono < row.get("boot_grace_until_mono", 0):
                    continue
                row.setdefault("unreachable_since", now)
                row.setdefault("unreachable_since_mono", mono)
                if mono - row["unreachable_since_mono"] < 30:
                    continue
                row.setdefault("failure_at", now)
                row.setdefault("cooldown_until", now + RETRY_COOLDOWN)
                if n == "eu-parent-01":
                    if not state.get("eu_reservation") or now - state["eu_reservation"] > 21600:
                        state["eu_reservation"] = now
                        row["repair_due"] = now + 600
                        row["repair_due_mono"] = mono + 600
                    if row.get("repair_due_mono") and mono >= row["repair_due_mono"]:
                        act(n, "os-stop")
                        row["restart_after_stop"] = True
                        row.pop("repair_due", None)
                        row.pop("repair_due_mono", None)
                elif row.get("held"):
                    pass
                elif (
                    "-work-" in n
                    and sum(
                        1
                        for m, r in state["nodes"].items()
                        if m != n
                        and m[:2] == n[:2]
                        and "-work-" in m
                        and (r.get("held") or (r.get("quarantined") and inventory[m]["status"] != "ACTIVE"))
                    )
                    >= MAX_UNHEALTHY
                ):
                    row["held"] = True
                    print(
                        json.dumps(
                            dict(
                                time=now,
                                server=n,
                                event="held-for-review",
                                reason="regional repair limit reached",
                                limit=MAX_UNHEALTHY,
                            )
                        ),
                        flush=True,
                    )
                else:
                    act(n, "os-stop")
                    row["quarantined"] = True
            elif server["status"] == "SHUTOFF" and row.get("restart_after_stop"):
                act(n, "os-start")
                row.update(restart_after_stop=False, restart_requested=True, boot_grace_until_mono=mono + 120)
                row.pop("unreachable_since", None)
                row.pop("unreachable_since_mono", None)
        if majority():
            state["us_no_majority_since"] = None
            state["us_no_majority_since_mono"] = None
        else:
            state["us_no_majority_since"] = state.get("us_no_majority_since") or now
            state["us_no_majority_since_mono"] = state.get("us_no_majority_since_mono") or mono
            if mono - state["us_no_majority_since_mono"] >= 300 and not state["retained_retry_used"]:
                eligible = [
                    (r["failure_at"], n)
                    for n, r in state["nodes"].items()
                    if n.startswith("us-work-")
                    and r.get("quarantined")
                    and now >= r.get("cooldown_until", float("inf"))
                    and inventory[n]["status"] == "SHUTOFF"
                    and not inventory[n].get("OS-EXT-STS:task_state")
                ]
                if eligible:
                    _, n = min(eligible)
                    act(n, "os-start")
                    state["retained_retry_used"] = True
                    state["nodes"][n].update(
                        quarantined=False, restart_requested=True, boot_grace_until_mono=mono + 120
                    )
                    state["nodes"][n].pop("unreachable_since", None)
                    state["nodes"][n].pop("unreachable_since_mono", None)
        save()
    except Exception as e:
        print(json.dumps(dict(time=time.time(), error=str(e))), flush=True)
    time.sleep(5)
