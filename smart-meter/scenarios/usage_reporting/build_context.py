"""Build namespace and helpers shared by build steps; importing this file performs no world writes."""

import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "world"))
import lab
import world_policy

OUT = lab.ARTIFACTS / world_policy.config().get("artifact_subdir", "unselected")


def require_build():
    config = world_policy.config()
    namespace = config.get("disk_subdir")
    if not config.get("active") or namespace != "build" or config.get("artifact_subdir") != namespace:
        raise RuntimeError("Select a fresh isolated build configuration in a released world slot")
    expected = {name: str(lab.VM / namespace / (name + ".qcow2")) for name in config["memory_mib"]}
    if config.get("fronts") != expected:
        raise RuntimeError("Every company front must be in the new namespace")
    if config.get("private_client_disk") != str(lab.VM / namespace / "ops-helper-01.qcow2"):
        raise RuntimeError("The build needs its own new private client disk")
    return config


def note(event, **data):
    require_build()
    row = dict(data)
    row.update(utc=datetime.now(UTC).isoformat(), host_epoch=data.get("host_epoch", time.time()), phase=event)
    with (OUT / "construction-events.jsonl").open("a") as stream:
        stream.write(json.dumps(row) + "\n")
    with (PROJECT / "world/artifacts/status.log").open("a") as stream:
        stream.write(row["utc"] + " build " + event + " " + json.dumps(data, separators=(",", ":")) + "\n")


def put(host, label, content, destination, mode="0644"):
    path = OUT / label
    path.write_text(content)
    lab.push(host, path, destination, mode)
