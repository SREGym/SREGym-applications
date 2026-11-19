"""The active world's configuration (world-config.json) and its guest memory sizes."""

import json
from pathlib import Path

CONFIG = Path(__file__).resolve().parent / "world-config.json"


def config():
    return json.loads(CONFIG.read_text()) if CONFIG.exists() else None


def apply(vms):
    data = config()
    if not data or not data.get("active"):
        return False
    for n, m in data["memory_mib"].items():
        if n not in vms:
            vms[n] = dict(data["instance_specs"][n])
        vms[n]["memory"] = m
    vms["ops-helper-01"]["memory"] = data["private_client_mib"]
    registry = CONFIG.parent / "public-instances.json"
    if registry.exists():
        vms.update(json.loads(registry.read_text()))
    return True
