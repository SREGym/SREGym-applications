#!/usr/bin/env python3
"""Resolve one host's fleet facts: generation defaults, then region overrides, then host overrides."""

import sys
from pathlib import Path

import yaml


def merge(base, extra):
    out = dict(base)
    for key, value in (extra or {}).items():
        out[key] = merge(out.get(key) or {}, value) if isinstance(value, dict) else value
    return out


def facts(root, host):
    root = Path(root)
    pools = yaml.safe_load((root / "pools.yaml").read_text())["pools"]
    generation = next(p["generation"] for p in pools.values() if host in (p.get("hosts") or []))
    result = yaml.safe_load((root / "generations" / (generation + ".yaml")).read_text()) or {}
    region = root / "overrides" / "regions" / (host[:2] + ".yaml")
    if region.exists():
        result = merge(result, (yaml.safe_load(region.read_text()) or {}).get("generations", {}).get(generation))
    own = root / "overrides" / "hosts" / (host + ".yaml")
    if own.exists():
        result = merge(result, yaml.safe_load(own.read_text()))
    return dict(result, generation=generation)


if __name__ == "__main__":
    value = facts(sys.argv[1], sys.argv[2])
    key = sys.argv[3] if len(sys.argv) > 3 else None
    if key:
        for part in key.split("."):
            value = value.get(part) if isinstance(value, dict) else None
    print(value)
