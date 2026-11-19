#!/usr/bin/env python3
"""Install each published package release once its wave reaches this host."""

import json
import os
import pathlib
import socket
import subprocess
import time
import urllib.request

URL = "http://10.72.0.23:3000/operations/platform/raw/branch/main/fleet/promotions.json"
CACHE = pathlib.Path("/var/cache/company/releases")


def field(deb, name):
    return subprocess.check_output(["dpkg-deb", "-f", str(deb), name], text=True).strip()


def installed(package):
    result = subprocess.run(["dpkg-query", "-W", "-f=${Version}", package], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


with urllib.request.urlopen(URL, timeout=30) as response:
    releases = json.load(response).get("releases", [])
host = socket.gethostname()
for release in releases:
    due = release.get("hosts", {}).get(host)
    if due is None or time.time() < due:
        continue
    root = CACHE / release["name"]
    debs = sorted(root.glob("*.deb"))
    if all(installed(field(deb, "Package")) == field(deb, "Version") for deb in debs):
        continue
    subprocess.run(["sha256sum", "--check", "--strict", "SHA256SUMS"], cwd=root, check=True)
    print("installing release", release["name"], flush=True)
    subprocess.run(
        ["apt-get", "install", "-y", *map(str, debs)],
        check=True,
        env={**os.environ, "DEBIAN_FRONTEND": "noninteractive"},
    )
