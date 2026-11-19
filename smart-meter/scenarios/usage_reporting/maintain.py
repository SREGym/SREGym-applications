"""Apply image runtime defaults and retain the host inventory."""

import json
import os
import pathlib
import subprocess
import time

p = pathlib.Path("/var/lib/company/operations")
p.mkdir(parents=True, exist_ok=True)
v = os.statvfs("/var/lib")
x = dict(
    at=time.time(),
    free_bytes=v.f_bavail * v.f_frsize,
    packages=subprocess.check_output(["dpkg-query", "-W", "-f=${Package} ${Version}\n"], text=True).splitlines(),
)
t = p / "inventory.tmp"
t.write_text(json.dumps(x))
t.replace(p / "inventory.json")
subprocess.run(["dpkg", "-i", "/var/cache/company/image-runtime/image-os-defaults.deb"], check=True)
