#!/usr/bin/env python3
"""Install the platform role and package channel timers from the platform repository."""

import pathlib
import socket
import subprocess
import urllib.request
import zlib

ROOT = "http://10.72.0.23:3000/operations/platform/raw/branch/main/"
UNITS = pathlib.Path("/etc/systemd/system")


def read(name):
    with urllib.request.urlopen(ROOT + name, timeout=30) as response:
        return response.read()


base = pathlib.Path("/opt/company")
base.mkdir(parents=True, exist_ok=True)
(base / "platform-role.sh").write_bytes(read("roles/platform/role.sh"))
(base / "platform-role.sh").chmod(0o755)
(base / "package.py").write_bytes(read("fleet/package.py"))

# Spread role runs across the half hour so hosts do not converge at once.
offset = zlib.crc32(socket.gethostname().encode()) % 1800
(UNITS / "company-role.service").write_text(
    "[Unit]\nDescription=Platform role\nAfter=network-online.target\n"
    "[Service]\nType=oneshot\n"
    "Environment=PLATFORM_REPO_URL=http://10.72.0.23:3000/operations/platform.git\n"
    "ExecStart=/bin/bash /opt/company/platform-role.sh\n"
)
(UNITS / "company-role.timer").write_text(
    "[Unit]\nDescription=Platform role every 30 minutes\n"
    f"[Timer]\nOnCalendar=*:{offset // 60:02d}/30:{offset % 60:02d}\nAccuracySec=1s\n"
    "[Install]\nWantedBy=timers.target\n"
)
(UNITS / "company-package.service").write_text(
    "[Unit]\nDescription=Package channel\nAfter=network-online.target\n"
    "[Service]\nType=oneshot\nExecStart=/usr/bin/python3 /opt/company/package.py\n"
)
(UNITS / "company-package.timer").write_text(
    "[Unit]\nDescription=Package channel every 5 minutes\n"
    "[Timer]\nOnCalendar=*:0/5\nRandomizedDelaySec=60\nAccuracySec=1s\n"
    "[Install]\nWantedBy=timers.target\n"
)
subprocess.run(["systemctl", "daemon-reload"], check=True)
timers = ["company-role.timer", "company-package.timer"]
if socket.gethostname().endswith("-parent-01"):
    # The capacity controller runs on each region's control plane from the role's checkout.
    checkout = "/var/lib/company/platform-role/checkout/fleet"
    (UNITS / "company-capacity.service").write_text(
        "[Unit]\nDescription=Capacity controller\nAfter=network-online.target company-role.service\n"
        "[Service]\nType=oneshot\nExecStart=/usr/bin/python3 " + checkout + "/capacity.py " + checkout + "\n"
    )
    (UNITS / "company-capacity.timer").write_text(
        "[Unit]\nDescription=Capacity controller every 30 minutes\n"
        "[Timer]\nOnCalendar=*:15/30\nAccuracySec=1s\n[Install]\nWantedBy=timers.target\n"
    )
    timers.append("company-capacity.timer")
subprocess.run(["systemctl", "daemon-reload"], check=True)
subprocess.run(["systemctl", "enable", "--now", *timers], check=True)
subprocess.run(["systemctl", "list-timers", "company-*", "--no-pager"], check=True)
