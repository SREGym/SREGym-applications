"""Start the private customer report clients."""

import hashlib
import json
import time
from pathlib import Path

from build_context import OUT, lab, note, require_build


def main():
    require_build()
    marker = OUT / "customer-activity-started.json"
    if marker.exists():
        return
    path = Path(__file__).with_name("customer_activity.py")
    lab.push("ops-helper-01", path, "/opt/private/customer_activity.py", "0700")
    unit = "[Unit]\nAfter=network-online.target\n[Service]\nExecStart=/usr/bin/python3 /opt/private/customer_activity.py\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n"
    text = OUT / "private-customer-activity.service"
    text.write_text(unit)
    lab.push("ops-helper-01", text, "/etc/systemd/system/private-customer-activity.service")
    result = lab.ssh(
        "ops-helper-01",
        "sudo -n systemctl daemon-reload; sudo -n systemctl enable --now private-customer-activity; systemctl is-active private-customer-activity",
        timeout=30,
    )
    assert result.strip() == "active", result
    marker.write_text(
        json.dumps(
            dict(
                host_epoch=time.time(),
                source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                timezones=["UTC", "America/Los_Angeles", "Europe/Berlin", "Asia/Tokyo"],
                request_interval_seconds=5,
            )
        )
        + "\n"
    )
    note("customer-activity-started", interval_seconds=5)


if __name__ == "__main__":
    main()
