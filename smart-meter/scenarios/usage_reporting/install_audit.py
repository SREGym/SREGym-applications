"""Install the isolated artifact-cache validation network."""

import subprocess
from pathlib import Path

units = {
    "company-artifact-bus.service": """[Unit]
Description=Artifact validation control bus
[Service]
User=systemd-network
Group=systemd-network
RuntimeDirectory=company-artifact-bus
ExecStart=/usr/bin/dbus-daemon --session --nofork --nopidfile --address=unix:path=/run/company-artifact-bus/socket
ExecStartPost=/usr/bin/python3 /opt/company/artifact_audit.py bus-ready
""",
    "company-artifact-network.service": """[Unit]
Description=Artifact validation network
After=network.target
Before=company-artifact-link.service company-artifact-cache.service
[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/bin/python3 /opt/company/artifact_audit.py setup
[Install]
WantedBy=multi-user.target
""",
    "company-artifact-link.service": """[Unit]
Description=Artifact validation link configuration
Requires=company-artifact-network.service company-artifact-bus.service
After=company-artifact-network.service company-artifact-bus.service
[Service]
Type=notify
NotifyAccess=all
PrivateMounts=yes
NetworkNamespacePath=/run/netns/artifact-audit
RuntimeDirectory=company-artifact-netif
BindPaths=/run/company-artifact-netif:/run/systemd/netif
Environment=DBUS_SYSTEM_BUS_ADDRESS=unix:path=/run/company-artifact-bus/socket
ExecStart=/usr/bin/python3 /opt/company/artifact_audit.py networkd
Restart=on-failure
RestartSec=3
""",
    "company-artifact-cache.service": """[Unit]
Description=Artifact validation cache endpoint
Requires=company-artifact-network.service
After=company-artifact-network.service
[Service]
ExecStart=/usr/bin/python3 /opt/company/artifact_audit.py serve
Restart=on-failure
RestartSec=3
""",
    "company-artifact-check.service": """[Unit]
Description=Runtime artifact validation
Requires=company-artifact-link.service company-artifact-cache.service
After=company-artifact-link.service company-artifact-cache.service
[Service]
Type=oneshot
ExecStartPre=/usr/bin/systemctl restart company-artifact-link.service
ExecStart=/usr/bin/ip netns exec artifact-audit /usr/bin/python3 /opt/company/artifact_audit.py check
""",
    "company-artifact-check.timer": """[Unit]
Description=Periodic runtime artifact validation
[Timer]
OnBootSec=90s
OnUnitActiveSec=5min
AccuracySec=1s
[Install]
WantedBy=timers.target
""",
}
for name, content in units.items():
    Path("/etc/systemd/system", name).write_text(content)
subprocess.run(["systemctl", "daemon-reload"], check=True)
subprocess.run(
    ["systemctl", "enable", "--now", "company-artifact-network.service", "company-artifact-check.timer"], check=True
)
subprocess.run(["systemctl", "start", "company-artifact-check.service"], check=True)
