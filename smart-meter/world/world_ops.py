#!/usr/bin/env python3
"""Native log and metric collectors and the systemd unit helper."""

from pathlib import Path

import lab
import world_policy
from world_boot import put


def unit(n, label, command, extra=""):
    put(
        n,
        n + "-" + label + ".service",
        "[Unit]\nAfter=network-online.target\n[Service]\n"
        + extra
        + "ExecStart="
        + command
        + "\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n",
        "/etc/systemd/system/" + label + ".service",
    )


def collector(n):
    cfg = world_policy.config()
    row = next(r for r in cfg["objects"] if Path(r["name"]).name == "promtail-linux-amd64.zip")
    lab.push(n, Path(row["former_host_path"]), "/var/lib/company/promtail-linux-amd64.zip")
    lab.ssh(
        n,
        "sudo -n mkdir -p /var/lib/company/bin /opt/company; sudo -n python3 -m zipfile -e /var/lib/company/promtail-linux-amd64.zip /var/lib/company/bin; sudo -n chmod 0755 /var/lib/company/bin/promtail-linux-amd64",
    )
    lab.push(n, lab.ROOT / "company_host_metrics.py", "/opt/company/host_metrics.py")
    unit(n, "company-host-metrics", "/usr/bin/python3 /opt/company/host_metrics.py")
    journal = """  - job_name: system
    journal:
      path: /var/log/journal
      max_age: 168h
      labels: {job: system, host: HOST, region: REGION}
    relabel_configs:
      - source_labels: [__journal__systemd_unit]
        target_label: unit
""".replace("HOST", n).replace("REGION", n[:2])
    pods = """  - job_name: pods
    static_configs:
      - targets: [localhost]
        labels:
          job: pods
          host: HOST
          region: eu
          __path__: /var/log/pods/*/*/*.log
""".replace("HOST", n)
    text = (
        "server:\n  http_listen_port: 9080\n  grpc_listen_port: 0\npositions:\n  filename: /var/lib/company/promtail-positions.yml\nclients:\n  - url: http://10.72.0.22:3100/loki/api/v1/push\nscrape_configs:\n"
        + journal
        + (pods if n.startswith("eu-") else "")
    )
    put(n, n + "-promtail.yml", text, "/var/lib/company/promtail.yml")
    unit(n, "company-promtail", "/var/lib/company/bin/promtail-linux-amd64 -config.file=/var/lib/company/promtail.yml")
    lab.ssh(
        n,
        "sudo -n timedatectl set-timezone "
        + ("America/New_York" if n.startswith("us-") else "UTC")
        + "; sudo -n systemctl daemon-reload; sudo -n systemctl enable --now company-host-metrics company-promtail; sudo -n systemctl restart company-promtail",
        timeout=120,
    )
