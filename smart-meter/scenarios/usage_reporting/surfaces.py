#!/usr/bin/env python3
"""Install company source, monitoring and reporting on empty stores."""

import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "world"))
import lab
import world_integrate
import world_ops
import world_policy
from build_context import OUT, note, put, require_build
from repository import repository

NAMES = list(require_build()["memory_mib"])
S = "eu-work-03"
P = "eu-parent-01"
L = "eu-work-02"


def service(n, label, command, extra="", user="ubuntu"):
    text = (
        "[Unit]\nAfter=network-online.target\n[Service]\nUser="
        + user
        + "\n"
        + extra
        + "ExecStart="
        + command
        + "\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n"
    )
    put(n, label + ".service", text, "/etc/systemd/system/" + label + ".service")


def main():
    require_build()
    assert Path(sys.argv[0]).is_absolute() and lab.owned(os.getpid(), "python")
    assert lab.pid_of(lab.VM / "qualification.pid") == os.getppid() and lab.owned(os.getppid(), "run_step.py"), (
        "Require the owning build step runner"
    )
    if not (OUT / "surface-binaries-complete.json").exists():
        for n in [S, P, L]:
            lab.ssh(
                n,
                "sudo -n mkdir -p /var/lib/company/bin /var/lib/company/artifact-store /opt/company; sudo -n chown -R ubuntu:ubuntu /var/lib/company",
            )
        for n, asset in [
            (S, "gitea-1.19.4-linux-amd64"),
            (P, "prometheus-2.44.0.linux-amd64.tar.gz"),
            (L, "loki-linux-amd64.zip"),
        ]:
            lab.push(n, lab.ROOT / "stage/sources" / asset, "/var/lib/company/" + asset)
        lab.ssh(S, "sudo -n install -m0755 /var/lib/company/gitea-1.19.4-linux-amd64 /var/lib/company/bin/gitea")
        lab.ssh(
            P,
            "sudo -n tar -C /var/lib/company/bin --strip-components=1 -xzf /var/lib/company/prometheus-2.44.0.linux-amd64.tar.gz",
        )
        lab.ssh(
            L,
            "sudo -n python3 -m zipfile -e /var/lib/company/loki-linux-amd64.zip /var/lib/company/bin; sudo -n chmod 0755 /var/lib/company/bin/loki-linux-amd64",
        )
        rows = {
            n: dict(path=p, sha256=lab.ssh(n, "sha256sum " + p).split()[0])
            for n, p in [
                (S, "/var/lib/company/bin/gitea"),
                (P, "/var/lib/company/bin/prometheus"),
                (L, "/var/lib/company/bin/loki-linux-amd64"),
            ]
        }
        (OUT / "surface-binaries-complete.json").write_text(json.dumps(rows, indent=2) + "\n")
        note("surface-binaries-installed", hosts=list(rows))
    else:
        for n, row in json.loads((OUT / "surface-binaries-complete.json").read_text()).items():
            assert lab.ssh(n, "sha256sum " + row["path"]).split()[0] == row["sha256"]
    g = (Path(__file__).with_name("gitea.ini")).read_text().replace("10.72.0.20", "10.72.0.23")
    put(S, "gitea.ini", g, "/var/lib/company/gitea.ini")
    loki = (Path(__file__).with_name("loki.yml")).read_text().replace("10.72.0.23", "10.72.0.22")
    put(L, "loki.yml", loki, "/var/lib/company/loki.yml")
    targets = (
        [lab.VMS[n]["ip"] + ":9100" for n in NAMES]
        + [lab.VMS[n]["ip"] + ":9080" for n in NAMES]
        + ["10.72.0.20:9090", "10.72.0.22:3100", "10.72.0.23:3000"]
    )
    put(
        P,
        "prometheus.yml",
        "global:\n  scrape_interval: 5s\n  evaluation_interval: 5s\nscrape_configs:\n  - job_name: operations\n    static_configs:\n      - targets: "
        + repr(targets)
        + "\n",
        "/var/lib/company/prometheus.yml",
    )
    service(
        S,
        "company-gitea",
        "/var/lib/company/bin/gitea web --config /var/lib/company/gitea.ini",
        "Environment=GITEA_WORK_DIR=/var/lib/company\nEnvironment=GOMEMLIMIT=96MiB\n",
    )
    service(
        S,
        "company-artifact-store",
        "/usr/bin/python3 -m http.server 8091 --bind 10.72.0.23 --directory /var/lib/company/artifact-store",
    )
    service(
        P,
        "company-prometheus",
        "/var/lib/company/bin/prometheus --config.file=/var/lib/company/prometheus.yml --storage.tsdb.path=/var/lib/company/prometheus --storage.tsdb.retention.time=7d --web.listen-address=10.72.0.20:9090",
    )
    service(L, "company-loki", "/var/lib/company/bin/loki-linux-amd64 -config.file=/var/lib/company/loki.yml")
    for n, units in [(S, "company-gitea company-artifact-store"), (P, "company-prometheus"), (L, "company-loki")]:
        lab.ssh(
            n,
            "sudo -n chown -R ubuntu:ubuntu /var/lib/company; sudo -n systemctl daemon-reload; sudo -n systemctl enable --now "
            + units,
            timeout=90,
        )
    # Publisher, source and installer objects; receipts and manifests stay out of the store.
    cfg = world_policy.config()
    public = []
    copied = set()
    for r in cfg["objects"]:
        base = Path(r["name"]).name
        if base in ["RECEIPT.json", "MANIFEST.json", "complete.json"] or base.endswith("verification.txt"):
            continue
        p = Path(r["former_host_path"])
        if base == "writer.py":
            p = OUT / "eu-writer.py"
        if base == "Gateway.java":
            p = lab.ROOT / "workloads/java-gateway/Gateway.java"
        digest = hashlib.file_digest(p.open("rb"), "sha256").hexdigest()
        size = p.stat().st_size
        if base not in ["writer.py", "Gateway.java"]:
            assert digest == r["sha256"] and size == r["bytes"], r["name"]
        if digest not in copied:
            lab.push(S, p, "/var/lib/company/artifact-store/" + digest, "0444")
            copied.add(digest)
        public.append(dict(name=r["name"], sha256=digest, bytes=size))
    put(S, "catalog.json", json.dumps(public, indent=2) + "\n", "/var/lib/company/artifact-store/catalog.json")
    (OUT / "company-source-catalog.json").write_text(json.dumps(public, indent=2) + "\n")
    repository()
    note("source-repository-published")
    world, here = lab.ROOT, Path(__file__).parent
    for source, dest in [
        (world / "company_mqtt_latest.py", "services/company_mqtt_latest.py"),
        (world / "company_backfill.py", "services/backfill.py"),
        (world / "company_telemetry_io.py", "services/telemetry_io.py"),
        (world / "company_segment_stream.py", "services/segment_stream.py"),
        (here / "bootstrap.py", "fleet/bootstrap.py"),
        (here / "node-setup.sh", "fleet/node-setup.sh"),
        (here / "schedule.py", "fleet/schedule.py"),
        (here / "rollout.py", "fleet/rollout.py"),
        (here / "package.py", "fleet/package.py"),
        (world / "company_fleet.py", "fleet/controller.py"),
    ]:
        lab.push(S, source, "/var/lib/company/platform-source/" + dest)
    lab.ssh(
        S,
        'sudo -n chown -R ubuntu:ubuntu /var/lib/company/platform-source; git -C /var/lib/company/platform-source add services fleet; git -C /var/lib/company/platform-source commit -m "Publish runtime provisioning and operator interfaces"; git -C /var/lib/company/platform-source push origin HEAD:main',
        timeout=90,
    )
    with ThreadPoolExecutor(4) as pool:
        list(pool.map(world_ops.collector, NAMES))
    import maintenance

    maintenance.phase("monitoring-initial")
    with ThreadPoolExecutor(2) as pool:
        list(pool.map(world_integrate.install_parent, ["us", "eu"]))
    note("surfaces-started", source_objects=len(public))
    (OUT / "surfaces-complete.json").write_text(
        json.dumps(dict(host_epoch=time.time(), source_objects=len(public))) + "\n"
    )


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        (OUT / ("surfaces-error-" + str(time.time_ns()) + ".json")).write_text(
            json.dumps(dict(error=repr(e), host_epoch=time.time())) + "\n"
        )
        raise
