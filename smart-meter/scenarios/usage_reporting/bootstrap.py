#!/usr/bin/env python3
"""Install a worker from the company's complete pinned offline installer catalog."""

import argparse
import hashlib
import json
import pathlib
import subprocess
import tarfile
import urllib.request

p = argparse.ArgumentParser()
p.add_argument("--application", action="store_true")
p.add_argument("--catalog", default="http://10.72.0.23:8091")
a = p.parse_args()
root = pathlib.Path("/var/cache/company/bootstrap")
root.mkdir(parents=True, exist_ok=True)
with urllib.request.urlopen(a.catalog + "/catalog.json") as r:
    catalog = {row["name"]: row for row in json.load(r)}


def digest(path):
    with path.open("rb") as f:
        value = hashlib.sha256()
        while chunk := f.read(1048576):
            value.update(chunk)
    return value.hexdigest()


def fetch(name, dest):
    row = catalog[name]
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size == row["bytes"]:
        if digest(dest) == row["sha256"]:
            return dest
    with urllib.request.urlopen(a.catalog + "/" + row["sha256"], timeout=60) as r, dest.open("wb") as f:
        while chunk := r.read(1048576):
            f.write(chunk)
    assert dest.stat().st_size == row["bytes"] and digest(dest) == row["sha256"], name
    return dest


names = [
    "containerd-1.6.20-linux-amd64.tar.gz",
    "runc.amd64",
    "cni-plugins-linux-amd64-v1.2.0.tgz",
    "crictl-v1.26.1-linux-amd64.tar.gz",
    "kubeadm",
    "kubelet",
    "kubectl",
]
for n in names:
    fetch("stage/" + n, root / n)
(root / "SHA256SUMS").write_text("".join(catalog["stage/" + n]["sha256"] + "  " + n + "\n" for n in names))
for prefix in ["stage/systemd-pin/", "stage/systemd-10/", "stage/netplan-0.105/", "stage/preflight-reused/"]:
    for name in catalog:
        if name.startswith(prefix) and (
            prefix == "stage/preflight-reused/" or name.endswith(".deb") or name.endswith("/SHA256SUMS")
        ):
            fetch(name, root / pathlib.Path(name).relative_to("stage"))
subprocess.run(["/bin/bash", "/opt/company/node-setup.sh"], check=True)
for image in ["pause", "kube-proxy"]:
    f = fetch("images/" + image + ".tar", root / (image + ".tar"))
    subprocess.run(["/usr/local/bin/ctr", "-n", "k8s.io", "images", "import", str(f)], check=True)
for image in ["cilium", "operator-generic"]:
    prefix = "external-inputs/image-cache/" + image + "-1.13.2/"
    dest = root / (image + "-1.13.2")
    for name in catalog:
        if name.startswith(prefix) and not name.endswith("complete.json"):
            fetch(name, dest / pathlib.Path(name).relative_to(prefix))
    tar = root / (image + ".tar")
    with tarfile.open(tar, "w") as archive:
        for leaf in ["index.json", "oci-layout", "blobs"]:
            archive.add(dest / leaf, arcname=leaf)
    subprocess.run(
        [
            "/usr/local/bin/ctr",
            "-n",
            "k8s.io",
            "images",
            "import",
            "--base-name",
            "quay.io/cilium/" + image,
            "--platform",
            "linux/amd64",
            "--digests",
            str(tar),
        ],
        check=True,
    )
print("Pinned worker runtime and local images installed; reboot before native kubeadm join.", flush=True)

if a.application:
    import shutil
    import zipfile

    defaults = fetch(
        "packages/image-os-defaults/image-os-defaults_1.2-1_all.deb",
        pathlib.Path("/var/cache/company/image-runtime/image-os-defaults.deb"),
    )
    subprocess.run(["dpkg", "-i", str(defaults)], check=True)
    for name in ["kafka_2.13-3.4.0.tgz", "apache-zookeeper-3.7.1-bin.tar.gz"]:
        f = fetch("stage/" + name, root / name)
        subprocess.run(["tar", "-C", "/opt", "-xzf", str(f)], check=True)
    f = fetch("images/temurin.tar", root / "temurin.tar")
    subprocess.run(["/usr/local/bin/ctr", "-n", "k8s.io", "images", "import", str(f)], check=True)
    for d in ["kafka", "zk"]:
        pathlib.Path("/var/lib/telemetry/" + d).mkdir(parents=True, exist_ok=True)
    dest = pathlib.Path("/var/cache/company/releases/current")
    dest.mkdir(parents=True, exist_ok=True)
    for family in ["systemd-10", "netplan-0.105"]:
        for f in (root / family).glob("*.deb"):
            shutil.copy2(f, dest / f.name)
    (dest / "SHA256SUMS").write_text(
        "".join(hashlib.sha256(f.read_bytes()).hexdigest() + "  " + f.name + "\n" for f in sorted(dest.glob("*.deb")))
    )
    for source, target in [
        ("schedule.py", "/opt/company/schedule.py"),
        ("maintain.py", "/opt/company/maintain.py"),
        ("package.py", "/opt/company/package.py"),
        ("profile.py", "/opt/company/profile.py"),
    ]:
        with urllib.request.urlopen(
            "http://10.72.0.23:3000/operations/platform/raw/branch/main/fleet/" + source, timeout=30
        ) as r:
            pathlib.Path(target).write_bytes(r.read())
    print("Kafka/ZooKeeper distributions and matching runtime image installed; data directories are empty.", flush=True)
    import socket

    artifact = fetch("stage/sources/promtail-linux-amd64.zip", root / "promtail.zip")
    binpath = pathlib.Path("/var/lib/company/bin")
    binpath.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(artifact) as z:
        z.extractall(binpath)
    (binpath / "promtail-linux-amd64").chmod(0o755)
    with urllib.request.urlopen(
        "http://10.72.0.23:3000/operations/host-observability/raw/branch/main/host_metrics.py", timeout=30
    ) as r:
        pathlib.Path("/opt/company/host_metrics.py").write_bytes(r.read())
    host = socket.gethostname()
    region = host[:2]
    config = (
        "server:\n  http_listen_port: 9080\n  grpc_listen_port: 0\npositions:\n  filename: /var/lib/company/promtail-positions.yml\nclients:\n  - url: http://10.72.0.22:3100/loki/api/v1/push\nscrape_configs:\n  - job_name: system\n    journal:\n      path: /var/log/journal\n      max_age: 168h\n      labels: {job: system, host: "
        + host
        + ", region: "
        + region
        + "}\n    relabel_configs:\n      - source_labels: [__journal__systemd_unit]\n        target_label: unit\n"
    )
    if region == "eu":
        config += (
            "  - job_name: pods\n    static_configs:\n      - targets: [localhost]\n        labels:\n          job: pods\n          host: "
            + host
            + "\n          region: eu\n          __path__: /var/log/pods/*/*/*.log\n"
        )
    pathlib.Path("/var/lib/company/promtail.yml").write_text(config)
    for label, command in [
        ("company-host-metrics", "/usr/bin/python3 /opt/company/host_metrics.py"),
        ("company-promtail", "/var/lib/company/bin/promtail-linux-amd64 -config.file=/var/lib/company/promtail.yml"),
    ]:
        pathlib.Path("/etc/systemd/system/" + label + ".service").write_text(
            "[Unit]\nAfter=network-online.target\n[Service]\nExecStart="
            + command
            + "\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n"
        )
    subprocess.run(["timedatectl", "set-timezone", "America/New_York" if region == "us" else "UTC"], check=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", "company-host-metrics", "company-promtail"], check=True)
