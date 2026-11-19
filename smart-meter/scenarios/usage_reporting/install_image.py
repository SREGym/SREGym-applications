#!/usr/bin/env python3
"""Create an offline installation image from the publisher base image."""

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "world"))
import lab
import world_policy
from build_context import OUT, note, require_build

N = "install-image-01"
V = lab.VM / world_policy.config()["disk_subdir"]
DISK = V / "installer-template.qcow2"
STAGE = lab.ROOT / "stage"


def push(p, dest):
    lab.push(N, p, dest)


def command(s, timeout=180):
    return lab.ssh(N, s, timeout=timeout)


def main():
    require_build()
    assert not DISK.exists(), "The installer disk must be new"
    assert Path(sys.argv[0]).is_absolute() and lab.owned(os.getpid(), "python")
    assert not lab.pid_of(lab.VM / "qualification.pid")
    (lab.VM / "qualification.pid").write_text(str(os.getpid()))
    assert not any(lab.pid_of(p) and lab.owned(lab.pid_of(p), "qemu-system-x86_64") for p in lab.VM.glob("*.pid"))
    with lab.BASE_IMAGE.open("rb") as source:
        assert hashlib.file_digest(source, "sha256").hexdigest() == lab.BASE_SHA256
    note("base-image-verified", sha256=lab.BASE_SHA256)
    lab.ensure_infrastructure()
    lab.VMS[N] = dict(memory=3072, cpus=2, ssh=22561, ip=None, n=0x61, fwd=[])
    subprocess.run(
        ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(lab.BASE_IMAGE), str(DISK), "20G"],
        check=True,
    )
    lab.write_seed(N)
    lab.start_qemu(N, lab.qemu_command(N, DISK))
    lab.wait_ssh(N, 180)
    command("sudo -n cloud-init status --wait", 180)
    note("install-image-booted", root=str(DISK))
    manifest = json.loads((STAGE / "MANIFEST.json").read_text())
    names = [
        "containerd-1.6.20-linux-amd64.tar.gz",
        "runc.amd64",
        "cni-plugins-linux-amd64-v1.2.0.tgz",
        "crictl-v1.26.1-linux-amd64.tar.gz",
        "kubeadm",
        "kubelet",
        "kubectl",
    ]
    sums = []
    for name in names:
        p = STAGE / name
        digest = hashlib.file_digest(p.open("rb"), "sha256").hexdigest()
        assert digest == manifest["files"][name]["sha256"]
        push(p, "/var/cache/company/bootstrap/" + name)
        sums.append(digest + "  " + name + "\n")
    path = OUT / "node-installer-SHA256SUMS"
    path.write_text("".join(sums))
    push(path, "/var/cache/company/bootstrap/SHA256SUMS")
    for directory in ["systemd-pin", "netplan-0.105", "preflight-reused"]:
        for p in sorted((STAGE / directory).iterdir()):
            if p.is_file() and (p.suffix == ".deb" or p.name == "SHA256SUMS"):
                push(p, "/var/cache/company/bootstrap/" + directory + "/" + p.name)
    push(Path(__file__).with_name("node-setup.sh"), "/opt/company/node-setup.sh")
    command("sudo -n bash /opt/company/node-setup.sh", 600)
    # Source and installer inventories are part of provisioning.
    (OUT / "installation-inventory.txt").write_text(
        command(
            "dpkg-query -W -f='${binary:Package}\t${Version}\t${Architecture}\n'; command -v git; uname -r; sudo -n swapon --show",
            30,
        )
    )
    note("node-packages-installed", systemd="249.11-0ubuntu3.9")
    for p in sorted((lab.ROOT / "images").glob("*.tar")):
        row = manifest["images"][p.stem]
        assert hashlib.file_digest(p.open("rb"), "sha256").hexdigest() == row["archive_sha256"]
        with p.open("rb") as f:
            lab.ssh(N, "sudo -n ctr -n k8s.io images import -", stdin=f, timeout=240)
    import cluster

    cluster.import_cilium(N)
    note("runtime-images-installed")
    for name in [
        "kafka_2.13-3.4.0.tgz",
        "apache-zookeeper-3.7.1-bin.tar.gz",
        "kafka_python-2.0.2-py2.py3-none-any.whl",
        "paho-mqtt-1.6.1.tar.gz",
        "pg8000-1.29.8-py3-none-any.whl",
        "scramp-1.4.4-py3-none-any.whl",
        "asn1crypto-1.5.1-py2.py3-none-any.whl",
        "python_dateutil-2.8.2-py2.py3-none-any.whl",
        "six-1.16.0-py2.py3-none-any.whl",
    ]:
        p = STAGE / name
        assert hashlib.file_digest(p.open("rb"), "sha256").hexdigest() == manifest["files"][name]["sha256"]
        push(p, "/var/cache/company/bootstrap/" + name)
    jdk = next((STAGE / "temurin-jdk-17.0.8_7").glob("*.tar.gz"))
    assert (
        hashlib.file_digest(jdk.open("rb"), "sha256").hexdigest()
        == "aa5fc7d388fe544e5d85902e68399d5299e931f9b280d358a3cbee218d6017b0"
    )
    push(jdk, "/var/cache/company/bootstrap/" + jdk.name)
    for p in (STAGE / "paho-java-1.2.5").glob("*.jar"):
        push(p, "/var/cache/company/bootstrap/" + p.name)
    for p, d in [
        (lab.ROOT / "workloads/gateway.py", "/opt/gateway/gateway.py"),
        (lab.ROOT / "workloads/java-gateway/Gateway.java", "/opt/gateway-java/src/Gateway.java"),
        (lab.ROOT / "workloads/java-gateway/GatewayKeyTest.java", "/opt/gateway-java/src/GatewayKeyTest.java"),
        (lab.ROOT / "company_reporting.py", "/opt/company/reporting.py"),
        (lab.ROOT / "company_mqtt_latest.py", "/opt/company/company_mqtt_latest.py"),
    ]:
        push(p, d)
    script = """set -eu
cd /var/cache/company/bootstrap
mkdir -p /opt/jdk /opt/gateway/lib /opt/gateway-java/classes /opt/gateway-java/lib /opt/writer/lib /opt/company/lib
for a in kafka_2.13-3.4.0.tgz apache-zookeeper-3.7.1-bin.tar.gz; do tar -C /opt -xzf "$a"; done
tar -C /opt/jdk --strip-components=1 -xzf OpenJDK17U-jdk_x64_linux_hotspot_17.0.8_7.tar.gz
for a in *.whl; do python3 -m zipfile -e "$a" /opt/company/lib; done
cp -r /opt/company/lib/* /opt/writer/lib/
cp -r /opt/company/lib/kafka /opt/gateway/lib/
mkdir -p /var/cache/company/bootstrap/paho-python
tar -C /var/cache/company/bootstrap/paho-python --strip-components=1 -xzf paho-mqtt-1.6.1.tar.gz
cp -r /var/cache/company/bootstrap/paho-python/src/paho /opt/gateway/lib/
cp /opt/kafka_2.13-3.4.0/libs/*.jar /opt/gateway-java/lib/
cp /var/cache/company/bootstrap/org.eclipse.paho.client.mqttv3-1.2.5.jar /opt/gateway-java/lib/
/opt/jdk/bin/javac -J-Xmx128m --release 17 -cp '/opt/gateway-java/lib/*' -d /opt/gateway-java/classes /opt/gateway-java/src/Gateway.java
/opt/jdk/bin/java -version
"""
    p = OUT / "install-applications.sh"
    p.write_text(script)
    push(p, "/opt/company/install-applications.sh")
    command("sudo -n bash /opt/company/install-applications.sh", 300)
    # The image has no network-policy initializer; cloud-init configures
    # first-boot networking from the instance seed.
    files = {
        "/etc/systemd/system/serial-getty@ttyS0.service.d/override.conf": "[Service]\nExecStart=\nExecStart=-/sbin/agetty --autologin ubuntu --noclear %I $TERM\n"
    }
    for target, text in files.items():
        p = OUT / Path(target).name
        p.write_text(text)
        push(p, target)
    command("sudo -n cloud-init clean; sudo -n truncate -s0 /etc/machine-id; sudo -n systemctl poweroff", 30)
    deadline = time.monotonic() + 120
    while lab.pid_of(lab.VM / (N + ".pid")):
        assert time.monotonic() < deadline, "Image shutdown did not complete"
        time.sleep(1)
    DISK.chmod(0o400)
    result = dict(host_epoch=time.time(), root=str(DISK), base_sha256=lab.BASE_SHA256)
    (OUT / "installation-root-complete.json").write_text(json.dumps(result, indent=2) + "\n")
    note("installation-image-saved", **result)
    (lab.VM / "qualification.pid").unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        (OUT / "installation-error.json").write_text(json.dumps(dict(error=repr(e), host_epoch=time.time())) + "\n")
        raise
