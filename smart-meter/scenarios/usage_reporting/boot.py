"""Boot the company instances and the private client host."""

import hashlib
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from build_context import OUT, lab, note, require_build, world_policy


def create_iso(directory, iso):
    subprocess.run(
        [
            "genisoimage",
            "-quiet",
            "-output",
            str(iso),
            "-volid",
            "cidata",
            "-joliet",
            "-rock",
            *map(str, sorted(directory.iterdir())),
        ],
        check=True,
    )
    iso.chmod(0o444)


FLEET = [
    "fleet/capacity.py",
    "fleet/generations/g-burst.yaml",
    "fleet/generations/g-control.yaml",
    "fleet/generations/g-elastic.yaml",
    "fleet/generations/g-ops.yaml",
    "fleet/generations/g-steady.yaml",
    "fleet/overrides/hosts/eu-work-02.yaml",
    "fleet/overrides/regions/eu.yaml",
    "fleet/overrides/regions/us.yaml",
    "fleet/pools.yaml",
    "fleet/resolve.py",
]


def role_files():
    here = Path(__file__).resolve().parent

    def block(path, source, mode):
        body = "".join("      " + line + "\n" for line in (here / source).read_text().splitlines())
        return "  - path: " + path + '\n    permissions: "' + mode + '"\n    content: |\n' + body

    bootstrap = "/var/lib/company/platform-role/bootstrap/"
    return block("/opt/company/platform-role.sh", "platform-role.sh", "0755") + "".join(
        block(bootstrap + name, name, "0755" if name.endswith(".py") else "0644") for name in FLEET
    )


def company_seed(name, cfg):
    directory = Path(cfg["seed_dirs"][name])
    iso = lab.VM / cfg["disk_subdir"] / (name + "-seed.iso")
    if iso.exists():
        return str(iso)
    assert not directory.exists(), "Preserve incomplete seed and resume explicitly: " + name
    directory.mkdir()
    (directory / "meta-data").write_text(
        "instance-id: company-" + uuid.uuid4().hex + "\nlocal-hostname: " + name + "\n"
    )
    (directory / "user-data").write_text(
        "#cloud-config\npackage_update: false\npackage_upgrade: false\nssh_pwauth: false\n"
        "ssh_authorized_keys:\n  - " + (lab.VM / "id_ed25519.pub").read_text().strip() + "\n"
        "write_files:\n"
        + lab.QUIET_SUDO
        + "  - path: /etc/default/kubelet\n    content: KUBELET_EXTRA_ARGS=--node-ip="
        + lab.VMS[name]["ip"]
        + "\n"
        + role_files()
        + "runcmd:\n  - [systemctl, mask, --now, apt-daily.timer, apt-daily-upgrade.timer, unattended-upgrades.service]\n"
        "  - [bash, /opt/company/platform-role.sh]\n"
    )
    (directory / "network-config").write_text(
        'version: 2\nethernets:\n  fabric0:\n    match: {macaddress: "' + lab.mac(name, 1) + '"}\n'
        "    addresses: [" + lab.VMS[name]["ip"] + "/24]\n    dhcp4: false\n    dhcp6: false\n"
        "    accept-ra: false\n    routes: [{to: default, via: 10.72.0.254}]\n"
    )
    create_iso(directory, iso)
    return str(iso)


def private_seed(cfg):
    name = "ops-helper-01"
    directory = Path(cfg["seed_dirs"][name])
    iso = lab.VM / cfg["disk_subdir"] / (name + "-seed.iso")
    if iso.exists():
        return str(iso)
    assert not directory.exists(), "Preserve incomplete private seed and resume explicitly"
    directory.mkdir()
    (directory / "meta-data").write_text(
        "instance-id: private-" + uuid.uuid4().hex + "\nlocal-hostname: " + name + "\n"
    )
    (directory / "user-data").write_text(
        "#cloud-config\npackage_update: false\npackage_upgrade: false\nssh_pwauth: false\n"
        "ssh_authorized_keys:\n  - " + (lab.VM / "id_ed25519.pub").read_text().strip() + "\n"
        "write_files:\n" + lab.QUIET_SUDO + "  - path: /etc/netplan/60-fabric.yaml\n    content: |\n"
        "      network:\n        version: 2\n        ethernets:\n          fabric0:\n"
        '            match: {macaddress: "' + lab.mac(name, 1) + '"}\n'
        "            addresses: [10.72.0.30/24]\n            dhcp4: false\n"
        "  - path: /etc/ssh/sshd_config.d/40-company-relay.conf\n    content: GatewayPorts clientspecified\n"
        "runcmd:\n  - [systemctl, mask, --now, apt-daily.timer, apt-daily-upgrade.timer, unattended-upgrades.service]\n"
        "  - [netplan, apply]\n  - [systemctl, reload, ssh]\n"
    )
    create_iso(directory, iso)
    return str(iso)


def main():
    cfg = require_build()
    if (OUT / "boot-complete.json").exists():
        assert all(lab.pid_of(lab.VM / (name + ".pid")) for name in list(cfg["memory_mib"]) + ["ops-helper-01"])
        return
    assert not lab.pid_of(lab.VM / "qualification.pid")
    (lab.VM / "qualification.pid").write_text(str(os.getpid()))
    try:
        installation = json.loads((OUT / "installation-root-complete.json").read_text())
        template = Path(installation["root"])
        assert template.is_file() and not (template.stat().st_mode & 0o222)
        assert not lab.pid_of(lab.VM / "install-image-01.pid")
        lab.ensure_infrastructure()
        cfg["single_fabric"] = True
        cfg["construction_phase"] = "regional-installation"
        cfg["construction_seed_names"] = []
        cfg["seeds"] = {name: company_seed(name, cfg) for name in cfg["memory_mib"]}
        cfg["private_client_seed_iso"] = private_seed(cfg)
        world_policy.CONFIG.write_text(json.dumps(cfg, indent=2) + "\n")
        note(
            "seeds-created",
            company_hosts=len(cfg["memory_mib"]),
            seeds_sha256={
                name: hashlib.sha256(Path(path).read_bytes()).hexdigest() for name, path in cfg["seeds"].items()
            },
        )
        helper = "ops-helper-01"
        disk = Path(cfg["private_client_disk"])
        if not disk.exists():
            subprocess.run(
                ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(lab.BASE_IMAGE), str(disk), "8G"],
                check=True,
            )
        if not lab.pid_of(lab.VM / (helper + ".pid")):
            lab.start_qemu(helper, lab.qemu_command(helper, disk))
        lab.wait_ssh(helper, 180)
        lab.ssh(helper, "sudo -n cloud-init status --wait", timeout=120)
        import world_snapshot

        world_snapshot.tunnels_start()
        for name, disk_name in cfg["fronts"].items():
            disk = Path(disk_name)
            if not disk.exists():
                subprocess.run(
                    ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(template), str(disk)],
                    check=True,
                )
            if not lab.pid_of(lab.VM / (name + ".pid")):
                lab.start_qemu(name, lab.qemu_command(name, disk))
        with ThreadPoolExecutor(8) as pool:
            list(pool.map(lambda name: lab.wait_ssh(name, 240), cfg["memory_mib"]))
        readiness = {}
        for name in cfg["memory_mib"]:
            readiness[name] = lab.ssh(
                name,
                "sudo -n cloud-init status --wait; hostname; systemctl is-active systemd-networkd containerd; "
                "test -x /usr/local/bin/kubeadm; test -x /usr/bin/git; date -u +%s.%N",
                timeout=120,
            )
        (OUT / "instance-readiness.json").write_text(json.dumps(readiness, indent=2) + "\n")
        (OUT / "boot-complete.json").write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
        note(
            "instances-booted",
            company_guest_mib=sum(cfg["memory_mib"].values()),
            private_client_mib=cfg["private_client_mib"],
        )
    finally:
        if (lab.VM / "qualification.pid").read_text() == str(os.getpid()):
            (lab.VM / "qualification.pid").unlink()


if __name__ == "__main__":
    main()
