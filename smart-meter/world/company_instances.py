"""Local provider implementation of the company's small recovery-instance quota."""

import json
import shlex
import subprocess
import urllib.request
import uuid
from pathlib import Path

import lab
import world_policy
import yaml

REGISTRY = lab.ROOT / "public-instances.json"
REPO = "/var/lib/company/platform-source"


def registry():
    return json.loads(REGISTRY.read_text()) if REGISTRY.exists() else {}


def load():
    rows = registry()
    lab.VMS.update(rows)
    return rows


def template():
    with urllib.request.urlopen(
        "http://127.0.0.1:28330/operations/platform/raw/branch/main/fleet/image.json", timeout=15
    ) as r:
        return json.load(r)


def publish(value):
    assert set(value) == {"cloud_config"} and set(value["cloud_config"]) <= {"write_files", "runcmd"}, (
        "Supported template fields: cloud_config.write_files and runcmd"
    )
    data = json.dumps(value, indent=2) + "\n"
    assert len(data) < 65536
    lab.ssh(
        "eu-work-03",
        "python3 -c "
        + shlex.quote(
            "from pathlib import Path;Path(" + repr(REPO + "/fleet/image.json") + ").write_text(" + repr(data) + ")"
        )
        + "; git -C "
        + REPO
        + " add fleet/image.json; git -C "
        + REPO
        + ' commit -m "Update instance initialization definition"; git -C '
        + REPO
        + " push origin HEAD:main",
        timeout=90,
    )
    return template()


def disk_root():
    return lab.VM / world_policy.config()["disk_subdir"]


def create(name):
    assert name == "recovery-01", "Recovery quota permits the named recovery-01 instance only"
    rows = load()
    assert not rows, "One recovery instance quota; delete the old recovery instance first"
    spec = dict(
        disk=str(disk_root() / (name + ".qcow2")),
        memory=2048,
        cpus=1,
        ssh=22501,
        ip="10.72.0.31",
        n=0x31,
        fwd=[],
        public_recovery=True,
    )
    lab.VMS[name] = spec
    definition = template()
    disk = disk_root() / (name + ".qcow2")
    assert not disk.exists()
    lab.known_hosts_path(name).unlink(missing_ok=True)
    subprocess.run(
        ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(lab.BASE_IMAGE), str(disk), "8G"],
        check=True,
    )
    lab.write_seed(name)
    seed = lab.seed_directory(name) / "user-data"
    base = yaml.safe_load(seed.read_text())
    base["runcmd"] = [x for x in base.get("runcmd", []) if "/var/cache/company/bootstrap" not in str(x)]
    for key, value in definition["cloud_config"].items():
        base.setdefault(key, []).extend(value)
    seed.write_text("#cloud-config\n" + yaml.safe_dump(base, sort_keys=False))
    REGISTRY.write_text(json.dumps({name: spec}, indent=2) + "\n")
    lab.start_qemu(name, lab.qemu_command(name, disk))
    lab.wait_ssh(name)
    lab.ssh(name, "sudo -n cloud-init status --wait", timeout=150)
    return dict(
        server=name,
        status="ACTIVE",
        ram=2048,
        local_store="Deleted with the instance; operator-created uploads are not retained after deletion.",
    )


def delete(name):
    rows = load()
    assert name in rows, "Only a recovery instance can be deleted by this operation"
    lab.stop_pid(lab.VM / (name + ".pid"), "qemu-system-x86_64", 10)
    disk = Path(rows[name]["disk"])
    assert disk.parent == disk_root()
    disk.unlink()
    rows.pop(name)
    REGISTRY.write_text(json.dumps(rows) + "\n")
    lab.VMS.pop(name, None)
    return dict(deleted=name, local_store_deleted=True)


def rebuild(name):
    """Replace a stopped company server's local root from the published initialization template."""
    cfg = world_policy.config()
    assert name in cfg["memory_mib"], "Company server required"
    assert not lab.pid_of(lab.VM / (name + ".pid")), "Stop the instance before replacing its local store"
    definition = template()
    disk = Path(cfg["fronts"][name])
    assert disk.parent == disk_root()
    directory = disk_root() / (name + "-provision-" + uuid.uuid4().hex)
    directory.mkdir()
    public_key = (lab.VM / "id_ed25519.pub").read_text().strip()
    data = dict(
        package_update=False,
        package_upgrade=False,
        ssh_pwauth=False,
        ssh_authorized_keys=[public_key],
        write_files=yaml.safe_load(lab.QUIET_SUDO),
        runcmd=[
            ["systemctl", "mask", "--now", "apt-daily.timer", "apt-daily-upgrade.timer", "unattended-upgrades.service"]
        ],
    )
    data["write_files"].append(
        dict(
            path="/etc/systemd/system/serial-getty@ttyS0.service.d/override.conf",
            content="[Service]\nExecStart=\nExecStart=-/sbin/agetty --autologin ubuntu --noclear %I $TERM\n",
        )
    )
    data["runcmd"].append(["systemctl", "daemon-reload"])
    data["runcmd"].append(["systemctl", "restart", "serial-getty@ttyS0"])
    for key, values in definition["cloud_config"].items():
        data.setdefault(key, []).extend(values)
    (directory / "user-data").write_text("#cloud-config\n" + yaml.safe_dump(data, sort_keys=False))
    (directory / "meta-data").write_text(
        "instance-id: company-" + uuid.uuid4().hex + "\nlocal-hostname: " + name + "\n"
    )
    network = {
        "version": 2,
        "ethernets": {
            "fabric0": {
                "match": {"macaddress": lab.mac(name, 1)},
                "addresses": [lab.VMS[name]["ip"] + "/24"],
                "dhcp4": False,
                "dhcp6": False,
                "accept-ra": False,
                "routes": [{"to": "default", "via": "10.72.0.254"}],
            }
        },
    }
    (directory / "network-config").write_text(yaml.safe_dump(network, sort_keys=False))
    iso = directory / "seed.iso"
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
            *[str(directory / n) for n in ["user-data", "meta-data", "network-config"]],
        ],
        check=True,
    )
    iso.chmod(0o444)
    # Rebuild deletes the server's local store.
    disk.unlink()
    subprocess.run(
        ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(lab.BASE_IMAGE), str(disk), "20G"],
        check=True,
    )
    cfg.setdefault("seeds", {})[name] = str(iso)
    world_policy.CONFIG.write_text(json.dumps(cfg, indent=2) + "\n")
    lab.known_hosts_path(name).unlink(missing_ok=True)
    lab.start_qemu(name, lab.qemu_command(name, disk))
    lab.wait_ssh(name)
    lab.ssh(name, "sudo -n cloud-init status --wait", timeout=180)
    return dict(
        server=name,
        local_store_deleted=True,
        status="ACTIVE",
        provisioning="Operating system initialized; install the published node runtime and join the appropriate regional cluster.",
    )
