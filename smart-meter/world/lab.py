#!/usr/bin/env python3
"""QEMU/KVM guests of the Smart Meter world, with their userspace switch and seed service.

Guests share a userspace switch (unix datagram sockets) on 10.72.0.0/24. No host
interface, bridge, tap, Docker or privilege is used.

Every world process (guests, switch, seed service, tools) runs in the user
systemd slice `smartmeter.slice`, so its memory.current and memory.peak are the
world's footprint. Guests run with KSM merging off,
virtio-balloon free-page reporting on and oom_score_adj 1000.

  python3 lab.py start NAME... | stop NAME... | stop-all | ssh NAME [command...] | status
"""

import argparse
import contextlib
import hashlib
import json
import os
import resource
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import world_policy

ROOT = Path(__file__).resolve().parent
VM = ROOT / "vm"
SEED = VM / "seed"
ARTIFACTS = ROOT / "artifacts"
SLICE = "smartmeter.slice"
SEED_PORT = 28201
SEED_ADDR = "10.0.2.100"  # guest-side address of the seed service (user-mode forward)
SWITCH = VM / "switch.sock"
BLOCK_FILE = VM / "switch-block"
BASE_IMAGE = ROOT / "base" / "ubuntu-22.04-20220420.img"
BASE_SHA256 = "de5e632e17b8965f2baf4ea6d2b824788e154d9a65df4fd419ec4019898e15cd"
RESCUE_MARK = "rescue"

# Guest specs; memory in MiB.
VMS = {
    "us-parent-01": {"memory": 1152, "cpus": 2, "ssh": 22401, "ip": "10.72.0.10", "n": 0x10, "fwd": []},
    "us-work-01": {"memory": 1280, "cpus": 2, "ssh": 22402, "ip": "10.72.0.11", "n": 0x11, "fwd": []},
    "us-work-02": {"memory": 1024, "cpus": 2, "ssh": 22403, "ip": "10.72.0.12", "n": 0x12, "fwd": []},
    "us-work-03": {
        "memory": 1536,
        "cpus": 2,
        "ssh": 22404,
        "ip": "10.72.0.13",
        "n": 0x13,
        # Device MQTT ingress: host 127.0.0.1:21883 -> EMQX NodePort 31883.
        "fwd": [(21883, 31883)],
    },
    # EU region.
    "eu-parent-01": {"memory": 1152, "cpus": 2, "ssh": 22411, "ip": "10.72.0.20", "n": 0x20, "fwd": []},
    "eu-work-01": {
        "memory": 1280,
        "cpus": 2,
        "ssh": 22412,
        "ip": "10.72.0.21",
        "n": 0x21,
        # EU device MQTT ingress: host 127.0.0.1:21884 -> EMQX NodePort 31883.
        "fwd": [(21884, 31883)],
    },
    "eu-work-02": {"memory": 1152, "cpus": 2, "ssh": 22413, "ip": "10.72.0.22", "n": 0x22, "fwd": []},
    "eu-work-03": {"memory": 1152, "cpus": 2, "ssh": 22414, "ip": "10.72.0.23", "n": 0x23, "fwd": []},
    # Private helper that runs the device clients; not a company host.
    "ops-helper-01": {"memory": 768, "cpus": 2, "ssh": 22421, "ip": "10.72.0.30", "n": 0x30, "fwd": []},
}
WORLD_ACTIVE = world_policy.apply(VMS)
CPU_LIMIT = 10 if WORLD_ACTIVE else 6
CPU_MAX = f"{CPU_LIMIT * 100000} 100000"
QEMU_OVERHEAD_BUDGET_MIB = 32


def log(message):
    line = f"{datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')} {message}"
    print(line, flush=True)
    ARTIFACTS.mkdir(exist_ok=True)
    with (ARTIFACTS / "lab.log").open("a") as output:
        output.write(line + "\n")


def mac(name, nic):
    return f"52:54:00:72:{nic:02x}:{VMS[name]['n']:02x}"


def known_hosts_path(name):
    world = world_policy.config()
    return VM / ((world.get("known_hosts_prefix", "") if world else "") + f"known_hosts.{name}")


def rescue_name(name):
    return f"{name}-{RESCUE_MARK}"


def register_rescue(name):
    rescue = rescue_name(name)
    spec = VMS[name]
    VMS[rescue] = {**spec, "ssh": spec["ssh"] + 100, "ip": None, "n": spec["n"] + 0x40, "fwd": [], "rescue_of": name}
    return rescue


def ssh_arguments(name, timeout=5):
    return [
        "/usr/bin/ssh",
        "-i",
        str(VM / "id_ed25519"),
        "-p",
        str(VMS[name]["ssh"]),
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={timeout}",
        "-o",
        "ServerAliveInterval=5",
        "-o",
        "ServerAliveCountMax=6",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "LogLevel=ERROR",
        "-o",
        f"UserKnownHostsFile={known_hosts_path(name)}",
        "ubuntu@127.0.0.1",
    ]


def ssh(name, command, timeout=120, check=True, stdin=None):
    result = subprocess.run(ssh_arguments(name) + [command], capture_output=True, timeout=timeout, stdin=stdin)
    if check and result.returncode:
        raise RuntimeError(
            f"{name}: {command[:200]!r} exited {result.returncode}: {result.stderr.decode(errors='replace')[-2000:]}"
        )
    return result.stdout.decode(errors="replace")


def push(name, source, destination, mode="0644"):
    with open(source, "rb") as data:
        ssh(name, f"sudo -n install -D -m {mode} /dev/stdin {destination}", stdin=data, timeout=900)


def in_slice(unit, command):
    """Run command inside a transient scope of the world slice."""
    return [
        "systemd-run",
        "--user",
        "--quiet",
        "--collect",
        "--scope",
        f"--slice={SLICE}",
        f"--unit={unit}",
        "-p",
        "MemoryAccounting=yes",
    ] + command


def slice_cgroup():
    path = subprocess.check_output(
        ["systemctl", "--user", "show", "-p", "ControlGroup", "--value", SLICE], text=True
    ).strip()
    return Path("/sys/fs/cgroup" + path) if path else None


def outer_cgroup():
    """Conservative envelope including preparation cache and the local session."""
    return Path(f"/sys/fs/cgroup/user.slice/user-{os.getuid()}.slice")


def running(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def pid_of(pid_file):
    try:
        pid = int(pid_file.read_text())
    except (FileNotFoundError, ValueError):
        return None
    return pid if running(pid) else None


def owned(pid, expected):
    try:
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except (FileNotFoundError, ProcessLookupError):
        return False
    return expected in command and str(ROOT) in command


def ensure_process_limits():
    # One userspace-network socket per device plus QEMU/runtime descriptors.
    # Some hosts default to 1,024 soft descriptors despite a much higher hard
    # limit. Change only this owned process and its future children.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(65536, hard)
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))


def start_background(unit, pid_file, command, log_name):
    ensure_process_limits()
    if pid_of(pid_file):
        return
    if WORLD_ACTIVE:
        # A persistent user service is independent of tool/session lifetime.
        name = unit + "-persistent-" + str(time.time_ns())
        subprocess.run(
            [
                "systemd-run",
                "--user",
                "--collect",
                "--unit=" + name,
                "--slice=" + SLICE,
                "--property=Type=exec",
                "--property=WorkingDirectory=" + str(ROOT),
                "--property=StandardOutput=append:" + str(ARTIFACTS / log_name),
                "--property=StandardError=append:" + str(ARTIFACTS / log_name),
                *command,
            ],
            check=True,
        )
        pid = int(
            subprocess.check_output(["systemctl", "--user", "show", name + ".service", "--property=MainPID", "--value"])
        )
        if not pid or not running(pid):
            raise RuntimeError(name + " failed to remain running")
        if not owned(pid, Path(command[-1]).name if len(command) == 1 else ""):
            raise RuntimeError(name + " lacks the owned absolute invocation")
        pid_file.write_text(str(pid))
        return
    # systemd-run --scope execs the command, so the child pid is the service itself.
    with (ARTIFACTS / log_name).open("a") as output:
        process = subprocess.Popen(
            in_slice(unit, command), stdout=output, stderr=subprocess.STDOUT, start_new_session=True
        )
    time.sleep(0.5)
    if process.poll() is not None:
        raise RuntimeError(f"{unit} exited at start; see {log_name}")
    pid_file.write_text(str(process.pid))


def limit_slice():
    """Give the world's slice its limits: 10 CPUs, 28 GiB, no swap. Runtime only, so every revive sets them again."""
    subprocess.run(
        [
            "systemctl",
            "--user",
            "set-property",
            "--runtime",
            SLICE,
            "CPUQuota=1000%",
            "MemoryMax=28G",
            "MemorySwapMax=0",
        ],
        check=True,
    )


def ensure_infrastructure():
    for directory in (VM, SEED, ARTIFACTS):
        directory.mkdir(exist_ok=True)
    key = VM / "id_ed25519"
    if not key.exists():
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True)
    start_background(
        "smart-meter-switch",
        VM / "switch.pid",
        [
            sys.executable,
            str(ROOT / "switch.py"),
            str(SWITCH),
            "--block-file",
            str(BLOCK_FILE),
            "--stats",
            str(ARTIFACTS / "switch-stats.json"),
        ],
        "switch.log",
    )
    seed_world = world_policy.config() or {}
    seed_command = (
        [sys.executable, str(ROOT / "company_seed_http.py")]
        if seed_world.get("seed_service_root")
        else [sys.executable, "-m", "http.server", str(SEED_PORT), "--bind", "127.0.0.1", "--directory", str(SEED)]
    )
    start_background("smart-meter-seed", VM / "seed.pid", seed_command, "seed-server.log")
    for _ in range(100):
        if SWITCH.exists():
            return
        time.sleep(0.1)
    raise RuntimeError("switch socket did not appear")


def seed_directory(name):
    world = world_policy.config() or {}
    return Path(world.get("seed_dirs", {}).get(name, str(SEED / name)))


# A cloud-config write_files entry for every seed. The build and the run tools drive each host as
# ubuntu through sudo, and sudo writes every command line to the journal. Without this, the build's
# own commands and checks would sit in the guest journals (and Loki) for an agent to read.
QUIET_SUDO = (
    "  - path: /etc/sudoers.d/20-automation\n"
    "    permissions: '0440'\n"
    "    content: |\n"
    "      # ubuntu is the automation account; its commands are not written to syslog.\n"
    "      Defaults:ubuntu !syslog\n"
)


def write_seed(name):
    spec = VMS[name]
    directory = seed_directory(name)
    directory.mkdir(parents=True, exist_ok=True)
    instance = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    public_key = (VM / "id_ed25519.pub").read_text().strip()
    (directory / "meta-data").write_text(f"instance-id: {name}-{instance}\nlocal-hostname: {name}\n")
    if not spec["ip"]:
        (directory / "user-data").write_text(
            "#cloud-config\npackage_update: false\npackage_upgrade: false\nssh_pwauth: false\n"
            f"ssh_authorized_keys:\n  - {public_key}\n"
            "write_files:\n"
            + QUIET_SUDO
            + "runcmd:\n  - [systemctl, mask, --now, apt-daily.timer, apt-daily-upgrade.timer]\n"
        )
        return
    (directory / "user-data").write_text(
        "#cloud-config\n"
        "package_update: false\npackage_upgrade: false\nssh_pwauth: false\n"
        f"ssh_authorized_keys:\n  - {public_key}\n"
        "write_files:\n" + QUIET_SUDO + "  - path: /etc/netplan/60-fabric.yaml\n"
        "    permissions: '0600'\n"
        "    content: |\n"
        "      network:\n"
        "        version: 2\n"
        "        ethernets:\n"
        "          fabric0:\n"
        f'            match: {{macaddress: "{mac(name, 1)}"}}\n'
        f"            addresses: [{spec['ip']}/24]\n"
        "            dhcp4: false\n"
        # Under restrict=on the user-mode DHCP offers no router. A host still has a default route; it
        # points at the user-mode gateway, which forwards nothing, so egress stays closed.
        "  - path: /etc/netplan/61-uplink-route.yaml\n"
        "    permissions: '0600'\n"
        "    content: |\n"
        "      network:\n"
        "        version: 2\n"
        "        ethernets:\n"
        "          enp0s3:\n"
        "            routes: [{to: default, via: 10.0.2.2}]\n"
        "runcmd:\n"
        "  - [systemctl, mask, --now, apt-daily.timer, apt-daily-upgrade.timer, unattended-upgrades.service]\n"
        "  - [netplan, apply]\n"
        "  - [sh, -c, 'mkdir -p /var/cache/company/bootstrap/stage; touch /var/cache/company/bootstrap/guest-marker']\n"
    )


def qemu_command(name, disk):
    spec = VMS[name]
    forwards = "".join(f",hostfwd=tcp:127.0.0.1:{host}-:{guest}" for host, guest in spec["fwd"])
    (VM / f"{name}.sock").unlink(missing_ok=True)
    command = [
        "qemu-system-x86_64",
        "-name",
        f"smart-meter-{name}",
        "-enable-kvm",
        "-cpu",
        "Westmere-v2,enforce",
        "-machine",
        "pc-q35-8.2,mem-merge=off",
        "-m",
        str(spec["memory"]),
        "-smp",
        str(spec["cpus"]),
        "-display",
        "none",
        "-daemonize",
        "-pidfile",
        str(VM / f"{name}.pid"),
        "-serial",
        f"file:{ARTIFACTS / f'serial-{name}.log'}",
        "-device",
        "virtio-balloon-pci,free-page-reporting=on",
        "-drive",
        f"file={disk},if=virtio,format=qcow2,cache=none,discard=unmap",
        # restrict=on: the guest reaches nothing outside except these explicit forwards, so no guest can
        # fetch packages, snaps or images on its own. The seed service is reached per connection.
        "-netdev",
        f"user,id=net0,restrict=on,hostfwd=tcp:127.0.0.1:{spec['ssh']}-:22{forwards},"
        f"guestfwd=tcp:{SEED_ADDR}:{SEED_PORT}-cmd:nc 127.0.0.1 {SEED_PORT}",
        "-device",
        f"virtio-net-pci,netdev=net0,mac={mac(name, 0)}",
        "-smbios",
        f"type=1,serial=ds=nocloud-net;s=http://{SEED_ADDR}:{SEED_PORT}/{name}/",
    ]
    if spec["ip"]:
        command += [
            "-netdev",
            f"dgram,id=net1,local.type=unix,local.path={VM / f'{name}.sock'},remote.type=unix,remote.path={SWITCH}",
            "-device",
            f"virtio-net-pci,netdev=net1,mac={mac(name, 1)}",
        ]
    if spec.get("rescue_of"):
        command += ["-device", "pcie-root-port,id=rp1,slot=1"]
    # Every guest has a QMP socket: the provider's stop is an ACPI power-down, as Nova's clean shutdown is.
    command += ["-qmp", f"unix:{VM / f'{name}.qmp'},server=on,wait=off"]
    world = world_policy.config()
    if world and name == "ops-helper-01" and world.get("private_client_seed_iso"):
        i = command.index("-smbios")
        del command[i : i + 2]
        command += ["-drive", "file=" + world["private_client_seed_iso"] + ",media=cdrom,readonly=on"]
    if world and (ROOT / "world-clock.json").exists():
        clock = json.loads((ROOT / "world-clock.json").read_text())
        value = clock["guest_epoch"] + time.time() - clock["host_epoch"]
        # A running native world clock is authoritative after construction
        # pauses. New boots must not count time while the world was held.
        if name != "ops-helper-01" and pid_of(VM / "ops-helper-01.pid"):
            state = qmp("ops-helper-01", [{"execute": "query-status"}])[-1].get("return", {})
            if state.get("running"):
                value = float(ssh("ops-helper-01", "date +%s.%N", timeout=10).strip())
        command += [
            "-rtc",
            "base=" + datetime.fromtimestamp(value, UTC).strftime("%Y-%m-%dT%H:%M:%S") + ",clock=vm",
        ]
    if world and world.get("single_fabric") and name in world["memory_mib"]:
        result = []
        i = 0
        while i < len(command):
            key = command[i]
            if key == "-smbios":
                i += 2
                continue
            if key == "-netdev" and command[i + 1].startswith("user,id=net0,"):
                i += 2
                continue
            if key == "-device" and command[i + 1].startswith("virtio-net-pci,netdev=net0"):
                i += 2
                continue
            if key == "-device" and command[i + 1].startswith("virtio-net-pci,netdev=net1"):
                result += [key, command[i + 1] + ",addr=0x4"]
                i += 2
                continue
            if key == "-serial":
                console = VM / (name + ".console")
                console.unlink(missing_ok=True)
                result += [
                    "-chardev",
                    f"socket,id=console,path={console},server=on,wait=off,logfile={ARTIFACTS}/serial-{name}.log",
                    "-serial",
                    "chardev:console",
                ]
                i += 2
                continue
            result.append(key)
            i += 1
        seed = world["seeds"][name]
        command = result + ["-drive", f"file={seed},media=cdrom,readonly=on,cache=none"]
    return command


def start_qemu(name, command):
    # Shared slot, including rescue roots. flock serializes independent
    # provider tasks and direct builder calls through the same admission path.
    import fcntl

    with (VM / "worker-slot.lock").open("a") as slot:
        fcntl.flock(slot, fcntl.LOCK_EX)
        slots = {"recovery-01", "us-work-04", "eu-work-04", "eu-work-05"}
        owner = name.removesuffix("-rescue")
        if owner in slots:
            busy = [n for n in slots if n != owner and (pid_of(VM / (n + ".pid")) or pid_of(VM / (n + "-rescue.pid")))]
            if busy:
                raise RuntimeError("One shared 2 GiB temporary-worker/rescue slot; busy: " + ",".join(busy))
        return _start_qemu_admitted(name, command)


def _start_qemu_admitted(name, command):
    import world_run_guard

    world_run_guard.require_open()
    guarded = world_run_guard.active() is not None
    if guarded and "-S" not in command:
        command = [*command, "-S"]
    if WORLD_ACTIVE:
        allocated = sum(spec["memory"] for n, spec in VMS.items() if pid_of(VM / (n + ".pid")))
        if not pid_of(VM / (name + ".pid")):
            allocated += VMS[name]["memory"]
        if allocated + 4096 > 30720:
            raise RuntimeError("Guest memory plus the 4 GiB host reserve would exceed the 30 GiB world limit")
    # A new provider boot may generate new guest keys (including cloud-init's
    # first boot). Keep transport trust scoped to this owned VM incarnation.
    known_hosts_path(name).unlink(missing_ok=True)
    (ARTIFACTS / f"qemu-command-{name}.json").write_text(json.dumps(command, indent=2) + "\n")
    subprocess.run(in_slice(f"smart-meter-vm-{name}", command), check=True)
    pid = int((VM / f"{name}.pid").read_text())
    Path(f"/proc/{pid}/oom_score_adj").write_text("1000")
    if guarded:
        # A queued native boot stays paused if its caller reaches the cutoff.
        world_run_guard.require_open()
        qmp(name, [{"execute": "cont"}])
    log(
        f"started {name}: {VMS[name]['cpus']} vCPU / {VMS[name]['memory']} MiB, ssh 127.0.0.1:{VMS[name]['ssh']}, "
        f"pid {pid}, oom_score_adj 1000"
    )
    return pid


def start(names):
    ensure_infrastructure()
    if not WORLD_ACTIVE:
        with BASE_IMAGE.open("rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != BASE_SHA256:
                raise SystemExit("pinned Ubuntu image checksum mismatch")
    for name in names:
        if pid_of(VM / f"{name}.pid"):
            log(f"{name} already running")
            continue
        disk = VM / f"{name}.qcow2"
        world = world_policy.config()
        if world and name in world.get("fronts", {}):
            disk = Path(world["fronts"][name])
        if VMS[name].get("public_recovery"):
            disk = Path(VMS[name]["disk"])
        if not disk.exists():
            subprocess.run(
                ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(BASE_IMAGE), str(disk), "20G"],
                check=True,
            )
            write_seed(name)
        start_qemu(name, qemu_command(name, disk))


def wait_ssh(name, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if subprocess.run(ssh_arguments(name, 5) + ["true"], capture_output=True, timeout=20).returncode == 0:
                return
        except subprocess.TimeoutExpired:
            pass
        time.sleep(3)
    raise RuntimeError(f"{name}: no SSH after {timeout}s")


def stop_pid(pid_file, expected, grace=60):
    pid = pid_of(pid_file)
    if pid:
        if not owned(pid, expected):
            raise SystemExit(f"refusing to stop unexpected process {pid}")
        os.kill(pid, signal.SIGTERM)
        for _ in range(grace * 10):
            if not running(pid):
                break
            time.sleep(0.1)
        else:
            os.kill(pid, signal.SIGKILL)
    pid_file.unlink(missing_ok=True)


def power_off(name, timeout=90):
    """Guest-initiated shutdown, then process stop. The disk is retained."""
    pid = pid_of(VM / f"{name}.pid")
    if not pid:
        return
    with contextlib.suppress(subprocess.TimeoutExpired):
        subprocess.run(ssh_arguments(name) + ["sudo -n systemctl poweroff"], capture_output=True, timeout=15)
    deadline = time.time() + timeout
    while time.time() < deadline and running(pid):
        time.sleep(0.5)
    stop_pid(VM / f"{name}.pid", "qemu-system-x86_64")
    log(f"{name} powered off; disk retained")


def acpi_stop(name, timeout=60):
    """Nova-style os-stop: ACPI power-down (independent of the guest network), then destroy after timeout.

    Nova's clean shutdown sends the ACPI shutdown request and waits shutdown_timeout (60 s by default)
    before destroying the domain. Returns 'clean' or 'destroyed'.
    """
    pid = pid_of(VM / f"{name}.pid")
    if not pid:
        return "not running"
    qmp(name, [{"execute": "system_powerdown"}])
    deadline = time.time() + timeout
    while time.time() < deadline and running(pid):
        time.sleep(0.5)
    outcome = "clean" if not running(pid) else "destroyed"
    stop_pid(VM / f"{name}.pid", "qemu-system-x86_64", grace=10)
    log(f"{name} stopped by ACPI power-down ({outcome}); disk retained")
    return outcome


def qmp(name, commands):
    """Send QMP commands to a guest started with a QMP socket; return the replies."""
    import socket as socket_module

    connection = socket_module.socket(socket_module.AF_UNIX, socket_module.SOCK_STREAM)
    connection.connect(str(VM / f"{name}.qmp"))
    stream = connection.makefile("rw")
    replies = [json.loads(stream.readline())]
    for command in [{"execute": "qmp_capabilities"}] + commands:
        stream.write(json.dumps(command) + "\n")
        stream.flush()
        while True:
            reply = json.loads(stream.readline())
            if "event" not in reply:
                replies.append(reply)
                break
    connection.close()
    return replies


def rescue_start(name):
    """Rescue a stopped instance within its own allocation.

    A distinct rescue image instance (a fresh overlay that never ran Kubernetes
    or Kafka) boots with the stopped instance's memory and CPU allocation. After
    it is up, the original disk is hot-attached read-only, so the two
    identically labeled images never compete at boot.
    """
    if pid_of(VM / f"{name}.pid"):
        raise RuntimeError(f"{name} is running; rescue needs a stopped instance")
    rescue = register_rescue(name)
    spec = VMS[name]
    ensure_infrastructure()
    world = world_policy.config()
    disk = (VM / world["disk_subdir"] / f"{rescue}.qcow2") if world else VM / f"{rescue}.qcow2"
    disk.unlink(missing_ok=True)
    known_hosts_path(rescue).unlink(missing_ok=True)
    subprocess.run(
        ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(BASE_IMAGE), str(disk), "8G"], check=True
    )
    write_seed(rescue)
    start_qemu(rescue, qemu_command(rescue, disk))
    wait_ssh(rescue)
    replies = qmp(
        rescue,
        [
            {
                "execute": "blockdev-add",
                "arguments": {
                    "driver": "qcow2",
                    "node-name": "original",
                    "read-only": True,
                    "file": {
                        "driver": "file",
                        "filename": str(
                            (world_policy.config() or {}).get("fronts", {}).get(name, VM / f"{name}.qcow2")
                        ),
                        "read-only": True,
                    },
                },
            },
            {
                "execute": "device_add",
                "arguments": {"driver": "virtio-blk-pci", "drive": "original", "id": "original-disk", "bus": "rp1"},
            },
        ],
    )
    if any("error" in reply for reply in replies):
        raise RuntimeError(f"attaching original disk failed: {replies}")
    log(f"{rescue}: rescue image up in {spec['memory']} MiB; original disk attached; read_only=True")
    return rescue


def rescue_stop(name):
    rescue = register_rescue(name)
    stop_pid(VM / f"{rescue}.pid", "qemu-system-x86_64", grace=30)
    world = world_policy.config()
    disk = (VM / world["disk_subdir"] / f"{rescue}.qcow2") if world else VM / f"{rescue}.qcow2"
    disk.unlink(missing_ok=True)
    log(f"{rescue}: rescue instance stopped and its scratch overlay removed")


def stop_all():
    for name in VMS:
        power_off(name)
        stop_pid(VM / f"{name}-{RESCUE_MARK}.pid", "qemu-system-x86_64")
    for service in ("sim", "provider", "seed", "switch"):
        stop_pid(VM / f"{service}.pid", "python", grace=10)
    log("all owned guests and world host services stopped; disks retained")


def status():
    rows = {}
    for name in VMS:
        pid = pid_of(VM / f"{name}.pid")
        rows[name] = {"pid": pid}
        if pid:
            rollup = {
                line.split(":")[0]: int(line.split()[1])
                for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines()[1:]
            }
            rows[name].update(rss_mib=rollup["Rss"] // 1024, pss_mib=rollup["Pss"] // 1024)
    cgroup = slice_cgroup()
    if cgroup and cgroup.exists():
        rows["slice"] = {
            "current_mib": int((cgroup / "memory.current").read_text()) >> 20,
            "peak_mib": int((cgroup / "memory.peak").read_text()) >> 20,
        }
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "stop", "stop-all", "ssh", "status"))
    parser.add_argument("rest", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    if arguments.action == "start":
        start(arguments.rest)
    elif arguments.action == "stop":
        for vm in arguments.rest:
            power_off(vm)
    elif arguments.action == "stop-all":
        stop_all()
    elif arguments.action == "status":
        status()
    else:
        vm, *command = arguments.rest
        raise SystemExit(subprocess.call(ssh_arguments(vm) + command))
