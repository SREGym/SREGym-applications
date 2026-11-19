#!/usr/bin/env python3
"""Local state capsules: immutable disks, native QEMU RAM/device streams, COW runs."""

import concurrent.futures as cf
import datetime
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import lab
import world_policy

STATES = ROOT / "vm/states"
STATUS = ROOT / "artifacts" / "status.log"


def status(s):
    with STATUS.open("a") as f:
        f.write(datetime.datetime.now(datetime.UTC).isoformat() + " " + s + "\n")


def write(p, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2) + "\n")


def result(n, cmd, args=None):
    r = lab.qmp(n, [dict(execute=cmd, **({"arguments": args} if args else {}))])[-1]
    if "error" in r:
        raise RuntimeError((n, cmd, r))
    return r.get("return")


def configure(state, fronts, run, agent_address=None):
    cfg = json.loads((state / "config.json").read_text())
    cfg.update(
        fronts={n: str(fronts / n) + ".qcow2" for n in cfg["memory_mib"]},
        private_client_disk=str(fronts / "ops-helper-01.qcow2"),
        artifact_subdir="runs",
        disk_subdir=str(fronts),
        known_hosts_prefix="run-" + hashlib.sha256(str(run).encode()).hexdigest()[:12] + "-",
        construction_inputs_local=True,
        authority="Frozen native response instance",
        probe_run=str(run),
        # The endpoints an agent uses also listen here, beside host loopback.
        agent_address=agent_address,
    )
    write(world_policy.CONFIG, cfg)
    write(ROOT / "public-instances.json", {})
    write(
        ROOT / "world-clock.json",
        {"guest_epoch": json.loads((state / "manifest.json").read_text())["clock"], "host_epoch": time.time()},
    )
    out = ROOT / "artifacts/runs"
    out.mkdir(parents=True, exist_ok=True)  # a fresh bundle install has no artifacts/ yet
    (out / "run-active.json").unlink(missing_ok=True)
    lab.VMS.update(cfg.get("instance_specs", {}))
    # Every retained company root belongs to this capsule.
    for n, m in cfg["memory_mib"].items():
        lab.VMS[n]["memory"] = m
    lab.VMS["ops-helper-01"]["memory"] = cfg["private_client_mib"]


def stop_owned():
    import world_run_guard

    for label, expected in [("world-tools", "world_tools.py"), ("provider", "provider.py")]:
        pid = lab.pid_of(lab.VM / (label + ".pid"))
        if pid and lab.owned(pid, expected):
            world_run_guard.stop_frozen_family(world_run_guard.freeze_family(pid, expected))
        (lab.VM / (label + ".pid")).unlink(missing_ok=True)
    for label in ["world-tunnel", "tool-relay"]:
        lab.stop_pid(lab.VM / (label + ".pid"), "ssh", 2)
    for p in list(lab.VM.glob("*.pid")):
        pid = lab.pid_of(p)
        if pid and lab.owned(pid, "qemu-system-x86_64"):
            lab.stop_pid(p, "qemu-system-x86_64", 2)
    for p in lab.VM.glob("*.console-pending"):
        p.unlink()


def export_native(n, path, compresslevel=1):
    assert 1 <= compresslevel <= 9
    path.parent.mkdir(parents=True, exist_ok=True)
    uri = "exec:gzip -" + str(compresslevel) + "c > " + shlex.quote(str(path))
    result(n, "migrate", {"uri": uri})
    end = time.monotonic() + 180
    while True:
        r = result(n, "query-migrate")
        if r.get("status") == "completed":
            break
        if r.get("status") in ["failed", "cancelled"]:
            raise RuntimeError((n, r))
        if time.monotonic() > end:
            raise TimeoutError((n, r))
        time.sleep(0.1)
    with path.open("rb") as f:
        os.fsync(f.fileno())
        os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    path.chmod(0o400)


def prepare(name, run, agent_address=None):
    aliases = json.loads((STATES / "aliases.json").read_text()) if (STATES / "aliases.json").exists() else {}
    state = STATES / aliases.get(name, name)
    assert (state / "ready.json").exists()
    run = Path(run).resolve()
    run.mkdir(parents=True, exist_ok=False)
    begin = time.monotonic()
    stop_owned()
    (run / "disks").mkdir()
    configure(state, run / "disks", run, agent_address)
    manifest = json.loads((state / "manifest.json").read_text())
    for n in manifest["allnames"]:
        subprocess.run(
            [
                "qemu-img",
                "create",
                "-q",
                "-f",
                "qcow2",
                "-F",
                "qcow2",
                "-b",
                str(state / "disks" / (n + ".qcow2")),
                str(run / "disks" / (n + ".qcow2")),
            ],
            check=True,
        )
    lab.stop_pid(lab.VM / "switch.pid", "python", 2)
    lab.limit_slice()  # the build's limits do not survive a host reboot or a fresh install
    lab.ensure_infrastructure()

    def one(n):
        cmd = lab.qemu_command(n, run / "disks" / (n + ".qcow2")) + ["-S", "-incoming", "defer"]
        lab.start_qemu(n, cmd)
        # A bundle install carries the memory as zstd, a build as gzip.
        ram = state / "ram" / (n + ".zst")
        ram, reader = (ram, "zstd -dc ") if ram.exists() else (ram.with_suffix(".gz"), "gzip -dc ")
        result(n, "migrate-incoming", {"uri": "exec:" + reader + shlex.quote(str(ram))})
        end = time.monotonic() + 90
        while True:
            r = result(n, "query-status")
            if r["status"] in ("paused", "prelaunch") and result(n, "query-migrate").get("status") == "completed":
                break
            if time.monotonic() > end:
                raise TimeoutError((n, r))
            time.sleep(0.05)
        with ram.open("rb") as source:
            os.posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)

    with cf.ThreadPoolExecutor(7) as pool:
        list(pool.map(one, manifest["live"]))
    out = ROOT / "artifacts/runs"
    shutil.copy2(state / "provider.jsonl", out / "provider.jsonl")
    write(
        run / "prepared.json",
        dict(
            state=name, seconds=time.monotonic() - begin, live=manifest["live"], manifest=str(state / "manifest.json")
        ),
    )
    return run, state, manifest, begin


def release(run, state, manifest):
    import world_snapshot

    begin = time.monotonic()
    clock = manifest["clock"]
    write(ROOT / "world-clock.json", {"guest_epoch": clock, "host_epoch": time.time()})
    with cf.ThreadPoolExecutor(7) as p:
        list(p.map(lambda n: result(n, "cont"), manifest["live"]))
    world_snapshot.tunnels_start()
    lab.wait_ssh("eu-work-03", timeout=90)
    world_snapshot.provider_start(state / "provider-state.json")
    import urllib.request

    limit = time.monotonic() + 10
    while True:
        try:
            with urllib.request.urlopen("http://127.0.0.1:28301/v2.1/servers/detail", timeout=1) as response:
                if len(json.load(response)["servers"]) >= 8:
                    break
        except Exception:
            pass
        if time.monotonic() > limit:
            raise TimeoutError("Provider inventory not ready")
        time.sleep(0.05)
    lab.start_background(
        "smart-meter-tools",
        lab.VM / "world-tools.pid",
        [sys.executable, str(ROOT / "world_tools.py")],
        "world-tools.log",
    )
    lab.ssh("eu-work-03", "sudo -n kill -CONT " + str(manifest["fleet_pid"]), timeout=10)
    controls = json.loads((state / "client-control-start.json").read_text())
    code = (
        "import pathlib,json;values="
        + repr(controls)
        + ";[(pathlib.Path('/var/lib/private/'+r+'-control.json').write_text(json.dumps(v))) for r,v in values.items()]"
    )
    lab.ssh("ops-helper-01", "sudo -n python3 -c " + shlex.quote(code), timeout=10)
    write(run / "released.json", dict(epoch=time.time(), release_seconds=time.monotonic() - begin))


def revive(name, run, agent_address=None):
    run, state, manifest, begin = prepare(name, run, agent_address)
    release(run, state, manifest)
    duration = time.monotonic() - begin
    write(run / "revive-result.json", dict(state=name, seconds=duration, under_one_minute=duration < 60))
    status(name + f" revived in {duration:.2f}s: " + str(run))


def assert_no_author_exports(cfg):
    # A host the healer holds is running but cut off from SSH; nothing could reach it after the cut.
    held = sorted(cfg.get("held_hosts", []))
    names = [n for n in cfg["memory_mib"] if lab.pid_of(lab.VM / (n + ".pid")) and n not in held]
    command = "sudo -n find / /run /tmp /var/tmp -xdev -type f \\( -name 'mqtt-*.jsonl' -o -name 'mqtt-export.py' \\) -print 2>/dev/null"
    found = dict(cf.ThreadPoolExecutor(6).map(lambda name: (name, lab.ssh(name, command, timeout=30).strip()), names))
    if any(found.values()):
        raise RuntimeError(("Author MQTT copy remains in world", found))
    return dict(
        live_hosts=names, held_hosts_not_reachable=held, no_author_mqtt_copies=True, stopped_roots_unchanged=True
    )


def capture(name, before_freeze=None):
    cfg = world_policy.config()
    capture_out = ROOT / "artifacts" / cfg.get("artifact_subdir", "runs")
    assert cfg["disk_subdir"] == "build", "Only a fresh construction namespace may become a benchmark state"
    assert cfg.get("control_start_path") and Path(cfg["control_start_path"]).is_file(), "Bind actual controls"
    assert cfg.get("starting_truth_path") and Path(cfg["starting_truth_path"]).is_file(), (
        "Freeze actual identities/source classes before capture"
    )
    author_copy_check = assert_no_author_exports(cfg)
    state = STATES / name
    state.mkdir(parents=True, exist_ok=False)
    (state / "ram").mkdir()
    (state / "disks").mkdir()
    manifest = dict(allnames=list(cfg["memory_mib"]) + ["ops-helper-01"])
    manifest["live"] = [n for n in manifest["allnames"] if lab.pid_of(lab.VM / (n + ".pid"))]
    assert all(Path(cfg["fronts"][n]).exists() for n in cfg["memory_mib"])
    # Administrative transports are replaceable; native application clients stay saved.
    import world_snapshot

    fleet = int(lab.ssh("eu-work-03", "systemctl show company-fleet -p MainPID --value").strip())
    lab.ssh("eu-work-03", "sudo -n kill -STOP " + str(fleet))
    provider = lab.pid_of(lab.VM / "provider.pid")
    if provider:
        import signal

        p = capture_out / "provider-state-native.json"
        p.unlink(missing_ok=True)
        os.kill(provider, signal.SIGUSR1)
        limit = time.monotonic() + 5
        while True:
            try:
                provider_state = json.loads(p.read_text())
                if "tokens" in provider_state and "counts" in provider_state:
                    break
            except (FileNotFoundError, json.JSONDecodeError):
                pass
            if time.monotonic() > limit:
                raise TimeoutError("Provider checkpoint")
            time.sleep(0.05)
        write(state / "provider-state.json", provider_state)
    else:
        raise RuntimeError("A fresh state requires its actual provider checkpoint")
    if before_freeze:
        before_freeze()
    clock = float(lab.ssh("ops-helper-01", "date +%s.%N", timeout=10).strip())
    world_snapshot.tunnels_stop()
    freeze_begin = time.monotonic()
    with cf.ThreadPoolExecutor(7) as p:
        list(p.map(lambda n: result(n, "stop"), manifest["live"]))
    freeze_end = time.monotonic()
    with cf.ThreadPoolExecutor(3) as p:
        list(p.map(lambda n: export_native(n, state / "ram" / (n + ".gz")), manifest["live"]))
    for n in manifest["live"]:
        lab.stop_pid(lab.VM / (n + ".pid"), "qemu-system-x86_64", 2)
    for n in manifest["allnames"]:
        disk = Path(cfg["private_client_disk"] if n == "ops-helper-01" else cfg["fronts"][n])
        target = state / "disks" / (n + ".qcow2")
        disk.rename(target)
        target.chmod(0o400)
    manifest.update(
        clock=clock,
        fleet_pid=fleet,
        tag=None,
        freeze_span_seconds=freeze_end - freeze_begin,
        captured_host_epoch=time.time(),
        disks={n: str(state / "disks" / (n + ".qcow2")) for n in manifest["allnames"]},
    )
    write(state / "manifest.json", manifest)
    write(state / "config.json", cfg)
    write(state / "author-copy-check.json", author_copy_check)
    shutil.copy2(capture_out / "provider.jsonl", state / "provider.jsonl")
    shutil.copy2(cfg["control_start_path"], state / "client-control-start.json")
    write(
        state / "ready.json",
        dict(native_RAM_devices=True, readonly_disks=True, freeze_span_seconds=freeze_end - freeze_begin),
    )
    status(name + " native RAM/device state and immutable COW disks saved")


if __name__ == "__main__":
    if sys.argv[1] == "capture":
        capture(sys.argv[2])
    else:
        revive(*sys.argv[1:4])  # STATE RUN [AGENT_ADDRESS]
