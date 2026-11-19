"""Select and lock a new build namespace."""

import fcntl
import json
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = HERE.parents[1] / "world"
LEAD = HERE.parents[1]
sys.path.insert(0, str(BUILD))
import lab
import world_policy


def status(message):
    with (LEAD / "STATUS.log").open("a") as stream:
        stream.write(datetime.now(UTC).isoformat() + " build " + message + "\n")


def main():
    import world_paths

    candidate = world_paths.load(HERE / "build-config.json")
    namespace = candidate["disk_subdir"]
    assert namespace == candidate["artifact_subdir"] == "build"
    directory = lab.VM / namespace
    out = lab.ARTIFACTS / namespace
    assert not directory.exists() and not out.exists(), "Fresh namespace required; preserve and resume existing work"
    for folder in (lab.VM, lab.ARTIFACTS):  # runtime folders, absent in a fresh checkout
        folder.mkdir(parents=True, exist_ok=True)
    with (lab.VM / "build-activation.lock").open("a+") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for proc in Path("/proc").iterdir():
            if not proc.name.isdigit():
                continue
            try:
                argv = (proc / "cmdline").read_bytes().split(b"\0")
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
            assert argv[0].rsplit(b"/", 1)[-1] not in (b"qemu-system-x86_64", b"qemu-img"), (
                "A VM or disk conversion is still running"
            )
        assert shutil.disk_usage(BUILD).free >= 30 * 2**30, "Archive completed artifacts before starting a build"
        for label, expected_command in [
            ("world-guard-build", "world_guard.py"),
            ("seed", "http.server"),
            ("switch", "switch.py"),
            ("world-tunnel", "ssh"),
            ("tool-relay", "ssh"),
        ]:
            pid = lab.pid_of(lab.VM / (label + ".pid"))
            if pid:
                assert lab.owned(pid, expected_command), (label, pid)
                lab.stop_pid(lab.VM / (label + ".pid"), expected_command, 10)
        directory.mkdir(mode=0o700)
        (directory / "seed").mkdir()
        out.mkdir(mode=0o700)
        world_policy.CONFIG.write_text(json.dumps(candidate, indent=2) + "\n")
        epoch = time.time()
        (BUILD / "world-clock.json").write_text(json.dumps(dict(guest_epoch=epoch, host_epoch=epoch)) + "\n")
        (BUILD / "public-instances.json").write_text("{}\n")
        (lab.VM / "switch-block").write_text("")
        # Prior guests have ended; remove their stale console reservations.
        for path in lab.VM.glob("*.console-pending"):
            path.unlink()
        lab.limit_slice()
        # A new process, so the guard reads the new namespace's config.
        lab.start_background(
            "smart-meter-build-guard",
            lab.VM / "world-guard-build.pid",
            [sys.executable, str(BUILD / "world_guard.py")],
            "world-build-guard.log",
        )
        (out / "initialization.json").write_text(
            json.dumps(dict(utc=datetime.now(UTC).isoformat(), namespace=namespace), indent=2) + "\n"
        )
        status("namespace " + namespace + " selected")


if __name__ == "__main__":
    main()
