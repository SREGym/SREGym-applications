"""Run marker; tools cannot queue new work once the run is closed for its freeze."""

import contextlib
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def marker():
    import world_policy

    return ROOT / "artifacts" / world_policy.config()["artifact_subdir"] / "run-active.json"


def active():
    p = marker()
    return json.loads(p.read_text()) if p.exists() else None


def close():
    a = active()
    if a:
        t = marker().with_suffix(".publishing")
        t.write_text(json.dumps({**a, "closed": True}) + "\n")
        t.replace(marker())


def require_open():
    a = active()
    if a and a.get("closed"):
        raise RuntimeError("The run has ended; no further company actions are accepted")


def cgroup_for(pid, expected):
    import lab

    assert lab.running(pid) and lab.owned(pid, expected)
    value = next(
        line.split(":", 2)[2]
        for line in Path("/proc/" + str(pid) + "/cgroup").read_text().splitlines()
        if line.startswith("0::")
    )
    p = Path("/sys/fs/cgroup" + value)
    assert p.is_relative_to(lab.slice_cgroup()) and p != lab.slice_cgroup()
    assert os.access(p / "cgroup.freeze", os.W_OK)
    return p


def freeze_family(pid, expected):
    p = cgroup_for(pid, expected)
    (p / "cgroup.freeze").write_text("1\n")
    d = time.monotonic() + 2
    while "frozen 1" not in (p / "cgroup.events").read_text():
        if time.monotonic() > d:
            raise TimeoutError("Owned service family did not freeze: " + str(p))
        time.sleep(0.002)
    members = []
    for f in sorted(set([p / "cgroup.procs", *p.glob("**/cgroup.procs")])):
        for v in f.read_text().split():
            n = int(v)
            proc = Path("/proc") / v
            try:
                assert proc.stat().st_uid == os.getuid()
                members.append(
                    dict(
                        pid=n,
                        start_ticks=proc.joinpath("stat").read_text().rsplit(")", 1)[1].split()[19],
                        command=proc.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace"),
                    )
                )
            except FileNotFoundError:
                pass
    return dict(main_pid=pid, cgroup=str(p), members=members, frozen=True)


def stop_frozen_family(row):
    import signal

    base = Path(row["cgroup"])
    assert base.is_relative_to(Path("/sys/fs/cgroup"))
    for member in row["members"]:
        pid = member["pid"]
        proc = Path("/proc") / str(pid)
        try:
            assert proc.stat().st_uid == os.getuid()
            ticks = proc.joinpath("stat").read_text().rsplit(")", 1)[1].split()[19]
            path = Path(
                "/sys/fs/cgroup"
                + next(
                    line.split(":", 2)[2]
                    for line in proc.joinpath("cgroup").read_text().splitlines()
                    if line.startswith("0::")
                )
            )
            if ticks == member["start_ticks"] and path.is_relative_to(base):
                os.kill(pid, signal.SIGKILL)
        except FileNotFoundError:
            pass
    # A just-killed task can briefly have an empty cmdline before it is reaped.
    # Let the exact killed family exit before the normal PID-file cleanup checks
    # command ownership; never interpret that transition as a different process.
    with contextlib.suppress(FileNotFoundError):
        (base / "cgroup.freeze").write_text("0\n")
    deadline = time.monotonic() + 5
    for member in row["members"]:
        proc = Path("/proc") / str(member["pid"])
        while True:
            try:
                fields = proc.joinpath("stat").read_text().rsplit(")", 1)[1].split()
                if fields[19] != member["start_ticks"] or fields[0] == "Z":
                    break
            except FileNotFoundError:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Stopped family has not exited: " + str(member["pid"]))
            time.sleep(0.01)
