#!/usr/bin/env python3
"""Live-world memory, swap, OOM, disk and CPU-quota watch."""

import json
import os
import shutil
import signal
import time
from pathlib import Path

import lab
import world_policy

OUT = lab.ARTIFACTS / world_policy.config()["artifact_subdir"]


def danger_reasons(row):
    reasons = []
    if row["nonfile_with_reserve"] >= 30 * 2**30:
        reasons.append("memory_with_reserve_at_or_above_30_GiB")
    if row["swap"]:
        reasons.append("swap")
    if row["oom"]:
        reasons.append("OOM")
    if row["host_available"] < 2 * 2**30:
        reasons.append("host_available_below_2_GiB")
    if row["disk_free"] < 10 * 2**30:
        reasons.append("disk_free_below_10_GiB")
    if row["cpu_max"] != lab.CPU_MAX:
        reasons.append("CPU_quota_changed")
    return reasons


def main():
    OUT.mkdir(exist_ok=True)
    cg = lab.slice_cgroup()
    cfg = world_policy.config()
    baseline = dict(x.split() for x in (cg / "memory.events").read_text().splitlines())
    while True:
        events = dict(x.split() for x in (cg / "memory.events").read_text().splitlines())
        current = int((cg / "memory.current").read_text())
        swap = int((lab.outer_cgroup() / "memory.swap.current").read_text())
        avail = next(
            int(x.split()[1]) * 1024
            for x in Path("/proc/meminfo").read_text().splitlines()
            if x.startswith("MemAvailable:")
        )
        row = dict(
            epoch=time.time(),
            owned_current=current,
            owned_peak=int((cg / "memory.peak").read_text()),
            outside_reserve=cfg["outside_reserve_mib"] * 2**20,
            total_with_reserve=current + cfg["outside_reserve_mib"] * 2**20,
            outer_current=int((lab.outer_cgroup() / "memory.current").read_text()),
            swap=swap,
            oom=int(events.get("oom", 0)) - int(baseline.get("oom", 0)),
            cpu_max=(cg / "cpu.max").read_text().strip(),
            cpu_stat=(cg / "cpu.stat").read_text(),
            cpu_pressure=(cg / "cpu.pressure").read_text(),
            io_pressure=(cg / "io.pressure").read_text(),
            memory_pressure=(cg / "memory.pressure").read_text(),
            host_available=avail,
            disk_free=shutil.disk_usage(lab.ROOT).free,
        )
        # The budget covers the world's slice and leaves the rest of the host free. The
        # user-wide cgroup also holds other sessions and their file cache, so it is
        # telemetry only. Count the whole slice plus the configured reserve; host
        # pressure, swap/OOM, CPU quota and the disk floor are checked independently.
        stat = dict(x.split() for x in (cg / "memory.stat").read_text().splitlines())
        # Reclaimable page cache is not memory danger; count anonymous/kernel use plus the reserve.
        row["file_cache"] = int(stat.get("file", 0))
        row["nonfile_with_reserve"] = current - row["file_cache"] + cfg["outside_reserve_mib"] * 2**20
        row["memory_scope"] = (
            "world slice non-file memory plus configured reserve; page cache and outer user cgroup are telemetry"
        )
        row["host_cpu_pressure"] = Path("/proc/pressure/cpu").read_text()
        row["host_load1"] = os.getloadavg()[0]
        row["CPU_PSI_and_load_are_telemetry_only"] = True
        row["stop_reasons"] = danger_reasons(row)
        with (OUT / "resources.jsonl").open("a") as f:
            f.write(json.dumps(row) + "\n")
        (OUT / "resource-current.json").write_text(json.dumps(row) + "\n")
        if row["stop_reasons"]:
            (OUT / "RESOURCE-STOP.json").write_text(json.dumps(row, indent=2) + "\n")
            runner = lab.pid_of(lab.VM / "qualification.pid")
            if runner and lab.owned(runner, "python"):
                os.kill(runner, signal.SIGINT)
            time.sleep(5)
            for p in lab.VM.glob("*.pid"):
                pid = lab.pid_of(p)
                if pid and lab.owned(pid, "qemu-system-x86_64"):
                    os.kill(pid, signal.SIGKILL)
            return
        time.sleep(2)


if __name__ == "__main__":
    main()
