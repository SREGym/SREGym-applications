#!/usr/bin/env python3
"""Build the world in a fresh namespace, run its history, then seal and save it.

Each finished step leaves a marker in the namespace's artifacts and is skipped
on rerun, so after a failure: fix the cause, then run this again.
"""

import json
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = HERE.parents[1] / "world"
OUT = BUILD / "artifacts/build"
PY = ["/usr/bin/python3", "-u", "-B"]


def log(message):
    print(time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()), message, flush=True)


def step(name, *args):
    marker = OUT / ("step-" + "-".join([name.removesuffix(".py"), *args]) + ".json")
    if marker.exists():
        return
    if (OUT / "boot-complete.json").exists():
        # Some steps restart the host tunnel as they finish; wait until it answers.
        subprocess.run(PY + ["-c", 'import lab; lab.wait_ssh("eu-work-03", timeout=120)'], cwd=BUILD, check=True)
    log("start " + " ".join([name, *args]))
    start = time.time()
    subprocess.run(PY + [str(BUILD / "run_step.py"), str(HERE / name), *args], check=True)
    marker.write_text(json.dumps(dict(seconds=round(time.time() - start, 1), at=time.time())) + "\n")
    log("done " + name + " in %.0f s" % (time.time() - start))


def clusters():
    marker = OUT / "step-clusters.json"
    if marker.exists():
        return
    log("start both clusters")
    children = [
        (
            region,
            subprocess.Popen(
                PY + [str(BUILD / "run_step.py"), str(HERE / "cluster_install.py"), region],
                stdout=(OUT / (region + "-cluster-install.log")).open("ab"),
                stderr=subprocess.STDOUT,
            ),
        )
        for region in ("us", "eu")
    ]
    codes = {region: process.wait() for region, process in children}
    if any(codes.values()):
        raise SystemExit(("cluster install failed", codes))
    # Later steps check these markers.
    for name in ("cluster-us-ready.json", "cluster-eu-ready.json", "clusters-ready.json", marker.name):
        (OUT / name).write_text(json.dumps(dict(at=time.time())) + "\n")
    log("done both clusters")


# The saved world needs nothing running on the host. revive.stop_owned stops the tools gateway, provider and
# tunnels; the build's own guard, seed and switch go here, so a later run never meets a second resource guard.
STOP_HELPERS = """
import lab, revive
revive.stop_owned()
for label, command in (('world-guard-build', 'world_guard.py'), ('seed', 'company_seed_http.py'),
                       ('seed', 'http.server'), ('switch', 'switch.py')):
    pid = lab.pid_of(lab.VM / (label + '.pid'))
    if pid and lab.owned(pid, command):
        lab.stop_pid(lab.VM / (label + '.pid'), command, 10)
"""


def stop_helpers():
    subprocess.run(PY + ["-c", STOP_HELPERS], cwd=BUILD, check=True)


def main():
    if not (OUT / "initialization.json").exists():
        log("start initialize")
        subprocess.run(PY + [str(HERE / "initialize.py")], check=True)
    for name in ("install_image.py", "boot.py"):
        step(name)
    clusters()
    for name in ("apps.py", "runtime.py", "inventory_packages.py", "start_customer_activity.py", "install_audits.py"):
        step(name)
    step("log_baseline.py", "--case", "01")
    step("apply_log_limit.py")
    step("verify_log_limit.py")
    step("history.py")
    step("seal.py")
    stop_helpers()
    log("usage-reporting saved")


if __name__ == "__main__":
    main()
