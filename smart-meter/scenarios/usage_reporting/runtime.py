#!/usr/bin/env python3
"""Start company automation and device traffic."""

import datetime
import json
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "world"))
import lab
import world_history as h
import world_ops
from build_context import OUT, note, put, require_build

NAMES = list(require_build()["memory_mib"])
REPO = "/var/lib/company/platform-source"


def calendar(value):
    return datetime.datetime.fromtimestamp(value, datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def git(message):
    lab.ssh(
        "eu-work-03",
        "sudo -n chown -R ubuntu:ubuntu "
        + REPO
        + "; git -C "
        + REPO
        + " add .; git -C "
        + REPO
        + " commit -m "
        + shlex.quote(message)
        + "; git -C "
        + REPO
        + " push origin HEAD:main",
        timeout=120,
    )


def main():
    assert Path(sys.argv[0]).is_absolute() and lab.owned(os.getpid(), "python")
    assert not lab.pid_of(lab.VM / "qualification.pid")
    (lab.VM / "qualification.pid").write_text(str(os.getpid()))
    assert (OUT / "apps-complete.json").exists()
    require_build()
    from deploy_controls import install as install_controls

    install_controls()
    if not (OUT / "initial-fleet-policy-published.json").exists():
        # The initial instance template adds no files or commands.
        put(
            "eu-work-03",
            "initial-image.json",
            json.dumps(dict(cloud_config=dict(write_files=[], runcmd=[])), indent=2) + "\n",
            REPO + "/fleet/image.json",
        )
        # A maintenance window five days out.
        staged_window = time.time() + 5 * 86400
        profiles = {n: dict(not_before=staged_window) for n in NAMES}
        promotions = dict(releases=[])
        policy = dict(
            package_window_utc=calendar(staged_window) + " through " + calendar(staged_window + 3600),
            freshness_seconds=120,
            role_reapply_at=calendar(staged_window + 16 * 60),
            profile_reapply_at=calendar(staged_window + 17 * 60),
            package_reapply_at=calendar(staged_window + 18 * 60),
            template_rollout=dict(
                max_surge=0,
                max_unavailable=1,
                paused=True,
                regions_parallel=True,
                serial_per_region=True,
                replacement_servers=["us-work-02"],
            ),
        )
        for name, value in [
            ("profiles.json", profiles),
            ("promotions.json", promotions),
            ("maintenance.json", policy),
        ]:
            put("eu-work-03", "initial-" + name, json.dumps(value, indent=2) + "\n", REPO + "/fleet/" + name)
        git("Publish staged fleet channels and maintenance policy")
        (OUT / "initial-fleet-policy-published.json").write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
        note("fleet-policy-published")

    def one(n):
        marker = OUT / ("initial-timers-" + n + ".json")
        if marker.exists():
            return
        lab.push(n, Path(__file__).with_name("schedule.py"), "/opt/company/schedule.py")
        value = lab.ssh(n, "sudo -n python3 /opt/company/schedule.py", timeout=90)
        marker.write_text(json.dumps(dict(host_epoch=time.time(), output=value)) + "\n")
        note("maintenance-timers-installed", host=n)

    with ThreadPoolExecutor(4) as pool:
        list(pool.map(one, NAMES))
    lab.push("eu-work-03", lab.ROOT / "company_fleet.py", "/opt/company/fleet.py")
    put(
        "eu-work-03",
        "fleet.service",
        "[Unit]\nAfter=network-online.target\n[Service]\nEnvironment=FLEET_RETRY_COOLDOWN_S=1800\nExecStart=/usr/bin/python3 -u /opt/company/fleet.py\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n",
        "/etc/systemd/system/company-fleet.service",
    )
    lab.push("eu-parent-01", Path(__file__).with_name("rollout.py"), "/opt/company/rollout.py")
    put(
        "eu-parent-01",
        "rollout.service",
        "[Unit]\nAfter=network-online.target\n[Service]\nExecStart=/usr/bin/python3 -u /opt/company/rollout.py\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n",
        "/etc/systemd/system/company-rollout.service",
    )
    # The provider (shared budgets, fresh state) and the company tools.
    lab.start_background(
        "smart-meter-build-provider",
        lab.VM / "provider.pid",
        [sys.executable, str(lab.ROOT / "provider.py")],
        "world-provider.log",
    )
    lab.start_background(
        "smart-meter-build-tools",
        lab.VM / "world-tools.pid",
        [sys.executable, str(lab.ROOT / "world_tools.py")],
        "world-tools.log",
    )
    import cluster

    def relay_ready():
        code = "import urllib.request,json; r=json.load(urllib.request.urlopen('http://10.72.0.30:28880/provider/v2.1/servers/detail',timeout=3)); print(len(r['servers']))"
        try:
            return lab.ssh("eu-work-03", "python3 -c " + shlex.quote(code), timeout=6).strip() == "8"
        except Exception:
            return False

    cluster.wait("guest-to-provider relay", relay_ready, 60, every=2)
    for n, unit in [("eu-work-03", "company-fleet"), ("eu-parent-01", "company-rollout")]:
        lab.ssh(n, "sudo -n systemctl daemon-reload; sudo -n systemctl enable --now " + unit)
    # MM2 in both directions between the regional clusters.
    code = (
        "import workloads_eu as w; w.REMOTE_BOOT="
        + repr(",".join(lab.VMS["us-work-0" + str(i)]["ip"] + ":" + str(31990 + i) for i in [1, 2, 3]))
        + '; t=w.mm2(); print(t.replace("us.config.storage.replication.factor=1","us.config.storage.replication.factor=2").replace("us.offset.storage.replication.factor=1","us.offset.storage.replication.factor=2").replace("us.status.storage.replication.factor=1","us.status.storage.replication.factor=2").replace("eu->us.replication.factor=1","eu->us.replication.factor=2"))'
    )
    text = subprocess.check_output(
        [sys.executable, "-c", code], env={**os.environ, "SMART_METER_REGION": "eu"}, cwd=lab.ROOT, text=True
    )
    p = OUT / "mm2.yaml"
    p.write_text(text)
    with p.open("rb") as f:
        subprocess.run(
            [
                sys.executable,
                "-c",
                'import cluster; print(cluster.kubectl("apply -f -",stdin=__import__("sys").stdin.buffer))',
            ],
            env={**os.environ, "SMART_METER_REGION": "eu"},
            stdin=f,
            cwd=lab.ROOT,
            check=True,
        )
    # Autoscaling is deployed with the pod-metrics migration.
    for r in ["us", "eu"]:
        for target in ["gateway", "writer"]:
            h.k(
                r,
                "-n telemetry autoscale deployment "
                + target
                + (
                    " --cpu-percent=75 --min=2 --max=2"
                    if r == "us" and target == "gateway"
                    else " --cpu-percent=75 --min=1 --max=2"
                ),
            )
    # Keep collector defaults until the 15-minute normal-workload baseline
    # sets the shared limit.
    lab.push("eu-work-03", lab.ROOT / "company_health_activity.py", "/opt/company/health_activity.py")
    lab.push("eu-work-03", lab.ROOT / "company_health_activity.py", REPO + "/services/health_activity.py")
    world_ops.unit("eu-work-03", "company-operations-health", "/usr/bin/python3 -u /opt/company/health_activity.py")
    lab.ssh("eu-work-03", "sudo -n systemctl daemon-reload; sudo -n systemctl enable --now company-operations-health")
    # The private device clients on ops-helper-01.
    helper = "ops-helper-01"
    lab.ssh(helper, "sudo -n mkdir -p /opt/private /var/lib/private; sudo -n chmod 0700 /opt/private /var/lib/private")
    lab.push(helper, lab.ROOT / "sim.py", "/opt/private/sim.py", "0700")
    from device_routing import install as install_device_routing

    install_device_routing()
    for r, edge in [("us", "us-work-03"), ("eu", "eu-work-01")]:
        put(helper, r + "-control.json", '{"rate":10}\n', "/var/lib/private/" + r + "-control.json", "0600")
        unit = (
            "[Unit]\nAfter=network-online.target\n[Service]\nLimitNOFILE=65536\nExecStart=/usr/bin/python3 /opt/private/sim.py --devices 1000 --rate 10 --ramp 45 --host "
            + lab.VMS[edge]["ip"]
            + " --port 31883 --region "
            + r
            + " --control /var/lib/private/"
            + r
            + "-control.json --stats /var/lib/private/"
            + r
            + "-stats.json --acks /var/lib/private/"
            + r
            + "-acks.log --billing-url http://"
            + lab.VMS[r + "-parent-01"]["ip"]
            + ":8080/api/usage\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n"
        )
        put(helper, r + "-devices.service", unit, "/etc/systemd/system/private-devices-" + r + ".service")
    lab.ssh(
        helper, "sudo -n systemctl daemon-reload; sudo -n systemctl enable --now private-devices-us private-devices-eu"
    )
    git("Enable service readiness, log summaries and native autoscaling policies")
    (OUT / "runtime-ready.json").write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
    note("runtime-started", sessions_per_region=1000, offered_rate_per_region=10)
    (lab.VM / "qualification.pid").unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        (OUT / ("runtime-install-error-" + str(time.time_ns()) + ".json")).write_text(
            json.dumps(dict(error=repr(e), host_epoch=time.time())) + "\n"
        )
        note("runtime-error", error=repr(e))
        raise
