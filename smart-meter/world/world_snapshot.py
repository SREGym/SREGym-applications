#!/usr/bin/env python3
"""Host tunnels and the provider endpoint for a revived world."""

import sys

import lab
import world_policy


def tunnels_stop():
    for label in ["world-tunnel", "tool-relay"]:
        lab.stop_pid(lab.VM / (label + ".pid"), "ssh", 10)


def tunnels_start():
    cfg = world_policy.config()
    cmd = lab.ssh_arguments("ops-helper-01")[:-1] + ["-N", "-o", "ExitOnForwardFailure=yes"]
    for n in cfg["memory_mib"]:
        cmd += ["-L", f"127.0.0.1:{lab.VMS[n]['ssh']}:{lab.VMS[n]['ip']}:22"]
    host_only = ["127.0.0.1"]
    # The endpoints an agent uses also listen on the address its container reaches the host on.
    agent = host_only + ([cfg["agent_address"]] if cfg.get("agent_address") else [])
    for addresses, host, n, p in [
        (host_only, 21883, "us-work-03", 31883),
        (host_only, 21884, "eu-work-01", 31883),
        (agent, 28310, cfg.get("eu_report_host", "eu-parent-01"), 8080),
        (agent, 28320, "us-parent-01", 8080),
        (agent, 28330, "eu-work-03", 3000),
        (agent, 28390, "eu-parent-01", 9090),
        (agent, 28391, "eu-work-02", 3100),
        (agent, 28392, "eu-work-03", 8091),
    ]:
        for address in addresses:
            cmd += ["-L", f"{address}:{host}:{lab.VMS[n]['ip']}:{p}"]
    cmd += ["ubuntu@127.0.0.1"]
    lab.start_background("smart-meter-world-tunnel", lab.VM / "world-tunnel.pid", cmd, "world-tunnel.log")
    cmd = lab.ssh_arguments("ops-helper-01")[:-1] + [
        "-N",
        "-o",
        "ExitOnForwardFailure=yes",
        "-R",
        "10.72.0.30:28880:127.0.0.1:28311",
        "ubuntu@127.0.0.1",
    ]
    lab.start_background("smart-meter-tool-relay", lab.VM / "tool-relay.pid", cmd, "world-tool-relay.log")


def provider_start(restore=None):
    command = [sys.executable, str(lab.ROOT / "provider.py")]
    if restore:
        command = ["env", "SMART_METER_PROVIDER_RESTORE=" + str(restore), *command]
    lab.start_background("smart-meter-provider", lab.VM / "provider.pid", command, "world-provider.log")
