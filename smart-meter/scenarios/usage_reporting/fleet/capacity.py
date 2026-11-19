#!/usr/bin/env python3
"""Capacity controller: hosts whose resolved capacity facts make them cluster-eligible join the region's cluster.

Runs on each region's control plane. Every eligible worker that is not a node yet joins through the
company operations gateway. Each run prints the eligible set, the current nodes and every join's result.
"""

import json
import shlex
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import yaml
from resolve import facts

GATEWAY = "http://10.72.0.30:28880"
KUBECONFIG = "/etc/kubernetes/admin.conf"
JOIN_FILE = "/var/cache/company/bootstrap/kubeadm-join.yaml"


def reconcile(root, region, nodes, join):
    """Join every cluster-eligible worker of the region that is not a node, and return the decision."""
    pools = yaml.safe_load((Path(root) / "pools.yaml").read_text())["pools"]
    hosts = sorted(h for p in pools.values() for h in (p.get("hosts") or []) if h.startswith(region + "-"))
    capacity = {h: facts(root, h).get("capacity") or {} for h in hosts}
    eligible = [h for h in hosts if capacity[h].get("cluster_eligible")]
    # Control planes are created by kubeadm init, never joined as workers.
    joining = [h for h in eligible if h not in nodes and capacity[h].get("role") != "control-plane"]
    return dict(region=region, eligible=eligible, joined=sorted(nodes), joins={h: join(h) for h in joining})


def join_through_gateway(host):
    try:
        words = subprocess.check_output(["kubeadm", "token", "create", "--print-join-command"], text=True).split()
        config = (
            "apiVersion: kubeadm.k8s.io/v1beta3\nkind: JoinConfiguration\ndiscovery:\n  bootstrapToken:\n"
            f"    apiServerEndpoint: {words[2]}\n    token: {words[words.index('--token') + 1]}\n"
            f"    caCertHashes: [{words[words.index('--discovery-token-ca-cert-hash') + 1]}]\n"
            f"nodeRegistration:\n  name: {host}\n  criSocket: unix:///run/containerd/containerd.sock\n"
            "  imagePullPolicy: Never\n"
        )
        command = (
            f"printf %s {shlex.quote(config)} | sudo -n install -D -m0600 /dev/stdin {JOIN_FILE}"
            f" && sudo -n kubeadm join --config {JOIN_FILE}"
        )
        request = urllib.request.Request(
            GATEWAY + "/exec",
            method="POST",
            data=json.dumps(dict(server=host, command=command, timeout=600)).encode(),
            headers={"Content-Type": "application/json", "X-Client": "capacity-controller"},
        )
        with urllib.request.urlopen(request, timeout=630) as response:
            result = json.load(response)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        return dict(error=str(error))
    return dict(exit_code=result.get("exit_code"), tail=result.get("stdout", "")[-400:])


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent
    names = subprocess.run(
        ["kubectl", "--kubeconfig", KUBECONFIG, "get", "nodes", "-o", "name"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    nodes = [n.split("/", 1)[1] for n in names]
    print(json.dumps(reconcile(root, socket.gethostname().split(".")[0][:2], nodes, join_through_gateway)))


if __name__ == "__main__":
    main()
