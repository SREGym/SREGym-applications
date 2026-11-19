#!/usr/bin/env python3
"""Initialize a regional company cluster using its cached pinned installation."""

import os
import sys
from pathlib import Path

region = sys.argv[1]
os.environ["SMART_METER_REGION"] = region
# The cluster starts without the L7 proxy; DNS visibility is enabled later through a Helm change.
os.environ["SMART_METER_CILIUM_EXTRA_VALUES"] = "l7Proxy: false\n"
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "world"))
import cluster

FLEET = Path(__file__).resolve().parent / "fleet"
sys.path.insert(0, str(FLEET))
import capacity
from build_context import OUT, note, require_build

require_build()
D = OUT / ("cluster-install-" + region)
D.mkdir(exist_ok=True)
cluster.ARTIFACTS = D
cluster.init_control_plane()
# Workers join where the capacity controller decides: every host whose resolved fleet facts make it cluster-eligible.
nodes = [n.split("/", 1)[1] for n in cluster.kubectl("get nodes -o name").split()]
for host in capacity.reconcile(FLEET, region, nodes, cluster.join)["joins"]:
    note("regional-worker-joined", region=region, host=host, by="capacity-controller")
(D / "nodes-ready.txt").write_text(cluster.kubectl("get nodes -o wide"))
note("cluster-installed", region=region)
