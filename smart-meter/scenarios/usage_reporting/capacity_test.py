#!/usr/bin/env python3
"""Check the capacity controller's decisions on the shipped fleet contract."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

FLEET = Path(__file__).resolve().parent / "fleet"
sys.path.insert(0, str(FLEET))
import capacity

joined = []
decision = capacity.reconcile(FLEET, "us", ["us-parent-01"], joined.append)
assert decision["eligible"] == ["us-parent-01", "us-work-01", "us-work-02", "us-work-03"], decision
assert joined == ["us-work-01", "us-work-02", "us-work-03"], joined
print("fresh US cluster: the controller joins all three workers, never the control plane")

joined.clear()
decision = capacity.reconcile(FLEET, "eu", ["eu-parent-01", "eu-work-01", "eu-work-02", "eu-work-03"], joined.append)
assert joined == [] and decision["joins"] == {}, decision
print("every eligible EU host is a node: the controller does nothing")

with tempfile.TemporaryDirectory() as folder:
    root = Path(folder) / "fleet"
    shutil.copytree(FLEET, root)
    (root / "overrides" / "regions" / "us.yaml").unlink()
    capacity.reconcile(root, "us", ["us-parent-01"], joined.append)
assert joined == ["us-work-03"], joined
print("without the US region override the elastic hosts are not eligible and never join")

# The real join path: a token from the local kubeadm, then one gateway exec on the new host.
seen = []


class Gateway(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        seen.append((self.path, self.headers["X-Client"], body))
        payload = json.dumps(dict(exit_code=0, stdout="This node has joined the cluster")).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


server = HTTPServer(("127.0.0.1", 0), Gateway)
threading.Thread(target=server.serve_forever, daemon=True).start()
capacity.GATEWAY = f"http://127.0.0.1:{server.server_port:d}"
with tempfile.TemporaryDirectory() as folder:
    fake = Path(folder) / "kubeadm"
    fake.write_text(
        '#!/bin/sh\necho "kubeadm join 10.72.0.10:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:1234 "\n'
    )
    fake.chmod(0o755)
    os.environ["PATH"] = folder + os.pathsep + os.environ["PATH"]
    result = capacity.join_through_gateway("us-work-04")
server.shutdown()
assert result == dict(exit_code=0, tail="This node has joined the cluster"), result
path, client, body = seen[0]
assert (
    path == "/exec" and client == "capacity-controller" and body["server"] == "us-work-04" and body["timeout"] == 600
), seen
for part in (
    "apiServerEndpoint: 10.72.0.10:6443",
    "token: abcdef.0123456789abcdef",
    "caCertHashes: [sha256:1234]",
    "name: us-work-04",
    "kubeadm join --config " + capacity.JOIN_FILE,
):
    assert part in body["command"], (part, body["command"])
subprocess.run(["sh", "-n", "-c", body["command"]], check=True)
print("a waiting host joins through one gateway exec with its own JoinConfiguration")
