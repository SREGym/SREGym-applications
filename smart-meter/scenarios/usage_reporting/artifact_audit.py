"""Check the local runtime artifact cache over the maintenance network."""

import hashlib
import json
import os
import pwd
import socket
import subprocess
import sys
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

index = {
    "us-parent-01": 1,
    "us-work-01": 2,
    "us-work-02": 3,
    "us-work-03": 4,
    "eu-parent-01": 5,
    "eu-work-01": 6,
    "eu-work-02": 7,
    "eu-work-03": 8,
    "us-work-04": 9,
    "eu-work-04": 10,
    "eu-work-05": 11,
}[socket.gethostname()]
outer_host = "192.0.2." + str(index * 4 + 1)
outer_client = "192.0.2." + str(index * 4 + 2)
host = "198.18.0." + str(index * 4 + 1)
client = "198.18.0." + str(index * 4 + 2)
table = str(100 + index)


def command(*args):
    subprocess.run(list(args), check=True)


if sys.argv[1] == "networkd":
    # The daemon needs sysfs for this network namespace, as in a container.
    # Its service owns a private mount namespace; the host mount is unchanged.
    command("mount", "-t", "sysfs", "-o", "ro,nosuid,nodev,noexec", "sysfs", "/sys")
    account = pwd.getpwnam("systemd-network")
    os.chown("/run/systemd/netif", account.pw_uid, account.pw_gid)
    os.execv(
        "/usr/bin/unshare",
        [
            "unshare",
            "--cgroup",
            "--pid",
            "--fork",
            "--mount-proc",
            "--kill-child",
            "/usr/bin/env",
            "container=artifact-audit",
            "/lib/systemd/systemd-networkd",
        ],
    )
elif sys.argv[1] == "bus-ready":
    until = time.monotonic() + 5
    while not Path("/run/company-artifact-bus/socket").exists():
        if time.monotonic() > until:
            raise TimeoutError("Artifact control bus did not start")
        time.sleep(0.05)
elif sys.argv[1] == "setup":
    command("ip", "netns", "add", "artifact-audit")
    command("ip", "link", "add", "audit-host", "type", "veth", "peer", "name", "audit0")
    command("ip", "address", "add", outer_host + "/30", "dev", "audit-host")
    command("ip", "link", "set", "audit-host", "up")
    command("ip", "link", "set", "audit0", "netns", "artifact-audit")
    command("ip", "netns", "exec", "artifact-audit", "ip", "link", "set", "lo", "up")
    config = Path("/run/systemd/network/88-artifact-audit.network")
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        "[Match]\nName=audit0\n[Network]\nAddress=" + outer_client + "/30\nLinkLocalAddressing=no\nIPv6AcceptRA=no\n"
    )
    # Both ends exchange encrypted traffic. The regular namespace daemon
    # manages only the underlay; it never shares the telemetry routing state.
    command("ip", "link", "add", "audit-wg", "type", "wireguard")
    command("ip", "netns", "exec", "artifact-audit", "ip", "link", "add", "audit-wg", "type", "wireguard")

    def keypair():
        private = subprocess.check_output(["openssl", "genpkey", "-algorithm", "X25519", "-outform", "DER"])
        public = subprocess.check_output(
            ["openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"], input=private
        )
        return private[-32:].hex(), public[-32:].hex()

    private_a, public_a = keypair()
    private_b, public_b = keypair()
    directory = Path("/var/lib/company/artifact-link")
    directory.mkdir(mode=0o700, exist_ok=True)
    for name, data in [
        (
            "host",
            dict(
                private=private_a,
                peer_public=public_b,
                allowed_ip=client,
                endpoint=outer_client,
                endpoint_port=51899,
                listen_port=51898,
            ),
        ),
        (
            "client",
            dict(
                private=private_b,
                peer_public=public_a,
                allowed_ip=host,
                endpoint=outer_host,
                endpoint_port=51898,
                listen_port=51899,
            ),
        ),
    ]:
        path = directory / (name + ".json")
        path.touch(mode=0o600)
        path.write_text(json.dumps(data))
    command("python3", "/opt/company/wireguard_link.py", str(directory / "host.json"))
    command(
        "ip",
        "netns",
        "exec",
        "artifact-audit",
        "python3",
        "/opt/company/wireguard_link.py",
        str(directory / "client.json"),
    )
    command("ip", "address", "add", host + "/30", "dev", "audit-wg")
    command("ip", "link", "set", "audit-wg", "up")
    command("ip", "netns", "exec", "artifact-audit", "ip", "address", "add", client + "/30", "dev", "audit-wg")
    command("ip", "netns", "exec", "artifact-audit", "ip", "link", "set", "audit-wg", "up")
    command("ip", "route", "add", client + "/32", "dev", "audit-wg", "table", table)
    command("ip", "rule", "add", "pref", str(1200 + index), "from", host + "/32", "lookup", table)
elif sys.argv[1] == "serve":

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            rows = []
            for p in sorted(Path("/var/cache/company").rglob("*.deb")):
                h = hashlib.sha256()
                with p.open("rb") as f:
                    while chunk := f.read(1048576):
                        h.update(chunk)
                rows.append(dict(path=str(p), bytes=p.stat().st_size, sha256=h.hexdigest()))
            body = json.dumps(dict(host=socket.gethostname(), at=time.time(), artifacts=rows)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    HTTPServer((host, 8098), Handler).serve_forever()
elif sys.argv[1] == "check":
    until = time.monotonic() + 30
    while True:
        try:
            data = json.load(urllib.request.urlopen("http://" + host + ":8098/", timeout=5))
            if not data["artifacts"]:
                raise RuntimeError("No cached artifacts")
            break
        except OSError:
            if time.monotonic() > until:
                raise
            time.sleep(1)
    out = Path("/var/lib/company/operations/artifact-audit.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps(dict(artifacts=len(data["artifacts"]), bytes=sum(r["bytes"] for r in data["artifacts"]))))
