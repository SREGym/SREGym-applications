"""Tests that the world's agent endpoints listen where an agent container reaches them; no world is started.

Usage: python3 -B -m pytest -q -p no:cacheprovider test_agent_endpoints.py
"""

import http.client
import importlib
import json
import sys
import threading
from contextlib import nullcontext
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lab
import world_policy

AGENT_PORTS = {28310, 28320, 28330, 28390, 28391, 28392}
CONFIG = {"memory_mib": {"eu-work-01": 1024}, "agent_address": "172.17.0.1"}


@pytest.fixture
def world_tools(monkeypatch):
    monkeypatch.setattr(world_policy, "config", lambda: CONFIG)
    sys.modules.pop("world_tools", None)
    return importlib.import_module("world_tools")


def tunnel_listeners(monkeypatch, config):
    import world_snapshot

    started = {}
    monkeypatch.setattr(world_policy, "config", lambda: config)
    monkeypatch.setattr(lab, "ssh_arguments", lambda _name: ["ssh", "ubuntu@127.0.0.1"])
    monkeypatch.setattr(lab, "start_background", lambda unit, _pid, cmd, _log: started.setdefault(unit, cmd))
    vms = {n: {"ip": "10.72.0.9", "ssh": 22000 + i} for i, n in enumerate(lab.VMS)}
    vms.update({n: {"ip": "10.72.0.9", "ssh": 22100} for n in config["memory_mib"]})
    monkeypatch.setattr(lab, "VMS", vms)
    world_snapshot.tunnels_start()
    cmd = started["smart-meter-world-tunnel"]
    listeners = {}
    for flag, spec in zip(cmd, cmd[1:], strict=False):
        if flag == "-L":
            address, port = spec.split(":")[:2]
            listeners.setdefault(int(port), set()).add(address)
    return listeners, started["smart-meter-tool-relay"]


def test_tunnels_add_the_agent_address_only_for_agent_endpoints(monkeypatch):
    listeners, relay = tunnel_listeners(monkeypatch, CONFIG)
    for port, addresses in listeners.items():
        expected = {"127.0.0.1", "172.17.0.1"} if port in AGENT_PORTS else {"127.0.0.1"}
        assert addresses == expected, port
    # Device ingress and guest SSH stay host-only.
    assert set(listeners) >= AGENT_PORTS | {21883, 21884, 22100}
    assert "10.72.0.30:28880:127.0.0.1:28311" in relay


def test_tunnels_stay_on_loopback_without_an_agent_address(monkeypatch):
    listeners, _relay = tunnel_listeners(monkeypatch, {"memory_mib": {"eu-work-01": 1024}})
    assert set().union(*listeners.values()) == {"127.0.0.1"}


@pytest.mark.parametrize(
    ("config", "addresses"),
    [(CONFIG, ["127.0.0.1", "172.17.0.1"]), ({"agent_address": None}, ["127.0.0.1"])],
)
def test_operations_api_listens_on_loopback_and_the_agent_address(world_tools, monkeypatch, config, addresses):
    bound = []

    class Server:
        def __init__(self, address, _handler):
            bound.append(address)

        def serve_forever(self):
            pass

    monkeypatch.setattr(world_tools, "ThreadingHTTPServer", Server)
    world_tools.serve(config)
    assert bound == [(address, 28311) for address in addresses]


def test_operations_api_serves_companyctl_for_the_address_it_was_fetched_from(world_tools):
    server = ThreadingHTTPServer(("127.0.0.1", 0), world_tools.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:

        def get(host):
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            connection.request("GET", "/companyctl", headers={"Host": host})
            response = connection.getresponse()
            return response.status, response.read().decode()

        status, script = get("host.docker.internal:28311")
        assert status == 200
        original = Path(world_tools.__file__).with_name("companyctl").read_text()
        assert original.count("http://127.0.0.1:28311") == 1
        assert script == original.replace("http://127.0.0.1:28311", "http://host.docker.internal:28311")
        assert get("bad'host:1")[0] == 400
    finally:
        server.shutdown()
        server.server_close()


def test_readiness_checks_every_agent_endpoint_on_both_addresses(tmp_path, monkeypatch):
    import sregym_lifecycle as lifecycle

    run = tmp_path / "run"
    run.mkdir()
    (run / "prepared.json").write_text('{"live": []}')
    servers = [f"{region}-{role}" for region in ("eu", "us") for role in ("parent-01", "work-01", "work-02", "work-03")]
    policy = SimpleNamespace(config=lambda: {**CONFIG, "probe_run": str(run)})
    monkeypatch.setattr(lifecycle, "modules", lambda _p: (None, None, policy, None))
    monkeypatch.setattr(lifecycle, "start_resource_guard", lambda _lab, _run: None)
    monkeypatch.setattr(
        lifecycle.subprocess, "check_output", lambda *_a, **_k: json.dumps({"servers": [{"name": n} for n in servers]})
    )
    probed = []
    monkeypatch.setattr(
        lifecycle.socket, "create_connection", lambda target, timeout: probed.append(target) or nullcontext()
    )
    lifecycle.ready(tmp_path, run)
    assert sorted(probed) == sorted(
        (address, port) for address in ("127.0.0.1", "172.17.0.1") for port in (*AGENT_PORTS, 28311)
    )
