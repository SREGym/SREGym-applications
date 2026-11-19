"""Tests for sregym_lifecycle.py with fake world modules; no world is started.

Usage: python3 -B -m pytest -q -p no:cacheprovider test_sregym_lifecycle.py
"""

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sregym_lifecycle as lifecycle


def fake_world(tmp_path, monkeypatch):
    run = tmp_path / "run"
    (run / "disks").mkdir(parents=True)
    vm = tmp_path / "vm"
    vm.mkdir()
    (vm / "eu-work-01.pid").write_text("123")
    (vm / "world-tools.pid").write_text("234")
    (vm / "provider.pid").write_text("345")
    (run / "disks/eu-work-01.qcow2").write_bytes(b"disk")
    (tmp_path / "public-instances.json").write_text("{}")
    config = {
        "probe_run": str(run),
        "fronts": {"eu-work-01": str(run / "disks/eu-work-01.qcow2")},
        "private_client_disk": str(run / "disks/ops-helper-01.qcow2"),
        "disk_subdir": str(run / "disks"),
    }
    paused = []
    events = []

    def result(name, command):
        if command == "stop":
            events.append("pause " + name)
            paused.append(name)
            return {}
        assert command == "query-status"
        return {"status": "paused"}

    def export(name, path):
        assert paused == ["eu-work-01"]
        assert (run / "native-cutoff.json").exists()
        path.write_bytes(b"native-ram")

    lab = SimpleNamespace(
        VM=vm,
        ROOT=tmp_path,
        RESCUE_MARK="rescue",
        pid_of=lambda p: int(p.read_text()) if p.exists() else None,
        owned=lambda pid, expected: (pid == 123) == (expected == "qemu-system-x86_64"),
        stop_pid=Mock(),
        ssh=Mock(side_effect=lambda *_a, **_k: events.append("clock") or "1700000000000000000\n"),
    )
    revive = SimpleNamespace(result=Mock(side_effect=result), export_native=export, stop_owned=Mock())
    guard = SimpleNamespace(
        freeze_family=Mock(side_effect=lambda pid, expected: events.append("freeze " + expected) or {"frozen": True}),
        stop_frozen_family=Mock(),
        close=Mock(side_effect=lambda: events.append("close")),
    )

    def probes():
        events.append("probe")
        return {"endpoints": {label: {"status": 200} for label in lifecycle.HEALTH_ENDPOINTS}}

    monkeypatch.setattr(lifecycle, "modules", lambda _p: (lab, revive, SimpleNamespace(config=lambda: config), guard))
    monkeypatch.setattr(lifecycle, "resource_problem", lambda _lab: None)
    monkeypatch.setattr(lifecycle, "start_endpoint_probes", probes)
    lab.events = events
    return run, lab, revive, config, guard


def test_freeze_pauses_before_export_and_preserves_native_images(tmp_path, monkeypatch):
    run, lab, revive, config, guard = fake_world(tmp_path, monkeypatch)
    lifecycle.freeze(tmp_path, run)
    capsule = run / "end-state"
    manifest = json.loads((capsule / "manifest.json").read_text())
    assert manifest["live"] == ["eu-work-01"]
    assert Path(manifest["ram"]["eu-work-01"]).read_bytes() == b"native-ram"
    disk = Path(manifest["disks"]["eu-work-01"])
    assert disk.read_bytes() == b"disk"
    assert disk.stat().st_mode & 0o222 == 0
    assert (run / "native-capture-complete.json").exists()
    assert not any(call.args[1] == "cont" for call in revive.result.call_args_list)
    assert guard.freeze_family.call_count == 2
    lifecycle.teardown(tmp_path, run)
    revive.stop_owned.assert_called_once()
    assert guard.stop_frozen_family.call_count == 2


def test_freeze_error_never_certifies_a_complete_capture(tmp_path, monkeypatch):
    run, _lab, revive, _config, _guard = fake_world(tmp_path, monkeypatch)
    revive.result.side_effect = RuntimeError("QMP unavailable")
    with pytest.raises(RuntimeError, match="Incomplete freeze"):
        lifecycle.freeze(tmp_path, run)
    assert (run / "native-cutoff.json").exists()
    assert not (run / "native-capture-complete.json").exists()


def test_freeze_closes_the_run_probes_and_reads_the_clock_before_pausing(tmp_path, monkeypatch):
    run, lab, _revive, _config, _guard = fake_world(tmp_path, monkeypatch)
    lifecycle.freeze(tmp_path, run)
    assert lab.events == ["close", "probe", "clock", "freeze world_tools.py", "freeze provider.py", "pause eu-work-01"]
    assert json.loads((run / "service-health-at-D.json").read_text())["endpoints"]["eu-health"]["status"] == 200
    (row,) = json.loads((run / "guest-health.jsonl").read_text())["rows"]
    cutoff = json.loads((run / "native-cutoff.json").read_text())
    assert cutoff["target_monotonic"] * 1e9 >= row["host_after"]["host_monotonic_ns"]


def test_freeze_tolerates_an_unreadable_guest_clock(tmp_path, monkeypatch):
    run, lab, _revive, _config, _guard = fake_world(tmp_path, monkeypatch)
    lab.ssh.side_effect = TimeoutError("ssh")
    lifecycle.freeze(tmp_path, run)
    assert not (run / "guest-health.jsonl").exists()
    assert (run / "native-capture-complete.json").exists()


def test_resource_problem_at_freeze_aborts_without_capture(tmp_path, monkeypatch):
    run, lab, revive, _config, guard = fake_world(tmp_path, monkeypatch)
    monkeypatch.setattr(lifecycle, "resource_problem", lambda _lab: "Host resource limit: swap")
    lifecycle.freeze(tmp_path, run)
    assert json.loads((run / "abort.json").read_text())["reason"] == "Host resource limit: swap"
    assert lab.events == []
    revive.result.assert_not_called()
    guard.freeze_family.assert_not_called()
    assert not (run / "end-state").exists()


def test_teardown_refuses_changed_ownership(tmp_path, monkeypatch):
    run, lab, revive, config, _guard = fake_world(tmp_path, monkeypatch)
    config["probe_run"] = str(tmp_path / "someone-else")
    with pytest.raises(RuntimeError, match="ownership changed"):
        lifecycle.teardown(tmp_path, run)
    revive.stop_owned.assert_not_called()
    lab.stop_pid.assert_not_called()


def test_teardown_stops_a_world_whose_run_folder_was_deleted(tmp_path, monkeypatch):
    run, lab, revive, _config, _guard = fake_world(tmp_path, monkeypatch)
    (run / "disks/eu-work-01.qcow2").unlink()
    (run / "disks").rmdir()
    run.rmdir()
    lifecycle.teardown(tmp_path, run)
    revive.stop_owned.assert_called_once()
    assert lab.stop_pid.call_args_list[0].args[0] == lab.VM / "world-guard-sregym.pid"
    assert not run.exists()


def test_resource_guard_must_report_a_fresh_sample(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts/runs"
    artifacts.mkdir(parents=True)
    run = tmp_path / "run"
    run.mkdir()
    sample = {"epoch": 100, "stop_reasons": [], "CPU_PSI_and_load_are_telemetry_only": True}
    (artifacts / "resource-current.json").write_text(json.dumps(sample))
    lab = SimpleNamespace(
        ROOT=tmp_path,
        VM=tmp_path / "vm",
        ARTIFACTS=tmp_path / "artifacts",
        start_background=Mock(),
        pid_of=lambda _p: 123,
        owned=lambda *_a: True,
    )
    monkeypatch.setattr(lifecycle.time, "time", lambda: 100)
    lifecycle.start_resource_guard(lab, run)
    assert json.loads((run / "resource-guard.json").read_text())["pid"] == 123
    assert lifecycle.resource_problem(lab) is None
    sample["stop_reasons"] = ["swap"]
    (artifacts / "resource-current.json").write_text(json.dumps(sample))
    assert lifecycle.resource_problem(lab) == "Host resource limit: swap"
    with pytest.raises(RuntimeError, match="preflight failed: swap"):
        lifecycle.start_resource_guard(lab, run)


def test_endpoint_probes_record_silent_endpoints_as_not_serving(tmp_path, monkeypatch):
    import http.server
    import threading
    import time

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/slow":
                time.sleep(3)
            self.send_response(200 if self.path == "/health" else 503)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    endpoints = {"health": base + "/health", "live": base + "/api/live", "slow": base + "/slow"}
    monkeypatch.setattr(lifecycle, "HEALTH_ENDPOINTS", endpoints)
    try:
        record = lifecycle.start_endpoint_probes()
        end = time.monotonic() + 5
        while len(record["endpoints"]) < 2 and time.monotonic() < end:
            time.sleep(0.02)
        lifecycle.record_endpoint_probes(tmp_path, record)
    finally:
        server.shutdown()
    written = json.loads((tmp_path / "service-health-at-D.json").read_text())["endpoints"]
    assert written["health"]["status"] == 200
    assert written["live"]["status"] == 503
    assert written["slow"] == {"status": None, "error": "No response before the freeze"}


def test_missing_or_stale_resource_sample_stops_the_run(tmp_path):
    artifacts = tmp_path / "artifacts/runs"
    artifacts.mkdir(parents=True)
    lab = SimpleNamespace(
        ROOT=tmp_path, VM=tmp_path, ARTIFACTS=tmp_path / "artifacts", pid_of=lambda _p: 123, owned=lambda *_a: True
    )
    assert lifecycle.resource_problem(lab) == "Host resource watcher stopped reporting"
    sample = artifacts / "resource-current.json"
    sample.write_text("{")
    assert lifecycle.resource_problem(lab) is None
    os.utime(sample, (0, 0))
    assert lifecycle.resource_problem(lab) == "Host resource watcher stopped reporting"


def test_teardown_stops_whichever_seed_server_revive_started(tmp_path, monkeypatch):
    run, lab, _revive, _config, _guard = fake_world(tmp_path, monkeypatch)
    commands = {77: "company_seed_http.py", 78: "switch.py"}
    (lab.VM / "seed.pid").write_text("77")
    (lab.VM / "switch.pid").write_text("78")
    lab.owned = lambda pid, expected: commands.get(pid) == expected
    lifecycle.teardown(tmp_path, run)
    stopped = [(c.args[0].name, c.args[1]) for c in lab.stop_pid.call_args_list]
    assert ("seed.pid", "company_seed_http.py") in stopped and ("switch.pid", "switch.py") in stopped
    assert ("seed.pid", "http.server") not in stopped
