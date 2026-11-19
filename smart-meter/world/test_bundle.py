"""Tests for bundle.py install; no world is needed.

Usage: python3 -B -m pytest -q -p no:cacheprovider test_bundle.py
"""

import gzip
import io
import json
import subprocess
import tarfile

import bundle
import pytest


def write_bundle(path, sums):
    with tarfile.open(path, "w") as tar:
        for rel, data in {
            "bundle.json": json.dumps(dict(state="usage-reporting")).encode(),
            "SHA256SUMS": sums.encode(),
            "state/ready.json": b"{}\n",
        }.items():
            info = tarfile.TarInfo(f"{bundle.TOP}/{rel}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


@pytest.mark.parametrize("sums", ["0" * 64 + "  state/ready.json\n", "not a checksum line\n"])
def test_failed_install_removes_its_extraction_folder(tmp_path, monkeypatch, sums):
    monkeypatch.setattr(bundle, "STATES", tmp_path / "states")
    write_bundle(tmp_path / "damaged.tar", sums)
    with pytest.raises((SystemExit, ValueError)):
        bundle.install(str(tmp_path / "damaged.tar"), None)
    assert list((tmp_path / "states").iterdir()) == []


def test_disks_on_different_backing_chains_round_trip(tmp_path, monkeypatch):
    world, state = tmp_path / "world", tmp_path / "saved" / "usage-reporting"
    build = tmp_path / "build-tree" / "world"  # the tree that built the state, exported with --root
    base = world / "base" / "base.qcow2"
    overlay = tmp_path / "build" / "overlay.qcow2"
    for path, data in {
        base: b"base",
        overlay: b"overlay",
        state / "ready.json": b"{}\n",
        state / "starting-truth.json": b"{}\n",
        state / "manifest.json": json.dumps(dict(allnames=["app-01", "ops-helper-01"])).encode(),
        state / "disks" / "app-01.qcow2": b"app",
        state / "disks" / "ops-helper-01.qcow2": b"ops",
        state / "ram" / "app-01.gz": gzip.compress(b"guest memory" * 1000),
        build / "vm" / "id_ed25519": b"key",
        build / "vm" / "id_ed25519.pub": b"key.pub",
        build / "vm" / "build" / "app-01-seed.iso": b"iso",
        build / "vm" / "build" / "ops-helper-01-seed.iso": b"helper iso",
        build / "vm" / "build" / "seed" / "app-01" / "user-data": b"#cloud-config\n",
        state / "config.json": json.dumps(
            dict(
                seed_service_root=str(build / "vm" / "build" / "seed"),
                seeds={"app-01": str(build / "vm" / "build" / "app-01-seed.iso")},
                private_client_seed_iso=str(build / "vm" / "build" / "ops-helper-01-seed.iso"),
            )
        ).encode(),
    }.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    # app-01 sits on overlay -> base; ops-helper-01 sits on the base image directly.
    chains = {"app-01.qcow2": [overlay, base], "ops-helper-01.qcow2": [base]}
    monkeypatch.setattr(bundle, "chain", lambda disk: chains[disk.name])
    monkeypatch.setattr(bundle, "pinned_inputs", lambda: {bundle.sha256(base): "base/base.qcow2"})
    monkeypatch.setattr(bundle, "WORLD", world)
    monkeypatch.setattr(bundle, "STATES", tmp_path / "states")
    rebased = []
    monkeypatch.setattr(bundle, "rebase", lambda disk, backing: rebased.append((disk, backing)))
    out = str(tmp_path / "out.tar.zst")
    bundle.export("usage-reporting", out, [f"{build}=@WORLD@"], tmp_path / "saved")
    base.unlink()  # a fresh install has not fetched any inputs: the base image travels in the bundle
    (world / "vm").mkdir()
    (world / "vm" / "id_ed25519").write_bytes(b"other key")  # made by a revive before the install
    with pytest.raises(SystemExit, match="differs"):
        bundle.install(out, None)
    assert not (tmp_path / "states" / "usage-reporting").exists()
    (world / "vm" / "id_ed25519").unlink()
    bundle.install(out, None)
    final = tmp_path / "states" / "usage-reporting"
    assert (world / "vm" / "id_ed25519").read_bytes() == b"key"
    assert (world / "vm" / "build" / "seed" / "app-01" / "user-data").read_bytes() == b"#cloud-config\n"
    assert json.loads((final / "config.json").read_text())["seeds"]["app-01"] == str(world / "vm/build/app-01-seed.iso")
    assert (final / "backing" / "overlay.qcow2").read_bytes() == b"overlay"
    # The memory arrives as zstd for revive to read; the build's gzip copy does not travel.
    assert subprocess.check_output(["zstd", "-dc", str(final / "ram" / "app-01.zst")]) == b"guest memory" * 1000
    assert not (final / "ram" / "app-01.gz").exists()
    assert not list(tmp_path.glob(".bundle-ram-*"))
    assert not (final / "backing" / "base.qcow2").exists()
    assert base.read_bytes() == b"base"
    assert rebased == [
        (final / "backing" / "overlay.qcow2", base),
        (final / "disks" / "app-01.qcow2", final / "backing" / "overlay.qcow2"),
        (final / "disks" / "ops-helper-01.qcow2", base),
    ]
