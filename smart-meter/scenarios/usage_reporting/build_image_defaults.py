"""Build and publish the image defaults package from its source."""

import hashlib
import json
import pathlib
import shutil
import subprocess
import tarfile

base = pathlib.Path("/var/lib/company/os-defaults")
base.mkdir(exist_ok=True)
tree = base / "1.2-1"
tree.mkdir(exist_ok=True)
files = {
    "usr/lib/systemd/networkd.conf.d/10-statistics.conf": "[Network]\nSpeedMeter=yes\nSpeedMeterIntervalSec=30s\n",
    "usr/lib/systemd/networkd.conf.d/20-defaults.conf": "[DHCPv4]\nDUIDType=vendor\n[DHCPv6]\nDUIDType=vendor\n",
    "usr/lib/systemd/resolved.conf.d/20-cache.conf": "[Resolve]\nCache=no\nReadEtcHosts=yes\n",
    "usr/lib/systemd/resolved.conf.d/30-discovery.conf": "[Resolve]\nLLMNR=no\nMulticastDNS=no\n",
    "usr/lib/systemd/journald.conf.d/10-storage.conf": "[Journal]\nStorage=persistent\nCompress=yes\n",
    "usr/lib/systemd/journald.conf.d/30-sync.conf": "[Journal]\nSyncIntervalSec=30s\n",
    "DEBIAN/control": "Package: image-os-defaults\nVersion: 1.2-1\nArchitecture: all\nMaintainer: Platform Operations <platform@smartmeter.invalid>\nDepends: systemd\nDescription: Shared runtime defaults for image-based compute instances\n",
}
for name, content in files.items():
    p = tree / name
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        assert p.read_text() == content, "Package source differs from the recorded files"
    else:
        p.write_text(content)
deb = base / "image-os-defaults_1.2-1_all.deb"
if not deb.exists():
    subprocess.run(["dpkg-deb", "--root-owner-group", "--build", str(tree), str(deb)], check=True)
assert subprocess.check_output(["dpkg-deb", "-f", str(deb), "Version"], text=True).strip() == "1.2-1"
source = base / "image-os-defaults-source.tar.gz"
if not source.exists():
    with tarfile.open(source, "w:gz") as archive:
        archive.add(tree, arcname="image-os-defaults")
store = pathlib.Path("/var/lib/company/artifact-store")
catalog = json.loads((store / "catalog.json").read_text())
for path, name in [
    (deb, "packages/image-os-defaults/" + deb.name),
    (source, "sources/image-os-defaults-source.tar.gz"),
]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    target = store / digest
    if target.exists():
        assert hashlib.sha256(target.read_bytes()).hexdigest() == digest
    else:
        shutil.copy2(path, target)
        target.chmod(0o444)
    existing = [row for row in catalog if row["name"] == name]
    if existing:
        assert len(existing) == 1 and existing[0]["sha256"] == digest
    else:
        catalog.append(dict(name=name, sha256=digest, bytes=path.stat().st_size))
temporary = store / ("catalog-" + str(__import__("os").getpid()) + ".new")
temporary.write_text(json.dumps(catalog, indent=2) + "\n")
temporary.chmod(0o444)
temporary.replace(store / "catalog.json")
print(
    json.dumps(
        {
            "package_version": "1.2-1",
            "files": list(files),
            "deb_sha256": hashlib.sha256(deb.read_bytes()).hexdigest(),
        }
    )
)
