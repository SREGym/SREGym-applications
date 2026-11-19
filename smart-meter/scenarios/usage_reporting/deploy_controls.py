"""Install the image defaults and maintenance controls."""

import hashlib
import json
import shlex
import time
from pathlib import Path

from build_context import OUT, lab, note, require_build

HERE = Path(__file__).resolve().parent
REPO = "/var/lib/company/platform-source"


def install():
    cfg = require_build()
    if (OUT / "controls-complete.json").exists():
        return
    assert (OUT / "apps-complete.json").exists() and not (OUT / "runtime-ready.json").exists()
    if not (OUT / "image-defaults-package-build.json").exists():
        script = HERE / "build_image_defaults.py"
        lab.push("eu-work-03", script, "/run/build-image-defaults.py")
        # Build the package on the host from its source.
        package = lab.ssh("eu-work-03", "python3 /run/build-image-defaults.py", timeout=60)
        (OUT / "image-defaults-package-build.json").write_text(
            json.dumps(dict(output=package, recipe_sha256=hashlib.sha256(script.read_bytes()).hexdigest())) + "\n"
        )
        note("image-defaults-published", version="1.2-1")
    for local, remote in [
        ("maintain.py", "fleet/maintain.py"),
        ("package.py", "fleet/package.py"),
        ("schedule.py", "fleet/schedule.py"),
        ("rollout.py", "fleet/rollout.py"),
    ]:
        lab.push("eu-work-03", HERE / local, REPO + "/" + remote)
    # Both published aliases run the same cached runtime installation.
    lab.push("eu-work-03", HERE / "node-setup.sh", REPO + "/fleet/runtime-install.sh")
    lab.ssh(
        "eu-work-03",
        "git -C "
        + REPO
        + " add fleet; git -C "
        + REPO
        + " diff --cached --quiet || git -C "
        + REPO
        + ' commit -m "Deploy image defaults and host maintenance tooling"; git -C '
        + REPO
        + " push origin HEAD:main",
        timeout=60,
    )
    release = json.loads((HERE / "platform-release-prepared.json").read_text())
    manifest = "".join(row["sha256"] + "  " + row["filename"] + "\n" for row in release["packages"])
    sums = OUT / "platform-release-SHA256SUMS"
    sums.write_text(manifest)
    for host in cfg["memory_mib"]:
        marker = OUT / ("controls-" + host + ".json")
        if marker.exists():
            continue
        code = "import hashlib,json,pathlib,urllib.request;root='http://10.72.0.23:8091/';rows=json.load(urllib.request.urlopen(root+'catalog.json'));row=next(r for r in rows if r['name']=='packages/image-os-defaults/image-os-defaults_1.2-1_all.deb');data=urllib.request.urlopen(root+row['sha256']).read();assert len(data)==row['bytes'] and hashlib.sha256(data).hexdigest()==row['sha256'];p=pathlib.Path('/var/cache/company/image-runtime/image-os-defaults.deb');p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)"
        lab.ssh(
            host,
            "sudo -n python3 -c "
            + shlex.quote(code)
            + "; sudo -n dpkg -i /var/cache/company/image-runtime/image-os-defaults.deb; sudo -n systemctl restart systemd-resolved systemd-journald; sudo -n journalctl --flush",
            timeout=60,
        )
        for name in ["maintain.py", "package.py", "schedule.py"]:
            lab.push(host, HERE / name, "/opt/company/" + name)
        for row in release["packages"]:
            path = HERE.parents[1] / row["source"]
            # Cached package metadata paths are relative to the project root.
            assert path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == row["sha256"]
            lab.push(host, path, "/var/cache/company/releases/current/" + row["filename"])
        lab.push(host, sums, "/var/cache/company/releases/current/SHA256SUMS")
        marker.write_text(json.dumps(dict(host_epoch=time.time(), version="1.2-1")) + "\n")
        note("host-controls-installed", host=host, version="1.2-1")
    (OUT / "controls-complete.json").write_text(
        json.dumps(dict(host_epoch=time.time(), hosts=len(cfg["memory_mib"]))) + "\n"
    )


if __name__ == "__main__":
    install()
