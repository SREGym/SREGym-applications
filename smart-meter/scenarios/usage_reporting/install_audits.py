"""Install the isolated artifact check on each active host."""

import json
import time
from pathlib import Path

from build_context import OUT, lab, note, require_build
from support import commit

HERE = Path(__file__).resolve().parent


def main():
    cfg = require_build()
    assert not lab.pid_of(lab.VM / "qualification.pid"), "Another build step is running"
    folder = OUT / "artifact-validation"
    folder.mkdir(exist_ok=True)
    if not (folder / "published.json").exists():
        for name, target in [
            ("artifact_audit.py", "services/artifact_audit.py"),
            ("wireguard_link.py", "services/wireguard_link.py"),
            ("install_audit.py", "fleet/install_artifact_audit.py"),
        ]:
            lab.push("eu-work-03", HERE / name, "/var/lib/company/platform-source/" + target)
        commit(
            "Deploy isolated artifact-cache validation",
            ["services/artifact_audit.py", "services/wireguard_link.py", "fleet/install_artifact_audit.py"],
        )
        (folder / "published.json").write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
    completed = []
    stopped = []
    for host in cfg["memory_mib"]:
        marker = folder / (host + ".json")
        if marker.exists():
            completed.append(host)
            continue
        if not lab.pid_of(lab.VM / (host + ".pid")):
            stopped.append(host)
            continue
        before = lab.ssh(host, "ip -4 rule; systemctl show systemd-networkd -p InvocationID", timeout=15)
        (folder / (host + "-before.txt")).write_text(before)
        for name in ["artifact_audit.py", "wireguard_link.py"]:
            lab.push(host, HERE / name, "/opt/company/" + name)
        lab.push(host, HERE / "install_audit.py", "/run/install-artifact-audit.py")
        native = lab.ssh(
            host,
            "sudo -n python3 /run/install-artifact-audit.py; systemctl is-active company-artifact-link company-artifact-cache; sudo -n cat /var/lib/company/operations/artifact-audit.json",
            timeout=90,
        )
        (folder / (host + "-native.txt")).write_text(native)
        after = lab.ssh(host, "ip -4 rule; systemctl show systemd-networkd -p InvocationID", timeout=15)
        (folder / (host + "-after.txt")).write_text(after)
        original = {line.strip() for line in before.splitlines() if line.strip()}
        assert original.issubset({line.strip() for line in after.splitlines()}), (host, before, after)
        assert "198.18.0." in after and '"artifacts": [' in native, (host, native, after)
        marker.write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
        completed.append(host)
        note("artifact-check-installed", host=host)
    (folder / "current-coverage.json").write_text(
        json.dumps(dict(completed=completed, stopped_pending=stopped), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
