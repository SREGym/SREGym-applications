"""Record each host's package inventory and dry-run the staged releases offline; nothing is installed."""

import hashlib
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from build_context import OUT, lab, note, require_build


def one(name):
    folder = OUT / "initial-package-inventories"
    folder.mkdir(exist_ok=True)
    receipt = folder / (name + ".json")
    if receipt.exists():
        row = json.loads(receipt.read_text())
        assert hashlib.sha256((folder / (name + ".tsv")).read_bytes()).hexdigest() == row["inventory_sha256"]
        return row
    command = "dpkg-query -W -f='${db:Status-Status}\\t${binary:Package}\\t${Version}\\t${Architecture}\\n'"
    # dpkg field escapes above are literal; retain installed entries only.
    native = lab.ssh(name, command, timeout=20)
    inventory = "".join(
        line.split("\t", 1)[1] + "\n" for line in native.splitlines() if line.split("\t", 1)[0] == "installed"
    )
    assert len(inventory.splitlines()) > 500, "Incomplete package inventory"
    path = folder / (name + ".tsv")
    path.write_text(inventory)
    args = ["apt-get", "-s", "--no-download", "install"]
    # Paths are fixed local package bundles in the newly installed image.
    shell = (
        "sudo -n "
        + " ".join(args)
        + " /var/cache/company/bootstrap/systemd-10/*.deb /var/cache/company/bootstrap/netplan-0.105/*.deb"
    )
    result = subprocess.run(lab.ssh_arguments(name) + [shell], capture_output=True, timeout=90, text=True)
    (folder / (name + ".dependency-simulation.txt")).write_text(result.stdout + result.stderr)
    row = dict(
        host=name,
        host_epoch=time.time(),
        inventory_sha256=hashlib.sha256(inventory.encode()).hexdigest(),
        packages=len(inventory.splitlines()),
        dependency_simulation_returncode=result.returncode,
        dependency_closure_verified=result.returncode == 0,
        command=shell,
    )
    receipt.write_text(json.dumps(row, indent=2) + "\n")
    note("package-inventory-recorded", **row)
    return row


def main():
    cfg = require_build()
    assert (OUT / "boot-complete.json").exists()
    with ThreadPoolExecutor(4) as pool:
        rows = list(pool.map(one, cfg["memory_mib"]))
    summary = dict(
        hosts=len(rows),
        all_dependency_simulations_passed=all(r["dependency_closure_verified"] for r in rows),
        complete_utc=datetime.now(UTC).isoformat(),
    )
    (OUT / "initial-package-inventories/complete.json").write_text(json.dumps(summary, indent=2) + "\n")
    note("package-inventories-complete", **summary)


if __name__ == "__main__":
    main()
