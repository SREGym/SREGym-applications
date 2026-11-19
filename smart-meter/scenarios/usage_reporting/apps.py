"""Install the regional cores and company surfaces on the completed clusters."""

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from build_context import OUT, lab, note, require_build

HERE = Path(__file__).resolve().parent
RUNNER = lab.ROOT / "run_step.py"


def execute(source, *args):
    row = dict(
        source=source, source_sha256=hashlib.sha256((HERE / source).read_bytes()).hexdigest(), host_epoch=time.time()
    )
    with (OUT / "executed-application-sources.jsonl").open("a") as stream:
        stream.write(json.dumps(row) + "\n")
    subprocess.run([sys.executable, "-u", "-B", str(RUNNER), str(HERE / source), *args], check=True)


def main():
    require_build()
    assert (OUT / "clusters-ready.json").exists()
    assert not lab.pid_of(lab.VM / "qualification.pid")
    (lab.VM / "qualification.pid").write_text(str(os.getpid()))
    try:
        for region in ("us", "eu"):
            marker = OUT / (region + "-core-complete.json")
            if marker.exists():
                continue
            execute("core_install.py", region)
            marker.write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
        if not (OUT / "surfaces-complete.json").exists():
            execute("surfaces.py")
            # The child surface installer owns the guard lease while
            # it is active and clears it at successful completion.
            (lab.VM / "qualification.pid").write_text(str(os.getpid()))
        if not (OUT / "eu-intake-complete.json").exists():
            execute("eu_intake.py")
            (OUT / "eu-intake-complete.json").write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
        note("applications-installed")
        (OUT / "apps-complete.json").write_text(json.dumps(dict(host_epoch=time.time())) + "\n")
    finally:
        lease = lab.VM / "qualification.pid"
        if lease.exists() and lease.read_text() == str(os.getpid()):
            lease.unlink()


if __name__ == "__main__":
    try:
        main()
    except BaseException as error:
        note("applications-error", error=repr(error))
        raise
