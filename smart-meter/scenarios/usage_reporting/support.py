"""Helpers shared by the EU intake and artifact check steps."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

from build_context import OUT, lab, note, require_build, world_policy

HERE = Path(__file__).resolve().parent
BUILD = lab.ROOT
REPO = "/var/lib/company/platform-source"
EVIDENCE = OUT / "eu-intake"


def modules():
    require_build()
    return lab, world_policy


def run(host, command, timeout=120, console=False):
    require_build()
    if not console:
        return lab.ssh(host, command, timeout=timeout)
    import world_tools

    result = world_tools.console(host, command, timeout)
    assert result["exit_code"] == 0, result
    return result["stdout"]


def k(region, command, timeout=180):
    require_build()
    return subprocess.check_output(
        [
            sys.executable,
            "-c",
            "import cluster;print(cluster.kubectl(" + repr(command) + ",timeout=" + str(timeout) + "))",
        ],
        cwd=BUILD,
        env={**os.environ, "SMART_METER_REGION": region},
        timeout=timeout + 10,
        text=True,
    )


def put(host, destination, content, mode="0644"):
    require_build()
    EVIDENCE.mkdir(exist_ok=True)
    path = EVIDENCE / ("input-" + hashlib.sha256(destination.encode()).hexdigest()[:16])
    path.write_text(content)
    lab.push(host, path, destination, mode)


def push(host, source, destination, mode="0644"):
    require_build()
    lab.push(host, HERE / source, destination, mode)


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save(name, value):
    EVIDENCE.mkdir(exist_ok=True)
    (EVIDENCE / name).write_text(json.dumps(value, indent=2) + "\n")


def load(name):
    return json.loads((EVIDENCE / name).read_text())


def capacity():
    return json.loads((OUT / "resource-current.json").read_text())


def apply(region, objects):
    import yaml

    text = yaml.safe_dump_all(objects, sort_keys=False)
    path = EVIDENCE / ("apply-" + region + ".yaml")
    path.write_text(text)
    subprocess.run(
        [sys.executable, "-c", 'import cluster,sys;print(cluster.kubectl("apply -f -",stdin=sys.stdin.buffer))'],
        cwd=BUILD,
        env={**os.environ, "SMART_METER_REGION": region},
        input=text.encode(),
        check=True,
        timeout=300,
    )


def commit(message, paths):
    import shlex

    run(
        "eu-work-03",
        "git -C "
        + REPO
        + " add -- "
        + " ".join(map(shlex.quote, paths))
        + "; git -C "
        + REPO
        + " commit -m "
        + shlex.quote(message)
        + "; git -C "
        + REPO
        + " push origin HEAD:main",
    )
    note("source-change-published", message=message, paths=paths)
