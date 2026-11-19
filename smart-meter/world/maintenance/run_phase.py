#!/usr/bin/env python3
"""Run one maintenance phase; commit only after the job succeeds."""

import argparse
import json
import os
import pwd
import shutil
import socket
import subprocess
import time
from pathlib import Path

PHASES = {
    "monitoring-initial": ("host-observability", "Publish the running host exporter"),
}
PLATFORM = Path("/var/lib/company/platform-source")
ROOT = Path("/var/lib/company/maintenance-source")


def run(*argv):
    return subprocess.check_output(argv, text=True, stderr=subprocess.STDOUT, timeout=100)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--phase", choices=PHASES, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0 or socket.gethostname() != "eu-work-03":
        raise RuntimeError("Run as root on eu-work-03")
    # Inherited Git variables would change commit dates and settings.
    if any(k.startswith("GIT_") for k in os.environ):
        raise RuntimeError("Unset the GIT_ environment variables first")
    payload = args.payload.resolve()
    name, title = PHASES[args.phase]
    repository = ROOT / name
    account = pwd.getpwnam("ubuntu")

    def git_at(directory, *parts):
        return run("runuser", "-u", "ubuntu", "--", "git", "-C", str(directory), *parts)

    def git(*parts):
        return git_at(repository, *parts)

    if not (PLATFORM / ".git").is_dir():
        raise RuntimeError("The platform checkout must exist first")
    if not repository.exists():
        repository.mkdir(parents=True)
        os.chown(repository, account.pw_uid, account.pw_gid)
        git("init", "--initial-branch=main")
        for key in ["user.name", "user.email"]:
            git("config", key, git_at(PLATFORM, "config", key).strip())
        origin = git_at(PLATFORM, "remote", "get-url", "origin").strip()
        if not origin.endswith("/operations/platform.git") or "10.72.0.23:3000/" not in origin:
            raise RuntimeError("Unexpected platform repository origin")
        git("remote", "add", "origin", origin.rsplit("/", 1)[0] + "/" + name + ".git")
    if git("status", "--porcelain").strip():
        raise RuntimeError("The repository has uncommitted changes")
    backup = Path("/var/lib/company/maintenance/rollback") / str(time.time_ns())
    backup.mkdir(parents=True)
    changes, installed = [], []

    def publish(data, relative):
        target = repository / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        os.chown(target, account.pw_uid, account.pw_gid)
        parent = target.parent
        while parent != repository:
            os.chown(parent, account.pw_uid, account.pw_gid)
            parent = parent.parent
        changes.append(relative)

    def install(data, destination, relative, mode=0o644):
        target = Path(destination)
        if target.is_symlink():
            raise RuntimeError("Refusing a symlink destination")
        saved = backup / target.relative_to("/")
        saved.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            shutil.copy2(target, saved)
        else:
            saved.with_name(saved.name + ".absent").touch()
        installed.append(destination)
        (backup / "installed.json").write_text(json.dumps(installed))
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".new")
        temporary.write_bytes(data)
        temporary.chmod(mode)
        temporary.replace(target)
        publish(data, relative)

    install((payload / "initial/host_metrics.py").read_bytes(), "/opt/company/host_metrics.py", "host_metrics.py")
    run("systemctl", "restart", "company-host-metrics.service")
    evidence = dict(unit=run("systemctl", "is-active", "company-host-metrics.service"))
    # The job's output is kept outside the public source.
    (backup / "executed.json").write_text(json.dumps(evidence, indent=2) + "\n")
    git("add", "--", *changes)
    staged = git("diff", "--cached", "--name-only").splitlines()
    if not staged or set(staged) - set(changes):
        raise RuntimeError("No meaningful source change, or unexpected staged paths")
    git("commit", "-m", title)
    print(
        json.dumps(
            dict(
                phase=args.phase,
                repository=str(repository),
                commit=git("rev-parse", "HEAD").strip(),
                rollback=str(backup),
                installed=installed,
                files=staged,
            )
        )
    )


if __name__ == "__main__":
    main()
