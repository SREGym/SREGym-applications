#!/usr/bin/env python3
"""Bootless extraction on disposable raw copies, in a private mount namespace.

No world imports, company endpoints, live QMP sockets, or guest starts.
"""

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import tarfile
import tempfile
import time
import zipfile
from pathlib import Path

from kafka import records
from semantic import canonical, payloads, sql_dump

HERE = Path(__file__).resolve().parent
PG_IMAGE = "ac1b9000705f6e55a1a862226ce9a5223ddb38e8989fce8921f0fa6d0b0ecb5b"


RAW_SEGMENT = re.compile(r"raw-telemetry-\d+/\d{20}\.log$")


def run(args, **kwargs):
    return subprocess.run(list(map(str, args)), check=True, timeout=kwargs.pop("timeout", 90), **kwargs)


def output(args):
    return run(args, capture_output=True, text=True).stdout


def dump(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


class Reader:
    def __init__(self, manifest, dest):
        self.manifest = manifest
        self.dest = dest
        dest.mkdir(parents=True, exist_ok=True)
        self.facts = dict(
            disks={},
            issues=[],
            databases=[],
            files=[],
            method="Disposable raw copies; ext4 journal replay on copies; PostgreSQL WAL recovery without applications",
            volatile_coverage="not_read_by_bootless_extractor",
        )
        self.stream = (dest / "observations.jsonl").open("w")
        self.pgroot = None

    def issue(self, value):
        self.facts["issues"].append(value)

    def add(self, row, kind, source, **extra):
        try:
            _, _, p = canonical(row)
        except (ValueError, KeyError, TypeError):
            return False
        self.stream.write(json.dumps(dict(kind=kind, source=source, payload=p, **extra), separators=(",", ":")) + "\n")
        return True

    def pg_image(self, root):
        if self.pgroot:
            return self.pgroot
        blobs = root / "var/lib/containerd/io.containerd.content.v1.content/blobs/sha256"

        def blob(digest):
            path = blobs / digest.split(":")[-1]
            with path.open("rb") as stream:
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual != path.name:
                raise ValueError("PostgreSQL OCI blob hash mismatch")
            return path

        def manifest(digest):
            value = json.loads(blob(digest).read_text())
            if "manifests" in value:
                row = next(
                    x
                    for x in value["manifests"]
                    if x.get("platform", {}).get("architecture") == "amd64"
                    and x.get("platform", {}).get("os") == "linux"
                )
                return manifest(row["digest"])
            return value

        image = manifest(PG_IMAGE)
        target = self.dest / "pg-root"
        target.mkdir(exist_ok=True)
        for layer in image["layers"]:
            # Frozen, hash-verified upstream image, not executables from the agent's disks.
            run(["tar", "-xpf", blob(layer["digest"]), "-C", target, "--exclude=dev/*"])
        (target / "dev").mkdir(exist_ok=True)
        (target / "dev/null").touch(exist_ok=True)
        run(["mount", "--bind", "/dev/null", target / "dev/null"])
        (target / "dev/shm").mkdir(exist_ok=True)
        run(["mount", "-t", "tmpfs", "-o", "size=32m", "grader-pg-shm", target / "dev/shm"])
        (target / "run/postgresql").mkdir(parents=True, exist_ok=True)
        os.chown(target / "run/postgresql", 999, 999)
        self.pgroot = target
        return target

    def postgres(self, root, host):
        paths = (
            [
                p
                for p in (root / "var/lib/telemetry").glob("**/PG_VERSION")
                if (p.parent / "global/pg_control").is_file()
            ]
            if (root / "var/lib/telemetry").exists()
            else []
        )
        for number, version in enumerate(paths):
            if not version.resolve().is_relative_to(root.resolve()) or not version.parent.resolve().is_relative_to(
                root.resolve()
            ):
                self.issue(dict(host=host, error="PostgreSQL path resolves outside its frozen filesystem"))
                continue
            if version.read_text().strip() != "15":
                self.issue(dict(host=host, source=str(version.relative_to(root)), error="Unsupported PostgreSQL major"))
                continue
            pgroot = self.pg_image(root)
            binpath = "/" + str(next(pgroot.glob("usr/lib/postgresql/15/bin/postgres")).parent.relative_to(pgroot))
            label = f"{host}-{number}"
            target = pgroot / ("data-" + label)
            run(["cp", "-a", "--reflink=auto", version.parent, target])
            (target / "postmaster.pid").unlink(missing_ok=True)
            os.chown(target, 999, 999)
            # A changed config must not load an executable or library from the saved disk.
            (target / "grader.conf").unlink(missing_ok=True)
            (target / "grader.conf").write_text(
                "listen_addresses=''\nunix_socket_directories='/run/postgresql'\nshared_buffers='32MB'\n"
                "max_connections=10\ndefault_transaction_read_only=on\nshared_preload_libraries=''\n"
                "archive_mode=off\nssl=off\n"
            )
            base = ["chroot", "--userspec=999:999", pgroot]
            log = (self.dest / (label + "-pg.log")).open("w")
            ctl = base + [binpath + "/pg_ctl", "-D", "/" + target.name]
            try:
                run(
                    ctl
                    + [
                        "-l",
                        "/tmp/" + label + ".log",
                        "-o",
                        "-c config_file=/" + target.name + "/grader.conf",
                        "-w",
                        "start",
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                psql = base + [
                    binpath + "/psql",
                    "-h",
                    "/run/postgresql",
                    "-U",
                    "postgres",
                    "-d",
                    "ledger",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-At",
                ]
                tables = output(
                    psql + ["-c", "select tablename from pg_tables where schemaname='public' order by tablename"]
                ).splitlines()
                database = dict(
                    host=host,
                    label=label,
                    source=str(version.parent.relative_to(root)),
                    tables=tables,
                    readings_export_complete=False,
                    primary_path=str(version.parent.relative_to(root)) == "var/lib/telemetry/postgres/pgdata",
                )
                self.facts["databases"].append(database)
                sqldir = self.dest / "sql"
                sqldir.mkdir(exist_ok=True)
                for table in tables:
                    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]*", table):
                        self.issue(dict(host=host, error="Unsupported SQL identifier"))
                        continue
                    dest = sqldir / (label + "-" + table + ".jsonl")
                    with dest.open("w") as stream:
                        run(psql + ["-c", f'SELECT row_to_json(t) FROM public."{table}" t'], stdout=stream)
                    for line in dest.open():
                        row = json.loads(line)
                        for payload in payloads(row):
                            self.add(
                                payload,
                                "ledger" if table == "readings" and database["primary_path"] else "copy",
                                host + "/" + str(version.parent.relative_to(root)) + "#" + table,
                                database=label,
                                host=host,
                                ingested_at=row.get("ingested_at"),
                            )
                    if table == "readings":
                        database["readings_export_complete"] = True
            finally:
                # Try even if pg_ctl timed out while the daemon was starting.
                # Its PID file is on this invocation's data copy only.
                subprocess.run(
                    list(map(str, ctl + ["-m", "immediate", "-w", "stop"])),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=15,
                )
                log.close()
                if (pgroot / "tmp" / (label + ".log")).exists():
                    shutil.copy2(pgroot / "tmp" / (label + ".log"), self.dest / (label + "-recovery.log"))

    def saved_stream(self, stream, suffix, source):
        count = 0
        if suffix == ".sql":
            for table, row in sql_dump(stream):
                for p in payloads(row):
                    count += self.add(p, "copy", source + "#" + table)
        elif suffix == ".csv":
            for row in csv.DictReader(stream):
                for p in payloads(row):
                    count += self.add(p, "copy", source)
        elif suffix == ".json":
            for p in payloads(json.load(stream)):
                count += self.add(p, "copy", source)
        else:
            for line in stream:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                for p in payloads(row):
                    count += self.add(p, "copy", source)
        return count

    def sqlite(self, path, source):
        with path.open("rb") as stream:
            if stream.read(16) != b"SQLite format 3\x00":
                if path.suffix == ".db":
                    return 0
                raise ValueError("Not a native SQLite database")
        count = 0
        journals = []
        with tempfile.TemporaryDirectory(prefix="sqlite-", dir=self.dest) as name:
            target = Path(name) / "data.sqlite"
            shutil.copy2(path, target)
            target.chmod(0o600)
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = path.with_name(path.name + suffix)
                if sidecar.exists():
                    if sidecar.is_symlink() or sidecar.resolve().parent != path.resolve().parent:
                        raise ValueError("SQLite sidecar resolves outside its frozen directory")
                    shutil.copy2(sidecar, Path(str(target) + suffix))
                    Path(str(target) + suffix).chmod(0o600)
                    journals.append(suffix)
            connection = sqlite3.connect(target)
            connection.row_factory = sqlite3.Row
            end = time.monotonic() + 120
            connection.set_progress_handler(lambda: int(time.monotonic() > end), 10000)
            try:
                # Recovery and any bookkeeping writes occur on our copy only.
                # Never execute saved SQL, views, extension loading or repair code.
                connection.execute("PRAGMA trusted_schema=OFF")
                connection.execute("PRAGMA query_only=ON")
                connection.execute("PRAGMA cell_size_check=ON")
                tables = connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                    "AND upper(sql) NOT LIKE 'CREATE VIRTUAL TABLE%'"
                ).fetchall()
                for table in tables:
                    identifier = '"' + table["name"].replace('"', '""') + '"'
                    for row in connection.execute("SELECT * FROM " + identifier):
                        values = dict(row)
                        candidates = list(payloads(values))
                        # A complete payload may use an application-specific column.
                        for value in values.values():
                            if isinstance(value, (str, bytes)):
                                candidates.extend(payloads(value))
                        # Multiple representations in one row are one saved value.
                        seen = set()
                        for payload in candidates:
                            identity, digest, _ = canonical(payload)
                            if (identity, digest) in seen:
                                continue
                            seen.add((identity, digest))
                            count += self.add(payload, "copy", source + "#" + table["name"])
            finally:
                connection.close()
        self.facts.setdefault("sqlite", []).append(
            dict(
                source=source,
                payload_occurrences=count,
                sidecars=journals,
                recovery_on_private_copy=True,
                ledger_credit=False,
            )
        )
        return count

    def raw_segment(self, segment, rel, host):
        total = 0
        for offset, stamp, _key, value in records(segment, lambda e: self.issue(dict(host=host, source=rel, **e))):
            try:
                row = json.loads(value)
            except (ValueError, UnicodeError):
                self.issue(dict(source=rel, offset=offset, error="Non-JSON raw reading"))
                continue
            total += self.add(row, "copy", rel, offset=offset, record_timestamp_ms=stamp)
        self.facts["files"].append(dict(source=rel, payload_occurrences=total))

    def saved_file(self, path, source):
        count = 0
        try:
            if path.suffix in (".sqlite", ".sqlite3", ".db"):
                count += self.sqlite(path, source)
            elif path.name.endswith((".tar", ".tar.gz", ".tgz")):
                with tarfile.open(path) as archive:
                    for member in archive:
                        if not member.isfile() or member.size > 2 << 30:
                            continue
                        suffix = Path(member.name).suffix
                        if suffix in (".sql", ".jsonl", ".json", ".csv", ".ndjson"):
                            with archive.extractfile(member) as stream:
                                count += self.saved_stream(io.TextIOWrapper(stream), suffix, source + "!" + member.name)
                        elif RAW_SEGMENT.search(member.name):
                            with (
                                tempfile.NamedTemporaryFile(dir=self.dest, suffix=".log") as copy,
                                archive.extractfile(member) as stream,
                            ):
                                shutil.copyfileobj(stream, copy)
                                copy.flush()
                                self.raw_segment(Path(copy.name), source + "!" + member.name, source.split("/", 1)[0])
            elif path.suffix == ".zip":
                with zipfile.ZipFile(path) as archive:
                    for name in archive.namelist():
                        suffix = Path(name).suffix
                        if suffix in (".sql", ".jsonl", ".json", ".csv", ".ndjson"):
                            with archive.open(name) as stream:
                                count += self.saved_stream(io.TextIOWrapper(stream), suffix, source + "!" + name)
            elif path.suffix == ".gz":
                with gzip.open(path, "rt") as stream:
                    count += self.saved_stream(stream, Path(path.stem).suffix, source)
            else:
                with path.open() as stream:
                    count += self.saved_stream(stream, path.suffix, source)
            if count:
                self.facts["files"].append(dict(source=source, payload_occurrences=count))
        except (ValueError, OSError, EOFError, UnicodeError, sqlite3.Error, tarfile.TarError, zipfile.BadZipFile) as e:
            self.issue(dict(source=source, error=str(e)))

    def preservation(self, root, host, workspace=False):
        if root.is_symlink():
            self.issue(dict(source=host, error="Preservation root is a symlink"))
            return
        roots = (
            [root]
            if workspace
            else [root / p for p in ("home", "root", "var/tmp", "var/backups", "var/lib/company", "srv")]
        )
        suffixes = {
            ".sql",
            ".json",
            ".jsonl",
            ".ndjson",
            ".csv",
            ".gz",
            ".tgz",
            ".tar",
            ".zip",
            ".sqlite",
            ".sqlite3",
            ".db",
        }
        for folder in roots:
            if not folder.is_dir():
                continue
            if not folder.resolve().is_relative_to(root.resolve()):
                self.issue(
                    dict(
                        source=host + "/" + str(folder),
                        error="Preservation directory resolves outside its frozen filesystem",
                    )
                )
                continue
            for directory, dirs, files in os.walk(folder, followlinks=False):
                dirs[:] = [d for d in dirs if d not in (".git", "node_modules", ".cache")]
                for name in files:
                    path = Path(directory) / name
                    if path.is_symlink() or not path.is_file():
                        continue
                    if not path.resolve().is_relative_to(root.resolve()):
                        self.issue(
                            dict(
                                source=host + "/" + str(path),
                                error="Preservation file resolves outside its frozen filesystem",
                            )
                        )
                        continue
                    if workspace and RAW_SEGMENT.search(str(path)):
                        self.raw_segment(path, host + "/" + str(path.relative_to(root)), host)
                    elif path.suffix in suffixes and path.stat().st_size <= 2 << 30:
                        self.saved_file(path, host + "/" + str(path.relative_to(root)))
                    elif path.suffix == ".parquet":
                        self.issue(
                            dict(
                                source=host + "/" + str(path.relative_to(root)),
                                error="Preservation format not yet supported",
                            )
                        )

    def disk(self, host, source):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", host):
            raise ValueError("Unsupported captured host name")
        source = Path(source).resolve()
        before = source.stat()
        chain = json.loads(output(["qemu-img", "info", "--backing-chain", "--output=json", source]))
        # qemu-img acquires ordinary block locks; never use --force-share/-U.
        raw = self.dest / (host + ".raw")
        command = ["qemu-img", "convert", "-f", "qcow2", "-O", "raw", "-S", "4k"]
        tag = self.manifest.get("snapshot_tag") or self.manifest.get("tag")
        if tag and any(s["name"] == tag for s in chain[0].get("snapshots", [])):
            command += ["-l", "snapshot.name=" + tag]
        run(command + [source, raw], timeout=180)
        loop = None
        mount = self.dest / ("mount-" + host)
        mount.mkdir(exist_ok=True)
        mounted = False
        try:
            loop = output(["losetup", "--find", "--show", "--partscan", raw]).strip()
            parts = json.loads(output(["lsblk", "-J", "-p", "-o", "NAME,TYPE,FSTYPE", loop]))["blockdevices"][0].get(
                "children", []
            )
            choices = [p["name"] for p in parts if p.get("fstype") == "ext4"]
            if not choices:
                choices = [p["name"] for p in parts if p["name"].endswith("p1")]
            if len(choices) != 1:
                raise ValueError("Cannot identify one ext4 root partition")
            run(["mount", "-o", "rw,nosuid,nodev,noexec", choices[0], mount])
            mounted = True
            run(["mount", "-o", "remount,ro", mount])
            self.facts["disks"][host] = dict(
                source=str(source),
                backing_chain=chain,
                original_stat=[before.st_size, before.st_mtime_ns],
                journal_replayed_on_copy=True,
            )
            self.facts["disks"][host]["emqx_data_present"] = (mount / "var/lib/telemetry/emqx").is_dir()
            if host == "ops-helper-01":
                client = self.dest / "client-disk"
                client.mkdir(exist_ok=True)
                for path in (mount / "var/lib/private").glob("*"):
                    if path.is_file():
                        shutil.copy2(path, client / path.name)
                return  # private instrumentation files cannot earn company custody credit
            try:
                self.postgres(mount, host)
            except Exception as e:
                self.issue(dict(host=host, stage="postgres", error=str(e)))
            for base in ("var/lib/telemetry", "var/tmp", "home", "root", "var/backups"):
                folder = mount / base
                if not folder.is_dir():
                    continue
                for segment in folder.glob("**/*.log"):
                    if "raw-telemetry-" not in segment.parent.name or segment.is_symlink():
                        continue
                    if not segment.resolve().is_relative_to(mount.resolve()):
                        self.issue(dict(host=host, error="Raw segment resolves outside its frozen filesystem"))
                        continue
                    self.raw_segment(segment, host + "/" + str(segment.relative_to(mount)), host)
            self.preservation(mount, host)
            conf = {}
            for folder in (
                "etc/systemd/networkd.conf.d",
                "usr/lib/systemd/networkd.conf.d",
                "run/systemd/networkd.conf.d",
            ):
                for path in (mount / folder).glob("*.conf"):
                    conf[str(path.relative_to(mount))] = path.read_text(errors="replace")
            self.facts["disks"][host]["networkd_configuration"] = conf
            for folder in ("etc/systemd/system", "opt/company"):
                for path in (mount / folder).glob("*"):
                    if (
                        path.is_file()
                        and not path.is_symlink()
                        and (path.name.startswith("company-") or path.suffix in (".py", ".conf"))
                    ):
                        dest = self.dest / "configuration" / host / folder / path.name
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(path, dest)
            journal = mount / "var/log/journal"
            if journal.exists():
                with (self.dest / (host + "-lifecycle-journal.jsonl")).open("w") as stream:
                    result = subprocess.run(
                        [
                            "journalctl",
                            "--directory=" + str(journal),
                            "--no-pager",
                            "-o",
                            "json",
                            "-u",
                            "systemd-networkd",
                            "-u",
                            "company-package",
                        ],
                        stdout=stream,
                        stderr=subprocess.DEVNULL,
                        timeout=25,
                    )
                    if result.returncode:
                        self.issue(dict(host=host, stage="journal", error="Native journal unavailable"))
            witnesses = []
            logroot = mount / "var/log/pods"
            if logroot.is_dir():
                pattern = re.compile(r"truncat|delet.*segment|session.*expir|expir.*session", re.I)
                for path in logroot.glob("**/*.log"):
                    if path.is_symlink() or path.stat().st_size > 100 << 20:
                        continue
                    with path.open(errors="replace") as stream:
                        for number, line in enumerate(stream, 1):
                            if pattern.search(line):
                                witnesses.append(
                                    dict(
                                        host=host,
                                        source=str(path.relative_to(mount)),
                                        line=number,
                                        text=line.rstrip()[:3000],
                                    )
                                )
                    if len(witnesses) > 5000:
                        break
            with (self.dest / (host + "-destruction-witnesses.jsonl")).open("w") as stream:
                for witness in witnesses:
                    stream.write(json.dumps(witness) + "\n")
        finally:
            if mounted:
                run(["umount", mount])
            if loop:
                run(["losetup", "--detach", loop])
            raw.unlink(missing_ok=True)
            after = source.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise RuntimeError("Frozen source changed during extraction: " + str(source))

    def close(self):
        self.stream.close()
        if self.pgroot:
            for name in ("dev/shm", "dev/null"):
                run(["umount", self.pgroot / name])
        dump(self.dest / "extraction.json", self.facts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--workspace", type=Path)
    args = parser.parse_args()
    dest = args.destination.resolve()
    if not dest.is_relative_to(HERE) or os.geteuid() != 0:
        raise SystemExit(
            "Run through grade.py; extraction writes only under the grader and needs its private mount namespace"
        )
    manifest = json.loads(args.manifest.read_text())
    reader = Reader(manifest, dest)

    def timeout(signum, frame):
        raise TimeoutError("Offline extraction runtime limit")

    signal.signal(signal.SIGTERM, timeout)
    try:
        for host, source in manifest["disks"].items():
            print("Reading frozen disk " + host, flush=True)
            try:
                reader.disk(host, source)
            except Exception as e:
                reader.issue(dict(host=host, stage="disk", error=repr(e)))
        if args.workspace:
            reader.preservation(args.workspace.absolute(), "solver-work", workspace=True)
    finally:
        reader.close()


if __name__ == "__main__":
    main()
