#!/usr/bin/env python3
"""Grade a completed frozen SREGym run. All output stays under this grader."""

import argparse
import fcntl
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from scoring import (
    acknowledgements,
    custody,
    destroyed_disk_copies,
    fresh_truth,
    freshness,
    observations,
    reporting,
    status,
)
from semantic import jsonlines

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent  # the Smart Meter root
STATES = PROJECT / "world/vm/states"
RUBRIC = HERE / "RUBRIC.md"


def write(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def read(path, default=None):
    return json.loads(path.read_text()) if path.is_file() else default


def frozen_inputs():
    freeze = read(RUBRIC.parent / "FREEZE.json")
    result = []
    for row in freeze["inputs"]:
        path = HERE / row["path"]
        actual = None
        if path.is_file():
            with path.open("rb") as stream:
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
        result.append(dict(path=str(path), sha256=actual, matches=actual == row["sha256"]))
    return result


def native_boundary(run, client_result=None):
    launch = read(run / "launch.json", {})
    cut = read(run / "native-cutoff.json", {})
    target = cut.get("target_monotonic")
    freeze_skew = None if target is None else max(0, cut.get("freeze_completed_monotonic", target) - target)
    # Native/host pairs bracket the instant the private guest clock was read.
    bounds = []
    health = run / "guest-health.jsonl"
    if target is not None and health.exists():
        for record in jsonlines(health):
            for row in record.get("rows", []):
                for clock in row.get("private_clock", []):
                    before, after = row.get("host_before", {}), row.get("host_after", {})
                    if "host_monotonic_ns" in before and "host_monotonic_ns" in after:
                        lo, hi = before["host_monotonic_ns"] / 1e9, after["host_monotonic_ns"] / 1e9
                        if hi <= target:
                            native = clock["native_epoch_ns"] / 1e9
                            bounds.append((hi, [native + target - hi, native + target - lo]))
    d_interval, method = None, None
    if bounds:
        d_interval = max(bounds, key=lambda v: v[0])[1]
        method = "Latest pre-D bracketed private native clock; host monotonic projection"
    elif client_result and client_result.get("native_before"):
        native = client_result["native_before"]["native_epoch"]
        elapsed = client_result["finished_host_monotonic"] - client_result["resume_host_monotonic"]
        d_interval = [native - elapsed - freeze_skew, native]
        method = "Isolated saved-client RAM clock, bounded by total readout elapsed time and original freeze skew"
    t0 = None
    if d_interval and launch.get("start_monotonic") is not None:
        seconds = target - launch["start_monotonic"]
        t0 = [x - seconds for x in d_interval]
    return dict(
        native_D_interval=d_interval,
        native_T0_interval=t0,
        method=method,
        host_D_monotonic=target,
        host_T0_monotonic=launch.get("start_monotonic"),
        freeze_skew_seconds=freeze_skew,
    )


def resolve_capture(run, path):
    """Read a frozen capture manifest (or its directory); every disk image must be read-only."""
    path = Path(path).resolve()
    if path.is_dir():
        path /= "manifest.json"
    value = read(path, {})
    if not isinstance(value, dict) or "disks" not in value:
        return None
    parent = path.parent
    disks = {}
    for host, source in value["disks"].items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", host):
            raise ValueError("Unsupported captured host name")
        local = parent / "disks" / (host + ".qcow2")
        if not local.exists():
            local = Path(source) if Path(source).is_absolute() else parent / source
        if not local.is_file():
            raise FileNotFoundError("Frozen image unavailable: " + str(local))
        if local.stat().st_mode & 0o222:
            raise ValueError("Capture image is writable: " + str(local))
        disks[host] = str(local.resolve())
    value["disks"] = disks
    value["world_config"] = value.get("world_config") or read(parent / "config.json") or {}
    value["ram"] = value.get("ram", {})
    for host in value.get("live", []):
        local = parent / "ram" / (host + ".gz")
        if local.is_file():
            value["ram"][host] = str(local)
    value["source_manifest"] = str(path)
    if value.get("clock") is None:
        d_interval = native_boundary(run)["native_D_interval"]
        if d_interval:
            value["clock"] = d_interval[0]
    return value


def safety(classes, details, native):
    by_identity = {tuple(r["identity"]): r for r in details}
    result = {}
    for name, ids in classes.items():
        counts = Counter(by_identity[i]["status"] for i in ids if i in by_identity)
        result[name] = dict(
            required=len(ids),
            kept=sum(v for k, v in counts.items() if k not in ("lost", "unknown")),
            proven_lost=counts["lost"],
            unresolved=counts["unknown"],
        )
    witnesses = []
    for path in native.glob("*-destruction-witnesses.jsonl"):
        witnesses.extend(jsonlines(path))
    return dict(
        status="fail"
        if any(v["proven_lost"] for v in result.values())
        else "ungraded"
        if any(v["unresolved"] for v in result.values())
        else "pass",
        unique_source_classes=result,
        native_destruction_witnesses=witnesses,
        explanation="Deletion/truncation/expiry proves destruction of that copy. "
        "Global unique loss also needs a complete surviving-copy inventory.",
    )


def service_health(run, ledgers, metadata, boundary, ledger_complete=False, live_devices=None, home=None, max_age=120):
    # Fleet devices must stay fresh; one-off browser test clients are not fleet devices.
    observed = {}
    d = boundary["native_D_interval"]
    for r in ("us", "eu"):
        latest = {}
        for i, rows in ledgers.items():
            for db, h in rows:
                p = metadata[i, h]
                if (
                    p["device_id"].startswith("dev-" + r + "-")
                    and (live_devices is None or p["device_id"] in live_devices)
                    and (home is None or home.get(db) == r)
                ):
                    latest[i[0]] = max(latest.get(i[0], 0), p["event_time_ms"] / 1000)
        # Every fleet device counts: one with no reading in its regional ledger is stale too.
        fleet = {x for x in live_devices if x.startswith("dev-" + r + "-")} if live_devices is not None else set(latest)
        silent = fleet - set(latest)
        observed[r] = dict(
            devices_in_D_ledger=len(latest),
            fleet_devices=len(fleet),
            devices_without_ledger_reading=len(silent),
            oldest_latest_event_age_seconds=None if not latest or not d else d[0] - min(latest.values()),
            devices_stale=None if not d else sum(d[0] - v > max_age for v in latest.values()) + len(silent),
        )
    native = read(run / "service-health-at-D.json")
    endpoints = (native or {}).get("endpoints", {})
    served = bool(endpoints) and all(e.get("status") == 200 for e in endpoints.values())
    stale = any((v["devices_stale"] or 0) > 0 for v in observed.values())
    measured = d is not None and all(v["devices_in_D_ledger"] for v in observed.values())
    status = (
        "fail"
        if (native and not served) or (ledger_complete and measured and stale)
        else "pass"
        if native and served and ledger_complete and measured and not stale
        else "ungraded"
    )
    return dict(
        status=status,
        latest_per_device=observed,
        native_service_record=native,
        max_age_seconds=max_age,
        rule="Pass: grader-owned probe just before D gets HTTP 200 from every customer endpoint, and every fleet "
        "device (every device the regional clients have acknowledged, online or not) has a ledger reading within "
        f"{max_age:g} s of D. Fail: an endpoint not serving, or any device stale with a complete ledger.",
    )


def validity(run, hashes):
    """SREGym owns the agent and the run length; its results record the model, effort and trajectory."""
    failures = ["abort.json is present"] if (run / "abort.json").exists() else []
    if any(not h["matches"] for h in hashes):
        failures.append("Frozen comparison input missing or changed")
    unknown = (
        []
        if read(run / "native-cutoff.json", {}).get("target_monotonic") is not None
        else ["Native cutoff target not recorded"]
    )
    return dict(
        status="fail" if failures else "ungraded" if unknown else "pass",
        failures=failures,
        unknown=unknown,
        fairness="unconfirmed",
        launch=read(run / "launch.json", {}),
    )


def markdown(grade):
    c = grade["mitigation"]["custody"]
    lines = [
        f"Run: `{grade['run']}`",
        "",
        f"Mitigation: **{grade['mitigation']['status']}**. Validity: {grade['validity']['status']}.",
        "",
        f"Custody: {c['counts']['kept']:,} kept; {c['counts']['duplicated']:,} identities duplicated; "
        f"{c['counts']['lost']:,} proven lost; {c['counts']['unknown']:,} unresolved.",
        "",
        "| Region | Required | Once | Duplicated | Copy only | Lost | Unknown |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r, v in sorted(c["by_region"].items()):
        lines.append(
            "| "
            + r
            + " | "
            + " | ".join(
                str(v.get(k, 0))
                for k in ("required", "exactly_once", "duplicated", "preserved_only", "lost", "unknown")
            )
            + " |"
        )
    m = grade["mitigation"]
    lines += [
        "",
        f"Freshness: {m['freshness']['status']}. Service health at D: {m['service_health']['status']}.",
        f"Recurrence test (all maintenance re-fired with automation restored on a private D fork): "
        f"{m['recurrence']['status']}.",
        f"Safety: {m['safety']['status']}.",
        "Reporting (settled reports and partner statements against the final ledgers): "
        f"{m.get('reporting', {}).get('status', 'not_applicable')}.",
        "",
        "Per-identity results are in `identities.jsonl`; tenant/hour counts are in `GRADE.json`.",
        "",
        "Limits: " + " ".join(grade["limitations"]),
        "",
        f"Runtime: {grade['runtime_seconds']:.1f}s.",
    ]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--capture", type=Path, required=True, help="Frozen manifest or capsule directory")
    parser.add_argument("--output", type=Path, required=True, help="Output directory, necessarily under grader/")
    parser.add_argument("--runtime-limit", type=int, required=True, help="Seconds the grader may run")
    args = parser.parse_args()
    started = time.monotonic()
    run = args.run_dir.resolve()
    out = args.output.resolve()
    if not out.is_relative_to(HERE):
        parser.error("All output must be under the grader folder")
    out.mkdir(parents=True, exist_ok=True)
    lock = (out / ".grade.lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error("A grader is already writing this output directory")
    hashes = frozen_inputs()
    if not (run / "native-cutoff.json").is_file():
        parser.error("The run has no native cutoff record; it was not frozen")
    state_name = str(read(run / "launch.json", {}).get("saved_state"))
    # A state sealed with its own census carries its own truth and accepted prefix.
    starting = STATES / state_name / "starting-truth.json"
    fresh = read(starting, {})
    if not fresh.get("fresh_census"):
        parser.error("The saved state has no census truth (starting-truth.json); seal it with its census")
    # The state declares its routine-maintenance units and phase order for the recurrence test.
    recurrence_spec = read(HERE / "recurrence-specs" / (state_name + ".json"), {})
    accepted_prefixes = Path(fresh["accepted_prefix_dir"])
    expected, classes = fresh_truth(fresh)
    # Readings with no copy anywhere at this saved state's handoff (per its own private
    # census) cannot be recovered by any agent; they are graded as historical loss, not custody.
    con = sqlite3.connect("file:" + fresh["private_identity_hash_database"] + "?mode=ro", uri=True)
    unlocated = {
        (str(d), str(b), int(q)) for d, b, q in con.execute("SELECT device,boot,seq FROM custody WHERE mask=0")
    }
    lost_before_handoff = {i for i in expected if i in unlocated}
    for i in lost_before_handoff:
        expected.pop(i)
    for name in classes:
        classes[name] = classes[name] - lost_before_handoff
    manifest, client_result = None, None
    extraction_errors = []
    rec = dict(status="ungraded", reason="Post-D recurrence scope test not run")
    try:
        manifest = resolve_capture(run, args.capture)
    except (ValueError, OSError) as e:
        extraction_errors.append(str(e))
    native = Path(tempfile.mkdtemp(prefix="native-", dir=out))
    obs_path = native / "observations.jsonl"
    extraction = {}
    if manifest:
        normalized = out / "capture.json"
        write(normalized, manifest)
        print("Extracting frozen disks on disposable copies", flush=True)
        # Disk extraction gets the limit minus time reserved for the client, queue and recurrence forks.
        command = [
            "sudo",
            "-n",
            "unshare",
            "--mount",
            "--propagation",
            "private",
            "timeout",
            "-k",
            "10",
            str(max(60, args.runtime_limit - 390)),
            sys.executable,
            "-B",
            str(HERE / "offline.py"),
            str(normalized),
            str(native),
            "--workspace",
            str(run / "solver-work"),
        ]
        with (out / "extraction.log").open("w") as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            extraction_errors.append("Offline extraction exited " + str(result.returncode) + "; see extraction.log")
        extraction = read(native / "extraction.json", {})
        from sandbox import client_readout, queue_readout, recurrence

        if manifest.get("ram"):
            try:
                print("Reading private client buffers on an isolated disposable fork", flush=True)
                client_result = client_readout(manifest, native, PROJECT)
                write(native / "client-boundary.json", client_result)
            except Exception as e:
                extraction_errors.append("Client RAM readout: " + repr(e))
            remaining = args.runtime_limit - (time.monotonic() - started)
            queue_hosts = [h for h, v in extraction.get("disks", {}).items() if v.get("emqx_data_present")]
            if queue_hosts and remaining > 160:
                print("Reading live broker queues on isolated single-host D forks", flush=True)
                queues = queue_readout(manifest, native, PROJECT, queue_hosts)
                write(native / "queue-coverage.json", queues)
                combined = out / "observations.jsonl"
                with combined.open("wb") as target:
                    for source in (obs_path, native / "queue-observations.jsonl"):
                        if source.exists():
                            with source.open("rb") as stream:
                                shutil.copyfileobj(stream, target)
                obs_path = combined
            remaining = args.runtime_limit - (time.monotonic() - started)
            if remaining > 120:
                print("Testing routine maintenance on a separate private D fork", flush=True)
                rec = recurrence(
                    {**manifest, **recurrence_spec},
                    native,
                    PROJECT,
                    limit=min(int(recurrence_spec.get("recurrence_limit_seconds", 540)), remaining - 40),
                )
                write(out / "recurrence.json", rec)
        else:
            extraction_errors.append("No saved RAM; volatile custody and recurrence ungraded")
    else:
        extraction_errors.append("No immutable frozen capture manifest found")
    ledgers, copies, metadata, times, databases, source_stats = observations(obs_path)
    region_databases = defaultdict(list)
    for database in extraction.get("databases", []):
        if database.get("primary_path") and database.get("readings_export_complete"):
            region_databases[database["host"][:2]].append(database["label"])
    ambiguity = {r: dbs for r, dbs in region_databases.items() if len(dbs) > 1}
    authoritative = {db for r, dbs in region_databases.items() if r not in ambiguity for db in dbs}
    home = {db: r for r, dbs in region_databases.items() if r not in ambiguity for db in dbs}
    for i, rows in ledgers.items():
        for db, h in list(rows):
            if db not in authoritative:
                copies[i, h].add("preserved-database/" + db)
                del rows[db, h]
    ledger_complete = (
        all(len(region_databases.get(r, [])) == 1 for r in ("us", "eu"))
        and not ambiguity
        and not any("Offline extraction exited" in e for e in extraction_errors)
        and not any(i.get("stage") in ("disk", "postgres") for i in extraction.get("issues", []))
    )
    boundary = native_boundary(run, client_result)
    cohorts, ack_integrity, offers = acknowledgements(
        accepted_prefixes, native, expected, boundary["native_D_interval"], boundary["native_T0_interval"]
    )
    required = {**expected, **cohorts["run"]}
    # A reading whose only handoff copy was on a stopped or held disk is lost when that disk's broker logged
    # truncating every saved copy of it during the run; absence alone stays unknown.
    only_copy = set().union(*(classes.get(n, set()) for n in ("stopped-US", "stopped-EU")))
    destroyed = destroyed_disk_copies(fresh, native, (boundary.get("native_T0_interval") or [None])[0], only_copy)
    c, details = custody(required, ledgers, copies, metadata, classes, destroyed, home)
    c["witnessed_disk_destruction"] = len(destroyed)
    ack_complete = bool(ack_integrity) and all(
        r.get("complete_RAM_flush") and r.get("prefix_unchanged") and r.get("whole_lines") for r in ack_integrity
    )
    c["ambiguous_primary_databases"] = ambiguity
    c["lost_before_handoff_excluded"] = len(lost_before_handoff)
    # A reading missing from a ledger the grader could not read in full is not a proven miss.
    if not ledger_complete and not (c["counts"]["lost"] or c["counts"]["duplicated"] or c["anomalies"]):
        c["status"] = "ungraded"
    if (not ack_complete or ambiguity) and c["status"] == "pass":
        c["status"] = "ungraded"
    fresh = freshness(
        cohorts["run"],
        offers,
        ledgers,
        times,
        boundary["native_D_interval"],
        ack_complete,
        ledger_complete,
        home=home,
    )
    # The fleet is every device the regional device clients have ever acknowledged
    # (browser test clients log separately). Devices kept offline still count.
    fleet = set()
    for region in ("eu", "us"):
        log = native / "client" / (region + "-acks.log")
        if log.exists():
            with log.open() as stream:
                fleet.update(line.split(" ", 1)[0] for line in stream if line.strip())
    health = service_health(
        run,
        ledgers,
        metadata,
        boundary,
        ledger_complete,
        live_devices=fleet or {i[0] for i in cohorts["run"]},
        home=home,
        max_age=float(recurrence_spec.get("service_health", {}).get("max_age_seconds", 120)),
    )
    # States whose opening covers partner statements grade reporting against the final ledgers.
    if recurrence_spec.get("reporting"):
        report = (
            reporting(
                native,
                {r: dbs[0] for r, dbs in region_databases.items() if len(dbs) == 1},
                boundary["native_D_interval"],
                settle=int(recurrence_spec["reporting"].get("settle_seconds", 900)),
                routine=(rec.get("routine_reporting") or {})
                if recurrence_spec.get("reporting_routine_seconds")
                else None,
            )
            if ledger_complete
            else dict(status="ungraded", reason="Ledger inventory incomplete")
        )
    else:
        report = dict(status="not_applicable", reason="This state does not grade reporting")
    safe = safety({**classes, "run-accepted": set(cohorts["run"])}, details, native)
    mitigation_status = status(
        [c["status"], fresh["status"], health["status"], safe["status"], rec["status"], report["status"]]
    )
    extras = {}
    for name, rows in cohorts.items():
        if name != "run":
            extras[name], _ = custody(rows, ledgers, copies, metadata, home=home)
    v = validity(run, hashes)
    grade = dict(
        schema="sregym-grade-v1",
        run=str(run),
        rubric=dict(
            path=str(RUBRIC),
            sha256=hashlib.sha256(RUBRIC.read_bytes()).hexdigest(),
            recurrence_boundary="In-run maintenance fires before D in the live world; the post-D re-fire with "
            "automation restored tests that the fix lands in every source of truth",
        ),
        validity=v,
        boundary=boundary,
        mitigation=dict(
            status=mitigation_status,
            custody=c,
            freshness=fresh,
            service_health=health,
            safety=safe,
            recurrence=rec,
            reporting=report,
        ),
        acknowledgement_integrity=ack_integrity,
        other_cohorts=extras,
        starting_identity_count=len(expected),
        observed_run_acknowledged=len(cohorts["run"]),
        observed_run_offered=len(offers),
        evidence=dict(
            frozen_inputs=hashes,
            capture=manifest.get("source_manifest") if manifest else None,
            native_readout=str(native),
            extraction_errors=extraction_errors,
            coverage_issues=extraction.get("issues", []),
            observed_databases={k: len(v) for k, v in databases.items()},
            source_occurrences=dict(source_stats),
        ),
        limitations=[
            "Uninventoried RAM queues and unsupported copies make missing payloads unknown, never proven loss.",
            "Acknowledgements outside the saved state's census are a separate, unclassified cohort.",
        ],
        runtime_seconds=round(time.monotonic() - started, 3),
    )
    write(out / "GRADE.json", grade)
    with (out / "identities.jsonl").open("w") as stream:
        for row in details:
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")
    (out / "GRADE.md").write_text(markdown(grade))
    print(str(out / "GRADE.json"), flush=True)
    print(str(out / "GRADE.md"), flush=True)


if __name__ == "__main__":
    main()
