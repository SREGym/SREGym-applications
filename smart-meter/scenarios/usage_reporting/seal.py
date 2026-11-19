#!/usr/bin/env python3
"""Seal the world once the history has settled, then save it as a revivable state.

1. The device clients' control files.
2. The starting truth: every reading a device client saw acknowledged is
   required; each copy found in the ledgers, the running Kafka logs and the
   stopped hosts' disks is recorded.
3. revive.capture(STATE).
4. The held host, which nothing can reach while the world runs, read from the
   saved state: its disk, and its broker's session queues on a disposable copy.

Run through world/run_step.py. Finished parts are skipped on rerun.
"""

import hashlib
import json
import shlex
import sqlite3
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import revive
import sim
import world_native_census as native
import world_policy
from build_context import OUT, lab, note, require_build

STATE = "usage-reporting"
SEAL = OUT / "seal"
GRADER = lab.ROOT.parent / "grader"
LIVE = [
    ("us-work-01", "sql", 1),
    ("eu-work-02", "sql", 2),
    ("us-work-01", "raw", 4),
    ("us-work-03", "raw", 8),
    ("eu-work-02", "raw", 16),
    ("eu-work-03", "raw", 32),
]
STOPPED = {"eu-work-01": 128}
# Held by the healer's mass-failure guard: running, cut off, read from the saved state.
HELD = {"us-work-02": 64}
SESSION = 256
# Mask bits, named as the grader names its sources.
SOURCE_BITS = {
    "1": "us-work-01/sql",
    "2": "eu-work-02/sql",
    "4": "us-work-01/raw",
    "8": "us-work-03/raw",
    "16": "eu-work-02/raw",
    "32": "eu-work-03/raw",
    "64": "stopped-US",
    "128": "stopped-EU",
    "256": "old-session",
}
PROVIDER = "http://127.0.0.1:28301/v2.1/servers/detail"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(name, value):
    (SEAL / name).write_text(json.dumps(value, indent=2) + "\n")


def topology():
    with urllib.request.urlopen(PROVIDER, timeout=10) as response:
        rows = {row["id"]: row for row in json.load(response)["servers"]}
    stopped = sorted(n for n in require_build()["memory_mib"] if rows[n]["status"] != "ACTIVE")
    fleet = json.loads(lab.ssh("eu-work-03", "sudo -n cat /var/lib/company/fleet-state.json", timeout=20))
    held = sorted(n for n, r in fleet["nodes"].items() if r.get("held"))
    if stopped != sorted(STOPPED) or held != sorted(HELD) or any(rows[n]["status"] != "ACTIVE" for n in HELD):
        raise RuntimeError(("Handoff topology differs from the census sources", stopped, held))
    return {n: rows[n]["status"] for n in rows}


def controls():
    path = SEAL / "client-control-start.json"
    if not path.exists():
        code = (
            "import json,pathlib;print(json.dumps({r:json.loads(pathlib.Path('/var/lib/private/'+r"
            "+'-control.json').read_text()) for r in ['us','eu']}))"
        )
        value = json.loads(lab.ssh("ops-helper-01", "sudo -n python3 -c " + shlex.quote(code), timeout=20))
        if not all(v["rate"] == 10 for v in value.values()):
            raise RuntimeError(("Device clients are not at their normal rate", value))
        path.write_text(json.dumps(value) + "\n")
    return path


def lines(host, command):
    process = subprocess.Popen(
        lab.ssh_arguments(host) + [command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        yield from process.stdout
        error = process.stderr.read()
        if process.wait(timeout=60):
            raise RuntimeError((host, command[:100], error[-2000:]))
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=15)


def stopped_disks():
    """Read the stopped hosts' disks offline with the grader's own reader."""
    dest = GRADER / "results" / "seal-stopped-disks"
    if not (dest / "extraction.json").exists():
        # The run's grader writes in results/, so the root disk reader must not be the one that creates it.
        dest.parent.mkdir(exist_ok=True)
        manifest = SEAL / "stopped-disks-manifest.json"
        manifest.write_text(json.dumps(dict(disks={h: require_build()["fronts"][h] for h in STOPPED})) + "\n")
        subprocess.run(
            [
                "sudo",
                "-n",
                "unshare",
                "--mount",
                "--propagation",
                "private",
                sys.executable,
                str(GRADER / "offline.py"),
                str(manifest),
                str(dest),
            ],
            check=True,
            timeout=3600,
            cwd=GRADER,
        )
    return dest


def census():
    path = SEAL / "identity-custody-private.sqlite"
    if path.exists():
        raise RuntimeError("A census database exists without its truth file; inspect it before rerunning")
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE custody(device text,boot text,seq integer,digest text,required integer,mask integer,"
        "tenant text,event_time_ms integer,quantity integer,us_sql_rows integer DEFAULT 0,eu_sql_rows integer DEFAULT 0,"
        "PRIMARY KEY(device,boot,seq)) WITHOUT ROWID"
    )
    count = 0

    def add(identity, digest, required, mask, fields=None, sql_column=None):
        nonlocal count
        prior = db.execute("SELECT digest FROM custody WHERE device=? AND boot=? AND seq=?", identity).fetchone()
        if prior and prior[0] != digest:
            raise RuntimeError(("Semantic value mismatch", identity))
        tenant, event_ms, quantity = fields or (None, None, None)
        rows = (1 if sql_column == "us" else 0, 1 if sql_column == "eu" else 0)
        db.execute(
            "INSERT INTO custody VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(device,boot,seq) DO UPDATE SET "
            "required=custody.required|excluded.required,mask=custody.mask|excluded.mask,"
            "tenant=coalesce(custody.tenant,excluded.tenant),event_time_ms=coalesce(custody.event_time_ms,excluded.event_time_ms),"
            "quantity=coalesce(custody.quantity,excluded.quantity),"
            "us_sql_rows=custody.us_sql_rows+excluded.us_sql_rows,eu_sql_rows=custody.eu_sql_rows+excluded.eu_sql_rows",
            (*identity, digest, required, mask, tenant, event_ms, quantity, *rows),
        )
        count += 1
        if count % 1000 == 0:
            db.commit()

    def reading(value, mask, sql_column=None):
        if "event_time_ms" in value:
            # The disk reader's payloads already carry millisecond event time,
            # the form payload_digest hashes; any mismatch fails in add().
            digest = hashlib.sha256(
                json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            ).hexdigest()
        else:
            digest = sim.payload_digest(json.dumps(value))
        event_ms = value["event_time_ms"] if "event_time_ms" in value else int(round(float(value["event_time"]) * 1000))
        add(
            [value[k] for k in ("device_id", "boot_id", "seq")],
            digest,
            0,
            mask,
            (value["tenant"], event_ms, int(value["quantity"])),
            sql_column,
        )

    # Required: every reading a device client saw acknowledged (flushed every ten seconds).
    # The grader's accepted prefix: exactly the complete lines read here.
    prefix = SEAL / "accepted-prefix-private"
    prefix.mkdir(mode=0o700)
    for region in ("us", "eu"):
        with (prefix / (region + "-acks.log.digests.jsonl")).open("w") as saved:
            for line in lines("ops-helper-01", "sudo -n cat /var/lib/private/" + region + "-acks.log.digests.jsonl"):
                if line.endswith("\n"):
                    saved.write(line)
                    row = json.loads(line)
                    add([row[k] for k in ("device_id", "boot_id", "seq")], row["sha256"], 2, 0)
    for host, kind, mask in LIVE:
        for line in lines(host, native.native(host, kind)):
            if kind == "sql":
                reading(json.loads(line), mask, host[:2])
            elif line.startswith("| offset:"):
                payload = line.split(" payload: ", 1)
                if len(payload) != 2:
                    raise RuntimeError("Unparsed native Kafka payload")
                reading(json.loads(payload[1]), mask)
    extracted = stopped_disks()
    for line in (extracted / "observations.jsonl").open():
        row = json.loads(line)
        reading(row["payload"], STOPPED[row["source"].split("/", 1)[0]])
    db.commit()

    def number(where):
        return db.execute("SELECT count(*) FROM custody WHERE " + where).fetchone()[0]

    classes = dict(
        required=number("required!=0"),
        stopped_us_only=number("required!=0 AND mask=64"),
        stopped_eu_only=number("required!=0 AND mask=128"),
        currently_unlocated=number("required!=0 AND mask=0"),
    )
    masks = dict(db.execute("SELECT mask,count(*) FROM custody WHERE required!=0 GROUP BY mask"))
    db.close()
    path.chmod(0o400)
    truth = dict(
        at=time.time(),
        fresh_census=True,
        classes=classes,
        source_masks=masks,
        sqlite_sha256=sha(path),
        accepted_prefix_dir=str(prefix),
        source_bits=SOURCE_BITS,
        stopped_disk_extraction=str(extracted),
        scope="Every reading the US and EU device clients saw acknowledged before this census. "
        "Later acknowledgements stay required through the saved client state.",
        limitations="Sequential live observations, not a proof of global absence.",
        payload_exports_in_world=False,
        private_identity_hash_database=str(path),
    )
    save("starting-truth.json", truth)
    note("seal-census-live", classes=classes)
    return truth


def held_census():
    """After capture: add the held host's readings from the saved state, then finish the truth.

    Its disk is read offline. Its broker's session queues exist only in memory, so they are
    read with the grader's own reader on a disposable copy of the saved state.
    """
    truth = json.loads((SEAL / "starting-truth.json").read_text())
    if truth.get("held_hosts_read"):
        return truth
    state = revive.STATES / STATE
    disks = json.loads((state / "manifest.json").read_text())["disks"]
    dest = GRADER / "results" / "seal-held-disks"
    if not (dest / "extraction.json").exists():
        dest.parent.mkdir(exist_ok=True)
        manifest = SEAL / "held-disks-manifest.json"
        manifest.write_text(json.dumps(dict(disks={h: disks[h] for h in HELD})) + "\n")
        subprocess.run(
            [
                "sudo",
                "-n",
                "unshare",
                "--mount",
                "--propagation",
                "private",
                sys.executable,
                str(GRADER / "offline.py"),
                str(manifest),
                str(dest),
            ],
            check=True,
            timeout=3600,
            cwd=GRADER,
        )
    sys.path.insert(0, str(GRADER))
    import grade
    import sandbox

    queues = GRADER / "results" / "seal-held-sessions"
    queues.mkdir(exist_ok=True)
    coverage = sandbox.queue_readout(
        grade.resolve_capture(state, state / "manifest.json"), queues, GRADER.parent, sorted(HELD)
    )
    if sorted(coverage) != sorted(HELD) or any(c["status"] != "positive_witnesses" for c in coverage.values()):
        raise RuntimeError(("The held host's session queues were not read", coverage))
    path = SEAL / "identity-custody-private.sqlite"
    path.chmod(0o600)
    db = sqlite3.connect(path)
    added = {}
    for source, mask in ((dest / "observations.jsonl", None), (queues / "queue-observations.jsonl", SESSION)):
        for line in source.open():
            row = json.loads(line)
            value = row["payload"]
            if "event_time_ms" in value:
                digest = hashlib.sha256(
                    json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
                ).hexdigest()
            else:
                digest = sim.payload_digest(json.dumps(value))
            identity = [value[k] for k in ("device_id", "boot_id", "seq")]
            prior = db.execute("SELECT digest FROM custody WHERE device=? AND boot=? AND seq=?", identity).fetchone()
            if prior and prior[0] != digest:
                raise RuntimeError(("Semantic value mismatch", identity))
            event_ms = (
                value["event_time_ms"] if "event_time_ms" in value else int(round(float(value["event_time"]) * 1000))
            )
            db.execute(
                "INSERT INTO custody(device,boot,seq,digest,required,mask,tenant,event_time_ms,quantity) VALUES(?,?,?,?,0,?,?,?,?) "
                "ON CONFLICT(device,boot,seq) DO UPDATE SET mask=custody.mask|excluded.mask, "
                # readings known before only by their acknowledged digest get their fields from the held host
                "tenant=COALESCE(custody.tenant,excluded.tenant), event_time_ms=COALESCE(custody.event_time_ms,excluded.event_time_ms), "
                "quantity=COALESCE(custody.quantity,excluded.quantity)",
                (
                    *identity,
                    digest,
                    mask or HELD[row["source"].split("/", 1)[0]],
                    value["tenant"],
                    event_ms,
                    int(value["quantity"]),
                ),
            )
            added[source.name] = added.get(source.name, 0) + 1
    db.commit()

    def number(where):
        return db.execute("SELECT count(*) FROM custody WHERE " + where).fetchone()[0]

    classes = dict(
        required=number("required!=0"),
        held_us_only=number("required!=0 AND mask=64"),
        stopped_eu_only=number("required!=0 AND mask=128"),
        session_only=number("required!=0 AND mask=256"),
        currently_unlocated=number("required!=0 AND mask=0"),
    )
    masks = dict(db.execute("SELECT mask,count(*) FROM custody WHERE required!=0 GROUP BY mask"))
    db.close()
    path.chmod(0o400)
    if not classes["session_only"]:
        raise RuntimeError(("No acknowledged reading exists only in the held host's broker memory", classes, coverage))
    truth.update(
        classes=classes,
        source_masks=masks,
        sqlite_sha256=sha(path),
        held_hosts_read=sorted(HELD),
        held_disk_extraction=str(dest),
        held_session_coverage=coverage,
        held_observations=added,
        held_note="The held host was read from the saved state after capture; it had no network since it was cut off.",
    )
    save("starting-truth.json", truth)
    note("seal-census", classes=classes)
    return truth


def main():
    require_build()
    assert (OUT / "history" / "settled.json").exists(), "Run history.py until the fleet settles"
    SEAL.mkdir(exist_ok=True)
    save("handoff-topology.json", topology())
    control = controls()
    if not (SEAL / "starting-truth.json").exists():
        census()
    config = json.loads(world_policy.CONFIG.read_text())
    config.update(
        control_start_path=str(control),
        starting_truth_path=str(SEAL / "starting-truth.json"),
        held_hosts=sorted(HELD),
    )
    world_policy.CONFIG.write_text(json.dumps(config, indent=2) + "\n")
    if not (revive.STATES / STATE / "manifest.json").exists():
        revive.capture(STATE)
    held_census()
    # The grader reads the state's own truth from its folder.
    (revive.STATES / STATE / "starting-truth.json").write_bytes((SEAL / "starting-truth.json").read_bytes())
    note("saved", state=STATE)


if __name__ == "__main__":
    main()
