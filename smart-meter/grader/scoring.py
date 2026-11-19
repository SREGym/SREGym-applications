"""Outcome scoring. No subprocesses, image access, or company connections."""

import datetime
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from semantic import canonical, epoch, identity, jsonlines, millis, region


def status(values):
    return (
        "fail"
        if "fail" in values
        else "ungraded"
        if any(v not in ("pass", "not_applicable") for v in values)
        else "pass"
    )


def fresh_truth(starting):
    """Truth from the saved state's own census.

    Required readings are those the device clients saw acknowledged before the
    census; each keeps the sources that held a copy, named by the census source bits.
    """
    import sqlite3

    bits = {int(bit): name for bit, name in starting["source_bits"].items()}
    sources = {name: set() for name in bits.values()}
    expected = {}
    con = sqlite3.connect("file:" + starting["private_identity_hash_database"] + "?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT device,boot,seq,digest,mask,tenant,event_time_ms,quantity FROM custody WHERE required!=0"
        )
        for device, boot, seq, digest, mask, tenant, event_ms, quantity in rows:
            i = (str(device), str(boot), int(seq))
            names = [name for bit, name in bits.items() if mask & bit]
            row = dict(device_id=i[0], boot_id=i[1], seq=i[2], sha256=digest, identity=i, cohort="T0", sources=names)
            if tenant is not None:
                row.update(tenant=tenant, event_time_ms=int(event_ms), quantity=int(quantity))
            row["region"] = region(row)
            expected[i] = row
            for name in names:
                sources[name].add(i)
    finally:
        con.close()
    classes = {}
    for name in ("stopped-US", "stopped-EU", "old-session"):
        classes[name] = sources[name] - set().union(*(s for n, s in sources.items() if n != name))
    classes["remaining-live"] = set(expected) - set().union(*classes.values())
    return expected, classes


def observations(path):
    ledgers, copies, metadata, times = defaultdict(Counter), defaultdict(set), {}, defaultdict(list)
    databases, source_stats = defaultdict(set), Counter()
    if not path.exists():
        return ledgers, copies, metadata, times, databases, source_stats
    for obs in jsonlines(path):
        if obs.get("host") == "ops-helper-01" or obs.get("source", "").startswith("ops-helper-01/"):
            continue  # private load generator is an ACK oracle, not company custody
        i, h, p = canonical(obs["payload"])
        metadata[i, h] = p
        source = obs["source"]
        source_stats[source] += 1
        if obs["kind"] == "ledger":
            db = obs.get("database", source)
            ledgers[i][db, h] += 1
            databases[db].add(i)
            if obs.get("ingested_at") is not None:
                times[i, h, db].append(epoch(obs["ingested_at"]))
        else:
            copies[i, h].add(source)
    return ledgers, copies, metadata, times, databases, source_stats


def acknowledgements(accepted_prefixes, native, expected, d_interval, t0_interval):
    cohorts = dict(run={}, boundary={}, in_flight_at_D={}, pre_run_additions={}, historical_unclassified={})
    integrity, offers = [], {}
    device_tenants = {}
    for row in expected.values():
        device = row["identity"][0]
        if device in device_tenants and device_tenants[device] != row.get("tenant"):
            device_tenants[device] = None
        else:
            device_tenants[device] = row.get("tenant")
    client = native / "client"
    if not client.is_dir() or not list(client.glob("*-acks.log.digests.jsonl")):
        client = native / "client-disk"
    for prefix in sorted(accepted_prefixes.glob("*-acks.log.digests.jsonl")):
        stream_region = prefix.name.split("-acks.", 1)[0]
        original = prefix.read_bytes()
        after = client / prefix.name
        row_integrity = dict(
            region=stream_region,
            complete_RAM_flush=(native / "client" / prefix.name).exists(),
            deadline_file_present=after.exists(),
            prefix_unchanged=None,
        )
        integrity.append(row_integrity)
        prefix_rows = list(jsonlines(prefix))
        for row in prefix_rows:
            i = identity(row)
            if i not in expected:
                cohorts["historical_unclassified"][i] = dict(
                    row, identity=i, cohort="historical_unclassified", region=stream_region
                )
        if not after.exists():
            continue
        content = after.read_bytes()
        row_integrity["prefix_unchanged"] = content.startswith(original)
        if not row_integrity["prefix_unchanged"]:
            continue
        # Require complete JSON lines, except one torn final line: a write in progress when the
        # world froze (no trailing newline, possibly zero bytes). Its identity cannot be known, so
        # it is excluded and recorded; any other invalid line is a coverage gap.
        row_integrity["whole_lines"] = True
        lines = content.splitlines()
        torn_final = bool(content) and not content.endswith(b"\n")
        seen = {}
        parsed = []
        for number, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                if number == len(lines) and torn_final:
                    row_integrity["torn_final_line"] = number
                    continue
                row_integrity["whole_lines"] = False
                row_integrity.setdefault("invalid_lines", []).append(number)
                continue
            parsed.append(row)
            i = identity(row)
            if i in seen and seen[i] != row["sha256"]:
                raise ValueError("Conflicting acknowledgement payload digests")
            seen[i] = row["sha256"]
        for row in parsed[len(prefix_rows) :]:
            i = identity(row)
            row.update(identity=i, cohort="run", region=stream_region)
            if device_tenants.get(i[0]):
                row["tenant"] = device_tenants[i[0]]
            if i in expected:
                if expected[i]["sha256"] != row["sha256"]:
                    raise ValueError("Acknowledgement conflicts with the saved census")
                continue
            at = float(row["accepted_at"])
            if not d_interval or not t0_interval:
                label = "boundary"
            elif at > d_interval[1]:
                # Native cached PUBACKs may be processed after fork resume.
                # Keep them uncertain, never silently add them to the D cohort.
                row["acceptance_boundary_reason"] = (
                    "Client observation after D; cached pre-D PUBACK or later observation"
                )
                label = "boundary"
            elif at >= d_interval[0] or t0_interval[0] <= at <= t0_interval[1]:
                label = "boundary"
            elif at >= t0_interval[1] and at > d_interval[0] - 120:
                # Accepted in the last 120 s before D: still in transit at the end, not yet due in
                # the ledger (the same rule freshness uses). Service health covers intake at D.
                label = "in_flight_at_D"
            elif at < t0_interval[0]:
                label = "pre_run_additions"
            else:
                label = "run"
            row["cohort"] = label
            cohorts[label][i] = row
        offer_path = client / (stream_region + "-acks.log.offers.jsonl")
        if offer_path.exists():
            for row in jsonlines(offer_path):
                i = identity(row)
                at = float(row["offered_at"])
                if t0_interval and d_interval and t0_interval[1] < at < d_interval[0]:
                    if i in offers and offers[i]["sha256"] != row["sha256"]:
                        raise ValueError("Conflicting offered-reading digests")
                    offers[i] = dict(row, region=stream_region)
        row_integrity.update(
            total_distinct_acknowledged=len(seen),
            prefix_rows=len(prefix_rows),
            post_prefix_rows=len(content[len(original) :].splitlines()),
        )
    return cohorts, integrity, offers


TRUNCATION = re.compile(r"\[UnifiedLog partition=([A-Za-z0-9._-]+), dir=[^\]]*\] Truncating to offset (\d+)")
CRI_TIME = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)")


def witness_epoch(text):
    """Guest wall-clock time of a container log line (CRI prefix), or None."""
    m = CRI_TIME.match(text)
    if not m:
        return None
    zone = "+00:00" if m.group(3) == "Z" else m.group(3)
    moment = datetime.datetime.fromisoformat(m.group(1) + zone).timestamp()
    return moment + (float("0." + m.group(2)) if m.group(2) else 0.0)


def destroyed_disk_copies(starting, native, t0_epoch, only_copy):
    """Only-copy disk readings that the source broker's own log shows were truncated away during the run.

    A reading counts only if every copy the seal saw on that disk lies at or above a truncation of its
    partition that the broker logged after T0. Absence alone stays unknown (guest RAM is not inventoried).
    """
    if t0_epoch is None or not only_copy:
        return set()
    cuts = {}
    for path in Path(native).glob("*-destruction-witnesses.jsonl"):
        for witness in jsonlines(path):
            m = TRUNCATION.search(witness.get("text", ""))
            at = witness_epoch(witness.get("text", ""))
            if m and at is not None and at >= t0_epoch:
                key = (witness["host"], m.group(1))
                cuts[key] = min(cuts.get(key, 1 << 62), int(m.group(2)))
    places = defaultdict(list)
    for key in ("held_disk_extraction", "stopped_disk_extraction"):
        folder = starting.get(key)
        if not folder or not (Path(folder) / "observations.jsonl").exists():
            continue
        for row in jsonlines(Path(folder) / "observations.jsonl"):
            if row.get("kind") != "copy" or "offset" not in row:
                continue
            parts = row["source"].split("/")
            value = row["payload"]
            i = (str(value["device_id"]), str(value["boot_id"]), int(value["seq"]))
            if i in only_copy:
                places[i].append((parts[0], parts[-2], int(row["offset"])))
    return {i for i, rows in places.items() if all((h, p) in cuts and o >= cuts[h, p] for h, p, o in rows)}


def custody(required, ledgers, copies, metadata, classes=None, destroyed=frozenset(), home=None):
    # home maps each authoritative database to its region: a reading counts only in its own region's ledger,
    # and a copy in the other region's ledger is one more occurrence.
    groups = defaultdict(Counter)
    details = []
    known_fields = ("exactly_once", "duplicated", "wrong_region", "preserved_only", "lost", "unknown")
    total, anomalies = Counter(), Counter()
    class_counts = defaultdict(Counter)
    for i, row in required.items():
        h = row["sha256"]
        p = metadata.get((i, h), row)
        r = p.get("region", row.get("region", region(p)))
        if r == "unknown":
            r = row.get("region", region(p))

        def own(db, r=r):
            return home is None or home.get(db) == r

        matching = {db: n for (db, digest), n in ledgers.get(i, {}).items() if digest == h and own(db)}
        elsewhere = sum(n for (db, digest), n in ledgers.get(i, {}).items() if digest == h and not own(db))
        other = sum(n for (db, digest), n in ledgers.get(i, {}).items() if digest != h)
        locations = sorted(copies.get((i, h), set()))
        occurrences = Counter()
        for (db, _), n in ledgers.get(i, {}).items():
            occurrences[db] += n
        duplicates = sum(max(0, occurrences[db] - 1) for db in matching) + (elsewhere if matching else 0)
        if matching:
            label = "duplicated" if duplicates else "exactly_once"
        elif elsewhere:
            label = "wrong_region"
        elif locations:
            label = "preserved_only"
        else:
            label = "lost" if i in destroyed else "unknown"
        if other:
            anomalies["identities_with_conflicting_ledger_payloads"] += 1
        event_ms = p.get("event_time_ms")
        if event_ms is None and "event_time" in p:
            event_ms = millis(p["event_time"])
        hour = int(event_ms) // 3600000 * 3600 if event_ms is not None else None
        t = p.get("tenant")
        key = (r, t, hour)
        groups[key][label] += 1
        total[label] += 1
        groups[key]["required"] += 1
        total["required"] += 1
        groups[key]["extra_ledger_occurrences"] += duplicates
        total["extra_ledger_occurrences"] += duplicates
        if "quantity" in p:
            groups[key][label + "_quantity"] += int(p["quantity"])
            total[label + "_quantity"] += int(p["quantity"])
        else:
            groups[key]["unknown_quantity_identities"] += 1
            total["unknown_quantity_identities"] += 1
        for name, ids in (classes or {}).items():
            if i in ids:
                class_counts[name][label] += 1
                class_counts[name]["required"] += 1
        details.append(
            dict(
                identity=i,
                sha256=h,
                cohort=row.get("cohort"),
                region=r,
                tenant=t,
                hour=hour,
                status=label,
                matching_ledgers=matching,
                recoverable_copy_locations=locations,
                extra_ledger_occurrences=duplicates,
                conflicting_ledger_occurrences=other,
            )
        )
    for f in known_fields:
        total.setdefault(f, 0)
    total["kept"] = total["exactly_once"] + total["duplicated"] + total["wrong_region"] + total["preserved_only"]
    for counts in groups.values():
        for field in known_fields:
            counts.setdefault(field, 0)
        counts["kept"] = (
            counts["exactly_once"] + counts["duplicated"] + counts["wrong_region"] + counts["preserved_only"]
        )
    for counts in class_counts.values():
        for field in known_fields:
            counts.setdefault(field, 0)
        counts["kept"] = (
            counts["exactly_once"] + counts["duplicated"] + counts["wrong_region"] + counts["preserved_only"]
        )
    buckets = [
        dict(region=k[0], tenant=k[1], hour=k[2], **{**{f: v.get(f, 0) for f in known_fields}, **v})
        for k, v in sorted(groups.items(), key=lambda kv: tuple("" if x is None else str(x) for x in kv[0]))
    ]
    by_region = defaultdict(Counter)
    by_tenant = defaultdict(Counter)
    by_hour = defaultdict(Counter)
    for k, counts in groups.items():
        by_region[k[0]].update(counts)
        by_tenant[k[:2]].update(counts)
        by_hour[(k[0], k[2])].update(counts)
    # Usage totals come from the ledgers, so a copy kept anywhere else does not count.
    result_status = "pass" if total["exactly_once"] == total["required"] and not anomalies else "fail"
    return dict(
        status=result_status,
        counts=dict(total),
        anomalies=dict(anomalies),
        by_region=dict(by_region),
        by_tenant=[dict(region=k[0], tenant=k[1], **v) for k, v in sorted(by_tenant.items(), key=str)],
        by_hour=[dict(region=k[0], hour=k[1], **v) for k, v in sorted(by_hour.items(), key=str)],
        buckets=buckets,
        source_classes={k: dict(v) for k, v in class_counts.items()},
        loss_semantics="lost requires complete payload-source coverage; "
        "missing from an incomplete inventory is unknown",
    ), details


def freshness(
    required,
    offers,
    ledgers,
    times,
    d_interval,
    ack_complete,
    ledger_complete=False,
    home=None,
):
    summary = defaultdict(Counter)
    if not d_interval:
        return dict(status="ungraded", reason="Native deadline clock not established")
    clock_lo, clock_hi = d_interval
    margin = max(1.0, clock_hi - clock_lo)  # native cross-guest clock uncertainty
    # Offer timestamps are the public freshness origin. ACK time is reported as
    # a secondary diagnostic when original-offer evidence is absent.
    all_events = {
        i: dict(r, origin="ack", start=float(r["accepted_at"])) for i, r in required.items() if "accepted_at" in r
    }
    for i, row in offers.items():
        all_events[i] = dict(row, start=float(row["offered_at"]), origin="offer")
    for i, row in all_events.items():
        r = row.get("region", region(row))
        counts = summary[r]
        counts["offered_observed" if row["origin"] == "offer" else "ack_only_observed"] += 1
        deadline = row["start"] + 120
        if deadline > clock_hi:
            counts["not_yet_due"] += 1
            continue
        if deadline >= clock_lo:
            counts["due_boundary_unknown"] += 1
            continue
        counts["due"] += 1
        matches = [
            t
            for (db, h), n in ledgers.get(i, {}).items()
            if h == row["sha256"] and (home is None or home.get(db) == r)
            for t in times.get((i, h, db), ())
        ]
        if not matches:
            counts["due_missing_D_ledger"] += 1
        else:
            delay = min(matches) - row["start"]
            counts["due_late_native_clock"] += delay > 120
            counts["due_on_time_native_clock"] += delay <= 120
            counts["maximum_native_delay_seconds"] = max(counts["maximum_native_delay_seconds"], delay)
        if row["origin"] == "offer":
            counts["admitted"] += i in required
            counts["unacknowledged_offers"] += i not in required
    due = sum(v["due"] for v in summary.values())
    missing = any(v["due_missing_D_ledger"] for v in summary.values())
    # End state only: every reading due by D is in the ledger at D. Lateness during the run is
    # reported but never fails a run (no in-run deadline); live freshness at D is service health.
    status = (
        "fail"
        if ledger_complete and missing
        else "pass"
        if ledger_complete and ack_complete and due and not missing
        else "ungraded"
    )
    return dict(
        status=status,
        clock_margin_seconds=margin,
        regions={r: dict(v) for r, v in sorted(summary.items())},
        freshness_seconds=120,
        acceptance_inventory_complete=ack_complete,
        ledger_inventory_complete=ledger_complete,
        limitations=[
            "Missing observed ledger rows are not absence proofs when the ledger inventory is incomplete",
            "Native delays are reported in guest time and never fail a run",
        ],
    )


def reporting(native, labels, d_interval, settle=900, routine=None):
    """Settled reports and partner statements must equal the final regional ledgers.

    `labels` names each region's authoritative ledger database. A settled hour ends at least
    `settle` seconds before D. Every (tenant, hour) present in either the report or
    the ledger is compared, so stale nonzero rows for hours the ledger no longer has also count.
    Partner statements are checked in both regions for both origins.
    """
    from datetime import datetime

    if not d_interval:
        return dict(status="ungraded", reason="Native deadline clock not established")
    if set(labels) != {"us", "eu"}:
        return dict(status="ungraded", reason="No single authoritative ledger per region", labels=labels)
    cutoff = d_interval[0] - settle

    def rows(label, table):
        path = Path(native) / "sql" / f"{label}-{table}.jsonl"
        if not path.is_file():
            raise FileNotFoundError(str(path))
        return jsonlines(path)

    def compare(report, truth):
        keys = {k for k in set(report) | set(truth) if k[1] + 3600 <= cutoff}
        wrong = sorted(k for k in keys if tuple(truth.get(k, (0, 0))) != tuple(report.get(k, (0, 0))))
        return dict(
            cells=len(keys),
            wrong=len(wrong),
            wrong_hours=sorted({k[1] for k in wrong}),
            examples=[dict(tenant=k[0], hour=k[1], reported=report.get(k), ledger=truth.get(k)) for k in wrong[:3]],
        )

    try:
        ledger = {}
        for region, label in labels.items():
            sums = defaultdict(lambda: [0, 0])
            for r in rows(label, "readings"):
                cell = sums[(r["tenant"], int(datetime.fromisoformat(r["event_time"]).timestamp()) // 3600 * 3600)]
                cell[0] += 1
                cell[1] += int(r["quantity"])
            ledger[region] = {k: tuple(v) for k, v in sums.items()}
        results = {}
        for region, label in labels.items():
            closed = {
                (r["tenant"], int(r["hour"])): (int(r["readings"]), int(r["quantity"]))
                for r in rows(label, "closed_reports")
            }
            results[region + "/settled_reports"] = compare(closed, ledger[region])
            partner = defaultdict(dict)
            for r in rows(label, "partner_totals"):
                partner[r["origin"]][(r["tenant"], int(r["hour"]))] = (int(r["readings"]), int(r["quantity"]))
            for origin in labels:
                results[f"{region}/partner_statements/{origin}"] = compare(partner[origin], ledger[origin])
    except (OSError, ValueError, KeyError) as e:
        return dict(status="ungraded", reason=repr(e))
    if routine is not None:
        # The same settled hours, read again on the post-D fork after routine reporting ran.
        if set(routine) != {"us", "eu"} or any("error" in v for v in routine.values()):
            return dict(
                status="ungraded", reason="Routine reporting cycle was not read", checks=results, routine=routine
            )

        def cell(x):
            return int(x["readings"]), int(x["quantity"])

        after = {r: {(x["tenant"], int(x["hour"])): cell(x) for x in v["ledger"]} for r, v in routine.items()}
        for region, v in routine.items():
            results[f"after_routine/{region}/settled_reports"] = compare(
                {(x["tenant"], int(x["hour"])): cell(x) for x in v["closed"]}, after[region]
            )
            partner = defaultdict(dict)
            for x in v["partner"]:
                partner[x["origin"]][(x["tenant"], int(x["hour"]))] = cell(x)
            for origin in ("us", "eu"):
                results[f"after_routine/{region}/partner_statements/{origin}"] = compare(partner[origin], after[origin])
    return dict(
        status="fail" if any(v["wrong"] for v in results.values()) else "pass",
        settled_before_native_epoch=cutoff,
        checks=results,
        rule="Pass: for every hour that ended at least "
        + str(settle)
        + " s before D, each region's settled reports equal its "
        "final ledger and both regions' partner statements equal the originating region's final ledger, per tenant, in "
        "readings and quantity, at D and again after a routine reporting cycle on the post-D fork. "
        "Fail: any settled cell differs.",
    )
