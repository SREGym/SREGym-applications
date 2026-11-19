"""Synthetic tests of the grader's boundary, custody, freshness, recurrence and reading rules."""

import gzip
import io
import json
import struct
import tempfile
import unittest
from collections import Counter, defaultdict
from pathlib import Path
from unittest.mock import patch

import grade
from kafka import crc32c, records
from sandbox import compare_probes, finish_recurrence, mac, recurrence_unit, terminal_unit, trigger
from scoring import acknowledgements, custody, freshness
from semantic import canonical, millis, payloads, sql_dump

HERE = Path(__file__).resolve().parent


def reading(seq=1, **extra):
    return dict(
        device_id="dev-us-0000",
        boot_id="boot",
        seq=seq,
        tenant="tenant-us-01",
        event_time=3600.001,
        quantity=7,
        **extra,
    )


def vint(value):
    n = (value << 1) ^ (value >> 63)
    out = bytearray()
    while n > 127:
        out.append((n & 127) | 128)
        n >>= 7
    return bytes(out) + bytes([n])


def batch(values, compression=0):
    data = bytearray()
    for offset, value in enumerate(values):
        key = b"dev-us-0000"
        record = b"\0" + vint(0) + vint(offset) + vint(len(key)) + key + vint(len(value)) + value + vint(0)
        data.extend(vint(len(record)) + record)
    packed = gzip.compress(data) if compression == 1 else bytes(data)
    body = bytearray(
        struct.pack(">ibIhiqqqhii", 0, 2, 0, compression, len(values) - 1, 3600001, 3600001, -1, -1, -1, len(values))
        + packed
    )
    struct.pack_into(">I", body, 5, crc32c(body[9:]))
    return struct.pack(">qi", 100, len(body)) + body


class Semantics(unittest.TestCase):
    def test_numeric_and_sql_timestamps_agree(self):
        p = reading()
        q = dict(p, event_time="1970-01-01T01:00:00.001+00:00")
        self.assertEqual(canonical(p), canonical(q))
        self.assertEqual(millis("2026-11-12T04:00:00.001-05:00"), 1794474000001)

    def test_order_and_nonsemantic_sql_fields(self):
        p = reading()
        self.assertEqual(canonical(p), canonical(dict(reversed(list(dict(p, id=55, ingested_at="later").items())))))
        self.assertNotEqual(canonical(p)[1], canonical(dict(p, quantity=8))[1])

    def test_hash_index_is_not_a_payload(self):
        i, h, p = canonical(reading())
        self.assertEqual(list(payloads(dict(identity=i, sha256=h))), [])
        full_index = dict(identity=i, sha256=h, tenant=p["tenant"], event_time_ms=p["event_time_ms"], quantity=7)
        self.assertEqual(canonical(next(payloads(full_index)))[1], h)
        self.assertEqual(
            len(list(payloads(dict(value=__import__("base64").b64encode(json.dumps(reading()).encode()).decode())))), 1
        )

    def test_copy_escape_and_incomplete_dump(self):
        sql = "COPY public.readings (device_id, boot_id, seq, tenant, event_time, quantity) FROM stdin;\n"
        sql += "dev-us-0000\tboot\t1\ttenant-us-01\t1970-01-01 01:00:00.001+00\t7\n\\.\n"
        rows = list(sql_dump(io.StringIO(sql)))
        self.assertEqual(canonical(rows[0][1])[1], canonical(reading())[1])
        with self.assertRaises(ValueError):
            list(sql_dump(io.StringIO(sql.rsplit("\\.", 1)[0])))

    def test_noninteger_semantic_quantity_is_not_rounded_into_a_match(self):
        with self.assertRaises(ValueError):
            canonical(dict(reading(), quantity=7.5))


class Custody(unittest.TestCase):
    def test_workspace_root_link_is_not_counted_as_a_saved_copy(self):
        from offline import Reader

        with tempfile.TemporaryDirectory(dir=HERE) as temporary:
            root = Path(temporary)
            outside = root / "elsewhere"
            outside.mkdir()
            (outside / "payload.jsonl").write_text(json.dumps(reading()) + "\n")
            link = root / "solver-work"
            link.symlink_to(outside, target_is_directory=True)
            reader = Reader({}, root / "output")
            reader.preservation(link, "solver-work", workspace=True)
            reader.close()
            self.assertEqual((root / "output/observations.jsonl").read_text(), "")
            self.assertTrue(reader.facts["issues"])

    def test_duplicates_replicas_wrong_values_and_unknowns(self):
        rows = [reading(n) for n in range(1, 6)]
        expected = {}
        metadata = {}
        for row in rows:
            i, h, p = canonical(row)
            expected[i] = dict(p, identity=i, sha256=h, region="us")
            metadata[i, h] = p
        keys = list(expected)

        def h(i):
            return expected[i]["sha256"]

        ledger = defaultdict(Counter)
        ledger[keys[0]]["us", h(keys[0])] = 1
        ledger[keys[0]]["eu-replica", h(keys[0])] = 1
        ledger[keys[1]]["us", h(keys[1])] = 3
        copies = defaultdict(set)
        copies[keys[2], h(keys[2])].add("company/backup.sql")
        ledger[keys[3]]["us", "wrong-hash"] = 1
        result, details = custody(expected, ledger, copies, metadata)
        self.assertEqual(
            {k: result["counts"][k] for k in ("exactly_once", "duplicated", "preserved_only", "unknown", "lost")},
            dict(exactly_once=1, duplicated=1, preserved_only=1, unknown=2, lost=0),
        )
        self.assertEqual(result["counts"]["extra_ledger_occurrences"], 2)
        self.assertEqual(result["status"], "fail")
        result, _ = custody(expected, ledger, copies, metadata, destroyed={keys[3], keys[4]})
        self.assertEqual(result["counts"]["lost"], 2)

    def test_missing_event_hour_never_uses_acceptance_hour(self):
        row = dict(identity=["dev-us-0000", "boot", 1], sha256="a", accepted_at=7205, region="us")
        result, _ = custody({tuple(row["identity"]): row}, {}, {}, {})
        self.assertIsNone(result["buckets"][0]["hour"])
        self.assertEqual(result["counts"]["unknown_quantity_identities"], 1)

    def test_cohort_boundaries_and_prefix_integrity(self):
        with tempfile.TemporaryDirectory(dir=HERE) as temporary:
            root = Path(temporary)
            # The folder starting-truth.json names, here as a bundle install lays it out.
            prefixes = root / "truth" / "accepted_prefix_dir"
            native = root / "native"
            prefixes.mkdir(parents=True)
            (native / "client").mkdir(parents=True)
            prefix = dict(device_id="dev-us-0000", boot_id="boot", seq=1, sha256="a", accepted_at=50)
            filename = "us-acks.log.digests.jsonl"
            original = json.dumps(prefix) + "\n"
            (prefixes / filename).write_text(original)
            added = [
                dict(prefix, seq=n, accepted_at=t)
                for n, t in [(2, 99), (3, 101), (4, 110), (5, 299), (6, 301), (7, 250)]
            ]
            (native / "client" / filename).write_text(original + "".join(json.dumps(r) + "\n" for r in added))
            cohorts, integrity, _ = acknowledgements(prefixes, native, {}, [298, 300], [100, 102])
            self.assertEqual(set(i[2] for i in cohorts["pre_run_additions"]), {2})
            self.assertEqual(set(i[2] for i in cohorts["run"]), {4})
            self.assertEqual(set(i[2] for i in cohorts["in_flight_at_D"]), {7})  # accepted < 120 s before D
            self.assertEqual(set(i[2] for i in cohorts["boundary"]), {3, 5, 6})
            self.assertIn("acceptance_boundary_reason", cohorts["boundary"]["dev-us-0000", "boot", 6])
            self.assertEqual(len(cohorts["historical_unclassified"]), 1)
            self.assertTrue(integrity[0]["prefix_unchanged"])


class Kafka(unittest.TestCase):
    def test_crc_reference(self):
        self.assertEqual(crc32c(b"123456789"), 0xE3069283)

    def test_plain_and_gzip_offsets_and_preallocation(self):
        for kind in (0, 1):
            with tempfile.TemporaryDirectory(dir=HERE) as temporary:
                path = Path(temporary) / "000.log"
                values = [json.dumps(reading(n)).encode() for n in (1, 2)]
                path.write_bytes(batch(values, kind) + bytes(100))
                issues = []
                actual = list(records(path, issues.append))
                self.assertEqual([v[0] for v in actual], [100, 101])
                self.assertEqual([v[3] for v in actual], values)
                self.assertEqual(issues, [])

    def test_corrupt_crc_and_nonzero_tail_are_unknown(self):
        with tempfile.TemporaryDirectory(dir=HERE) as temporary:
            path = Path(temporary) / "000.log"
            data = bytearray(batch([json.dumps(reading()).encode()]))
            data[-1] ^= 1
            path.write_bytes(data + bytes(20) + b"bad")
            issues = []
            self.assertEqual(list(records(path, issues.append)), [])
            self.assertEqual(len(issues), 2)


class Recurrence(unittest.TestCase):
    def test_generated_native_trigger_compiles(self):
        class Fake:
            def console(self, *args, **kwargs):
                return dict(exit_code=0, stdout=json.dumps(dict(exit_code=0, rule_transitions=[])))

        def check(code):
            compile(code, "native-trigger", "exec")
            return "not executed"

        with patch("sandbox.python_command", side_effect=check):
            self.assertEqual(trigger(Fake(), "host", "company-role")["exit_code"], 0)

    def test_terminal_state_requires_fresh_invocation(self):
        before = dict(InvocationID="old", ExecMainStartTimestampMonotonic="100")
        current = dict(
            before,
            MainPID="0",
            ActiveState="inactive",
            SubState="dead",
            Result="success",
            ExecMainStatus="0",
            ExecMainExitTimestampMonotonic="120",
        )
        self.assertIsNone(terminal_unit(before, current))
        current.update(InvocationID="new", ExecMainStartTimestampMonotonic="200", ExecMainExitTimestampMonotonic="220")
        self.assertEqual(terminal_unit(before, current), 0)
        current.update(ActiveState="activating", MainPID="999")
        self.assertIsNone(terminal_unit(before, current))

    def test_recurrence_unit_comes_from_the_spec(self):
        self.assertEqual(recurrence_unit("role", {}), "company-role")
        self.assertEqual(recurrence_unit("role", dict(recurrence_units=dict(role="other-role.service"))), "other-role")
        with self.assertRaises(ValueError):
            recurrence_unit("role", dict(recurrence_units=dict(role="bad unit")))

    def test_private_fork_keeps_original_mac_addresses(self):
        self.assertEqual(mac("us-parent-01", 1), "52:54:00:72:01:10")
        self.assertEqual(mac("ops-helper-01", 0), "52:54:00:72:00:30")

    def test_missing_probe_is_not_a_policy_or_connectivity_loss(self):
        before = dict(rules=[dict(table=2004, fwmark=512)], connections={"pod:5432": True}, networkd="old")
        self.assertEqual(compare_probes(before, dict(error="console failed"), []), ([], [], False))
        removed, lost, witness = compare_probes(before, dict(error="console failed"), [dict(rules=[])])
        self.assertTrue(removed)
        self.assertEqual(lost, [])
        self.assertTrue(witness)

    def test_partial_matrix_keeps_observed_failures(self):
        result = dict(
            hosts={
                "us-work-01": dict(
                    before={},
                    triggers=[
                        dict(
                            unit="company-role",
                            removed_policy_rules=[["2004"]],
                            lost_connectivity=[],
                            actual_network_lifecycle_witness=True,
                            command=dict(exit_code=0, automation_restored=True),
                        )
                    ],
                )
            },
            error="budget exhausted",
        )
        self.assertEqual(finish_recurrence(result)["status"], "fail")


class FrozenTruth(unittest.TestCase):
    def test_host_name_is_checked_before_any_image_operation(self):
        with tempfile.TemporaryDirectory(dir=HERE) as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(dict(disks={"../../outside": "not-an-image"})))
            with self.assertRaises(ValueError):
                grade.resolve_capture(root, manifest)

    def test_unread_ledger_is_not_a_freshness_failure(self):
        i, h, p = canonical(reading())
        required = {i: dict(p, sha256=h, accepted_at=3700, region="us")}
        unknown = freshness(required, {}, {}, {}, [4000, 4001], True, False)
        known = freshness(required, {}, {}, {}, [4000, 4001], True, True)
        self.assertEqual(unknown["status"], "ungraded")
        self.assertEqual(known["status"], "fail")

    def test_freshness_grades_the_end_state_and_only_reports_lateness(self):
        i, h, p = canonical(reading())
        required = {i: dict(p, sha256=h, accepted_at=3700, region="us")}
        ledgers = {i: {("us-db", h): 1}}
        on_time = freshness(required, {}, ledgers, {(i, h, "us-db"): [3750]}, [4000, 4001], True, True)
        late = freshness(required, {}, ledgers, {(i, h, "us-db"): [3900]}, [4000, 4001], True, True)
        no_ack = freshness(required, {}, ledgers, {(i, h, "us-db"): [3750]}, [4000, 4001], False, True)
        self.assertEqual(on_time["status"], "pass")
        self.assertEqual(late["status"], "pass")
        self.assertEqual(late["regions"]["us"]["due_late_native_clock"], 1)
        self.assertEqual(no_ack["status"], "ungraded")


if __name__ == "__main__":
    unittest.main()


def test_sregym_run_validity_needs_the_freeze_record_and_no_run_length(tmp_path):
    import json as _json

    run = tmp_path
    (run / "launch.json").write_text(
        _json.dumps({"runner": "sregym", "start_monotonic": 10.0, "saved_state": "usage-reporting"})
    )
    assert grade.validity(run, [])["status"] == "ungraded"
    (run / "native-cutoff.json").write_text(_json.dumps({"target_monotonic": 2500.0}))
    v = grade.validity(run, [])
    assert v["status"] == "pass", v
    (run / "abort.json").write_text("{}")
    assert grade.validity(run, [])["status"] == "fail"


def test_service_health_ignores_devices_that_did_not_send_during_the_run(tmp_path):
    import json as _json

    (tmp_path / "service-health-at-D.json").write_text(_json.dumps({"endpoints": {"us-health": {"status": 200}}}))
    live = ("dev-us-0001", "b", 1)
    old = ("dev-us-browser-tcp-01", "b", 1)
    ledgers = {live: {("db", "h1"): 1}, old: {("db", "h2"): 1}}
    metadata = {
        (live, "h1"): {"device_id": "dev-us-0001", "event_time_ms": 1_000_000},
        (old, "h2"): {"device_id": "dev-us-browser-tcp-01", "event_time_ms": 1},
    }
    boundary = {"native_D_interval": [1_050.0, 1_051.0]}
    # Without the live set the old test device is stale; with it, only live devices count.
    assert (
        grade.service_health(tmp_path, ledgers, metadata, boundary, True)["latest_per_device"]["us"]["devices_stale"]
        == 1
    )
    result = grade.service_health(tmp_path, ledgers, metadata, boundary, True, live_devices={"dev-us-0001"})
    assert result["latest_per_device"]["us"]["devices_stale"] == 0


def test_disk_reading_is_lost_only_when_its_broker_truncated_every_copy_after_t0(tmp_path):
    from scoring import destroyed_disk_copies, witness_epoch

    held = tmp_path / "held"
    held.mkdir()
    rows = [
        ("a", "raw-telemetry-2", 10),
        ("a", "raw-telemetry-2", 11),
        ("b", "raw-telemetry-2", 5),
        ("c", "raw-telemetry-0", 50),
    ]
    with (held / "observations.jsonl").open("w") as f:
        for device, part, offset in rows:
            f.write(
                json.dumps(
                    dict(
                        kind="copy",
                        source="us-work-02/var/lib/telemetry/kafka/kafka/" + part + "/00000000000000000000.log",
                        payload=dict(device_id=device, boot_id="x", seq=1),
                        offset=offset,
                    )
                )
                + "\n"
            )
    native = tmp_path / "native"
    native.mkdir()

    def line(when, part, cut):
        return (
            when
            + " stdout F [x] INFO [UnifiedLog partition="
            + part
            + ", dir=/data/kafka] Truncating to offset "
            + str(cut)
            + " (kafka.log.UnifiedLog)"
        )

    t0 = witness_epoch("2026-11-07T12:50:00.000000000-05:00 x")
    with (native / "us-work-02-destruction-witnesses.jsonl").open("w") as f:
        f.write(
            json.dumps(dict(host="us-work-02", text=line("2026-11-07T12:40:00.1-05:00", "raw-telemetry-0", 0))) + "\n"
        )  # before T0
        f.write(
            json.dumps(dict(host="us-work-02", text=line("2026-11-07T12:58:24.433382203-05:00", "raw-telemetry-2", 8)))
            + "\n"
        )
    only = {("a", "x", 1), ("b", "x", 1), ("c", "x", 1)}
    lost = destroyed_disk_copies({"held_disk_extraction": str(held)}, native, t0, only)
    # a: both copies above the cut; b: below the cut, so it survives on the log; c: only an earlier truncation
    assert lost == {("a", "x", 1)}
    assert destroyed_disk_copies({"held_disk_extraction": str(held)}, native, t0, {("b", "x", 1)}) == set()
    assert witness_epoch("2026-11-07T17:58:24Z x") == witness_epoch("2026-11-07T12:58:24-05:00 x")


def test_witnessed_destruction_turns_unknown_into_lost():
    row = dict(
        sha256="h",
        identity=("a", "x", 1),
        device_id="a",
        boot_id="x",
        seq=1,
        tenant="t",
        event_time_ms=0,
        quantity=1,
        region="us",
    )
    required = {("a", "x", 1): row}
    c, details = custody(required, {}, {}, {}, {}, {("a", "x", 1)})
    assert details[0]["status"] == "lost"
    c, details = custody(required, {}, {}, {}, {}, set())
    assert details[0]["status"] == "unknown"


def test_a_copy_outside_the_ledger_fails_custody():
    i = ("a", "x", 1)
    row = dict(
        sha256="h", identity=i, device_id="a", boot_id="x", seq=1, tenant="t", event_time_ms=0, quantity=1, region="us"
    )
    c, _ = custody({i: row}, {}, {(i, "h"): {"work/raw-telemetry-0.tar"}}, {})
    assert c["counts"]["preserved_only"] == 1 and c["status"] == "fail"
    c, _ = custody({i: row}, {i: Counter({("us", "h"): 1})}, {}, {})
    assert c["status"] == "pass"


def test_custody_counts_a_reading_only_in_its_own_regional_ledger():
    i = ("dev-us-0001", "x", 1)
    row = dict(
        sha256="h",
        identity=i,
        device_id="dev-us-0001",
        boot_id="x",
        seq=1,
        tenant="tenant-us-01",
        event_time_ms=0,
        quantity=1,
        region="us",
    )
    home = {"ledger-us": "us", "ledger-eu": "eu"}
    c, d = custody({i: row}, {i: Counter({("ledger-us", "h"): 1})}, {}, {}, home=home)
    assert c["status"] == "pass" and d[0]["status"] == "exactly_once"
    # Only in the other region's ledger: missing from its own.
    c, d = custody({i: row}, {i: Counter({("ledger-eu", "h"): 1})}, {}, {}, home=home)
    assert c["status"] == "fail" and d[0]["status"] == "wrong_region"
    # Once in each region: one extra occurrence.
    c, d = custody({i: row}, {i: Counter({("ledger-us", "h"): 1, ("ledger-eu", "h"): 1})}, {}, {}, home=home)
    assert c["status"] == "fail" and d[0]["status"] == "duplicated" and d[0]["extra_ledger_occurrences"] == 1


def test_service_health_counts_a_fleet_device_with_no_ledger_reading(tmp_path):
    (tmp_path / "service-health-at-D.json").write_text(json.dumps({"endpoints": {"us-health": {"status": 200}}}))
    live = ("dev-us-0001", "b", 1)
    ledgers = {live: {("ledger-us", "h1"): 1}}
    metadata = {(live, "h1"): {"device_id": "dev-us-0001", "event_time_ms": 1_000_000}}
    boundary = {"native_D_interval": [1_050.0, 1_051.0]}
    fleet = {"dev-us-0001", "dev-us-0002"}
    us = grade.service_health(
        tmp_path, ledgers, metadata, boundary, True, live_devices=fleet, home={"ledger-us": "us"}
    )["latest_per_device"]["us"]
    assert us["devices_without_ledger_reading"] == 1 and us["devices_stale"] == 1
    # A reading in the other region's ledger does not make a device fresh.
    us = grade.service_health(
        tmp_path, ledgers, metadata, boundary, True, live_devices={"dev-us-0001"}, home={"ledger-us": "eu"}
    )["latest_per_device"]["us"]
    assert us["devices_in_D_ledger"] == 0


def test_recurrence_fails_a_host_that_starts_without_cilium_rules():
    def host(tables):
        return dict(
            before=dict(rules=[dict(table=t) for t in tables]),
            after=dict(rules=[dict(table=t) for t in tables]),
            triggers=[
                dict(
                    unit="company-role",
                    phase="role",
                    removed_policy_rules=[],
                    lost_connectivity=[],
                    actual_network_lifecycle_witness=True,
                    command=dict(exit_code=0, automation_restored=True),
                )
            ],
        )

    names = [
        "us-parent-01",
        "us-work-01",
        "us-work-02",
        "us-work-03",
        "eu-parent-01",
        "eu-work-01",
        "eu-work-02",
        "eu-work-03",
    ]
    result = dict(hosts={n: host([2004, 2005]) for n in names}, declared_matrix=["role"], witness_phases=["role"])
    assert finish_recurrence(result)["status"] == "pass"
    result = dict(
        hosts={n: host([2004, 2005] if n != "us-work-02" else []) for n in names},
        declared_matrix=["role"],
        witness_phases=["role"],
    )
    r = finish_recurrence(result)
    assert r["status"] == "fail" and r["hosts_without_rules_before"] == ["us-work-02"]


def test_freshness_needs_the_reading_in_its_own_regional_ledger():
    i = ("dev-eu-0001", "x", 1)
    offers = {i: dict(sha256="h", offered_at=1000.0, region="eu")}
    ledgers = {i: Counter({("ledger-us", "h"): 1})}
    times = {(i, "h", "ledger-us"): [1001.0]}
    f = freshness(
        {},
        offers,
        ledgers,
        times,
        [2000.0, 2001.0],
        True,
        True,
        home={"ledger-us": "us", "ledger-eu": "eu"},
    )
    assert f["status"] == "fail", f
    ledgers = {i: Counter({("ledger-eu", "h"): 1})}
    times = {(i, "h", "ledger-eu"): [1001.0]}
    f = freshness(
        {},
        offers,
        ledgers,
        times,
        [2000.0, 2001.0],
        True,
        True,
        home={"ledger-us": "us", "ledger-eu": "eu"},
    )
    assert f["status"] == "pass", f


def test_service_health_allows_one_reporting_interval_plus_the_freshness_budget(tmp_path):
    import json as _json

    (tmp_path / "service-health-at-D.json").write_text(_json.dumps({"endpoints": {"us-health": {"status": 200}}}))
    live = ("dev-us-0001", "b", 1)
    ledgers = {live: {("ledger-us", "h1"): 1}}
    metadata = {(live, "h1"): {"device_id": "dev-us-0001", "event_time_ms": 1_000_000}}
    boundary = {"native_D_interval": [1_150.0, 1_151.0]}  # the latest reading is 150 s old at D
    args = (tmp_path, ledgers, metadata, boundary, True)
    assert (
        grade.service_health(*args, live_devices={"dev-us-0001"}, max_age=120)["latest_per_device"]["us"][
            "devices_stale"
        ]
        == 1
    )
    assert (
        grade.service_health(*args, live_devices={"dev-us-0001"}, max_age=220)["latest_per_device"]["us"][
            "devices_stale"
        ]
        == 0
    )


def test_recurrence_role_must_clone_the_platform_repository():
    checkout = "/var/lib/company/platform-role/checkout"

    def host(traced):
        return dict(
            before=dict(rules=[dict(table=t) for t in (2004, 2005)]),
            after=dict(rules=[]),
            fresh_checkouts=[dict(phase="role", path=checkout, exit_code=0)],
            triggers=[
                dict(
                    unit="company-role",
                    phase="role",
                    removed_policy_rules=[],
                    lost_connectivity=[],
                    actual_network_lifecycle_witness=True,
                    command=dict(exit_code=0, automation_restored=True, traced_commands=traced),
                )
            ],
        )

    names = [
        "us-parent-01",
        "us-work-01",
        "us-work-02",
        "us-work-03",
        "eu-parent-01",
        "eu-work-01",
        "eu-work-02",
        "eu-work-03",
    ]
    clone = [["git", "clone", "-q", "http://10.72.0.23:3000/operations/platform.git", checkout]]
    result = dict(hosts={n: host(clone) for n in names}, declared_matrix=["role"], witness_phases=["role"])
    assert finish_recurrence(result)["status"] == "pass"
    # A role unit replaced by a plain networkd restart never clones the repository: the fleet's fix was not tested.
    restart = [["systemctl", "restart", "systemd-networkd"]]
    result = dict(
        hosts={n: host(clone if n != "eu-work-01" else restart) for n in names},
        declared_matrix=["role"],
        witness_phases=["role"],
    )
    r = finish_recurrence(result)
    assert r["status"] == "fail" and r["role_without_source_clone"] == [dict(host="eu-work-01", unit="company-role")]
