"""Run the real healer loop against a fake provider, network and clock.

Usage: python3 company_fleet_test.py [path to company_fleet.py, default: the one beside this file]
"""

import contextlib
import io
import json
import os
import sys
import tempfile
from pathlib import Path


class Stop(BaseException):
    pass


T0 = 1000.0


def run(path, timeline, reachable, offset, end=1300.0, stale=True):
    """timeline(name, t) -> (status, task); reachable(name, t) -> bool.
    Returns list of (t, server, action) the healer issued."""
    d = tempfile.mkdtemp()
    statefile = os.path.join(d, "fleet-state.json")
    nodes = {}
    if stale:
        # Days-old failure row, as left when the healer first stopped the host.
        nodes["eu-work-01"] = dict(
            last_status="RESCUE",
            quarantined=True,
            unreachable_since=1.0,
            unreachable_since_mono=1.0,
            failure_at=1.0,
            cooldown_until=21601.0,
        )
    Path(statefile).write_text(
        json.dumps(
            dict(
                nodes=nodes,
                eu_reservation=None,
                us_no_majority_since=None,
                retained_retry_used=False,
                enabled=True,
            ),
        )
    )
    src = Path(path).read_text().replace("/var/lib/company/fleet-state.json", statefile)
    pre, loop = src.split("\nwhile True:", 1)
    ns = {}
    exec(pre, ns)
    clock = [T0 + offset]
    actions = []
    names = {ip: n for n, ip in ns["NODES"].items()}

    class FakeTime:
        time = staticmethod(lambda: clock[0])
        monotonic = staticmethod(lambda: clock[0])

        @staticmethod
        def sleep(s):
            clock[0] += s
            if clock[0] > end:
                raise Stop

    def api(p, body=None):
        if p == "/v2.1/servers/detail":
            rows = []
            for n in ns["NODES"]:
                status, task = timeline(n, clock[0])
                rows.append({"id": n, "status": status, "OS-EXT-STS:task_state": task})
            return {"servers": rows}
        actions.append((round(clock[0], 1), p.split("/")[3], next(iter(body))))
        return {}

    ns.update(time=FakeTime, api=api, majority=lambda: True, reach=lambda ip: reachable(names[ip], clock[0]))
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            exec("while True:" + loop, ns)
    except Stop:
        pass
    return actions


def fast_restore(n, t):
    # Rescued, then unrescue starting at t=1012: RESCUE until 1013, SHUTOFF for
    # 0.8 s, ACTIVE from 1013.8, provider task done at 1014.4 (2.4 s in total).
    if n != "eu-work-01":
        return "ACTIVE", None
    if t < 1012:
        return "RESCUE", None
    if t < 1013:
        return "RESCUE", "unrescuing"
    if t < 1013.8:
        return "SHUTOFF", "unrescuing"
    if t < 1014.4:
        return "ACTIVE", "unrescuing"
    return "ACTIVE", None


def boots_in_45s(n, t):
    return n != "eu-work-01" or t >= 1060


def never_boots(n, t):
    return n != "eu-work-01"


def drops_at_1100(n, t):
    return n != "eu-work-01" or t < 1100


def steady(n, t):
    return "ACTIVE", None


def two_us_workers(n, t):
    # us-work-01 is stopped by the healer about 30 s after it loses its network.
    return ("SHUTOFF", None) if n == "us-work-01" and t >= 1136 else ("ACTIVE", None)


def two_us_reach(n, t):
    return not ((n == "us-work-01" and t >= 1100) or (n == "us-work-02" and t >= 1200))


def us_work_01_returns(n, t):
    # us-work-01 is stopped about 30 s after it loses its network and is back at t=1250.
    return ("SHUTOFF", None) if n == "us-work-01" and 1136 <= t < 1250 else ("ACTIVE", None)


def stray_answer(n, t):
    # us-work-02 is held at about t=1235 and stays cut off, except one probe answered at t=1300.
    if n == "us-work-01":
        return not 1100 <= t < 1250
    return n != "us-work-02" or t < 1200 or 1300 <= t < 1305


def recovers_then_fails(n, t):
    # us-work-02 answers steadily from t=1300 to 1400, then loses its network again.
    if n == "us-work-01":
        return not 1100 <= t < 1250
    return n != "us-work-02" or t < 1200 or 1300 <= t < 1400


path = (
    sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "company_fleet.py")
)
offsets = [i / 10 for i in range(50)]
bad = [o for o in offsets if run(path, fast_restore, boots_in_45s, o)]
print(f"fast restore, boot takes 45 s: host stopped in {len(bad)}/50 poll phases")
# The 120 s boot grace covers a 45 s boot.
assert not bad
stops = [run(path, fast_restore, never_boots, o) for o in offsets]
first = sorted(a[0][0] for a in stops if a)
print(
    f"fast restore, host never boots: stopped in {len(first)}/50 phases, "
    f"first stop t={first[0] if first else None}..{first[-1] if first else None}"
)
# Grace starts at the first poll p in [1014.4, 1019.4) that sees the settled ACTIVE; stop at p + 120 + 30.
assert len(first) == 50 and 1164.4 <= first[0] <= first[-1] < 1169.4
drops = [run(path, steady, drops_at_1100, o, stale=False) for o in offsets]
first = sorted(a[0][0] for a in drops if a)
print(
    f"healthy host loses network at t=1100: stopped in {len(first)}/50 phases, "
    f"t={first[0] if first else None}..{first[-1] if first else None}"
)
# Stop 30 s after the first poll in [1100, 1105) that finds the host unreachable.
assert len(first) == 50 and 1130.0 <= first[0] <= first[-1] < 1135.0
held = [run(path, two_us_workers, two_us_reach, o, end=1400.0, stale=False) for o in offsets]
first = sum(1 for a in held if [x for x in a if x[1] == "us-work-01"])
second = sum(1 for a in held if [x for x in a if x[1] == "us-work-02"])
print(f"two US workers fail in turn: first stopped in {first}/50 phases, second stopped in {second}/50")
assert first == 50 and second == 0
stray = sum(
    1
    for o in offsets
    if [x for x in run(path, us_work_01_returns, stray_answer, o, end=1500.0, stale=False) if x[1] == "us-work-02"]
)
print(f"held worker answers one stray probe: stopped in {stray}/50 phases")
recovered = sum(
    1
    for o in offsets
    if [
        x
        for x in run(path, us_work_01_returns, recovers_then_fails, o, end=1500.0, stale=False)
        if x[1] == "us-work-02"
    ]
)
print(f"held worker recovers, then fails with no other US repair: stopped in {recovered}/50 phases")
assert stray == 0 and recovered == 50
