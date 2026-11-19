"""Verify the frozen ingestion limit over ten wall-clock minutes of normal service."""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from build_context import OUT, lab, note, require_build
from log_baseline import collector, loki


def main():
    cfg = require_build()
    applied = json.loads((OUT / "measured-log-limit-applied.json").read_text())
    folder = OUT / "limited-normal-verification"
    folder.mkdir(exist_ok=False)
    live = [name for name in cfg["memory_mib"] if lab.pid_of(lab.VM / (name + ".pid"))]
    assert len(live) == 8
    with ThreadPoolExecutor(4) as pool:
        before = list(pool.map(lambda h: collector(h, folder, "start"), live))
    first = loki()
    rows = []
    note("log-limit-check-started", seconds=600, live_collectors=live)
    with (folder / "samples.jsonl").open("x", buffering=1) as output:
        for index in range(1, 61):
            delay = first["monotonic"] + index * 10 - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            row = loki()
            rows.append(row)
            output.write(json.dumps(row) + "\n")
    with ThreadPoolExecutor(4) as pool:
        after = list(pool.map(lambda h: collector(h, folder, "end"), live))
    assert (
        rows[-1]["discarded_bytes"] == first["discarded_bytes"]
        and rows[-1]["discarded_samples"] == first["discarded_samples"]
    ), "Loki discarded entries under the limit"
    assert all(
        a["host"] == b["host"]
        and a["dropped_entries"] == b["dropped_entries"]
        and a["dropped_bytes"] == b["dropped_bytes"]
        for a, b in zip(after, before, strict=False)
    ), "Collectors dropped entries under the limit"
    result = dict(
        actual_elapsed_seconds=rows[-1]["monotonic"] - first["monotonic"],
        before=before,
        after=after,
        frozen_ingestion_rate_mb=applied["ingestion_rate_mb"],
        normal_drops_observed=False,
    )
    (folder / "complete.json").write_text(json.dumps(result, indent=2) + "\n")
    note("log-limit-check-complete", **result)


if __name__ == "__main__":
    main()
