"""Measure 15 wall-clock minutes of normal log ingestion and freeze the ingestion limit."""

import argparse
import hashlib
import json
import math
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from build_context import OUT, lab, note, require_build


def counter(text, name):
    return sum(
        float(match.group(1))
        for line in text.splitlines()
        if (match := re.fullmatch(re.escape(name) + r"(?:\{[^}]*\})?\s+([0-9.eE+-]+)(?:\s+\d+)?", line))
    )


def loki():
    with urllib.request.urlopen("http://127.0.0.1:28391/metrics", timeout=10) as response:
        text = response.read().decode()
    return dict(
        host_epoch=time.time(),
        monotonic=time.monotonic(),
        received_bytes=counter(text, "loki_distributor_bytes_received_total"),
        discarded_bytes=counter(text, "loki_discarded_bytes_total"),
        discarded_samples=counter(text, "loki_discarded_samples_total"),
        rate_limited_bytes=counter(
            "\n".join(line for line in text.splitlines() if 'reason="rate_limited"' in line),
            "loki_discarded_bytes_total",
        ),
    )


def collector(host, folder, label):
    command = "python3 -c \"import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9080/metrics',timeout=5).read().decode())\""
    metrics = lab.ssh(host, command, timeout=10)
    (folder / (host + "-" + label + "-metrics.txt")).write_text(metrics)
    return dict(
        host=host,
        dropped_entries=counter(metrics, "promtail_dropped_entries_total"),
        dropped_bytes=counter(metrics, "promtail_dropped_bytes_total"),
    )


def main():
    cfg = require_build()
    assert (OUT / "runtime-ready.json").exists()
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="01")
    case = parser.parse_args().case
    assert case.isdigit() and len(case) == 2
    folder = OUT / ("log-baseline-" + case)
    folder.mkdir(exist_ok=False)
    # Keep the installed configurations and binary option defaults. The
    # configs use no per-client batch/backoff override.
    defaults = lab.ssh("eu-work-02", "/var/lib/company/bin/promtail-linux-amd64 -help 2>&1", timeout=15, check=False)
    (folder / "promtail-help.txt").write_text(defaults)
    assert re.search(r"client\.batch-size-bytes.*?default 1048576", defaults, re.S)
    configurations = {}
    for host in cfg["memory_mib"]:
        text = lab.ssh(host, "cat /var/lib/company/promtail.yml", timeout=10)
        assert "batchsize:" not in text and "backoff_config:" not in text
        (folder / (host + "-promtail.yml")).write_text(text)
        configurations[host] = hashlib.sha256(text.encode()).hexdigest()
    with ThreadPoolExecutor(4) as pool:
        before = list(pool.map(lambda h: collector(h, folder, "start"), cfg["memory_mib"]))
    first = loki()
    previous = first
    (OUT / "log-baseline-current.json").write_text(json.dumps(dict(case=case, folder=str(folder), start=first)) + "\n")
    rows = []
    note("log-baseline-started", seconds=900, interval_seconds=10, rate_per_region=10)
    with (folder / "samples.jsonl").open("x", buffering=1) as output:
        for index in range(1, 91):
            delay = first["monotonic"] + index * 10 - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            current = loki()
            assert current["received_bytes"] >= previous["received_bytes"], "Loki counter reset during the baseline"
            row = {
                **current,
                "bytes_per_second": (current["received_bytes"] - previous["received_bytes"])
                / (current["monotonic"] - previous["monotonic"]),
            }
            rows.append(row)
            output.write(json.dumps(row) + "\n")
            previous = current
    with ThreadPoolExecutor(4) as pool:
        after = list(pool.map(lambda h: collector(h, folder, "end"), cfg["memory_mib"]))
    assert all(
        b["host"] == a["host"]
        and a["dropped_entries"] == b["dropped_entries"]
        and a["dropped_bytes"] == b["dropped_bytes"]
        for a, b in zip(after, before, strict=False)
    ), "Collectors dropped entries during the baseline"
    assert (
        rows[-1]["discarded_bytes"] == first["discarded_bytes"]
        and rows[-1]["discarded_samples"] == first["discarded_samples"]
    ), "Loki discarded entries during the baseline"
    p95 = sorted(r["bytes_per_second"] for r in rows)[math.ceil(0.95 * len(rows)) - 1]
    assert p95 > 0
    result = dict(
        actual_elapsed_seconds=rows[-1]["monotonic"] - first["monotonic"],
        samples=len(rows),
        normal_10_second_p95_bytes_per_second=p95,
        frozen_ingestion_rate_bytes_per_second=2 * p95,
        ingestion_rate_mb=2 * p95 / 2**20,
        ingestion_burst_size_mb=1,
        max_promtail_batch_bytes=1048576,
        normal_drops_observed=False,
        config_sha256=configurations,
        collectors_before=before,
        collectors_after=after,
    )
    (folder / "frozen-limit.json").write_text(json.dumps(result, indent=2) + "\n")
    note("log-baseline-complete", **result)


if __name__ == "__main__":
    try:
        main()
    except BaseException as error:
        note("log-baseline-failed", error=repr(error))
        raise
