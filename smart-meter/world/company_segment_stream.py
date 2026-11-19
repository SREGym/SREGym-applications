#!/usr/bin/env python3
"""Print the records of raw-telemetry segment files in a directory as JSON lines, using kafka-dump-log."""

import argparse
import base64
import json
import os
import pathlib
import re
import subprocess
import sys

p = argparse.ArgumentParser()
p.add_argument("directory")
p.add_argument("--kafka", default="/opt/kafka")
a = p.parse_args()
files = sorted(pathlib.Path(a.directory).glob("raw-telemetry-*/*.log"))
assert files, "No raw-telemetry segment files"
count = 0
for f in files:
    partition = int(f.parent.name.rsplit("-", 1)[1])
    env = {**os.environ, "KAFKA_HEAP_OPTS": "-Xms32m -Xmx96m"}
    process = subprocess.Popen(
        [a.kafka + "/bin/kafka-dump-log.sh", "--files", str(f), "--deep-iteration", "--print-data-log"],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )
    for line in process.stdout:
        if not line.startswith("| offset:"):
            continue
        m = re.search(r"offset: (\d+) CreateTime: (\d+).*? key: (.*?) payload: (.*)$", line.rstrip("\n"))
        assert m, ("Unsupported record", line)
        offset, stamp, key, value = m.groups()
        r = json.loads(value)
        assert all(x in r for x in ["device_id", "boot_id", "seq", "tenant", "event_time", "quantity"])
        assert key == r["device_id"], ("Record key does not match device_id", str(f), offset)

        def encode(b):
            return base64.b64encode(b.encode()).decode()

        print(
            json.dumps(
                dict(
                    topic="raw-telemetry",
                    partition=partition,
                    offset=int(offset),
                    timestamp=int(stamp),
                    key=encode(key),
                    value=encode(value),
                    headers=[],
                )
            ),
            flush=True,
        )
        count += 1
    assert process.wait() == 0, ("kafka-dump-log failed", str(f))
print(
    json.dumps(
        dict(
            segment_files=len(files),
            records=count,
            headers="kafka-dump-log prints header names only, so headers are dropped; payload, key and event time are kept.",
        )
    ),
    file=sys.stderr,
)
