#!/usr/bin/env python3
"""Export a topic's records as JSON lines, or produce JSON lines into a topic."""

import argparse
import base64
import json
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

p = argparse.ArgumentParser()
p.add_argument("action", choices=["export", "import"])
p.add_argument("--bootstrap", required=True)
p.add_argument("--topic", default="raw-telemetry")
a = p.parse_args()


def enc(v):
    return None if v is None else base64.b64encode(v).decode()


def dec(v):
    return None if v is None else base64.b64decode(v, validate=True)


if a.action == "export":
    c = KafkaConsumer(
        bootstrap_servers=a.bootstrap.split(","), enable_auto_commit=False, group_id=None, request_timeout_ms=30000
    )
    ids = c.partitions_for_topic(a.topic)
    assert ids, "No topic partitions"
    tps = [TopicPartition(a.topic, x) for x in sorted(ids)]
    c.assign(tps)
    lo = c.beginning_offsets(tps)
    hi = c.end_offsets(tps)
    for tp in tps:
        c.seek(tp, lo[tp])
    done = {tp for tp in tps if lo[tp] == hi[tp]}
    count = 0
    last = time.monotonic()
    while len(done) < len(tps):
        batch = c.poll(timeout_ms=1000)
        for tp, rs in batch.items():
            for r in rs:
                if r.offset < hi[tp]:
                    print(
                        json.dumps(
                            dict(
                                topic=r.topic,
                                partition=r.partition,
                                offset=r.offset,
                                timestamp=r.timestamp,
                                key=enc(r.key),
                                value=enc(r.value),
                                headers=[(k, enc(v)) for k, v in r.headers],
                            )
                        ),
                        flush=True,
                    )
                    count += 1
                    last = time.monotonic()
                if r.offset >= hi[tp] - 1:
                    done.add(tp)
        if time.monotonic() - last > 60:
            raise TimeoutError("Export stopped before reaching the partition ends")
    print(
        json.dumps(
            dict(
                beginning={str(t.partition): lo[t] for t in tps},
                end={str(t.partition): hi[t] for t in tps},
                records=count,
            )
        ),
        file=sys.stderr,
    )
    c.close()
else:
    p = KafkaProducer(bootstrap_servers=a.bootstrap.split(","), acks="all", compression_type="gzip")
    count = 0
    for line in sys.stdin:
        r = json.loads(line)
        value = dec(r["value"])
        reading = json.loads(value)
        assert all(k in reading for k in ["device_id", "boot_id", "seq", "tenant", "event_time", "quantity"])
        p.send(
            a.topic,
            key=dec(r["key"]),
            value=value,
            headers=[(k, dec(v)) for k, v in r["headers"]]
            + [("imported_from", f"{r['topic']}:{r['partition']}:{r['offset']}".encode())],
        ).get(timeout=30)
        count += 1
    p.flush()
    p.close()
    print(json.dumps(dict(imported=count)), file=sys.stderr)
