#!/usr/bin/env python3
"""Insert raw-telemetry records from Kafka into the ledger, from a consumer group's committed offsets or the earliest offsets."""

import argparse
import json
import logging
import time

import pg8000.dbapi
from kafka import KafkaConsumer, TopicPartition

p = argparse.ArgumentParser()
p.add_argument("--bootstrap", required=True)
p.add_argument("--database", required=True)
p.add_argument("--port", type=int, default=31432)
p.add_argument("--group", default="ledger-writer")
p.add_argument("--from-earliest", action="store_true")
p.add_argument("--max-age-seconds", type=float, default=7200)
p.add_argument("--idle-seconds", type=float, default=10)
a = p.parse_args()
consumer = KafkaConsumer(
    bootstrap_servers=a.bootstrap.split(","),
    group_id=a.group,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    max_poll_records=500,
    api_version_auto_timeout_ms=15000,
)
# Read the selected group's committed positions without joining its live
# membership or changing the intake writer's assignment/checkpoint.
parts = consumer.partitions_for_topic("raw-telemetry")
assert parts == set(range(6)), parts
tps = [TopicPartition("raw-telemetry", x) for x in sorted(parts)]
consumer.assign(tps)
begins = consumer.beginning_offsets(tps)
ends = consumer.end_offsets(tps)
for tp in tps:
    committed = None if a.from_earliest else consumer.committed(tp)
    consumer.seek(tp, committed if committed is not None and begins[tp] <= committed <= ends[tp] else begins[tp])
c = pg8000.dbapi.connect(host=a.database, port=a.port, user="postgres", database="ledger", timeout=10)
counts = dict(read=0, discarded_age=0, inserted=0, failed=0)
batch = {}
last_progress = time.monotonic()
while True:
    for tp, messages in batch.items():
        if tp not in ends:
            raise RuntimeError("Replay assignment changed during bounded work")
        for m in messages:
            if m.offset >= ends[tp]:
                continue
            r = json.loads(m.value)
            counts["read"] += 1
            last_progress = time.monotonic()
            if a.max_age_seconds and time.time() - r["event_time"] > a.max_age_seconds:
                counts["discarded_age"] += 1
                continue
            try:
                q = c.cursor()
                q.execute(
                    "INSERT INTO readings(delivery_id,device_id,boot_id,seq,tenant,event_time,quantity,kafka_partition,kafka_offset) VALUES (%s,%s,%s,%s,%s,to_timestamp(%s),%s,%s,%s) ON CONFLICT(delivery_id) DO NOTHING",
                    (
                        dict(m.headers).get("delivery_id", b"").decode() or f"{m.partition}:{m.offset}",
                        r["device_id"],
                        r["boot_id"],
                        r["seq"],
                        r["tenant"],
                        r["event_time"],
                        r["quantity"],
                        m.partition,
                        m.offset,
                    ),
                )
                c.commit()
                counts["inserted"] += q.rowcount
            except Exception as e:
                c.rollback()
                counts["failed"] += 1
                logging.error("Replay transaction failed: %s", e)
    if all(consumer.position(tp) >= ends[tp] for tp in tps):
        break
    if time.monotonic() - last_progress > max(60, a.idle_seconds):
        raise TimeoutError("Replay stopped before reaching the partition ends")
    batch = consumer.poll(timeout_ms=1000)
consumer.close()
c.close()
print(
    json.dumps(
        dict(
            **counts,
            partition_ends={str(p.partition): v for p, v in ends.items()},
            selected_group=a.group,
            group_membership_unchanged=True,
        )
    )
)
