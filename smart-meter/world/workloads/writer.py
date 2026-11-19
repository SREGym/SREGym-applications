"""US ledger writer: raw Kafka to the PostgreSQL ledger.

Company first-party code (kafka-python 2.0.2, pg8000). Each poll's records are
written in one transaction. Rows are unique on delivery ID, so an MQTT
redelivery that reaches Kafka again becomes a second row.
Offsets auto-commit on the consumer's timer, and after a failed transaction the
loop moves on to the next poll: committed offsets can pass
records that never became durable in the ledger.

With ROLLUP_TOPIC set, an outbox thread publishes revisioned hourly aggregates:
every ROLLUP_INTERVAL seconds, a full snapshot of each (origin region, tenant,
UTC event hour) in the last two hours, with a new revision each time. MM2
mirrors the local-origin rollup topic to the other region.
"""

import json
import logging
import os
import threading
import time

import pg8000.dbapi
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("writer")

INSERT = (
    "INSERT INTO readings (delivery_id, device_id, boot_id, seq, tenant, event_time, quantity, "
    "kafka_partition, kafka_offset) VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), %s, %s, %s) "
    "ON CONFLICT (delivery_id) DO NOTHING"
)


def connect():
    while True:
        try:
            connection = pg8000.dbapi.connect(
                host=os.environ.get("PG_HOST", "postgres"), user="postgres", database="ledger", timeout=10
            )
            log.info("connected to ledger database")
            return connection
        except Exception as error:  # database restarting or unreachable
            log.error("database connect failed: %s", error)
            time.sleep(5)


ROLLUP = """SELECT tenant, extract(epoch FROM date_trunc('hour', event_time AT TIME ZONE 'UTC'))::bigint,
                   count(*), sum(quantity) FROM readings
            WHERE event_time >= date_trunc('hour', now()) - interval '2 hours' GROUP BY 1, 2"""


def publish_rollups(topic, region, interval):
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"].split(","),
                acks="all",
                client_id=f"outbox-{region}",
                api_version_auto_timeout_ms=15000,
            )
        except Exception as error:
            log.error("outbox broker discovery failed: %s; retrying", error)
            time.sleep(5)
    connection = None
    while True:
        time.sleep(interval)
        try:
            connection = connection or connect()
            cursor = connection.cursor()
            cursor.execute(ROLLUP)
            rows = cursor.fetchall()
            connection.commit()
            revision = int(time.time() * 1000)
            for tenant, hour, count, quantity in rows:
                snapshot = {
                    "origin_region": region,
                    "tenant": tenant,
                    "hour": hour,
                    "revision": revision,
                    "readings": count,
                    "quantity": int(quantity),
                }
                producer.send(topic, key=f"{region}|{tenant}|{hour}".encode(), value=json.dumps(snapshot).encode())
            producer.flush(timeout=60)
            log.info("published %d rollup snapshots at revision %d", len(rows), revision)
        except Exception as error:
            log.error("rollup publication failed: %s", error)
            connection = None


if os.environ.get("ROLLUP_TOPIC"):
    threading.Thread(
        target=publish_rollups,
        daemon=True,
        args=(
            os.environ["ROLLUP_TOPIC"],
            os.environ.get("REGION", "us"),
            float(os.environ.get("ROLLUP_INTERVAL", "60")),
        ),
    ).start()

consumer = KafkaConsumer(
    os.environ.get("RAW_TOPIC", "raw-telemetry"),
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"].split(","),
    group_id="ledger-writer",
    client_id=os.environ.get("POD_NAME", "writer"),
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    auto_offset_reset="earliest",
    max_poll_records=500,
)
database = connect()
written = 0
while True:
    batches = consumer.poll(timeout_ms=1000)
    rows = []
    for partition, records in batches.items():
        for record in records:
            reading = json.loads(record.value)
            headers = dict(record.headers or [])
            delivery = headers.get("delivery_id", b"").decode() or f"{partition.partition}:{record.offset}"
            rows.append(
                (
                    delivery,
                    reading["device_id"],
                    reading["boot_id"],
                    reading["seq"],
                    reading["tenant"],
                    reading["event_time"],
                    reading["quantity"],
                    partition.partition,
                    record.offset,
                )
            )
    if not rows:
        continue
    try:
        cursor = database.cursor()
        cursor.executemany(INSERT, rows)
        database.commit()
        written += len(rows)
        if written // 5000 != (written - len(rows)) // 5000:
            log.info("written %d", written)
    except Exception as error:
        log.error("batch of %d rows failed: %s; continuing with the next poll", len(rows), error)
        try:
            database.rollback()
        except Exception:
            database = connect()
