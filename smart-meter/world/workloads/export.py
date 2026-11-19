"""Export raw-telemetry up to the end offsets read at start."""

import json
import os

from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"].split(","), enable_auto_commit=False, consumer_timeout_ms=60000
)
partitions = [TopicPartition("raw-telemetry", i) for i in range(6)]
consumer.assign(partitions)
consumer.seek_to_beginning(*partitions)
beginning = consumer.beginning_offsets(partitions)
end = consumer.end_offsets(partitions)
done = {tp for tp in partitions if end[tp] <= beginning[tp]}
exported = 0
for message in consumer:
    tp = TopicPartition(message.topic, message.partition)
    if message.offset < end[tp]:
        row = [
            message.partition,
            message.offset,
            json.loads(message.value),
            dict(message.headers or []).get("delivery_id", b"").decode(),
        ]
        if os.environ.get("EXPORT_KEYS") == "1":
            row.append(None if message.key is None else message.key.decode())
        print(json.dumps(row))
        exported += 1
    if message.offset >= end[tp] - 1:
        done.add(tp)
    if len(done) == len(partitions):
        break
if len(done) != len(partitions):
    raise RuntimeError("bounded raw export did not reach every end offset")
print(
    "#export_bounds "
    + json.dumps(
        {
            "beginning": {tp.partition: beginning[tp] for tp in partitions},
            "end": {tp.partition: end[tp] for tp in partitions},
            "records_exported": exported,
        }
    ),
    flush=True,
)
consumer.close()
