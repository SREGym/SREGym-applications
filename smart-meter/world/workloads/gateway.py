"""Forward regional MQTT readings to Kafka."""

import json
import logging
import os
import sys
import time
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("gateway")

CLIENT_ID = os.environ.get("CLIENT_ID") or os.environ["POD_NAME"]
TOPIC = os.environ.get("RAW_TOPIC", "raw-telemetry")
SUBSCRIPTION = os.environ.get("SUBSCRIPTION", "telemetry/#")

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"].split(","),
    client_id=f"gateway-{CLIENT_ID}",
    api_version_auto_timeout_ms=15000,
    acks="all",
    compression_type="gzip",
    linger_ms=5,
    retries=5,
    max_in_flight_requests_per_connection=1,
    request_timeout_ms=15000,
)
state = {"forwarded": 0}


def on_connect(client, userdata, flags, rc):
    log.info("connected rc=%s session_present=%s client_id=%s", rc, flags.get("session present"), CLIENT_ID)
    if rc == 0:
        client.subscribe(SUBSCRIPTION, qos=1)


def on_message(client, userdata, message):
    reading = json.loads(message.payload)
    delivery = str(uuid.uuid4())
    producer.send(
        TOPIC, key=reading["device_id"].encode(), value=message.payload, headers=[("delivery_id", delivery.encode())]
    ).get(timeout=30)
    state["forwarded"] += 1
    if state["forwarded"] % 1000 == 0:
        log.info("forwarded %d", state["forwarded"])


client = mqtt.Client(client_id=CLIENT_ID, clean_session=False, protocol=mqtt.MQTTv311)
client.on_connect = on_connect
client.on_message = on_message
client.max_inflight_messages_set(32)
client.connect(os.environ.get("MQTT_HOST", "emqx"), int(os.environ.get("MQTT_PORT", "1884")), keepalive=30)
client.loop_start()
while True:
    time.sleep(5)
    if not client._thread or not client._thread.is_alive():
        log.error("MQTT loop stopped (callback or connection failure); exiting for restart")
        sys.exit(1)
