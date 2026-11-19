#!/usr/bin/env python3
"""Device fleet simulator: MQTT 3.1.1 persistent sessions publishing QoS 1 readings.

Each device keeps a resend buffer until PUBACK, resends unacknowledged packets
with DUP after reconnecting, and keeps its boot ID and sequence across
reconnects. A reading's identity is (device_id, boot_id, seq). The aggregate
offered rate is spread round-robin over all devices.

Control file (JSON, polled each second): {"rate": <readings/s>}. Stats are
written every 10 s. Acknowledged identities and value digests are appended to
local files; an acknowledged payload leaves the resend buffer. Boot IDs and
quantities use OS entropy.

  python3 sim.py --devices 1000 --rate 10 --port 21883 --region us
"""

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import random
import secrets
import signal
import struct
import time
import urllib.request
from decimal import Decimal
from pathlib import Path


def varint(n):
    out = bytearray()
    while True:
        byte, n = n % 128, n // 128
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def string(text):
    data = text.encode()
    return struct.pack("!H", len(data)) + data


def payload_digest(payload):
    """Hash a reading's values; field order and whitespace do not change the digest.

    Event time is numeric UTC seconds at millisecond precision.
    """
    reading = json.loads(payload)
    reading["event_time_ms"] = int(Decimal(str(reading.pop("event_time"))) * 1000)
    canonical = json.dumps(reading, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(canonical).hexdigest()


class Device:
    def __init__(self, fleet, index):
        self.fleet = fleet
        self.id = f"dev-{fleet.region}-{index:04d}"
        self.tenant = f"tenant-{fleet.region}-{index % fleet.tenants + 1:02d}"
        self.boot = secrets.token_hex(16)
        self.seq = 0
        self.pid = 0
        self.unacked = {}  # packet id -> (reading bytes, identity)
        self.writer = None
        self.connected = asyncio.Event()

    def next_pid(self):
        while True:
            self.pid = self.pid % 65535 + 1
            if self.pid not in self.unacked:
                return self.pid

    def publish_frame(self, pid, payload, dup):
        topic = f"{self.fleet.topic_root}/{self.tenant}/{self.id}"
        body = string(topic) + struct.pack("!H", pid) + payload
        return bytes([0x32 | (0x08 if dup else 0)]) + varint(len(body)) + body

    def reading(self):
        self.seq += 1
        identity = (self.id, self.boot, self.seq)
        payload = json.dumps(
            {
                "device_id": self.id,
                "boot_id": self.boot,
                "seq": self.seq,
                "tenant": self.tenant,
                "event_time": round(time.time(), 3),
                "quantity": secrets.randbelow(1000) + 1,
            },
            separators=(",", ":"),
        ).encode()
        pid = self.next_pid()
        self.unacked[pid] = (payload, identity)
        self.fleet.stats["published"] += 1
        # The offer log keeps identity, digest and time only, never the payload.
        self.fleet.offers.write(
            json.dumps(
                {
                    "device_id": identity[0],
                    "boot_id": identity[1],
                    "seq": identity[2],
                    "offered_at": time.time(),
                    "offered_monotonic_ns": time.monotonic_ns(),
                    "sha256": payload_digest(payload),
                },
                separators=(",", ":"),
            )
            + "\n"
        )
        if self.writer and self.connected.is_set():
            with contextlib.suppress(ConnectionError, RuntimeError):
                self.writer.write(self.publish_frame(pid, payload, False))

    async def run(self):
        backoff = 1.0
        while True:
            writer = None
            try:
                reader, writer = await asyncio.open_connection(self.fleet.host, self.fleet.port)
                connect = string("MQTT") + bytes([4, 0x00]) + struct.pack("!H", self.fleet.keepalive) + string(self.id)
                writer.write(bytes([0x10]) + varint(len(connect)) + connect)
                header = await asyncio.wait_for(reader.readexactly(4), 30)
                if header[0] != 0x20 or header[3] != 0:
                    raise ConnectionError(f"CONNACK refused {header.hex()}")
                self.writer = writer
                self.connected.set()
                self.fleet.stats["connects"] += 1
                backoff = 1.0
                for pid, (payload, _) in list(self.unacked.items()):
                    writer.write(self.publish_frame(pid, payload, True))
                    self.fleet.stats["resent"] += 1
                ping = asyncio.create_task(self.pinger(writer))
                try:
                    await self.receive(reader)
                finally:
                    ping.cancel()
            except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError):
                pass
            finally:
                if self.connected.is_set():
                    self.fleet.stats["disconnects"] += 1
                self.connected.clear()
                self.writer = None
                if writer is not None:
                    writer.close()
            await asyncio.sleep(backoff * (0.5 + random.random()))
            backoff = min(backoff * 2, 30.0)

    async def pinger(self, writer):
        while True:
            await asyncio.sleep(self.fleet.keepalive * 0.75)
            writer.write(b"\xc0\x00")

    async def receive(self, reader):
        while True:
            first = (await reader.readexactly(1))[0]
            length, shift = 0, 0
            while True:
                byte = (await reader.readexactly(1))[0]
                length |= (byte & 0x7F) << shift
                shift += 7
                if not byte & 0x80:
                    break
            body = await reader.readexactly(length) if length else b""
            if first >> 4 == 4:  # PUBACK
                pid = struct.unpack("!H", body[:2])[0]
                entry = self.unacked.pop(pid, None)
                if entry:
                    self.fleet.stats["acked"] += 1
                    if self.fleet.billing_url:
                        reading = json.loads(entry[0])
                        day = int(reading["event_time"] // 86400) * 86400
                        row = self.fleet.usage.setdefault(day, dict(day=day, readings=0, quantity=0))
                        row["readings"] += 1
                        row["quantity"] += reading["quantity"]
                    accepted_at = time.time()
                    self.fleet.acks.write("%s %s %d %.3f\n" % (*entry[1], accepted_at))  # noqa: UP031
                    self.fleet.digests.write(
                        json.dumps(
                            {
                                "device_id": entry[1][0],
                                "boot_id": entry[1][1],
                                "seq": entry[1][2],
                                "accepted_at": round(accepted_at, 3),
                                "accepted_monotonic_ns": time.monotonic_ns(),
                                "digest_version": 1,
                                "sha256": payload_digest(entry[0]),
                            },
                            separators=(",", ":"),
                        )
                        + "\n"
                    )


class Fleet:
    def __init__(self, args):
        self.region, self.host, self.port = args.region, args.host, args.port
        self.tenants, self.keepalive = args.tenants, args.keepalive
        self.rate = args.rate
        self.topic_root = "telemetry"  # a release can move devices to a new topic root
        self.billing_url = args.billing_url
        self.usage = {}
        self.usage_source = secrets.token_hex(16)
        self.control, self.stats_path = args.control, args.stats
        self.stats = {"published": 0, "acked": 0, "resent": 0, "connects": 0, "disconnects": 0}
        self.acks = open(args.acks, "a", buffering=1 << 16)  # noqa: SIM115
        self.digests = open(args.acks.with_name(args.acks.name + ".digests.jsonl"), "a", buffering=1 << 16)  # noqa: SIM115
        self.offers = open(args.acks.with_name(args.acks.name + ".offers.jsonl"), "a", buffering=1 << 16)  # noqa: SIM115
        self.devices = [Device(self, i) for i in range(args.devices)]

    async def ticker(self):
        index, next_time = 0, time.monotonic()
        while True:
            if self.rate <= 0:
                await asyncio.sleep(0.5)
                next_time = time.monotonic()
                continue
            self.devices[index % len(self.devices)].reading()
            index += 1
            next_time += 1.0 / self.rate
            delay = next_time - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            elif delay < -2:
                next_time = time.monotonic()

    async def housekeeping(self):
        while True:
            if self.control and self.control.exists():
                try:
                    control = json.loads(self.control.read_text())
                    self.rate = float(control.get("rate", self.rate))
                    self.topic_root = str(control.get("topic_root", self.topic_root))
                except (ValueError, OSError):
                    pass
            if int(time.time()) % 10 == 0 and self.stats_path:
                connected = sum(d.connected.is_set() for d in self.devices)
                outstanding = sum(len(d.unacked) for d in self.devices)
                self.stats_path.write_text(
                    json.dumps(
                        {
                            "time": time.time(),
                            "pid": os.getpid(),
                            "rate": self.rate,
                            "devices": len(self.devices),
                            "connected": connected,
                            "outstanding": outstanding,
                            **self.stats,
                        }
                    )
                    + "\n"
                )
                self.acks.flush()
                self.digests.flush()
                self.offers.flush()
            await asyncio.sleep(1)

    async def billing(self):
        # Fleet billing feed: accepted readings and quantity per UTC event day.
        # No event identities, digests or payloads leave the client.
        def send(payload):
            request = urllib.request.Request(
                self.billing_url, data=payload, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read(65536)

        while True:
            await asyncio.sleep(10)
            if self.billing_url:
                payload = json.dumps(
                    dict(source=self.usage_source, revision=int(time.time() * 1000), rows=list(self.usage.values()))
                ).encode()
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(send, payload)

    async def main(self, ramp):
        tasks = []
        for device in self.devices:
            tasks.append(asyncio.create_task(device.run()))
            await asyncio.sleep(ramp / len(self.devices))
        tasks += [
            asyncio.create_task(self.ticker()),
            asyncio.create_task(self.housekeeping()),
            asyncio.create_task(self.billing()),
        ]
        await asyncio.gather(*tasks)


async def run_fleet(args):
    fleet = Fleet(args)
    task = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await fleet.main(args.ramp)
    except asyncio.CancelledError:
        pass
    finally:
        fleet.acks.close()
        fleet.digests.close()
        fleet.offers.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--devices", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=10.0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=21883)
    parser.add_argument("--region", default="us")
    parser.add_argument("--tenants", type=int, default=20)
    parser.add_argument("--keepalive", type=int, default=60)
    parser.add_argument("--ramp", type=float, default=60.0, help="seconds to spread initial connects over")
    parser.add_argument("--control", type=Path)
    parser.add_argument("--stats", type=Path)
    parser.add_argument("--acks", type=Path, required=True)
    parser.add_argument("--billing-url", help="fleet usage endpoint for daily totals")
    args = parser.parse_args()
    asyncio.run(run_fleet(args))


if __name__ == "__main__":
    main()
