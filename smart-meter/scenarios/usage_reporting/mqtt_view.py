"""Bounded endpoint samples plus the ledger's latest device readings."""

import concurrent.futures
import json
import secrets
import socket
import struct
import time

from company_mqtt_latest import packet, string, varint


def endpoint(host, port):
    try:
        with socket.create_connection((host, port), 1) as sock:
            sock.settimeout(1)
            body = string("MQTT") + bytes([4, 2]) + struct.pack("!H", 15) + string("portal-" + secrets.token_hex(8))
            sock.sendall(b"\x10" + varint(len(body)) + body)
            first, body = packet(sock)
            if first != 0x20 or body[-1] != 0:
                raise ValueError("CONNACK rejected")
            body = b"\x00\x01" + string("telemetry/#") + b"\x00"
            sock.sendall(b"\x82" + varint(len(body)) + body)
            samples = []
            end = time.monotonic() + 1
            while len(samples) < 256 and time.monotonic() < end:
                sock.settimeout(max(0.01, end - time.monotonic()))
                try:
                    first, body = packet(sock)
                except TimeoutError:
                    break
                if first >> 4 == 3:
                    size = int.from_bytes(body[:2], "big")
                    samples.append(json.loads(body[2 + size :]))
            sock.sendall(b"\xe0\x00")
            return dict(host=host, port=port, connected=True, samples=samples)
    except (OSError, ValueError, EOFError) as error:
        return dict(host=host, port=port, connected=False, error=str(error), samples=[])


def live(sql, region):
    endpoints = [("10.72.0.21", 31883), ("10.72.0.23", 31884)]
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        observed = list(pool.map(lambda pair: endpoint(*pair), endpoints))
    rows = sql(
        "SELECT DISTINCT ON(device_id) device_id,boot_id,seq,tenant,extract(epoch FROM event_time),quantity FROM readings ORDER BY device_id,event_time DESC,seq DESC"
    )
    now = time.time()
    latest = [
        dict(
            device_id=r[0],
            boot_id=r[1],
            seq=r[2],
            tenant=r[3],
            event_time=float(r[4]),
            quantity=r[5],
            stale=now - float(r[4]) > 120,
        )
        for r in rows
    ]
    by_tenant = []
    for i in range(1, 21):
        tenant = f"tenant-eu-{i:02d}"
        devices = [r for r in latest if r["tenant"] == tenant and r["device_id"].startswith("dev-eu-")]
        fresh = sum(not r["stale"] for r in devices)
        by_tenant.append(
            dict(tenant=tenant, expected_devices=50, fresh_devices=fresh, missing_or_stale_devices=max(0, 50 - fresh))
        )
    return dict(
        region=region,
        view="Latest ledger values and live MQTT endpoint samples",
        rows=latest,
        mqtt_endpoints=observed,
        tenants=by_tenant,
    )
