"""Read the broker's retained browser-device view."""

import json
import secrets
import socket
import struct
import time


def varint(n):
    b = bytearray()
    while True:
        x, n = n % 128, n // 128
        b.append(x | (128 if n else 0))
        if not n:
            return bytes(b)


def string(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def packet(s):
    def read(n):
        b = b""
        while len(b) < n:
            c = s.recv(n - len(b))
            if not c:
                raise EOFError("MQTT stream closed")
            b += c
        return b

    first = read(1)[0]
    n = shift = 0
    for _ in range(4):
        x = read(1)[0]
        n |= (x & 127) << shift
        shift += 7
        if not x & 128:
            break
    else:
        raise ValueError("Malformed MQTT length")
    if n > 262144:
        raise ValueError("MQTT view record too large")
    return first, read(n)


def snapshot(host, port=31883):
    s = socket.create_connection((host, port), 3)
    s.settimeout(2)
    try:
        body = string("MQTT") + bytes([4, 2]) + struct.pack("!H", 15) + string("portal-" + secrets.token_hex(8))
        s.sendall(b"\x10" + varint(len(body)) + body)
        first, b = packet(s)
        if first != 0x20 or b[-1] != 0:
            raise RuntimeError("MQTT view connection rejected")
        body = b"\x00\x01" + string("telemetry/browser/#") + b"\x00"
        s.sendall(b"\x82" + varint(len(body)) + body)
        rows = []
        until = time.monotonic() + 1.2
        while len(rows) < 256 and time.monotonic() < until:
            s.settimeout(max(0.02, until - time.monotonic()))
            try:
                first, b = packet(s)
            except TimeoutError:
                break
            if first >> 4 == 3:
                n = struct.unpack("!H", b[:2])[0]
                offset = 2 + n + (2 if (first >> 1) & 3 else 0)
                r = json.loads(b[offset:])
                rows.append(r)
        s.sendall(b"\xe0\x00")
        return rows
    finally:
        s.close()
