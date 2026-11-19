"""Client-side MQTT endpoint selection; streams bytes without retaining payloads."""

import asyncio
import json
import re
import time
from pathlib import Path

PRIMARY = ("10.72.0.21", 31883)
SECONDARY = ("10.72.0.23", 31884)


def cohort(client):
    match = re.fullmatch(r"dev-eu-(\d{4})", client)
    if not match or int(match[1]) >= 1000:
        raise ValueError("Unexpected fleet client")
    # tenant=index%20; groups of twenty ensure exactly 30/50 per tenant.
    return "secondary" if (int(match[1]) // 20) % 5 < 3 else "primary"


async def connect_frame(reader):
    first = await reader.readexactly(1)
    if first != b"\x10":
        raise ValueError("Expected CONNECT")
    encoded = bytearray()
    length = shift = 0
    for _ in range(4):
        b = (await reader.readexactly(1))[0]
        encoded.append(b)
        length |= (b & 127) << shift
        if not b & 128:
            break
        shift += 7
    else:
        raise ValueError("Malformed CONNECT")
    if length > 4096:
        raise ValueError("CONNECT too large")
    body = await reader.readexactly(length)
    if body[:7] != b"\x00\x04MQTT\x04" or len(body) < 12:
        raise ValueError("Only the existing MQTT 3.1.1 fleet is supported")
    size = int.from_bytes(body[10:12], "big")
    client = body[12 : 12 + size].decode()
    if len(body[12 : 12 + size]) != size:
        raise ValueError("Short client ID")
    return first + bytes(encoded) + body, client


async def pipe(reader, writer):
    while data := await reader.read(65536):
        writer.write(data)
        await writer.drain()


COUNTS = dict(primary=0, secondary=0, errors=0)


async def handle(reader, writer):
    remote = None
    tasks = []
    try:
        frame, client = await asyncio.wait_for(connect_frame(reader), 5)
        try:
            side = cohort(client)
        except ValueError:
            # Other clients keep their original endpoint.
            side = "primary"
        upstream, remote = await asyncio.wait_for(
            asyncio.open_connection(*(SECONDARY if side == "secondary" else PRIMARY)), 3
        )
        remote.write(frame)
        await remote.drain()
        COUNTS[side] += 1
        tasks = [asyncio.create_task(pipe(reader, remote)), asyncio.create_task(pipe(upstream, writer))]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except (ValueError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError):
        COUNTS["errors"] += 1
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for stream in (writer, remote):
            if stream:
                stream.close()


async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", 31884)
    async with server:
        while True:
            Path("/run/mqtt-route/counts.json").write_text(json.dumps(dict(at=time.time(), **COUNTS)))
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
