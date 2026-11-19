"""Semantic comparison of saved readings.

Identity is device/boot/sequence. Custody requires the payload, never an index
or a count. Milliseconds and integer quantities match sim.payload_digest.
"""

import base64
import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

FIELDS = {"device_id", "boot_id", "seq", "tenant", "quantity"}


def epoch(value):
    if isinstance(value, str):
        try:
            return float(Decimal(value))
        except Exception:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    return float(value)


def millis(value):
    if isinstance(value, str):
        try:
            return int(Decimal(value) * 1000)
        except Exception:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                raise ValueError("Timestamp has no timezone") from None
            # The queue reader ships this source to guests, whose Python may predate datetime.UTC.
            d = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)  # noqa: UP017
            return (d.days * 86400 + d.seconds) * 1000 + d.microseconds // 1000
    return int(Decimal(str(value)) * 1000)


def canonical(row):
    p = {k: str(row[k]) for k in ("device_id", "boot_id", "tenant")}
    integers = {}
    for k in ("seq", "quantity"):
        try:
            value = Decimal(str(row[k]))
        except InvalidOperation as e:
            raise ValueError("Invalid semantic " + k) from e
        integers[k] = int(value)
        if value != integers[k]:
            raise ValueError("Noninteger semantic " + k)
    p.update(
        **integers, event_time_ms=int(row["event_time_ms"]) if "event_time_ms" in row else millis(row["event_time"])
    )
    digest = hashlib.sha256(
        json.dumps(p, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    return (p["device_id"], p["boot_id"], p["seq"]), digest, p


def identity(row):
    v = row.get("identity")
    if v is None:
        v = (row["device_id"], row["boot_id"], row["seq"])
    return str(v[0]), str(v[1]), int(v[2])


def region(row):
    for value in (row.get("tenant", ""), row.get("device_id", ""), (row.get("identity") or [""])[0]):
        m = re.match(r"(?:tenant|dev)-(us|eu|browser)-", value)
        if m:
            return m[1]
    return row.get("region", "unknown")


def jsonlines(path):
    # Logs captured from a running guest can end in a torn write: a final line with no
    # newline, or zero bytes where the file was extended but the data never reached disk.
    # Both are skipped; any other invalid line is an error.
    with Path(path).open() as stream:
        for number, line in enumerate(stream, 1):
            if line.strip():
                try:
                    yield json.loads(line)
                except ValueError as e:
                    if not line.strip("\0 \t\r\n") or not line.endswith("\n"):
                        continue
                    raise ValueError(f"{path}:{number}: invalid JSON") from e


def payloads(value, depth=0):
    """Decode the ordinary JSON/base64 record wrappers used by saved exports."""
    if depth > 5:
        return
    if isinstance(value, dict):
        if value.keys() >= FIELDS and ("event_time" in value or "event_time_ms" in value):
            yield value
            return
        if (
            "identity" in value
            and isinstance(value["identity"], (list, tuple))
            and len(value["identity"]) == 3
            and {"tenant", "quantity"} <= value.keys()
            and ("event_time" in value or "event_time_ms" in value)
        ):
            p = {k: value[k] for k in ("tenant", "quantity")}
            p.update(device_id=value["identity"][0], boot_id=value["identity"][1], seq=value["identity"][2])
            p.update({k: value[k] for k in ("event_time", "event_time_ms") if k in value})
            yield p
            return
        for key, child in value.items():
            if isinstance(child, (dict, list)) or key in ("payload", "value"):
                yield from payloads(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            yield from payloads(child, depth + 1)
    elif isinstance(value, (str, bytes)):
        try:
            decoded = json.loads(value)
        except (ValueError, TypeError):
            try:
                decoded = json.loads(base64.b64decode(value, validate=True))
            except (ValueError, TypeError):
                return
        yield from payloads(decoded, depth + 1)


def copy_unescape(value):
    if value == r"\N":
        return None
    escapes = {"t": "\t", "n": "\n", "r": "\r", "b": "\b", "f": "\f", "v": "\v", "\\": "\\"}
    return re.sub(
        r"\\([0-7]{1,3}|x[0-9a-fA-F]{1,2}|.)",
        lambda m: chr(int(m[1], 8))
        if m[1][0] in "01234567"
        else chr(int(m[1][1:], 16))
        if m[1].startswith("x")
        else escapes.get(m[1], m[1]),
        value,
    )


def sql_dump(stream):
    """Parse COPY data; never execute saved SQL."""
    table = None
    columns = []
    for line in stream:
        match = re.match(r'COPY (?:public\.)?"?(\w+)"? \(([^)]+)\) FROM stdin;', line)
        if match:
            table, columns = match[1], [c.strip().strip('"') for c in match[2].split(",")]
        elif line.rstrip("\r\n") == r"\.":
            table = None
        elif table:
            fields = line.rstrip("\r\n").split("\t")
            if len(fields) != len(columns):
                raise ValueError("Incomplete PostgreSQL COPY row")
            yield table, dict(zip(columns, map(copy_unescape, fields), strict=True))
    if table:
        raise ValueError("Incomplete PostgreSQL COPY section")
