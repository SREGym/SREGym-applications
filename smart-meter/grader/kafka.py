"""Read Kafka v2 batches on offline disks, retaining offsets and bad tails.

Records are decoded directly, including CRC32C, without starting Java.
"""

import ctypes
import ctypes.util
import gzip
import struct
import subprocess


def crc32c(data):
    crc = 0xFFFFFFFF
    for b in data:
        crc = TABLE[(crc ^ b) & 255] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def _table():
    out = []
    for n in range(256):
        for _ in range(8):
            n = (n >> 1) ^ (0x82F63B78 if n & 1 else 0)
        out.append(n)
    return out


TABLE = _table()


def varint(data, pos):
    value = 0
    for shift in range(0, 70, 7):
        b = data[pos]
        pos += 1
        value |= (b & 127) << shift
        if not b & 128:
            return (value >> 1) ^ -(value & 1), pos
    raise ValueError("Invalid Kafka varint")


def decompress(data, kind):
    if kind == 0:
        return data
    if kind == 1:
        return gzip.decompress(data)
    if kind == 4:
        p = subprocess.run(["zstd", "-dc"], input=data, capture_output=True, check=True, timeout=15)
        return p.stdout
    if kind == 3:
        lib = ctypes.CDLL(ctypes.util.find_library("lz4"))
        ctx = ctypes.c_void_p()
        lib.LZ4F_createDecompressionContext.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint]
        lib.LZ4F_decompress.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.c_void_p,
        ]
        lib.LZ4F_decompress.restype = ctypes.c_size_t
        lib.LZ4F_isError.argtypes = [ctypes.c_size_t]
        lib.LZ4F_freeDecompressionContext.argtypes = [ctypes.c_void_p]
        if lib.LZ4F_createDecompressionContext(ctypes.byref(ctx), 100):
            raise ValueError("LZ4 context")
        out, pos = bytearray(), 0
        try:
            while pos < len(data):
                target = ctypes.create_string_buffer(1 << 20)
                size, consumed = ctypes.c_size_t(len(target)), ctypes.c_size_t(len(data) - pos)
                status = lib.LZ4F_decompress(ctx, target, ctypes.byref(size), data[pos:], ctypes.byref(consumed), None)
                if lib.LZ4F_isError(status) or (not consumed.value and not size.value):
                    raise ValueError("Invalid LZ4 frame")
                out.extend(target.raw[: size.value])
                pos += consumed.value
                if len(out) > 256 << 20:
                    raise ValueError("Oversized LZ4 batch")
        finally:
            lib.LZ4F_freeDecompressionContext(ctx)
        return bytes(out)
    raise ValueError(f"Unsupported Kafka compression {kind}")


def records(path, issue):
    """Yield (offset, timestamp_ms, key, value); issue receives exact coverage gaps."""
    length = path.stat().st_size
    pos = 0
    with path.open("rb") as stream:
        while pos < length:
            head = stream.read(12)
            if not head or not any(head):
                # Preallocation is harmless only if the entire remaining tail is zero.
                nonzero = any(head)
                while chunk := stream.read(1 << 20):
                    nonzero |= any(chunk)
                if nonzero:
                    issue(dict(path=str(path), offset=pos, error="nonzero unframed tail"))
                return
            if len(head) != 12:
                issue(dict(path=str(path), offset=pos, error="short batch header"))
                return
            base, size = struct.unpack(">qi", head)
            if size < 49 or size > 100_000_000 or pos + 12 + size > length:
                issue(dict(path=str(path), offset=pos, error="invalid or torn batch", tail_bytes=length - pos))
                return
            body = stream.read(size)
            try:
                if body[4] != 2:
                    raise ValueError("Unsupported Kafka magic")
                if crc32c(body[9:]) != struct.unpack_from(">I", body, 5)[0]:
                    raise ValueError("CRC32C mismatch")
                attributes = struct.unpack_from(">h", body, 9)[0]
                stamp = struct.unpack_from(">q", body, 15)[0]
                count = struct.unpack_from(">i", body, 45)[0]
                payload = decompress(body[49:], attributes & 7)
                cursor = 0
                for _ in range(count):
                    record_len, cursor = varint(payload, cursor)
                    end = cursor + record_len
                    if record_len < 0 or end > len(payload):
                        raise ValueError("Torn record")
                    cursor += 1  # record attributes
                    delta, cursor = varint(payload, cursor)
                    offset, cursor = varint(payload, cursor)
                    keylen, cursor = varint(payload, cursor)
                    key = payload[cursor : cursor + max(0, keylen)]
                    cursor += max(0, keylen)
                    valuelen, cursor = varint(payload, cursor)
                    value = payload[cursor : cursor + max(0, valuelen)]
                    cursor += max(0, valuelen)
                    if cursor > end:
                        raise ValueError("Invalid record lengths")
                    cursor = end  # headers are not reading payloads
                    if not attributes & 0x20:  # exclude transaction-control records
                        yield base + offset, stamp + delta, key, value
                if cursor != len(payload):
                    raise ValueError("Unexpected trailing batch bytes")
            except (ValueError, IndexError, struct.error, OSError, subprocess.SubprocessError) as e:
                issue(dict(path=str(path), offset=pos, error=str(e)))
            pos += 12 + size
