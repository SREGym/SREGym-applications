"""Pure Python PostgreSQL v3.0 wire protocol client for PgBouncer / PostgreSQL.

Provides deterministic message framing (recv_exact, recv_message) and query execution
strictly against the configured PgBouncer connection pool. Fails closed without simulation.
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
import time

logger = logging.getLogger("data-api.pg_wire")


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Reads exactly n bytes from the socket, raising ConnectionError on premature EOF."""
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("unexpected EOF reading postgres wire protocol")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_message(sock: socket.socket) -> tuple[bytes, bytes]:
    """Reads a framed PostgreSQL message: 1 byte type, 4 byte length, and body."""
    mtype = recv_exact(sock, 1)
    mlen = struct.unpack("!I", recv_exact(sock, 4))[0]
    body = recv_exact(sock, mlen - 4) if mlen > 4 else b""
    return mtype, body


def execute_pg_query(
    host: str,
    port: int,
    latency_seconds: float,
    cancel_event: threading.Event,
    db_user: str = "postgres",
    db_name: str = "agentic_db",
) -> tuple[bool, str]:
    """Executes SELECT pg_sleep(...) via PgBouncer using PostgreSQL v3.0 wire protocol.

    Returns:
        (True, "ok") on successful query execution.
        (False, error_message) on failure, timeout, or cancellation.
    """
    try:
        sock = socket.create_connection((host, port), timeout=max(2.0, latency_seconds + 5.0))
    except Exception as exc:
        return False, f"database connection unavailable ({host}:{port}): {exc}"

    try:
        # 1. StartupMessage: len (int32), protocol version 196608 (int32), key-value params
        user_bytes = db_user.encode("utf-8")
        db_bytes = db_name.encode("utf-8")
        payload = b"user\x00" + user_bytes + b"\x00database\x00" + db_bytes + b"\x00\x00"
        msg_len = 4 + 4 + len(payload)
        sock.sendall(struct.pack("!II", msg_len, 196608) + payload)

        # 2. Wait for ReadyForQuery ('Z')
        sock.settimeout(5.0)
        while True:
            mtype, body = recv_message(sock)
            if mtype == b"Z":
                break
            if mtype == b"E":
                sock.close()
                return False, f"PostgreSQL startup error: {body.decode('utf-8', errors='replace')}"

        # 3. Send Query 'Q': SELECT pg_sleep(latency_seconds), id, key, value FROM knowledge LIMIT 1;
        sql = f"SELECT pg_sleep({latency_seconds:.4f}), id, key, value FROM knowledge LIMIT 1;\x00".encode("utf-8")
        sock.sendall(b"Q" + struct.pack("!I", 4 + len(sql)) + sql)

        # 4. Receive query responses with cancellation checking
        sock.settimeout(0.2)
        start_q = time.time()
        timeout_limit = latency_seconds + 5.0

        while time.time() - start_q < timeout_limit:
            if cancel_event.is_set():
                sock.close()
                return False, "Query cancelled during execution"
            try:
                mtype, body = recv_message(sock)
                if mtype == b"Z":
                    sock.close()
                    return True, "ok"
                if mtype == b"E":
                    sock.close()
                    return False, f"PostgreSQL query error: {body.decode('utf-8', errors='replace')}"
            except socket.timeout:
                continue
            except Exception as exc:
                sock.close()
                return False, f"PostgreSQL read error: {exc}"

        sock.close()
        return False, "PostgreSQL query execution timed out"
    except Exception as exc:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM):
            sock.close()
        return False, f"database query failed: {exc}"
