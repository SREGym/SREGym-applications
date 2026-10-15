#!/usr/bin/env python3
"""Data API Service entrypoint for Agentic Retry Platform.

Wires together:
1. Bounded Data Plane (:8002) for database query traffic with immediate 503 shedding.
2. Isolated Control Plane (:8003) for Prometheus metrics, readiness, fault control, and cancellation.
3. Pure PostgreSQL wire protocol execution targeting PgBouncer.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading

from control_server import create_control_server
from data_server import create_data_server
from data_state import DataAPIState

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"data-api","msg":"%(message)s"}',
)
logger = logging.getLogger("data-api")

# Configuration
PORT = int(os.environ.get("PORT", "8002"))
CONTROL_PORT = int(os.environ.get("CONTROL_PORT", "8003"))

PGBOUNCER_HOST = os.environ.get("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.environ.get("PGBOUNCER_PORT", "5432"))
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

CONCURRENCY_LIMIT = int(os.environ.get("CONCURRENCY_LIMIT", "25"))
QUEUE_CAPACITY = int(os.environ.get("QUEUE_CAPACITY", "40"))
MAX_REQUEST_HANDLERS = int(os.environ.get("MAX_REQUEST_HANDLERS", "64"))
NORMAL_LATENCY = float(os.environ.get("NORMAL_LATENCY", "0.10"))
FAULT_LATENCY = float(os.environ.get("FAULT_LATENCY", "1.50"))


def main():
    state = DataAPIState(
        concurrency_limit=CONCURRENCY_LIMIT,
        queue_capacity=QUEUE_CAPACITY,
        max_request_handlers=MAX_REQUEST_HANDLERS,
        normal_latency=NORMAL_LATENCY,
        fault_latency=FAULT_LATENCY,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
    )

    control_server = create_control_server(
        host="0.0.0.0",
        port=CONTROL_PORT,
        state=state,
        pgbouncer_host=PGBOUNCER_HOST,
        pgbouncer_port=PGBOUNCER_PORT,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
    )

    data_server = create_data_server(
        host="0.0.0.0",
        port=PORT,
        state=state,
        pgbouncer_host=PGBOUNCER_HOST,
        pgbouncer_port=PGBOUNCER_PORT,
        max_request_handlers=MAX_REQUEST_HANDLERS,
    )

    control_thread = threading.Thread(
        target=control_server.serve_forever,
        name="control-plane-server",
        daemon=True,
    )
    control_thread.start()

    logger.info(
        f"Data API started: data_plane=0.0.0.0:{PORT} (max_handlers={MAX_REQUEST_HANDLERS}, "
        f"db_workers={CONCURRENCY_LIMIT}, queue={QUEUE_CAPACITY}), "
        f"control_plane=0.0.0.0:{CONTROL_PORT}, pgbouncer={PGBOUNCER_HOST}:{PGBOUNCER_PORT}"
    )

    def shutdown(signum, frame):
        logger.info("Shutting down Data API servers...")
        data_server.server_close()
        control_server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        data_server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        shutdown(None, None)


if __name__ == "__main__":
    main()
