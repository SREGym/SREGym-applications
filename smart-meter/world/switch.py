#!/usr/bin/env python3
"""Userspace learning Ethernet switch for QEMU dgram netdevs over unix sockets.

Each guest NIC is `-netdev dgram,local.type=unix,local.path=<vm>.sock,
remote.type=unix,remote.path=<switch>.sock`. Every datagram is one Ethernet
frame. The switch learns source MACs per peer socket, forwards known unicast and
floods broadcast, multicast and unknown destinations. No host interface,
bridge, tap or privilege is involved.

Frames to or from a peer listed in a block file are dropped while the file
names it, which cuts one guest off the fabric (both ways).

  python3 switch.py <switch.sock> [--block-file FILE] [--stats FILE]
"""

import argparse
import json
import os
import socket
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--block-file", type=Path)
    parser.add_argument("--stats", type=Path)
    args = parser.parse_args()
    args.path.unlink(missing_ok=True)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
    sock.bind(str(args.path))
    table = {}  # mac -> peer path
    peers = set()
    blocked, blocked_checked = set(), 0.0
    counts = {"frames": 0, "bytes": 0, "flooded": 0, "dropped_blocked": 0, "send_errors": 0}
    last_stats = time.time()
    while True:
        frame, peer = sock.recvfrom(65536)
        now = time.time()
        if args.block_file and now - blocked_checked > 0.5:
            try:
                blocked = {line.strip() for line in args.block_file.read_text().splitlines() if line.strip()}
            except FileNotFoundError:
                blocked = set()
            blocked_checked = now
        if not peer or len(frame) < 14:
            continue
        counts["frames"] += 1
        counts["bytes"] += len(frame)
        peers.add(peer)
        source, destination = frame[6:12], frame[0:6]
        table[source] = peer
        if Path(peer).stem in blocked:
            counts["dropped_blocked"] += 1
            continue
        if destination[0] & 1 or destination not in table:
            targets = [p for p in peers if p != peer]
            counts["flooded"] += 1
        else:
            targets = [table[destination]]
        for target in targets:
            if Path(target).stem in blocked:
                counts["dropped_blocked"] += 1
                continue
            try:
                sock.sendto(frame, target)
            except (ConnectionRefusedError, FileNotFoundError):
                peers.discard(target)
                counts["send_errors"] += 1
            except BlockingIOError:
                counts["send_errors"] += 1
        if args.stats and now - last_stats > 10:
            args.stats.write_text(
                json.dumps({"time": now, "pid": os.getpid(), "peers": sorted(peers), "macs": len(table), **counts})
                + "\n"
            )
            last_stats = now


if __name__ == "__main__":
    main()
