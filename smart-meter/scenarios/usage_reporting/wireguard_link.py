"""Configure the artifact link using the kernel's WireGuard generic-netlink API."""

import json
import os
import socket
import struct
import sys
from pathlib import Path


def attribute(kind, value):
    size = len(value) + 4
    return struct.pack("HH", size, kind) + value + b"\0" * (-size % 4)


def attributes(value):
    while len(value) >= 4:
        size, kind = struct.unpack_from("HH", value)
        if size < 4 or size > len(value):
            raise ValueError("Invalid netlink attribute")
        yield kind & 0x3FFF, value[4:size]
        value = value[(size + 3) & ~3 :]


def request(sock, family, command, data, ack=False):
    payload = struct.pack("BBH", command, 1, 0) + data
    sock.send(struct.pack("IHHII", 16 + len(payload), family, 1 | (4 if ack else 0), 1, 0) + payload)
    response = sock.recv(65536)
    kind = struct.unpack_from("H", response, 4)[0]
    if kind == 2:
        error = struct.unpack_from("i", response, 16)[0]
        if error:
            raise OSError(-error, os.strerror(-error))
        return b""
    return response[20:]


def configure(config):
    with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 16) as sock:
        sock.settimeout(5)
        sock.bind((0, 0))
        sock.connect((0, 0))
        response = request(sock, 16, 3, attribute(2, b"wireguard\0"))
        family = struct.unpack("H", dict(attributes(response))[1])[0]
        allowed = attribute(1, struct.pack("H", socket.AF_INET))
        allowed += attribute(2, socket.inet_aton(config["allowed_ip"])) + attribute(3, b"\x20")
        endpoint = struct.pack("H", socket.AF_INET) + struct.pack("!H", config["endpoint_port"])
        endpoint += socket.inet_aton(config["endpoint"]) + b"\0" * 8
        peer = attribute(1, bytes.fromhex(config["peer_public"]))
        peer += attribute(3, struct.pack("I", 2)) + attribute(4, endpoint)
        peer += attribute(9 | 0x8000, attribute(0 | 0x8000, allowed))
        device = attribute(2, b"audit-wg\0") + attribute(3, bytes.fromhex(config["private"]))
        device += attribute(5, struct.pack("I", 1)) + attribute(6, struct.pack("H", config["listen_port"]))
        device += attribute(8 | 0x8000, attribute(0 | 0x8000, peer))
        request(sock, family, 1, device, ack=True)


if __name__ == "__main__":
    configure(json.loads(Path(sys.argv[1]).read_text()))
