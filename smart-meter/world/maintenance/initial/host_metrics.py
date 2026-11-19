#!/usr/bin/env python3
"""Small host metrics exporter for the company's monitoring."""

import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        mem = {
            line.split(":")[0]: int(line.split()[1]) * 1024
            for line in Path("/proc/meminfo").read_text().splitlines()
            if len(line.split()) >= 3
        }
        vm = dict(line.split() for line in Path("/proc/vmstat").read_text().splitlines())
        out = []
        for k in ["MemTotal", "MemAvailable", "Cached", "SwapTotal", "SwapFree"]:
            out.append("host_memory_" + k.lower() + "_bytes " + str(mem[k]))
        for k in ["oom_kill", "pswpin", "pswpout", "pgmajfault"]:
            out.append("host_" + k + "_total " + vm[k])
        out += ["host_clock_seconds " + str(time.time()), "host_load1 " + str(os.getloadavg()[0])]
        for kind in ["cpu", "io", "memory"]:
            for line in Path("/proc/pressure/" + kind).read_text().splitlines():
                words = line.split()
                out.append(
                    'host_pressure_avg10{resource="'
                    + kind
                    + '",kind="'
                    + words[0]
                    + '"} '
                    + dict(x.split("=") for x in words[1:])["avg10"]
                )
        payload = ("\n".join(out) + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


HTTPServer(("0.0.0.0", 9100), Handler).serve_forever()
