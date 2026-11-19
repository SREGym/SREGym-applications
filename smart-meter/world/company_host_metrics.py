#!/usr/bin/env python3
"""Small host metrics exporter for the company's monitoring."""

import json
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
        for mount in ["/", "/var/lib/company", "/var/lib/telemetry"]:
            if not Path(mount).is_dir():
                continue
            fs = os.statvfs(mount)
            for metric, value in [
                ("size_bytes", fs.f_blocks * fs.f_frsize),
                ("available_bytes", fs.f_bavail * fs.f_frsize),
                ("files", fs.f_files),
                ("files_free", fs.f_favail),
            ]:
                out.append("host_filesystem_" + metric + '{mountpoint="' + mount + '"} ' + str(value))
        for report, field in [("packages", "packages"), ("cache", "packages"), ("exports", "exports")]:
            path = Path("/var/lib/company/maintenance") / (report + ".json")
            if not path.is_file():
                continue
            try:
                if path.stat().st_size > 4 * 1024 * 1024:
                    continue
                result = json.loads(path.read_text())
                out.append('host_maintenance_records{report="' + report + '"} ' + str(len(result[field])))
                out.append(
                    'host_maintenance_collected_seconds{report="' + report + '"} ' + str(float(result["collected_at"]))
                )
                if report == "exports":
                    for key in ["capacity_ok", "complete"]:
                        out.append("host_export_" + key + " " + str(int(result[key])))
            except (OSError, ValueError, KeyError, TypeError):
                continue
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
