#!/usr/bin/env python3
"""Service readiness and weekly operations-log summary."""

import json
import logging
import pathlib
import time
import urllib.parse
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
TOOLS = "http://10.72.0.30:28880"
ROOT = pathlib.Path("/var/lib/company/operations")
ROOT.mkdir(parents=True, exist_ok=True)


def execute(server, command):
    req = urllib.request.Request(
        TOOLS + "/exec",
        data=json.dumps(dict(server=server, command=command, timeout=5)).encode(),
        headers={"Content-Type": "application/json", "X-Client": "operations-health"},
    )
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.load(r)


while True:
    result = {"at": time.time(), "services": {}, "log_summary": {}}
    for region in ["us", "eu"]:
        try:
            x = execute(
                region + "-parent-01",
                "sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl --request-timeout=3s get --raw=/readyz; sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl --request-timeout=3s get componentstatuses -o json",
            )
            result["services"][region] = x
            logging.info("Regional service check %s exit=%s", region, x["exit_code"])
        except Exception as e:
            result["services"][region] = {"error": str(e)}
            logging.error("Regional service check %s: %s", region, e)
    try:
        params = urllib.parse.urlencode(
            dict(
                query='{job="system"} |~ "(?i)(error|warn|fail|timeout)"',
                start=str(int((time.time() - 7 * 86400) * 1e9)),
                end=str(int(time.time() * 1e9)),
                limit=5000,
                direction="backward",
            )
        )
        with urllib.request.urlopen("http://10.72.0.22:3100/loki/api/v1/query_range?" + params, timeout=25) as r:
            data = json.load(r)
        counts = {}
        for stream in data["data"]["result"]:
            host = stream["stream"].get("host", "unknown")
            counts[host] = counts.get(host, 0) + len(stream["values"])
        result["log_summary"] = {
            "returned_by_host": counts,
            "returned_entries": sum(counts.values()),
            "request_limit": 5000,
            "horizon_days": 7,
        }
        logging.info("Operations log summary returned %s entries", sum(counts.values()))
    except Exception as e:
        result["log_summary"] = {"error": str(e)}
        logging.error("Operations log summary: %s", e)
    temporary = ROOT / "health.tmp"
    temporary.write_text(json.dumps(result, indent=2) + "\n")
    temporary.replace(ROOT / "health.json")
    time.sleep(60)
