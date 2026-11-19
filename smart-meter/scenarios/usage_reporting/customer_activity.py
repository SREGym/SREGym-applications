"""Aggregate report requests across tenants and supported timezones."""

import hashlib
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path("/var/lib/private/customer-activity")
ROOT.mkdir(parents=True, exist_ok=True)
index = 0
success = errors = 0
while True:
    region = ("us", "eu")[index % 2]
    tenant = "tenant-" + region + f"-{(index // 2) % 20 + 1:02d}"
    timezone = ["UTC", "America/Los_Angeles", "Europe/Berlin", "Asia/Tokyo"][(index // 40) % 4]
    host = "10.72.0.10" if region == "us" else "10.72.0.22"
    row = dict(at=time.time(), region=region, tenant=tenant, timezone=timezone)
    query = urllib.parse.urlencode(dict(tenant=tenant, timezone=timezone))
    try:
        with urllib.request.urlopen("http://" + host + ":8080/api/reports?" + query, timeout=25) as response:
            data = response.read()
            row.update(status=response.status, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        success += 1
    except Exception as error:
        row["error"] = repr(error)
        errors += 1
    with (ROOT / "requests.jsonl").open("a") as stream:
        stream.write(json.dumps(row) + "\n")
    temporary = ROOT / "stats.new"
    temporary.write_text(json.dumps(dict(at=time.time(), success=success, errors=errors, last=row)) + "\n")
    temporary.replace(ROOT / "stats.json")
    index += 1
    time.sleep(5)
