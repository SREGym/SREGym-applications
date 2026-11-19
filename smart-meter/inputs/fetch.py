#!/usr/bin/env python3
"""Fetch Smart Meter's pinned inputs into world/ and verify every byte.

Usage: python3 inputs/fetch.py [--mirror URL] [--only PREFIX ...] [--dry-run]

Each entry in inputs/inputs.json names its destination under world/ and its sha256.
Sources, in order:
  * small generated files (receipts, checksum lists), shipped in inputs/generated/;
  * the recorded upstream URL (with `member`, one file taken from that archive);
  * Ubuntu packages by exact file name from the Launchpad primary archive;
  * the Smart Meter mirror (--mirror, or SMART_METER_INPUTS_MIRROR) for exact container image archives and image caches,
    which registries can only re-export as different bytes; the build verifies those bytes.
Files already present with the right sha256 are kept. Nothing unverified is left in place.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORLD = HERE.parent / "world"
LAUNCHPAD = "https://launchpad.net/ubuntu/+archive/primary/+files/"


def sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def download(url, target):
    request = urllib.request.Request(url, headers={"User-Agent": "smart-meter-inputs/1"})
    with urllib.request.urlopen(request, timeout=120) as response, open(target, "wb") as out:
        shutil.copyfileobj(response, out, 1 << 20)


def source(entry, mirror):
    generated = HERE / "generated" / entry["dest"]
    if generated.is_file():
        return "generated", generated
    if entry.get("source"):
        return "url", entry["source"]
    if entry["dest"].endswith(".deb"):
        # apt cache file names encode the version epoch (1%3a); archive file names have none
        return "url", LAUNCHPAD + re.sub(r"_\d+%3[aA]", "_", Path(entry["dest"]).name)
    if mirror:
        suffix = ".tar" if entry["kind"] in ("oci-layout", "dir") else ""
        return "mirror", mirror.rstrip("/") + "/" + entry["dest"] + suffix
    raise SystemExit(f"{entry['dest']} needs the Smart Meter mirror (--mirror URL); {entry.get('source_note', '')}")


def present(entry):
    dest = WORLD / entry["dest"]
    if entry["kind"] in ("oci-layout", "dir"):
        return dest.is_dir() and any(dest.iterdir())
    return dest.is_file() and entry.get("sha256") and sha256(dest) == entry["sha256"]


def fetch(entry, mirror, dry):
    if present(entry):
        return "present"
    how, where = source(entry, mirror)
    if dry:
        return f"would fetch ({how}): {where}"
    dest = WORLD / entry["dest"]
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=dest.parent) as tmp:
        staged = Path(tmp) / dest.name
        if how == "generated":
            shutil.copy2(where, staged)
        else:
            download(where, staged)
        if entry.get("member"):
            with tarfile.open(staged) as tar:
                data = tar.extractfile(entry["member"]).read()
            staged.write_bytes(data)
        if entry["kind"] in ("oci-layout", "dir"):
            unpacked = Path(tmp) / "unpacked"
            with tarfile.open(staged) as tar:
                tar.extractall(unpacked, filter="data")
            staged = unpacked / dest.name
        elif entry.get("sha256") and sha256(staged) != entry["sha256"]:
            raise SystemExit(f"sha256 mismatch for {entry['dest']} from {where}")
        staged.rename(dest)
    return f"fetched ({how})"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mirror", default=os.environ.get("SMART_METER_INPUTS_MIRROR"))
    parser.add_argument("--only", nargs="*", default=None, help="fetch only destinations starting with these prefixes")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    for entry in json.loads((HERE / "inputs.json").read_text())["inputs"]:
        if args.only and not entry["dest"].startswith(tuple(args.only)):
            continue
        print(entry["dest"], "-", fetch(entry, args.mirror, args.dry_run), flush=True)


if __name__ == "__main__":
    sys.exit(main())
