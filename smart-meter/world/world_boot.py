#!/usr/bin/env python3
"""Build output folder, company host names and the note/put helpers shared by build steps."""

import json
import time

import lab
import world_policy

OUT = lab.ARTIFACTS / world_policy.config()["artifact_subdir"]
NAMES = list(world_policy.config()["memory_mib"])


def note(event, **data):
    (lab.ARTIFACTS / "phase").write_text("world-" + event + "\n")
    row = dict(epoch=time.time(), event=event, **data)
    with (OUT / "events.jsonl").open("a") as f:
        f.write(json.dumps(row) + "\n")
    lab.log("World " + event + ": " + json.dumps(data)[:400])
    with (lab.ROOT / "STATE.md").open("a") as f:
        f.write(
            "\n- "
            + time.strftime("%H:%M UTC", time.gmtime())
            + ": world "
            + event
            + "; "
            + json.dumps(data)[:500]
            + "\n"
        )


def put(n, label, content, dest, mode="0644"):
    p = OUT / label
    p.write_text(content)
    lab.push(n, p, dest, mode)
