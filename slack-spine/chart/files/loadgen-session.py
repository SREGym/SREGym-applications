"""Start each workload process with a new request-ID prefix."""

import os
import sys
import uuid
from pathlib import Path


def start_session() -> Path:
    root = Path(os.environ.get("GRADER_DIR", "/grader"))
    directory = root / uuid.uuid4().hex
    directory.mkdir(parents=True)
    # External ledger readers resolve this link; the sidecar uses the real path.
    pending = root / f".current-{directory.name}"
    pending.symlink_to(directory.name)
    pending.replace(root / "current")
    os.environ["GRADER_DIR"] = str(directory)
    return directory


if __name__ == "__main__":
    start_session()
    os.execv(sys.executable, [sys.executable, "loadgen_sidecar.py"])
