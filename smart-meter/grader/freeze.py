import hashlib
import json
import sys
import time
from pathlib import Path

here = Path(sys.argv[1])
inputs = [
    dict(path=str(p.relative_to(here)), sha256=hashlib.sha256(p.read_bytes()).hexdigest())
    for p in [here / "RUBRIC.md", *sorted((here / "recurrence-specs").glob("*.json"))]
]
(here / "FREEZE.json").write_text(
    json.dumps(
        dict(
            frozen_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            note="Inputs that define a grade; a mismatch makes the grade invalid.",
            inputs=inputs,
        ),
        indent=2,
    )
    + "\n"
)
sys.path.insert(0, str(here))
sys.argv = ["grade"]
import grade

h = grade.frozen_inputs()
print(len(h), "frozen inputs, all match:", all(x["matches"] for x in h))
