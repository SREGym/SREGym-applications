"""Install a region's core applications on new stores."""

import os
import runpy
import sys

from build_context import lab, note, require_build

cfg = require_build()
region = sys.argv[1]
assert region in ("us", "eu")
assert (lab.ARTIFACTS / cfg["artifact_subdir"] / "clusters-ready.json").exists()
os.environ["SMART_METER_REGION"] = region
sys.argv = [str(lab.ROOT / "world_region.py"), region, "--resume"]
runpy.run_path(sys.argv[0], run_name="__main__")
note("core-installed", region=region)
