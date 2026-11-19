#!/usr/bin/env python3
"""Run kubectl against one regional cluster from a build step."""

import os
import subprocess
import sys

import lab


def k(region, args):
    env = {**os.environ, "SMART_METER_REGION": region}
    return subprocess.check_output(
        [sys.executable, "-c", "import cluster; print(cluster.kubectl(" + repr(args) + "))"],
        env=env,
        cwd=lab.ROOT,
        text=True,
    )
