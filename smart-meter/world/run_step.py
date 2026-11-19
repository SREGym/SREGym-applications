"""Run one construction step of a scenario under the build guard's process identity."""

import runpy
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
kits = {path.resolve() for path in (root / "scenarios").iterdir() if path.is_dir()}
source = Path(sys.argv[1]).resolve()
assert source.parent in kits, "Construction steps run only from a scenario folder"
prepared = source.parent  # the step imports from its own kit
assert source.name in {
    "install_image.py",
    "boot.py",
    "cluster_install.py",
    "apps.py",
    "core_install.py",
    "surfaces.py",
    "eu_intake.py",
    "runtime.py",
    "inventory_packages.py",
    "start_customer_activity.py",
    "install_audits.py",
    "log_baseline.py",
    "apply_log_limit.py",
    "verify_log_limit.py",
    "history.py",
    "seal.py",
}
sys.path.insert(0, str(prepared))
sys.argv = [str(source), *sys.argv[2:]]
runpy.run_path(str(source), run_name="__main__")
