"""Deploy the regional reporting profiles before device traffic starts."""

import hashlib
import json
import shlex
from pathlib import Path

from build_context import OUT, lab, note, require_build


def main():
    require_build()
    marker = OUT / "reporting-profiles-complete.json"
    if marker.exists():
        return
    assert not (OUT / "runtime-ready.json").exists()
    script = Path(__file__).with_name("publish_reporting_profiles.py")
    lab.push("eu-work-03", script, "/run/company-profiles.py")
    result = lab.ssh("eu-work-03", "python3 /run/company-profiles.py", timeout=90)
    for region in ("us", "eu"):
        code = (
            "import json,pathlib,urllib.request;root='http://10.72.0.23:3000/operations/platform/raw/branch/main/';profiles=json.load(urllib.request.urlopen(root+'fleet/application-profiles.json'));pathlib.Path('/opt/company/application-profile.json').write_text(json.dumps(profiles["
            + repr(region)
            + "]));pathlib.Path('/opt/company/reporting.py').write_bytes(urllib.request.urlopen(root+'services/reporting.py').read())"
        )
        lab.ssh(
            region + "-parent-01",
            "sudo -n python3 -c "
            + shlex.quote(code)
            + "; sudo -n systemctl restart company-reporting; systemctl is-active company-reporting",
            timeout=60,
        )
    lab.ssh(
        "us-parent-01",
        "sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl -n telemetry scale deployment gateway --replicas=2; sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl -n telemetry rollout status deployment/gateway --timeout=90s",
        timeout=100,
    )
    marker.write_text(
        json.dumps(dict(result=result, source_recipe_sha256=hashlib.sha256(script.read_bytes()).hexdigest())) + "\n"
    )
    note("reporting-profiles-deployed", regions=["us", "eu"])


if __name__ == "__main__":
    main()
