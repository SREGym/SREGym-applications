"""Publish the company source repository before the history starts."""

import json
from pathlib import Path

from build_context import OUT, lab, put, require_build

NAMES = list(require_build()["memory_mib"])


def repository():
    n = "eu-work-03"
    # One initial import of the platform files; no earlier commits.
    require_build()
    remote = "/var/lib/company/platform-source"
    lab.ssh(n, "test ! -e " + remote)
    lab.ssh(
        n,
        "sudo -n mkdir -p "
        + remote
        + "/services "
        + remote
        + "/fleet "
        + remote
        + "/roles/platform/networkd.conf.d "
        + remote
        + "/fleet/generations "
        + remote
        + "/fleet/overrides/regions "
        + remote
        + "/fleet/overrides/hosts; sudo -n chown -R ubuntu:ubuntu "
        + remote,
    )
    files = [
        (lab.ROOT / "company_reporting.py", "services/reporting.py"),
        (OUT / "eu-writer.py", "services/writer.py"),
        (lab.ROOT / "workloads/java-gateway/Gateway.java", "services/Gateway.java"),
        (lab.ROOT / "workloads/java-gateway/GatewayKeyTest.java", "services/GatewayKeyTest.java"),
    ]
    here = Path(__file__).resolve().parent
    files += [
        (here / "platform-role.sh", "roles/platform/role.sh"),
        *[
            (here / name, name)
            for name in [
                "fleet/capacity.py",
                "fleet/generations/g-burst.yaml",
                "fleet/generations/g-control.yaml",
                "fleet/generations/g-elastic.yaml",
                "fleet/generations/g-ops.yaml",
                "fleet/generations/g-steady.yaml",
                "fleet/overrides/hosts/eu-work-02.yaml",
                "fleet/overrides/regions/eu.yaml",
                "fleet/overrides/regions/us.yaml",
                "fleet/pools.yaml",
                "fleet/resolve.py",
            ]
        ],
    ]
    for p, target in files:
        lab.push(n, p, remote + "/" + target)
    readme = """# Smart Meter platform

Customer event-hour usage, settlement and partner reporting. Live reports cover
seven days. SQL and broker data are the operational stores.

The on-call CLI provides inventory, guest commands, streaming file reads and
provider stop/start/rescue/unrescue. Inventory access reports a command target,
setup readiness and available methods. Use that target for commands and the
server ID for lifecycle operations; setup readiness does not guarantee a later
transport operation. Normal guest tools include kubectl, psql,
Kafka tools, systemd, package metadata and the published source archives. Portal APIs
include accounts, reports, ledger, usage, partner, revisions, closed reports,
product checks and their ordinary cache/projection/recompute operations.

All application source and normal fleet definitions live in this repository.
Build the ingress with the installed Temurin JDK and its pinned Paho/Kafka
libraries. The company artifact catalog supplies source archives and installers.

The platform role in roles/platform converges every host every 30 minutes.
Host policy is published in operations/host-policy, monitoring services in
operations/host-observability, installer/source reconciliation in
operations/source-catalog, and report capacity checks in
operations/export-operations. Their scheduled results are ordinary host metrics
and the artifact store's operations/catalog-inventory.json. Repositories become
available when their services are installed.
"""
    put(n, "company-source-readme.md", readme, remote + "/README.md")
    put(
        n,
        "company-fleet.json",
        json.dumps(
            dict(
                servers={name: dict(address=lab.VMS[name]["ip"], region=name[:2]) for name in NAMES},
            ),
            indent=2,
        )
        + "\n",
        remote + "/fleet/inventory.json",
    )
    put(
        n,
        "company-maintenance.json",
        json.dumps(
            dict(
                package_channel=dict(
                    gated=True,
                    order=["canary", "zone-b", "zone-a"],
                    zones=dict(
                        canary=["eu-work-02", "us-work-03"],
                        **{"zone-b": ["eu-work-03"]},
                        **{"zone-a": ["eu-work-01", "us-work-01", "us-work-02"]},
                    ),
                    control_planes="weekly window after the last worker wave",
                ),
                platform_role=dict(every="30min", source="roles/platform"),
                template_rollout=dict(max_surge=0, max_unavailable=1, paused=True),
            ),
            indent=2,
        )
        + "\n",
        remote + "/fleet/maintenance.json",
    )
    code = """import subprocess,json,urllib.request,base64,pathlib
root=pathlib.Path('/var/lib/company/platform-source')
subprocess.run(['sudo','-n','chown','-R','ubuntu:ubuntu',str(root)],check=True)
gitea='/var/lib/company/bin/gitea';config='/var/lib/company/gitea.ini'
subprocess.run([gitea,'--config',config,'admin','user','create','--username','operations','--password','smartmeter-local-operations','--email','operations@smartmeter.invalid','--must-change-password=false'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
auth=base64.b64encode(b'operations:smartmeter-local-operations').decode()
req=urllib.request.Request('http://10.72.0.23:3000/api/v1/user/repos',data=json.dumps({'name':'platform','private':False}).encode(),headers={'Authorization':'Basic '+auth,'Content-Type':'application/json'})
with urllib.request.urlopen(req,timeout=30) as r:assert r.status==201
def git(*args):return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.STDOUT).decode().strip()
assert not (root/'.git').exists(), 'Existing source history cannot be reused'
git('init','--initial-branch=main');git('config','user.name','Platform Automation');git('config','user.email','platform@smartmeter.invalid');git('add','.')
git('commit','-m','Initialize customer reporting and fleet services')
git('remote','add','origin','http://operations:smartmeter-local-operations@10.72.0.23:3000/operations/platform.git');git('push','-u','origin','HEAD:main')
print(json.dumps({'head':git('rev-parse','HEAD'),'commits':int(git('rev-list','--count','HEAD')),'files':git('ls-files').splitlines()}))
"""
    put(n, "company-repository-import.py", code, "/var/lib/company/company-repository-import.py")
    result = lab.ssh(
        n, "env GITEA_WORK_DIR=/var/lib/company python3 /var/lib/company/company-repository-import.py", timeout=180
    )
    (OUT / "company-repository.json").write_text(result)
