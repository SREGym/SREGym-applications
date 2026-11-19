"""Build hooks that run maintenance phases on the hosts."""

import json
import shlex

import lab
from world_boot import OUT

HOST = "eu-work-03"
REMOTE = "/run/company-construction"


def register(name):
    # Runs on the Gitea host with the platform checkout's own credentials.
    code = """import base64,json,subprocess,sys,urllib.parse,urllib.request
from pathlib import Path
origin=subprocess.check_output(['git','-C','/var/lib/company/platform-source','remote','get-url','origin'],text=True).strip()
u=urllib.parse.urlsplit(origin)
assert u.hostname=='10.72.0.23' and u.port==3000 and u.path=='/operations/platform.git'
assert u.username and u.password
credentials=(urllib.parse.unquote(u.username)+':'+urllib.parse.unquote(u.password)).encode()
headers={'Authorization':'Basic '+base64.b64encode(credentials).decode(),'Content-Type':'application/json'}
assert sys.argv[1] in ['host-observability']
for name in [sys.argv[1]]:
 request=urllib.request.Request('http://10.72.0.23:3000/api/v1/user/repos',data=json.dumps({'name':name,'private':False}).encode(),headers=headers)
 with urllib.request.urlopen(request,timeout=15) as response:assert response.status==201
print(json.dumps({'created':1}))
"""
    lab.ssh(HOST, "python3 -c " + shlex.quote(code) + " " + shlex.quote(name), timeout=90)


def phase(name):
    initial = {"monitoring-initial": "host-observability"}
    if name in initial:
        register(initial[name])
    payload = lab.ROOT / "maintenance"
    for path in sorted(payload.rglob("*")):
        if path.is_file():
            lab.push(HOST, path, REMOTE + "/" + str(path.relative_to(payload)))
    result = lab.ssh(
        HOST,
        "sudo -n python3 " + REMOTE + "/run_phase.py --payload " + REMOTE + " --phase " + shlex.quote(name),
        timeout=180,
    )
    receipt = json.loads(result)
    folder = OUT / "executed-maintenance"
    folder.mkdir(exist_ok=True)
    (folder / (name + ".json")).write_text(json.dumps(receipt, indent=2) + "\n")
    # The job has committed; make the commit available to the next consumer.
    repository = receipt["repository"]
    assert repository == "/var/lib/company/platform-source" or repository.startswith(
        "/var/lib/company/maintenance-source/"
    )
    lab.ssh(HOST, "git -C " + shlex.quote(repository) + " push origin HEAD:main", timeout=90)
    return receipt
