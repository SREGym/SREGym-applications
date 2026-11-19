"""Resume a serial-per-region maintenance rollout from its durable step journal."""

import json
import pathlib
import socket
import threading
import time
import urllib.request

BASE = "http://10.72.0.30:28880"
SOURCE = "http://10.72.0.23:3000/operations/platform/raw/branch/main/"
STATE = pathlib.Path("/var/lib/company/rollout-state.json")
LOCK = threading.Lock()
state = json.loads(STATE.read_text()) if STATE.exists() else {"completed": {}, "current": {}, "errors": {}}


def save():
    with LOCK:
        p = STATE.with_suffix(".tmp")
        p.write_text(json.dumps(state, indent=2) + "\n")
        p.replace(STATE)


def call(path, data=None, timeout=240):
    req = urllib.request.Request(
        BASE + path,
        data=None if data is None else json.dumps(data).encode(),
        headers={"Content-Type": "application/json", "X-Client": "instance-rollout"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
        return json.loads(b) if b else {}


def policy():
    with urllib.request.urlopen(SOURCE + "fleet/maintenance.json", timeout=20) as r:
        return json.load(r)


def server(n):
    return call("/provider/v2.1/servers/" + n)["server"]


def wait(n, status, limit=240):
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        s = server(n)
        if s["status"] == status and s["OS-EXT-STS:task_state"] is None:
            return
        time.sleep(2)
    raise TimeoutError(n + " did not reach " + status)


def execute(n, c, timeout=240):
    r = call("/exec", {"server": n, "command": c, "timeout": timeout}, timeout + 30)
    if r["exit_code"]:
        raise RuntimeError(str(r))
    return r


def one(n):
    if n in state["completed"]:
        return
    row = state["current"].setdefault(n, {"began": time.time(), "step": "prepare"})
    save()
    if row["step"] == "prepare":
        execute(n, "sudo -n python3 /opt/company/schedule.py", 90)
        policy()["template_rollout"]
        row["step"] = "stopping"
        save()
    if row["step"] == "stopping" and n == socket.gethostname():
        row["step"] = "ready"
        save()
        execute(n, "sudo -n systemd-run --unit=company-instance-reboot --on-active=1s /sbin/reboot", 15)
        time.sleep(30)
        return
    if row["step"] == "stopping":
        s = server(n)
        if s["status"] == "ACTIVE" and (not s["OS-EXT-STS:task_state"]):
            call("/provider/v2.1/servers/" + n + "/action", {"os-stop": None})
        wait(n, "SHUTOFF")
        row["step"] = "provision" if "-work-" in n else "starting"
        save()
    if row["step"] == "provision":
        call("/provider/v2.1/servers/" + n + "/action", {"rebuild": None})
        wait(n, "ACTIVE", 300)
        row["step"] = "install"
        save()
    if row["step"] == "install":
        cmd = (
            "sudo -n mkdir -p /opt/company; sudo -n python3 -c \"import urllib.request,pathlib; b='"
            + SOURCE
            + "'; [(pathlib.Path('/opt/company/'+d).write_bytes(urllib.request.urlopen(b+s).read())) for s,d in [('fleet/bootstrap.py','bootstrap.py'),('fleet/node-setup.sh','node-setup.sh')]]\"; sudo -n python3 /opt/company/bootstrap.py --application"
        )
        execute(n, cmd, 600)
        row["step"] = "pin-reboot"
        save()
    if row["step"] == "pin-reboot":
        call("/provider/v2.1/servers/" + n + "/action", {"os-stop": None})
        wait(n, "SHUTOFF")
        row["step"] = "join-start"
        save()
    if row["step"] in ["starting", "join-start"]:
        s = server(n)
        if s["status"] == "SHUTOFF" and (not s["OS-EXT-STS:task_state"]):
            call("/provider/v2.1/servers/" + n + "/action", {"os-start": None})
        wait(n, "ACTIVE")
        row["step"] = "join" if row["step"] == "join-start" else "ready"
        save()
    if row["step"] == "join":
        parent = "us-parent-01"
        execute(
            parent,
            "sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl delete node "
            + n
            + " --ignore-not-found; sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl delete ciliumnode "
            + n
            + " --ignore-not-found",
            90,
        )
        token = (
            execute(parent, "sudo -n kubeadm token create --ttl 1h --print-join-command", 60)["stdout"]
            .strip()
            .splitlines()[-1]
        )
        execute(n, "sudo -n " + token + " --node-name " + n, 180)
        row["step"] = "ready"
        save()
    if row["step"] == "ready":
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            try:
                execute(n, "hostname; systemctl is-active kubelet", 15)
                parent = n[:2] + "-parent-01"
                execute(
                    parent,
                    "sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl --request-timeout=10s wait --for=condition=Ready node/"
                    + n
                    + " --timeout=5s",
                    20,
                )
                if "-work-" in n:
                    data = json.loads(
                        execute(
                            parent,
                            "sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf kubectl --request-timeout=10s -n telemetry get pods --field-selector spec.nodeName="
                            + n
                            + " -o json",
                            20,
                        )["stdout"]
                    )
                    pods = [p for p in data["items"] if not p["metadata"].get("deletionTimestamp")]
                    assert pods and all(
                        any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
                        for p in pods
                    ), "Workloads not ready on " + n
                break
            except Exception:
                time.sleep(3)
        else:
            raise TimeoutError("Worker did not become reachable: " + n)
        execute(n, "sudo -n python3 /opt/company/schedule.py", 90)
        state["completed"][n] = {"at": time.time(), "replacement": "-work-" in n}
        state["current"].pop(n, None)
        save()
    print(json.dumps({"event": "rollout-completed", "server": n, "at": time.time()}), flush=True)


def region(names):
    for n in names:
        try:
            one(n)
        except Exception as e:
            state["errors"][n] = {"at": time.time(), "error": repr(e)}
            save()
            print(repr(e), flush=True)
            return


while True:
    try:
        p = policy()
        roll = p["template_rollout"]
        if not roll.get("paused", True) and time.time() >= roll["not_before"]:
            a = threading.Thread(target=region, args=(["us-parent-01", "us-work-03", "us-work-02", "us-work-01"],))
            b = threading.Thread(target=region, args=(["eu-parent-01", "eu-work-01", "eu-work-03", "eu-work-02"],))
            a.start()
            b.start()
            a.join()
            b.join()
            if len(state["completed"]) == 8:
                print("All declared instance actions completed", flush=True)
                break
        time.sleep(5)
    except Exception as e:
        print(repr(e), flush=True)
        time.sleep(5)
