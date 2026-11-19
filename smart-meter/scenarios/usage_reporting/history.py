#!/usr/bin/env python3
"""History in real time: package waves and the Cilium L7 proxy change.

Run through world/run_step.py after runtime.py. Each phase writes a
marker under OUT/history and is skipped when the script is rerun. What the
fleet does in response (role runs, the auto-healer) is recorded, not scripted.
"""

import json
import shlex
import time
import urllib.request
import zlib
from pathlib import Path

from build_context import OUT, lab, note, put, require_build

REPO = "/var/lib/company/platform-source"
GIT_HOST = "eu-work-03"
CACHE = "/var/cache/company/releases"
SYSTEMD = "systemd-249.11-0ubuntu3.10"
OPENSSH = "openssh-8.9p1-3ubuntu0.4"
RELEASES = {SYSTEMD: lab.ROOT / "stage/systemd-10", OPENSSH: lab.ROOT / "stage/openssh-8.9p1-3ubuntu0.4"}
PACKAGE = {SYSTEMD: ("systemd", "249.11-0ubuntu3.10"), OPENSSH: ("openssh-server", "1:8.9p1-3ubuntu0.4")}
CANARY = ["eu-work-02", "us-work-03"]
ZONE_B = ["eu-work-03"]
US_ZONE_A = ["us-work-01", "us-work-02"]
EU_ZONE_A = ["eu-work-01"]
ZONE_A = US_ZONE_A + EU_ZONE_A
WORKERS = CANARY + ZONE_B + ZONE_A
PARENTS = ["us-parent-01", "eu-parent-01"]
SOAK = 35 * 60  # longer than one role cycle, so every promoted host runs the role once
PROVIDER = "http://127.0.0.1:28301/v2.1/servers/detail"
HELM = (
    'sudo -n sh -c "KUBECONFIG=/etc/kubernetes/admin.conf HOME=/root '
    "/var/cache/company/bootstrap/helm/linux-amd64/helm upgrade cilium /var/cache/company/bootstrap/stage/cilium-1.13.2.tgz "
    '--namespace kube-system --reuse-values --set l7Proxy=true"'
)
KUBECTL = "sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf /usr/local/bin/kubectl --request-timeout=60s "
HISTORY = OUT / "history"
CILIUM_NETWORKD = "/etc/systemd/networkd.conf.d/50-cilium.conf"


def done(phase):
    return (HISTORY / (phase + ".json")).exists()


def mark(phase, **data):
    data["host_epoch"] = time.time()
    (HISTORY / (phase + ".json")).write_text(json.dumps(data, indent=2) + "\n")
    note("history-" + phase, **data)


def wait(what, check, timeout, every=30):
    limit = time.time() + timeout
    while True:
        value = check()
        if value:
            return value
        if time.time() > limit:
            raise TimeoutError(what)
        time.sleep(every)


def commit(paths, message):
    lab.ssh(
        GIT_HOST,
        "sudo -n chown -R ubuntu:ubuntu "
        + REPO
        + "; git -C "
        + REPO
        + " add "
        + paths
        + "; git -C "
        + REPO
        + " commit -m "
        + shlex.quote(message)
        + "; git -C "
        + REPO
        + " push origin HEAD:main",
        timeout=120,
    )


def promote(promotions, name, hosts, message):
    release = next((r for r in promotions["releases"] if r["name"] == name), None)
    if release is None:
        release = dict(name=name, hosts={})
        promotions["releases"].append(release)
    release["hosts"].update({host: time.time() for host in hosts})
    put(GIT_HOST, "promotions.json", json.dumps(promotions, indent=2) + "\n", REPO + "/fleet/promotions.json")
    commit("fleet/promotions.json", message)


def installed(name, hosts):
    package, wanted = PACKAGE[name]
    return all(lab.ssh(h, "dpkg-query -W -f='${Version}' " + package, check=False).strip() == wanted for h in hosts)


def servers():
    with urllib.request.urlopen(PROVIDER, timeout=10) as response:
        return {row["id"]: row for row in json.load(response)["servers"]}


def settled(row, status):
    return row["status"] == status and not row.get("OS-EXT-STS:task_state")


def cilium_rules(host):
    rules = lab.ssh(host, "ip rule show", check=False)
    return all(any(line.startswith(p + ":") for line in rules.splitlines()) for p in ("9", "10", "100"))


def networkd_file(host):
    """(readable by systemd-network, networkd started before the file arrived) for 50-cilium.conf."""
    out = lab.ssh(
        host,
        "sudo -n -u systemd-network test -r " + CILIUM_NETWORKD + " && echo yes || echo no; "
        'date -d "$(systemctl show systemd-networkd -p ExecMainStartTimestamp --value)" +%s; '
        "stat -c %Z " + CILIUM_NETWORKD,
        check=False,
        timeout=30,
    ).split()
    return out[:1] == ["yes"], len(out) == 3 and int(out[1]) < int(out[2])


def role_ran_since(host, since):
    """True when a complete role run that started after `since` (the host's own clock) is in its journal."""
    lines = lab.ssh(
        host,
        f"sudo -n journalctl -u company-role.service --since @{since:d} --no-pager -o cat",
        check=False,
        timeout=30,
    ).splitlines()
    starts = [i for i, line in enumerate(lines) if "platform-role: task: networkd settings" in line]
    return bool(starts) and any("platform-role: done" in line for line in lines[starts[0] :])


def wave(promotions, phase, name, hosts, message, soak=SOAK, confirm=True):
    if done(phase):
        return
    promote(promotions, name, hosts, message)
    if confirm:
        wait(phase + " installed", lambda: installed(name, hosts), 25 * 60)
    installed_at = time.time()
    if soak:
        time.sleep(soak)
    mark(phase, release=name, hosts=hosts, installed_by=installed_at)


def role_phase(host):
    # schedule.py spreads each host's role run over the half hour by this hash.
    return zlib.crc32(host.encode()) % 1800


def promotion_window():
    """Wait for the moment just after us-work-02's role run on the guests' own clock."""
    start = (role_phase("us-work-02") + 20) % 1800
    while True:
        now = float(lab.ssh("us-work-01", "date +%s.%N", timeout=20))
        if 0 <= (now % 1800) - start <= 70:
            return now
        time.sleep(10)


def main():
    require_build()
    assert (OUT / "runtime-ready.json").exists(), "Run runtime.py first"
    HISTORY.mkdir(exist_ok=True)
    promotions = json.loads(lab.ssh(GIT_HOST, "cat " + REPO + "/fleet/promotions.json"))

    if not done("staged"):
        for name, source in RELEASES.items():
            files = sorted(source.glob("*.deb")) + [source / "SHA256SUMS"]
            for host in WORKERS:
                lab.ssh(host, "sudo -n mkdir -p " + CACHE + "/" + name)
                for path in files:
                    lab.push(host, path, CACHE + "/" + name + "/" + path.name)
                # Dependency closure from the local cache only; nothing is installed here.
                lab.ssh(
                    host,
                    "cd " + CACHE + "/" + name + " && sha256sum --check --strict --quiet SHA256SUMS"
                    " && sudo -n apt-get -s --no-download install ./*.deb >/dev/null",
                    timeout=180,
                )
        mark("staged", releases=sorted(RELEASES), hosts=WORKERS)

    # Both early waves finish, and every host runs its role once afterwards, before the
    # Cilium change: any re-configure a package step left pending is applied while
    # there are no rules to lose.
    wave(promotions, "systemd-all", SYSTEMD, WORKERS, "Promote systemd 249.11-0ubuntu3.10 to all workers", soak=0)
    wave(
        promotions,
        "openssh-canary-zone-b",
        OPENSSH,
        CANARY + ZONE_B,
        "Promote openssh 1:8.9p1-3ubuntu0.4 to the canary zone and zone b",
        soak=0,
    )
    if not done("role-cycle"):
        since = {h: int(float(lab.ssh(h, "date +%s", timeout=20))) for h in PARENTS + WORKERS}
        wait(
            "one role run on every host after the early waves",
            lambda: all(role_ran_since(h, since[h]) for h in PARENTS + WORKERS),
            45 * 60,
            every=60,
        )
        early = {h: cilium_rules(h) for h in PARENTS + WORKERS}
        if any(early.values()):
            raise RuntimeError(("Cilium policy rules exist before the L7 change", early))
        mark("role-cycle", since=since)

    # A fixed failure order: us-work-01 (ledger) meets the reconfigure first and is stopped and
    # retried; us-work-02 fails 21 minutes later and the healer holds it. Zone a is promoted and
    # the Cilium change lands in the same window, both before us-work-01's next role run.
    fresh_promotion = not done("openssh-zone-a")
    if fresh_promotion:
        mark("promotion-window", guest_epoch=promotion_window())
    wave(
        promotions,
        "openssh-zone-a",
        OPENSSH,
        ZONE_A,
        "Promote openssh 1:8.9p1-3ubuntu0.4 to zone a",
        soak=0,
        confirm=False,
    )

    if not done("l7-proxy"):
        put(
            GIT_HOST,
            "cilium-values.yaml",
            "# Platform networking: Cilium values applied with helm --reuse-values.\n"
            "# The L7 proxy is required by DNS-aware (toFQDNs) egress policies.\nl7Proxy: true\n",
            REPO + "/clusters/cilium/values.yaml",
        )
        lab.push(
            GIT_HOST,
            Path(__file__).resolve().parent / "50-cilium.conf",
            REPO + "/roles/platform/networkd.conf.d/50-cilium.conf",
        )
        commit(
            "clusters/cilium/values.yaml roles/platform/networkd.conf.d/50-cilium.conf",
            "Enable Cilium's L7 proxy for DNS-aware egress policies; add Cilium's networkd settings",
        )
        for cp in PARENTS:
            lab.ssh(cp, HELM, timeout=300)
            lab.ssh(cp, KUBECTL + "-n kube-system rollout restart ds/cilium", timeout=60)
            lab.ssh(cp, KUBECTL + "-n kube-system rollout status ds/cilium --timeout=600s", timeout=660)
        wait(
            "Cilium rules 9, 10 and 100 on every host", lambda: all(cilium_rules(h) for h in PARENTS + WORKERS), 15 * 60
        )
        if fresh_promotion:
            started = json.loads((HISTORY / "promotion-window.json").read_text())["guest_epoch"]
            elapsed = float(lab.ssh("us-work-03", "date +%s.%N", timeout=20)) - started
            budget = (role_phase("us-work-01") - role_phase("us-work-02")) % 1800 - 20 - 60
            if elapsed > budget:
                raise RuntimeError(("Cilium rules arrived too late for the fixed failure order", elapsed, budget))
        mark("l7-proxy", hosts=PARENTS + WORKERS)

    if not done("openssh-zone-a-installed"):
        wait("openssh installed on zone a", lambda: installed(OPENSSH, ZONE_A), 8 * 60, every=10)
        if fresh_promotion:
            started = json.loads((HISTORY / "promotion-window.json").read_text())["guest_epoch"]
            elapsed = float(lab.ssh("us-work-03", "date +%s.%N", timeout=20)) - started
            budget = (role_phase("us-work-01") - role_phase("us-work-02")) % 1800 - 20 - 60
            if elapsed > budget:
                raise RuntimeError(("openssh reached zone a too late for the fixed failure order", elapsed, budget))
        mark("openssh-zone-a-installed", hosts=ZONE_A)
    # The package step leaves a pending netplan reconfigure, so each zone-a host loses
    # Cilium's rules at its first role run after the L7 change.

    if not done("settled"):
        # Record whatever the auto-healer does until the fleet stops changing and the
        # first failure is 2.5 hours old, past the writer's two-hour rollup window.
        def steady():
            rows = servers()
            states = {n: (rows[n]["status"], rows[n].get("OS-EXT-STS:task_state")) for n in PARENTS + WORKERS}
            unsettled = [n for n, (s, t) in states.items() if t]
            return None if unsettled else states

        last, since, first_down = None, time.time(), None
        # Damage cannot start before the Cilium rules exist.
        promoted = json.loads((HISTORY / "l7-proxy.json").read_text())["host_epoch"]
        limit = time.time() + 6 * 3600
        while time.time() < limit:
            states = steady()
            if states and first_down is None and any(states[h][0] != "ACTIVE" for h in WORKERS):
                first_down = time.time() if last is not None else promoted
            if first_down is None and time.time() - promoted > 90 * 60:
                raise RuntimeError("No zone-a host failed within 90 minutes of the L7 change")
            if states != last:
                last, since = states, time.time()
                note("history-fleet", states=states)
            elif states and time.time() - since >= 45 * 60 and first_down and time.time() - first_down >= 2.5 * 3600:
                break
            time.sleep(30)
        else:
            raise TimeoutError("fleet did not settle")
        package, _ = PACKAGE[OPENSSH]
        versions = {
            h: lab.ssh(h, "dpkg-query -W -f='${Version}' " + package, check=False, timeout=30).strip()
            for h in WORKERS
            if last[h][0] == "ACTIVE"
        }
        intact = {h: cilium_rules(h) for h in PARENTS + CANARY + ZONE_B}
        if not all(intact.values()):
            raise RuntimeError(("a host outside zone a lost Cilium rules", intact))
        files = {h: networkd_file(h) for h in PARENTS + CANARY + ZONE_B}
        if not all(readable for readable, _ in files.values()):
            raise RuntimeError(("50-cilium.conf missing or not readable by systemd-network", files))
        if not all(before for _, before in files.values()):
            raise RuntimeError(("systemd-networkd started after 50-cilium.conf arrived", files))
        elastic = {
            h: lab.ssh(
                h, "test -e " + CILIUM_NETWORKD + " && echo present || echo absent", check=False, timeout=30
            ).strip()
            for h in ZONE_A
            if last[h][0] == "ACTIVE"
        }
        if any(state == "present" for state in elastic.values()):
            raise RuntimeError(("50-cilium.conf on a running elastic host", elastic))
        mark("settled", states=last, openssh_on_running_hosts=versions, first_failure_epoch=first_down)


if __name__ == "__main__":
    main()
