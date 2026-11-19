#!/usr/bin/env python3
"""Start one regional core on clean stores during the build."""

import json
import os
import shlex
import sys
import time

REGION = sys.argv[1]
os.environ["SMART_METER_REGION"] = REGION
import cluster
import lab
import workloads as us
import yaml
from world_boot import OUT

if REGION == "eu":
    import workloads_eu as eu

    eu.IDS = OUT / "eu-cluster-ids.json"

NS = "telemetry"


def k(s, **kw):
    return cluster.kubectl(s, **kw)


def apply(text):
    p = OUT / (REGION + "-core.yaml")
    p.write_text(text)
    with p.open("rb") as f:
        k("apply -f -", stdin=f, timeout=300)


def main():
    cluster.wait(REGION + " API", lambda: k("get --raw=/readyz").strip() == "ok", 600)
    if "--resume" not in sys.argv:
        k("delete namespace telemetry --wait=true --timeout=240s", timeout=270, check=False)
        for i in [1, 2, 3]:
            n = f"{REGION}-work-0{i}"
            lab.ssh(
                n,
                "sudo -n rm -rf /var/lib/telemetry/kafka /var/lib/telemetry/zk /var/lib/telemetry/postgres /var/lib/telemetry/emqx",
                timeout=120,
            )
    writer = (lab.ROOT / "workloads/writer.py").read_text()
    writer = writer[writer.find("import json") :]
    p = OUT / (REGION + "-writer.py")
    p.write_text('"""Transactional ledger ingestion and hourly outbox publication."""\n' + writer)
    ledger = f"{REGION}-work-0" + ("2" if REGION == "eu" else "1")
    lab.push(ledger, p, "/opt/writer/writer.py")
    docs = list(
        yaml.safe_load_all(eu.render("core") + eu.render("gateway") if REGION == "eu" else us.render([1, 2, 3], True))
    )
    extra = []
    for d in docs:
        if not d:
            continue
        name = d["metadata"]["name"]
        if REGION == "us" and d["kind"] == "ConfigMap" and name.startswith("zk-"):
            i = int(name.split("-")[-1])
            extra.append(
                dict(
                    apiVersion="v1",
                    kind="Service",
                    metadata=dict(name="zk-external-" + str(i), namespace=NS),
                    spec=dict(
                        type="NodePort",
                        selector=dict(app=name),
                        ports=[dict(name="client", port=2181, targetPort=2181, nodePort=32180 + i)],
                    ),
                )
            )
        if d["kind"] == "ConfigMap" and name.startswith("kafka-"):
            i = int(name.split("-")[-1])
            p = d["data"]["server.properties"]
            lines = []
            for line in p.splitlines():
                if line.startswith("listeners="):
                    line += ",EXTERNAL://0.0.0.0:19092"
                if line.startswith("advertised.listeners="):
                    line += f",EXTERNAL://{lab.VMS[f'{REGION}-work-0{i}']['ip']}:{31990 + i}"
                if line.startswith("listener.security.protocol.map="):
                    line += ",EXTERNAL:PLAINTEXT"
                lines.append(line)
            if not any(x.startswith("listener.security.protocol.map=") for x in lines):
                lines.append("listener.security.protocol.map=PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT")
            d["data"]["server.properties"] = "\n".join(lines) + "\n"
            extra.append(
                dict(
                    apiVersion="v1",
                    kind="Service",
                    metadata=dict(name="broker-external-" + str(i), namespace=NS),
                    spec=dict(
                        type="NodePort",
                        selector=dict(app=name),
                        ports=[dict(name="external", port=19092, targetPort=19092, nodePort=31990 + i)],
                    ),
                )
            )
        if d["kind"] == "Deployment" and name == "emqx":
            env = d["spec"]["template"]["spec"]["containers"][0]["env"]
            env[:] = [r for r in env if r["name"] != "EMQX_MQTT__SESSION_EXPIRY_INTERVAL"]
            env.append(dict(name="EMQX_MQTT__SESSION_EXPIRY_INTERVAL", value="3600s"))
        if d["kind"] == "Service" and any(p.get("nodePort") == 31883 for p in d["spec"].get("ports", [])):
            d["spec"]["ports"].append(dict(name="browser", port=8083, targetPort=8083, nodePort=31885, protocol="TCP"))
        if d["kind"] == "Deployment" and name == "writer":
            env = d["spec"]["template"]["spec"]["containers"][0]["env"]
            env[:] = [r for r in env if r["name"] not in ["REGION", "ROLLUP_TOPIC"]]
            env.extend([dict(name="REGION", value=REGION), dict(name="ROLLUP_TOPIC", value="rollup-" + REGION)])
    extra.append(
        dict(
            apiVersion="v1",
            kind="Service",
            metadata=dict(name="ledger-external", namespace=NS),
            spec=dict(
                type="NodePort", selector=dict(app="postgres"), ports=[dict(port=5432, targetPort=5432, nodePort=31432)]
            ),
        )
    )
    apply(yaml.safe_dump_all([d for d in docs if d] + extra, sort_keys=False))

    def running():
        rows = json.loads(k("-n telemetry get pods -o json"))["items"]
        names = [
            r["metadata"]["name"]
            for r in rows
            if r["status"].get("phase") == "Running"
            and all(c.get("ready") for c in r["status"].get("containerStatuses", []))
        ]
        required = ["kafka-1", "kafka-2", "kafka-3", "emqx", "postgres"] + (
            ["zk-1", "zk-2", "zk-3"] if REGION == "us" else []
        )
        return all(any(n.startswith(v) for n in names) for v in required)

    cluster.wait(REGION + " real core", running, 900, every=5)
    target = lab.VMS[f"{REGION}-work-03"]["ip"]
    probe = "import socket; s=socket.create_connection((" + repr(target) + ",31993),3);s.close()"
    cluster.wait(
        REGION + " actual Kafka listener",
        lambda: "yes" in lab.ssh(cluster.CP, "python3 -c " + shlex.quote(probe) + " && echo yes", check=False),
        300,
        every=3,
    )
    if REGION == "us":
        # Endpoints the fleet controller uses; each needs a published Service and a complete srvr response.
        code = """import socket,json
rows=[]
for i,ip in enumerate(['10.72.0.11','10.72.0.12','10.72.0.13'],1):
 with socket.create_connection((ip,32180+i),2) as s:
  s.settimeout(2);s.sendall(b'srvr');data=b''
  while len(data)<16384:
   part=s.recv(min(4096,16384-len(data)))
   if not part:break
   data+=part
   if b'Mode: leader' in data or b'Mode: follower' in data:break
 rows.append(dict(server=ip,port=32180+i,response=data.decode()))
print(json.dumps(rows))
"""

        def zk_ready():
            try:
                rows = json.loads(lab.ssh(cluster.CP, "python3 -c " + shlex.quote(code), timeout=10))
            except Exception:
                return False
            good = sum("Mode: leader" in r["response"] or "Mode: follower" in r["response"] for r in rows)
            if good == 3:
                (OUT / "us-zk-ordinary-readiness.json").write_text(json.dumps(rows, indent=2) + "\n")
            return good == 3

        cluster.wait("US actual fleet quorum health endpoints", zk_ready, 180, every=3)
    admin = "kafka-2-0" if REGION == "eu" else "kafka-3-0"

    # A broker's pod can be ready before it has registered with the cluster; topics need all three.
    def registered():
        out = k(
            f"-n telemetry exec {admin} -- env KAFKA_HEAP_OPTS=-Xmx64m /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092",
            timeout=120,
            check=False,
        )
        return out.count("(id: ") == 3

    cluster.wait(REGION + " three Kafka brokers registered", registered, 300, every=5)

    def cli(args):
        return k(
            f"-n telemetry exec {admin} -- env KAFKA_HEAP_OPTS=-Xmx64m /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "
            + args,
            timeout=180,
        )

    if REGION == "us":
        cli(
            "--create --if-not-exists --topic raw-telemetry --replica-assignment 1:2,1:2,1:2,1:3,1:3,1:3 --config min.insync.replicas=1 --config unclean.leader.election.enable=true --config retention.ms=259200000"
        )
    else:
        cli(
            "--create --if-not-exists --topic raw-telemetry --partitions 6 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=259200000"
        )
    cli(
        f"--create --if-not-exists --topic rollup-{REGION} --partitions 3 --replication-factor "
        + ("3" if REGION == "eu" else "2")
    )
    k("-n telemetry rollout restart deployment/writer deployment/gateway")
    (OUT / (REGION + "-native-core.json")).write_text(
        json.dumps(
            dict(
                epoch=time.time(),
                raw_topic=cli("--describe --topic raw-telemetry"),
                pods=k("-n telemetry get pods -o wide"),
                external_listeners=True,
                regional_mode="KRaft RF3/minISR2" if REGION == "eu" else "ZooKeeper RF2/minISR1/unclean election",
            )
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
