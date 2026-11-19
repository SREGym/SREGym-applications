#!/usr/bin/env python3
"""EU telemetry workloads: three combined KRaft broker/controllers (Kafka 3.4.0), EMQX 5.0.26 and the
Java gateway on eu-work-01, PostgreSQL 15.3 with the ledger writer and rollup outbox on eu-work-02, and
MirrorMaker 2 on eu-work-03.

Raw topic: RF3, min.insync.replicas=2, producers acks=all, unclean election off. MM2 runs both regional
flows in one dedicated-mode process: local-origin rollup-eu to the remote cluster and the remote
rollup-us back.

  SMART_METER_REGION=eu python3 workloads_eu.py stage | apply | topics
"""

import base64
import json
import os
import sys
import uuid

os.environ.setdefault("SMART_METER_REGION", "eu")
import workloads
from cluster import kubectl
from lab import ARTIFACTS, ROOT, log, push, ssh

NS = "telemetry"
STAGE = ROOT / "stage"
BROKER_NODE = {1: "eu-work-01", 2: "eu-work-02", 3: "eu-work-03"}
EDGE, LEDGER, MIRROR = "eu-work-01", "eu-work-02", "eu-work-03"
TEMURIN, PYTHON = workloads.TEMURIN, workloads.PYTHON
HEAP = {
    "kafka": "-Xms384m -Xmx384m",
    "mm2": "-Xms128m -Xmx128m",
    "gateway": "-Xms48m -Xmx48m",
}
BOOT = ",".join(f"kafka-{i}-0.kafka-{i}.{NS}.svc.cluster.local:9092" for i in BROKER_NODE)
REMOTE_BOOT = ""  # the US brokers; the build sets them (scenarios/usage_reporting/runtime.py)
CLIENT_JARS = [
    "kafka-clients-3.4.0.jar",
    "lz4-java-1.8.0.jar",
    "snappy-java-1.1.8.4.jar",
    "zstd-jni-1.5.2-1.jar",
    "slf4j-api-1.7.36.jar",
    "slf4j-reload4j-1.7.36.jar",
    "reload4j-1.2.19.jar",
    "jackson-databind-2.13.4.2.jar",
    "jackson-core-2.13.4.jar",
    "jackson-annotations-2.13.4.jar",
]
IDS = ARTIFACTS / "eu-cluster-ids.json"


def cluster_ids():
    if not IDS.exists():

        def make():
            # Kafka's Uuid format; like Kafka's own generator, never start with '-' (tools read it as an option).
            while True:
                value = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
                if not value.startswith("-"):
                    return value

        IDS.write_text(json.dumps({"eu": make(), "us": make()}))
    return json.loads(IDS.read_text())


def stage():
    files = json.loads((STAGE / "MANIFEST.json").read_text())["files"]
    for node in BROKER_NODE.values():
        (ARTIFACTS / "SHA256SUMS.kafka").write_text(
            f"{files['kafka_2.13-3.4.0.tgz']['sha256']}  kafka_2.13-3.4.0.tgz\n"
        )
        push(node, STAGE / "kafka_2.13-3.4.0.tgz", "/var/cache/company/bootstrap/stage/kafka_2.13-3.4.0.tgz")
        push(node, ARTIFACTS / "SHA256SUMS.kafka", "/var/cache/company/bootstrap/stage/SHA256SUMS.kafka")
        ssh(
            node,
            'sudo -n sh -euc "cd /var/cache/company/bootstrap/stage && sha256sum --check --strict SHA256SUMS.kafka && '
            'tar -C /opt -xzf kafka_2.13-3.4.0.tgz && mkdir -p /var/lib/telemetry/kafka"',
            timeout=300,
        )
        log(f"{node}: Kafka 3.4.0 staged")
    # Java gateway: compiled on the edge node with the verified Temurin JDK, run on the Temurin JRE image.
    jdk = next((STAGE / "temurin-jdk-17.0.8_7").glob("*.tar.gz"))
    for item in (jdk, STAGE / "temurin-jdk-17.0.8_7" / "SHA256SUMS"):
        push(EDGE, item, f"/var/cache/company/bootstrap/stage/jdk/{item.name}")
    for item in (STAGE / "paho-java-1.2.5").iterdir():
        if item.suffix == ".jar" or item.name == "SHA256SUMS":
            push(EDGE, item, f"/var/cache/company/bootstrap/stage/paho/{item.name}")
    push(EDGE, ROOT / "workloads" / "java-gateway" / "Gateway.java", "/opt/gateway-java/src/Gateway.java")
    push(EDGE, ROOT / "workloads" / "java-gateway" / "GatewayKeyTest.java", "/opt/gateway-java/src/GatewayKeyTest.java")
    libs = " ".join(f"/opt/kafka_2.13-3.4.0/libs/{jar}" for jar in CLIENT_JARS)
    ssh(
        EDGE,
        'sudo -n sh -euc "cd /var/cache/company/bootstrap/stage/jdk && sha256sum --check --strict SHA256SUMS && '
        "cd /var/cache/company/bootstrap/stage/paho && sha256sum --check --strict SHA256SUMS && "
        "mkdir -p /opt/jdk /opt/gateway-java/lib /opt/gateway-java/classes && "
        f"tar -C /opt/jdk --strip-components=1 -xzf /var/cache/company/bootstrap/stage/jdk/{jdk.name} && "
        f"cp {libs} /var/cache/company/bootstrap/stage/paho/*.jar /opt/gateway-java/lib/ && "
        '/opt/jdk/bin/javac -J-Xmx128m --release 17 -cp \\"/opt/gateway-java/lib/*\\" '
        "-d /opt/gateway-java/classes /opt/gateway-java/src/Gateway.java && "
        '/opt/jdk/bin/java -version 2>&1 | head -1"',
        timeout=900,
    )
    log(f"{EDGE}: Java gateway compiled with Temurin JDK 17.0.8+7 against Paho 1.2.5 and kafka-clients 3.4.0")
    workloads.stage_ledger(LEDGER)
    push(LEDGER, ROOT / "workloads" / "export.py", "/opt/writer/export.py")
    import cluster

    for node in BROKER_NODE.values():
        cluster.prune_staged_binaries(node)


def statefulset(name, node, heap, properties, cluster_id, ports="[{containerPort: 9092}, {containerPort: 9093}]"):
    body = "\n".join("    " + line for line in properties.strip().splitlines())
    return f"""---
apiVersion: v1
kind: ConfigMap
metadata: {{name: {name}, namespace: {NS}}}
data:
  server.properties: |
{body}
---
apiVersion: v1
kind: Service
metadata: {{name: {name}, namespace: {NS}}}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {{app: {name}}}
  ports: [{{name: kafka, port: 9092}}, {{name: controller, port: 9093}}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {{name: {name}, namespace: {NS}}}
spec:
  serviceName: {name}
  replicas: 1
  selector: {{matchLabels: {{app: {name}}}}}
  template:
    metadata: {{labels: {{app: {name}, component: kafka}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {node}}}
      terminationGracePeriodSeconds: 60
      containers:
      - name: kafka
        image: {TEMURIN}
        imagePullPolicy: Never
        command: [sh, -c, '/opt/kafka/bin/kafka-storage.sh format -t {cluster_id} -c /conf/server.properties --ignore-formatted && exec /opt/kafka/bin/kafka-server-start.sh /conf/server.properties']
        env:
        - {{name: KAFKA_HEAP_OPTS, value: "{heap}"}}
        - {{name: LOG_DIR, value: /data/logs}}
        ports: {ports}
        volumeMounts:
        - {{name: data, mountPath: /data}}
        - {{name: conf, mountPath: /conf}}
        - {{name: dist, mountPath: /opt/kafka, readOnly: true}}
      volumes:
      - {{name: data, hostPath: {{path: /var/lib/telemetry/kafka, type: DirectoryOrCreate}}}}
      - {{name: conf, configMap: {{name: {name}}}}}
      - {{name: dist, hostPath: {{path: /opt/kafka_2.13-3.4.0, type: Directory}}}}
"""


def kraft(i, cluster_id):
    voters = ",".join(f"{j}@kafka-{j}-0.kafka-{j}.{NS}.svc.cluster.local:9093" for j in BROKER_NODE)
    return statefulset(
        f"kafka-{i}",
        BROKER_NODE[i],
        HEAP["kafka"],
        f"""
process.roles=broker,controller
node.id={i}
controller.quorum.voters={voters}
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://kafka-{i}-0.kafka-{i}.{NS}.svc.cluster.local:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
broker.rack={BROKER_NODE[i]}
log.dirs=/data/kafka
num.partitions=6
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
log.retention.hours=72
log.segment.bytes=67108864
log.roll.ms=3600000
log.retention.check.interval.ms=300000
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=3000
""",
        cluster_id,
    )


def emqx():
    return workloads.emqx().replace(f"kubernetes.io/hostname: {workloads.EMQX_NODE}", f"kubernetes.io/hostname: {EDGE}")


def gateway():
    return f"""---
apiVersion: apps/v1
kind: Deployment
metadata: {{name: gateway, namespace: {NS}}}
spec:
  replicas: 1
  strategy: {{type: Recreate}}
  selector: {{matchLabels: {{app: gateway}}}}
  template:
    metadata: {{labels: {{app: gateway}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {EDGE}}}
      containers:
      - name: gateway
        image: {TEMURIN}
        imagePullPolicy: Never
        command: [java, {", ".join(HEAP["gateway"].split())}, -Dlog4j.configuration=file:/opt/kafka/config/tools-log4j.properties, -cp, '/opt/gateway-java/classes:/opt/gateway-java/lib/*', Gateway]
        env:
        - {{name: CLIENT_ID, value: eu-gateway}}
        - {{name: KAFKA_BOOTSTRAP, value: "{BOOT}"}}
        - {{name: MQTT_HOST, value: emqx}}
        - {{name: MQTT_PORT, value: "1884"}}
        volumeMounts:
        - {{name: code, mountPath: /opt/gateway-java, readOnly: true}}
        - {{name: dist, mountPath: /opt/kafka, readOnly: true}}
      volumes:
      - {{name: code, hostPath: {{path: /opt/gateway-java, type: Directory}}}}
      - {{name: dist, hostPath: {{path: /opt/kafka_2.13-3.4.0, type: Directory}}}}
"""


def ledger():
    text = workloads.ledger(list(BROKER_NODE)).replace(
        f"kubernetes.io/hostname: {workloads.LEDGER_NODE}", f"kubernetes.io/hostname: {LEDGER}"
    )
    outbox = (
        "        - {name: ROLLUP_TOPIC, value: rollup-eu}\n"
        "        - {name: REGION, value: eu}\n"
        "        - {name: PG_HOST, value: postgres}\n"
    )
    return text.replace("        - {name: PG_HOST, value: postgres}\n", outbox)


def mm2():
    properties = f"""
clusters=eu, us
eu.bootstrap.servers={BOOT}
us.bootstrap.servers={REMOTE_BOOT}
eu->us.enabled=true
eu->us.topics=rollup-eu
us->eu.enabled=true
us->eu.topics=rollup-us
groups.exclude=.*
sync.group.offsets.enabled=false
tasks.max=1
eu.config.storage.replication.factor=3
eu.offset.storage.replication.factor=3
eu.status.storage.replication.factor=3
us.config.storage.replication.factor=1
us.offset.storage.replication.factor=1
us.status.storage.replication.factor=1
eu->us.replication.factor=1
eu->us.heartbeats.topic.replication.factor=1
eu->us.checkpoints.topic.replication.factor=1
eu->us.offset-syncs.topic.replication.factor=3
us->eu.replication.factor=3
us->eu.heartbeats.topic.replication.factor=3
us->eu.checkpoints.topic.replication.factor=3
us->eu.offset-syncs.topic.replication.factor=1
eu.producer.acks=all
us.producer.acks=all
"""
    body = "\n".join("    " + line for line in properties.strip().splitlines())
    return f"""---
apiVersion: v1
kind: ConfigMap
metadata: {{name: mm2, namespace: {NS}}}
data:
  mm2.properties: |
{body}
---
apiVersion: apps/v1
kind: Deployment
metadata: {{name: mm2, namespace: {NS}}}
spec:
  replicas: 1
  strategy: {{type: Recreate}}
  selector: {{matchLabels: {{app: mm2}}}}
  template:
    metadata: {{labels: {{app: mm2}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {MIRROR}}}
      containers:
      - name: mm2
        image: {TEMURIN}
        imagePullPolicy: Never
        command: [/opt/kafka/bin/connect-mirror-maker.sh, /conf/mm2.properties]
        env:
        - {{name: KAFKA_HEAP_OPTS, value: "{HEAP["mm2"]}"}}
        - {{name: LOG_DIR, value: /tmp/logs}}
        volumeMounts:
        - {{name: conf, mountPath: /conf}}
        - {{name: dist, mountPath: /opt/kafka, readOnly: true}}
      volumes:
      - {{name: conf, configMap: {{name: mm2}}}}
      - {{name: dist, hostPath: {{path: /opt/kafka_2.13-3.4.0, type: Directory}}}}
"""


def render(part):
    ids = cluster_ids()
    head = f"---\napiVersion: v1\nkind: Namespace\nmetadata: {{name: {NS}}}\n"
    parts = {
        "core": head + "".join(kraft(i, ids["eu"]) for i in BROKER_NODE) + emqx() + ledger(),
        "gateway": gateway(),
        "mm2": mm2(),
    }
    return parts[part]


def apply(part):
    path = ARTIFACTS / f"workloads-eu-{part}.yaml"
    path.write_text(render(part))
    with path.open("rb") as handle:
        log(kubectl("apply -f -", stdin=handle).strip().replace("\n", "; "))


def kx(pod, command, timeout=600, check=False):
    return kubectl(f"-n {NS} exec {pod} -- sh -c '{command}'", timeout=timeout, check=check)


def topics():
    out = []
    for name, partitions in (("raw-telemetry", 6), ("rollup-eu", 3)):
        out.append(
            kx(
                "kafka-2-0",
                f"KAFKA_HEAP_OPTS=-Xmx64m /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create "
                f"--if-not-exists --topic {name} --partitions {partitions} --replication-factor 3 "
                "--config min.insync.replicas=2 --config retention.ms=259200000",
                check=True,
            )
        )
    log(" ".join(line.strip() for line in out))


if __name__ == "__main__":
    action = sys.argv[1]
    if action == "stage":
        stage()
    elif action == "apply":
        apply(sys.argv[2])
    elif action == "topics":
        topics()
    else:
        print(render(sys.argv[2]))
