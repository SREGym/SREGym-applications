#!/usr/bin/env python3
"""Telemetry workloads on the US cluster: ZooKeeper 3.7.1, Kafka 3.4.0, EMQX 5.0.26, Python gateway.

Brokers and ZooKeeper members run as one-replica StatefulSets pinned to their
nodes, with data on the node's local root (hostPath under /var/lib/telemetry).
Apache binaries are the verified tarballs, extracted on each node and run on the
Temurin 17.0.8 JRE image. EMQX devices use the default listener (NodePort
31883); the gateway uses a separate listener in zone `gateway` with a
100,000-message queue and a 128 MB forced-shutdown heap bound.

  python3 workloads.py stage NODE...        # extract binaries on nodes
  python3 workloads.py apply --brokers 3 [--brokers 1 --brokers 2]
"""

import argparse
import json

from cluster import kubectl
from lab import ARTIFACTS, ROOT, log, push, ssh

STAGE = ROOT / "stage"
BROKER_NODE = {1: "us-work-01", 2: "us-work-02", 3: "us-work-03"}
EMQX_NODE = "us-work-02"
GATEWAY_NODE = "us-work-01"
NS = "telemetry"
TEMURIN = "docker.io/library/eclipse-temurin:17.0.8_7-jre-jammy"
EMQX = "docker.io/emqx/emqx:5.0.26"
PYTHON = "docker.io/library/python:3.11.5-slim-bookworm"
HEAPS = {"kafka": "-Xms384m -Xmx384m", "zk_mib": 40}


def stage(node):
    files = json.loads((STAGE / "MANIFEST.json").read_text())["files"]
    needed = [
        "kafka_2.13-3.4.0.tgz",
        "apache-zookeeper-3.7.1-bin.tar.gz",
        "kafka_python-2.0.2-py2.py3-none-any.whl",
        "paho-mqtt-1.6.1.tar.gz",
    ]
    sums = "".join(f"{files[f]['sha256']}  {f}\n" for f in needed)
    (ARTIFACTS / "SHA256SUMS.workloads").write_text(sums)
    for file in needed:
        push(node, STAGE / file, f"/var/cache/company/bootstrap/stage/{file}")
    push(node, ARTIFACTS / "SHA256SUMS.workloads", "/var/cache/company/bootstrap/stage/SHA256SUMS.workloads")
    push(node, ROOT / "workloads" / "gateway.py", "/opt/gateway/gateway.py")
    ssh(
        node,
        'sudo -n sh -euc "cd /var/cache/company/bootstrap/stage && sha256sum --check --strict SHA256SUMS.workloads && '
        "tar -C /opt -xzf kafka_2.13-3.4.0.tgz && tar -C /opt -xzf apache-zookeeper-3.7.1-bin.tar.gz && "
        "mkdir -p /opt/gateway/lib && python3 -m zipfile -e kafka_python-2.0.2-py2.py3-none-any.whl "
        "/opt/gateway/lib && tar -xzf paho-mqtt-1.6.1.tar.gz -C /tmp && "
        "cp -r /tmp/paho-mqtt-1.6.1/src/paho /opt/gateway/lib/ && "
        'mkdir -p /var/lib/telemetry/kafka /var/lib/telemetry/zk /var/lib/telemetry/emqx"',
        timeout=300,
    )
    log(f"{node}: Kafka 3.4.0, ZooKeeper 3.7.1 and gateway libraries staged")
    import cluster

    cluster.prune_staged_binaries(node)


def zk_members(brokers):
    return ",".join(f"zk-{i}-0.zk-{i}.{NS}.svc.cluster.local:2181" for i in brokers)


def zookeeper(i, brokers):
    servers = "\n".join(f"    server.{j}=zk-{j}-0.zk-{j}.{NS}.svc.cluster.local:2888:3888" for j in brokers)
    standalone = "true" if len(brokers) == 1 else "false"
    return f"""---
apiVersion: v1
kind: ConfigMap
metadata: {{name: zk-{i}, namespace: {NS}}}
data:
  zoo.cfg: |
    tickTime=2000
    initLimit=10
    syncLimit=5
    dataDir=/data
    clientPort=2181
    quorumListenOnAllIPs=true
    standaloneEnabled={standalone}
    admin.enableServer=false
    4lw.commands.whitelist=srvr,mntr,ruok,stat
{servers}
---
apiVersion: v1
kind: Service
metadata: {{name: zk-{i}, namespace: {NS}}}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {{app: zk-{i}}}
  ports: [{{name: client, port: 2181}}, {{name: peer, port: 2888}}, {{name: election, port: 3888}}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {{name: zk-{i}, namespace: {NS}}}
spec:
  serviceName: zk-{i}
  replicas: 1
  selector: {{matchLabels: {{app: zk-{i}}}}}
  template:
    metadata: {{labels: {{app: zk-{i}, component: zookeeper}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {BROKER_NODE[i]}}}
      terminationGracePeriodSeconds: 30
      containers:
      - name: zookeeper
        image: {TEMURIN}
        imagePullPolicy: Never
        command: [sh, -c, 'echo {i} > /data/myid && exec /opt/zookeeper/bin/zkServer.sh start-foreground /conf/zoo.cfg']
        env:
        - {{name: ZK_SERVER_HEAP, value: "{HEAPS["zk_mib"]}"}}
        - {{name: ZOO_LOG_DIR, value: /data/logs}}
        ports: [{{containerPort: 2181}}, {{containerPort: 2888}}, {{containerPort: 3888}}]
        volumeMounts:
        - {{name: data, mountPath: /data}}
        - {{name: conf, mountPath: /conf}}
        - {{name: dist, mountPath: /opt/zookeeper, readOnly: true}}
      volumes:
      - {{name: data, hostPath: {{path: /var/lib/telemetry/zk, type: DirectoryOrCreate}}}}
      - {{name: conf, configMap: {{name: zk-{i}}}}}
      - {{name: dist, hostPath: {{path: /opt/apache-zookeeper-3.7.1-bin, type: Directory}}}}
"""


def kafka(i, brokers):
    replication = min(3, len(brokers))
    return f"""---
apiVersion: v1
kind: ConfigMap
metadata: {{name: kafka-{i}, namespace: {NS}}}
data:
  server.properties: |
    broker.id={i}
    broker.rack={BROKER_NODE[i]}
    listeners=PLAINTEXT://0.0.0.0:9092
    advertised.listeners=PLAINTEXT://kafka-{i}-0.kafka-{i}.{NS}.svc.cluster.local:9092
    zookeeper.connect={zk_members(brokers)}
    zookeeper.connection.timeout.ms=18000
    log.dirs=/data/kafka
    num.partitions=6
    default.replication.factor={min(2, len(brokers))}
    min.insync.replicas=1
    offsets.topic.replication.factor={replication}
    transaction.state.log.replication.factor={replication}
    transaction.state.log.min.isr=1
    log.retention.hours=72
    log.segment.bytes=67108864
    log.roll.ms=3600000
    log.retention.check.interval.ms=300000
    auto.create.topics.enable=false
    group.initial.rebalance.delay.ms=3000
---
apiVersion: v1
kind: Service
metadata: {{name: kafka-{i}, namespace: {NS}}}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {{app: kafka-{i}}}
  ports: [{{name: kafka, port: 9092}}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {{name: kafka-{i}, namespace: {NS}}}
spec:
  serviceName: kafka-{i}
  replicas: 1
  selector: {{matchLabels: {{app: kafka-{i}}}}}
  template:
    metadata: {{labels: {{app: kafka-{i}, component: kafka}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {BROKER_NODE[i]}}}
      terminationGracePeriodSeconds: 60
      containers:
      - name: kafka
        image: {TEMURIN}
        imagePullPolicy: Never
        command: [/opt/kafka/bin/kafka-server-start.sh, /conf/server.properties]
        env:
        - {{name: KAFKA_HEAP_OPTS, value: "{HEAPS["kafka"]}"}}
        - {{name: LOG_DIR, value: /data/logs}}
        ports: [{{containerPort: 9092}}]
        volumeMounts:
        - {{name: data, mountPath: /data}}
        - {{name: conf, mountPath: /conf}}
        - {{name: dist, mountPath: /opt/kafka, readOnly: true}}
      volumes:
      - {{name: data, hostPath: {{path: /var/lib/telemetry/kafka, type: DirectoryOrCreate}}}}
      - {{name: conf, configMap: {{name: kafka-{i}}}}}
      - {{name: dist, hostPath: {{path: /opt/kafka_2.13-3.4.0, type: Directory}}}}
"""


def emqx():
    return f"""---
apiVersion: v1
kind: Service
metadata: {{name: emqx-devices, namespace: {NS}}}
spec:
  type: NodePort
  selector: {{app: emqx}}
  ports: [{{name: mqtt, port: 1883, nodePort: 31883}}]
---
apiVersion: v1
kind: Service
metadata: {{name: emqx, namespace: {NS}}}
spec:
  selector: {{app: emqx}}
  ports: [{{name: gateway, port: 1884}}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {{name: emqx, namespace: {NS}}}
spec:
  replicas: 1
  strategy: {{type: Recreate}}
  selector: {{matchLabels: {{app: emqx}}}}
  template:
    metadata: {{labels: {{app: emqx}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {EMQX_NODE}}}
      initContainers:
      - name: data-owner
        image: {EMQX}
        imagePullPolicy: Never
        securityContext: {{runAsUser: 0}}
        command: [sh, -c, 'chown -R 1000:1000 /opt/emqx/data']
        volumeMounts: [{{name: data, mountPath: /opt/emqx/data}}]
      containers:
      - name: emqx
        image: {EMQX}
        imagePullPolicy: Never
        env:
        - {{name: EMQX_NODE__NAME, value: emqx@127.0.0.1}}
        - {{name: EMQX_LISTENERS__TCP__GATEWAY__BIND, value: "1884"}}
        - {{name: EMQX_LISTENERS__TCP__GATEWAY__ZONE, value: gateway}}
        - {{name: EMQX_ZONES__GATEWAY__MQTT__MAX_MQUEUE_LEN, value: "1000000"}}
        - {{name: EMQX_ZONES__GATEWAY__MQTT__SESSION_EXPIRY_INTERVAL, value: "7d"}}
        # EMQX 5.0.26 reads force_shutdown as one struct per zone; a partial
        # override leaves check_oom/2 without its other keys and crashes the
        # connection every 100 incoming packets. Set the whole struct.
        - {{name: EMQX_ZONES__GATEWAY__FORCE_SHUTDOWN__ENABLE, value: "false"}}
        - {{name: EMQX_ZONES__GATEWAY__FORCE_SHUTDOWN__MAX_MAILBOX_SIZE, value: "1000"}}
        - {{name: EMQX_ZONES__GATEWAY__FORCE_SHUTDOWN__MAX_HEAP_SIZE, value: "128MB"}}
        ports: [{{containerPort: 1883}}, {{containerPort: 1884}}, {{containerPort: 18083}}]
        volumeMounts: [{{name: data, mountPath: /opt/emqx/data}}]
      volumes:
      - {{name: data, hostPath: {{path: /var/lib/telemetry/emqx, type: DirectoryOrCreate}}}}
"""


def gateway(brokers):
    bootstrap = ",".join(f"kafka-{i}-0.kafka-{i}.{NS}.svc.cluster.local:9092" for i in brokers)
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
      nodeSelector: {{kubernetes.io/hostname: {GATEWAY_NODE}}}
      containers:
      - name: gateway
        image: {PYTHON}
        imagePullPolicy: Never
        command: [python3, -u, /opt/gateway/gateway.py]
        env:
        - name: POD_NAME
          valueFrom: {{fieldRef: {{fieldPath: metadata.name}}}}
        - {{name: PYTHONPATH, value: /opt/gateway/lib}}
        - {{name: KAFKA_BOOTSTRAP, value: "{bootstrap}"}}
        - {{name: MQTT_HOST, value: emqx}}
        - {{name: MQTT_PORT, value: "1884"}}
        volumeMounts: [{{name: code, mountPath: /opt/gateway, readOnly: true}}]
      volumes:
      - {{name: code, hostPath: {{path: /opt/gateway, type: Directory}}}}
"""


LEDGER_NODE = "us-work-01"
POSTGRES = "docker.io/library/postgres:15.3"
WRITER_WHEELS = [
    "pg8000-1.29.8-py3-none-any.whl",
    "scramp-1.4.4-py3-none-any.whl",
    "asn1crypto-1.5.1-py2.py3-none-any.whl",
    "python_dateutil-2.8.2-py2.py3-none-any.whl",
    "six-1.16.0-py2.py3-none-any.whl",
    "kafka_python-2.0.2-py2.py3-none-any.whl",
]
SCHEMA = """CREATE TABLE readings (
  id bigserial PRIMARY KEY,
  delivery_id text NOT NULL UNIQUE,
  device_id text NOT NULL,
  boot_id text NOT NULL,
  seq bigint NOT NULL,
  tenant text NOT NULL,
  event_time timestamptz NOT NULL,
  quantity integer NOT NULL,
  kafka_partition integer,
  kafka_offset bigint,
  ingested_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX readings_identity ON readings (device_id, boot_id, seq);
"""


def stage_ledger(node=LEDGER_NODE):
    files = json.loads((STAGE / "MANIFEST.json").read_text())["files"]
    sums = "".join(f"{files[f]['sha256']}  {f}\n" for f in WRITER_WHEELS)
    (ARTIFACTS / "SHA256SUMS.writer").write_text(sums)
    for file in WRITER_WHEELS:
        push(node, STAGE / file, f"/var/cache/company/bootstrap/stage/{file}")
    push(node, ARTIFACTS / "SHA256SUMS.writer", "/var/cache/company/bootstrap/stage/SHA256SUMS.writer")
    push(node, ROOT / "workloads" / "writer.py", "/opt/writer/writer.py")
    unpack = " && ".join(f"python3 -m zipfile -e {w} /opt/writer/lib" for w in WRITER_WHEELS)
    ssh(
        node,
        f'sudo -n sh -euc "cd /var/cache/company/bootstrap/stage && sha256sum --check --strict SHA256SUMS.writer && '
        f'mkdir -p /opt/writer/lib /var/lib/telemetry/postgres && {unpack}"',
        timeout=300,
    )
    log(f"{node}: writer libraries staged")


def ledger(brokers):
    bootstrap = ",".join(f"kafka-{i}-0.kafka-{i}.{NS}.svc.cluster.local:9092" for i in brokers)
    schema = "\n".join("    " + line for line in SCHEMA.splitlines())
    return f"""---
apiVersion: v1
kind: ConfigMap
metadata: {{name: ledger-schema, namespace: {NS}}}
data:
  schema.sql: |
{schema}
---
apiVersion: v1
kind: Service
metadata: {{name: postgres, namespace: {NS}}}
spec:
  selector: {{app: postgres}}
  ports: [{{name: pg, port: 5432}}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {{name: postgres, namespace: {NS}}}
spec:
  serviceName: postgres
  replicas: 1
  selector: {{matchLabels: {{app: postgres}}}}
  template:
    metadata: {{labels: {{app: postgres}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {LEDGER_NODE}}}
      containers:
      - name: postgres
        image: {POSTGRES}
        imagePullPolicy: Never
        args: [-c, shared_buffers=32MB, -c, max_connections=20]
        env:
        - {{name: POSTGRES_HOST_AUTH_METHOD, value: trust}}
        - {{name: POSTGRES_DB, value: ledger}}
        - {{name: PGDATA, value: /var/lib/postgresql/data/pgdata}}
        ports: [{{containerPort: 5432}}]
        volumeMounts:
        - {{name: data, mountPath: /var/lib/postgresql/data}}
        - {{name: schema, mountPath: /docker-entrypoint-initdb.d}}
      volumes:
      - {{name: data, hostPath: {{path: /var/lib/telemetry/postgres, type: DirectoryOrCreate}}}}
      - {{name: schema, configMap: {{name: ledger-schema}}}}
---
apiVersion: apps/v1
kind: Deployment
metadata: {{name: writer, namespace: {NS}}}
spec:
  replicas: 1
  strategy: {{type: Recreate}}
  selector: {{matchLabels: {{app: writer}}}}
  template:
    metadata: {{labels: {{app: writer}}}}
    spec:
      nodeSelector: {{kubernetes.io/hostname: {LEDGER_NODE}}}
      containers:
      - name: writer
        image: {PYTHON}
        imagePullPolicy: Never
        command: [python3, -u, /opt/writer/writer.py]
        env:
        - name: POD_NAME
          valueFrom: {{fieldRef: {{fieldPath: metadata.name}}}}
        - {{name: PYTHONPATH, value: /opt/writer/lib}}
        - {{name: KAFKA_BOOTSTRAP, value: "{bootstrap}"}}
        - {{name: PG_HOST, value: postgres}}
        volumeMounts: [{{name: code, mountPath: /opt/writer, readOnly: true}}]
      volumes:
      - {{name: code, hostPath: {{path: /opt/writer, type: Directory}}}}
"""


def render(brokers, with_ledger=False):
    parts = [f"---\napiVersion: v1\nkind: Namespace\nmetadata: {{name: {NS}}}\n"]
    for i in brokers:
        parts.append(zookeeper(i, brokers))
        parts.append(kafka(i, brokers))
    parts += [emqx(), gateway(brokers)]
    if with_ledger:
        parts.append(ledger(brokers))
    return "".join(parts)


def apply(brokers, with_ledger=False):
    text = render(brokers, with_ledger)
    path = ARTIFACTS / f"workloads-{'-'.join(map(str, brokers))}.yaml"
    path.write_text(text)
    with path.open("rb") as handle:
        log(kubectl("apply -f -", stdin=handle).strip().replace("\n", "; "))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("stage", "apply", "render"))
    parser.add_argument("nodes", nargs="*")
    parser.add_argument("--brokers", type=int, action="append")
    arguments = parser.parse_args()
    if arguments.action == "stage":
        for node in arguments.nodes:
            stage(node)
    elif arguments.action == "apply":
        apply(sorted(arguments.brokers))
    else:
        print(render(sorted(arguments.brokers)))
