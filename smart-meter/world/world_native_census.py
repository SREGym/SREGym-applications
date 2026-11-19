"""Commands that list every reading in a host's ledger or raw Kafka segments, for the seal's census."""

import json
import shlex

import lab

SQL = "select json_build_object('device_id',device_id,'boot_id',boot_id,'seq',seq,'tenant',tenant,'event_time',round(extract(epoch from event_time),3),'quantity',quantity) from readings"


def podpid(n, name):
    return json.loads(lab.ssh(n, "sudo -n crictl inspect $(sudo -n crictl ps --name " + name + " -q)"))["info"]["pid"]


def native(n, kind):
    pid = podpid(n, "postgres" if kind == "sql" else "kafka")
    if kind == "sql":
        return (
            "sudo -n nsenter -t "
            + str(pid)
            + " -m -p -r -w -- /usr/bin/psql -U postgres -d ledger -At -v ON_ERROR_STOP=1 -c "
            + shlex.quote(SQL)
        )
    inner = 'files=$(find /data/kafka/raw-telemetry-* -maxdepth 1 -name "*.log" -type f | sort | paste -sd,); test -n "$files"; exec /opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments --files "$files" --deep-iteration --print-data-log'
    return (
        'sudo -n env JAVA_HOME=/opt/java/openjdk KAFKA_HEAP_OPTS="-Xms16m -Xmx64m" nsenter -t '
        + str(pid)
        + " -m -p -r -w -- /bin/sh -ec "
        + shlex.quote(inner)
    )
