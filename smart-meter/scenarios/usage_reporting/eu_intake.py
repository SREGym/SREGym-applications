"""Install independent EU intake before any new private device sessions exist."""

import ast
import copy
import json

from support import (
    BUILD,
    EVIDENCE,
    OUT,
    REPO,
    apply,
    capacity,
    commit,
    k,
    load,
    modules,
    note,
    push,
    put,
    require_build,
    run,
    save,
    sha,
)

SECOND = "eu-work-03"


def patch_reporting(current):
    # The reporting source was normalized with ast.unparse; match the return
    # expression, not its whitespace.
    class LiveView(ast.NodeTransformer):
        count = 0

        def visit_Return(self, node):
            if any(
                isinstance(v, ast.Constant) and v.value == "Latest retained browser-device readings"
                for v in ast.walk(node)
            ):
                self.count += 1
                extra = ast.parse(
                    "if REGION=='eu':\n from mqtt_view import live\n return self.reply(200,live(sql,REGION))"
                ).body[0]
                return [extra, node]
            return node

    patch = LiveView()
    tree = patch.visit(ast.parse(current))
    assert patch.count == 1, "Reporting source drift"
    changed = ast.unparse(ast.fix_missing_locations(tree)) + "\n"
    compile(changed, "reporting.py", "exec")
    return changed


def stage_pair_inputs():
    lab, cfgmod = modules()
    inputs = {row["name"]: row for row in cfgmod.config()["objects"]}
    available = int(run(SECOND, "awk '/MemAvailable:/ {print $2}' /proc/meminfo").strip()) * 1024
    assert available >= 832 * 2**20, (
        "Existing 3 GiB guest lacks headroom; an extra 768 MiB cannot be hot-added to the saved QEMU configuration"
    )
    # Import the cached broker and JDK images; stage the pinned JDK and Paho library.
    for image in ["emqx", "temurin"]:
        assert sha(BUILD / "images" / (image + ".tar")) == inputs["images/" + image + ".tar"]["sha256"]
        lab.push(SECOND, BUILD / "images" / (image + ".tar"), "/var/cache/company/intake/" + image + ".tar")
        run(SECOND, "sudo -n ctr -n k8s.io images import /var/cache/company/intake/" + image + ".tar", 180)
    jdk = next((BUILD / "stage/temurin-jdk-17.0.8_7").glob("*.tar.gz"))
    assert sha(jdk) == inputs["stage/temurin-jdk-17.0.8_7/" + jdk.name]["sha256"]
    assert (
        sha(BUILD / "stage/paho-java-1.2.5/org.eclipse.paho.client.mqttv3-1.2.5.jar")
        == inputs["stage/paho-java-1.2.5/org.eclipse.paho.client.mqttv3-1.2.5.jar"]["sha256"]
    )
    lab.push(SECOND, jdk, "/var/cache/company/intake/jdk.tar.gz")
    lab.push(
        SECOND,
        BUILD / "stage/paho-java-1.2.5/org.eclipse.paho.client.mqttv3-1.2.5.jar",
        "/opt/gateway-java-b/lib/paho.jar",
    )


def install_pair_staged():
    put(SECOND, "/opt/gateway-java-b/src/Gateway.java", (BUILD / "workloads/java-gateway/Gateway.java").read_text())
    jars = [
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
    run(
        SECOND,
        "set -e; sudo -n mkdir -p /opt/jdk-b /opt/gateway-java-b/classes; sudo -n tar -C /opt/jdk-b --strip-components=1 -xzf /var/cache/company/intake/jdk.tar.gz; sudo -n cp "
        + " ".join("/opt/kafka_2.13-3.4.0/libs/" + j for j in jars)
        + " /opt/gateway-java-b/lib/; "
        + 'sudo -n /opt/jdk-b/bin/javac -J-Xmx128m --release 17 -cp "/opt/gateway-java-b/lib/*" -d /opt/gateway-java-b/classes /opt/gateway-java-b/src/Gateway.java',
        240,
    )
    old = json.loads(k("eu", "-n telemetry get deploy/emqx -o json"))
    broker = dict(
        apiVersion="apps/v1",
        kind="Deployment",
        metadata=dict(name="emqx-b", namespace="telemetry"),
        spec=copy.deepcopy(old["spec"]),
    )
    spec = broker["spec"]
    spec["replicas"] = 1
    spec["selector"] = dict(matchLabels={"app": "emqx-b"})
    spec["template"]["metadata"] = dict(labels={"app": "emqx-b"})
    pod = spec["template"]["spec"]
    pod.pop("nodeName", None)
    pod["nodeSelector"] = {"kubernetes.io/hostname": SECOND}
    for v in pod["volumes"]:
        if v.get("hostPath", {}).get("path") == "/var/lib/telemetry/emqx":
            v["hostPath"]["path"] = "/var/lib/telemetry/emqx-b"
    for c in pod["containers"]:
        assert c["image"].endswith(":5.0.26"), c["image"]
        c["resources"] = dict(requests=dict(memory="256Mi", cpu="100m"), limits=dict(memory="512Mi"))
        env = {r["name"]: r for r in c.get("env", [])}
        # Separate Erlang node identity; every other setting is the first broker's.
        env["EMQX_NODE__NAME"] = dict(name="EMQX_NODE__NAME", value="emqx_b@127.0.0.1")
        c["env"] = list(env.values())
    gateway = dict(
        apiVersion="apps/v1",
        kind="Deployment",
        metadata=dict(name="gateway-b", namespace="telemetry"),
        spec=dict(
            replicas=1,
            strategy=dict(type="Recreate"),
            selector=dict(matchLabels={"app": "gateway-b"}),
            template=dict(
                metadata=dict(labels={"app": "gateway-b"}),
                spec=dict(
                    nodeSelector={"kubernetes.io/hostname": SECOND},
                    containers=[
                        dict(
                            name="gateway",
                            image="docker.io/library/eclipse-temurin:17.0.8_7-jre-jammy",
                            imagePullPolicy="Never",
                            command=[
                                "java",
                                "-Xms48m",
                                "-Xmx48m",
                                "-cp",
                                "/opt/gateway-java/classes:/opt/gateway-java/lib/*",
                                "Gateway",
                            ],
                            env=[
                                dict(name=n, value=v)
                                for n, v in {
                                    "CLIENT_ID": "eu-gateway-b",
                                    "KAFKA_BOOTSTRAP": "10.72.0.22:31992,10.72.0.23:31993",
                                    "MQTT_HOST": "emqx-b",
                                    "MQTT_PORT": "1884",
                                }.items()
                            ],
                            resources=dict(requests=dict(memory="96Mi", cpu="50m"), limits=dict(memory="192Mi")),
                            volumeMounts=[dict(name="code", mountPath="/opt/gateway-java", readOnly=True)],
                        )
                    ],
                    volumes=[dict(name="code", hostPath=dict(path="/opt/gateway-java-b", type="Directory"))],
                ),
            ),
        ),
    )
    services = [
        dict(
            apiVersion="v1",
            kind="Service",
            metadata=dict(name=name, namespace="telemetry"),
            spec=dict(
                selector={"app": "emqx-b"}, ports=[dict(name="mqtt", port=port, targetPort=port, **extra)], **kind
            ),
        )
        for name, port, extra, kind in [
            ("emqx-b", 1884, {}, {}),
            ("emqx-b-devices", 1883, {"nodePort": 31884}, {"type": "NodePort"}),
        ]
    ]
    # Respect existing enforcement by cloning policies with the exact old
    # selectors. Broadening a live selector would mix the two brokers.
    policies = []
    for resource in ["networkpolicies.networking.k8s.io", "ciliumnetworkpolicies.cilium.io"]:
        listing = json.loads(k("eu", "-n telemetry get " + resource + " -o json"))
        for original in listing["items"]:
            encoded = json.dumps(original["spec"])
            if "emqx" not in encoded and "gateway" not in encoded:
                continue

            # Replace exact app label values only, retaining all other policy.
            def visit(value):
                if isinstance(value, dict):
                    return {
                        key: ("emqx-b" if val == "emqx" else "gateway-b" if val == "gateway" else visit(val))
                        for key, val in value.items()
                    }
                if isinstance(value, list):
                    return [visit(v) for v in value]
                return value

            policies.append(
                dict(
                    apiVersion=original["apiVersion"],
                    kind=original["kind"],
                    metadata=dict(name=original["metadata"]["name"] + "-b", namespace="telemetry"),
                    spec=visit(original["spec"]),
                )
            )
    items = [broker, gateway, *services, *policies]
    save("eu-objects.json", items)
    apply("eu", items)
    for name in ["emqx-b", "gateway-b"]:
        k("eu", "-n telemetry rollout status deploy/" + name + " --timeout=120s", 140)
    save(
        "eu-pair-installed.json",
        dict(
            pods=k("eu", "-n telemetry get pods -o wide"),
            extra_guest_allocation_mib=0,
            workload_memory_limit_mib=704,
            capacity=capacity(),
        ),
    )


def install_pair():
    stage_pair_inputs()
    install_pair_staged()


def reporting():
    current = run("eu-parent-01", "cat /opt/company/reporting.py")
    changed = patch_reporting(current)
    put("eu-work-02", "/opt/company/reporting.py", changed)
    put(
        "eu-work-02",
        "/opt/company/application-profile.json",
        run("eu-parent-01", "cat /opt/company/application-profile.json"),
    )
    push("eu-work-02", "mqtt_view.py", "/opt/company/mqtt_view.py")
    put("eu-work-02", "/opt/company/company_mqtt_latest.py", (BUILD / "company_mqtt_latest.py").read_text())
    lab, cfgmod = modules()
    for name in ["pg8000", "scramp", "asn1crypto", "python_dateutil", "six", "kafka_python"]:
        wheel = next((BUILD / "stage").glob(name + "-*.whl"))
        lab.push("eu-work-02", wheel, "/var/cache/company/reporting/" + wheel.name)
        run(
            "eu-work-02",
            "sudo -n python3 -m zipfile -e /var/cache/company/reporting/" + wheel.name + " /opt/company/lib",
            45,
        )
    unit = run("eu-parent-01", "cat /etc/systemd/system/company-reporting.service")
    put("eu-work-02", "/etc/systemd/system/company-reporting.service", unit)
    run("eu-parent-01", "sudo -n systemctl disable --now company-reporting")
    run("eu-work-02", "set -e; sudo -n systemctl daemon-reload; sudo -n systemctl enable --now company-reporting")
    put("eu-work-03", REPO + "/services/reporting.py", changed)
    push("eu-work-03", "mqtt_view.py", REPO + "/services/mqtt_view.py")
    put("eu-work-03", REPO + "/fleet/eu-intake.json", json.dumps(load("eu-objects.json"), indent=2) + "\n")
    commit(
        "Add independent EU intake and ledger-host reporting",
        ["services/reporting.py", "services/mqtt_view.py", "fleet/eu-intake.json"],
    )
    cfg = cfgmod.config()
    cfg["eu_report_host"] = "eu-work-02"
    cfgmod.CONFIG.write_text(json.dumps(cfg, indent=2) + "\n")
    import world_snapshot

    world_snapshot.tunnels_stop()
    world_snapshot.tunnels_start()


def main():
    require_build()
    assert (OUT / "surfaces-complete.json").exists()
    assert not (OUT / "runtime-ready.json").exists(), "Install routing before device processes/sessions"
    if not (EVIDENCE / "eu-pair-installed.json").exists():
        install_pair()
    note("eu-pair-installed", host=SECOND)
    from reporting_profiles import main as deploy_reporting_profiles

    deploy_reporting_profiles()
    reporting()
    note("eu-reporting-moved", host="eu-work-02")


if __name__ == "__main__":
    main()
