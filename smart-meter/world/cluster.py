#!/usr/bin/env python3
"""Regional kubeadm cluster helpers for the build.

Initializes the control plane on <region>-parent-01 (Kubernetes 1.26.3, etcd 3.5.7,
kube-proxy kept), installs Cilium 1.13.2 (L7 proxy, legacy host routing, kube-proxy
replacement disabled), imports the cached Cilium images and joins workers.
"""

import json
import os
import shlex
import subprocess
import time

import lab
from lab import ARTIFACTS, ROOT, log, push, ssh

STAGE = ROOT / "stage"
CILIUM_CACHE = ROOT / "external" / "image-cache"
CILIUM_IMAGES = {
    "cilium": "85708b11d45647c35b9288e0de0706d24a5ce8a378166cadc700f756cc1a38d6",
    "operator-generic": "a1982c0a22297aaac3563e428c330e17668305a41865a842dec53d241c5490ab",
}
# SMART_METER_REGION selects the regional cluster: 'us' or 'eu'.
REGION = os.environ.get("SMART_METER_REGION", "us")
CP = f"{REGION}-parent-01"
SUBNETS = {"us": ("10.42.0.0/16", "10.43.0.0/16"), "eu": ("10.52.0.0/16", "10.53.0.0/16")}
POD_SUBNET, SERVICE_SUBNET = SUBNETS[REGION]
NODE_FILES = [
    "kubeadm",
    "kubelet",
    "kubectl",
    "containerd-1.6.20-linux-amd64.tar.gz",
    "runc.amd64",
    "cni-plugins-linux-amd64-v1.2.0.tgz",
    "crictl-v1.26.1-linux-amd64.tar.gz",
]
K = "sudo -n env KUBECONFIG=/etc/kubernetes/admin.conf /usr/local/bin/kubectl --request-timeout=60s"


def wait(what, check, timeout, every=5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if check():
                log(f"ready: {what}")
                return
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            last = error
        time.sleep(every)
    raise RuntimeError(f"timed out waiting for {what}: {last}")


def kubectl(arguments, timeout=120, check=True, stdin=None):
    return ssh(CP, f"{K} {arguments}", timeout=timeout, check=check, stdin=stdin)


def import_cilium(name):
    for image, expected in CILIUM_IMAGES.items():
        directory = CILIUM_CACHE / f"{image}-1.13.2"
        if not (directory / "blobs" / "sha256" / expected).is_file():
            raise RuntimeError(f"cache lacks {image} manifest {expected}")
        tar = subprocess.Popen(
            ["tar", "-C", str(directory), "-cf", "-", "oci-layout", "index.json", "blobs"], stdout=subprocess.PIPE
        )
        try:
            ssh(
                name,
                f"sudo -n /usr/local/bin/ctr --namespace k8s.io images import --base-name "
                f"quay.io/cilium/{image} --platform linux/amd64 --digests -",
                stdin=tar.stdout,
                timeout=900,
            )
        finally:
            tar.stdout.close()
            tar.wait(timeout=60)


def prune_staged_binaries(name):
    """Reclaim verified installation copies; retain package promotion inputs."""
    files = NODE_FILES + [
        "kafka_2.13-3.4.0.tgz",
        "apache-zookeeper-3.7.1-bin.tar.gz",
        "OpenJDK17U-jdk_x64_linux_hotspot_17.0.8_7.tar.gz",
        "jdk/OpenJDK17U-jdk_x64_linux_hotspot_17.0.8_7.tar.gz",
    ]
    disk = lab.VM / f"{name}.qcow2"
    before = disk.stat().st_blocks * 512
    paths = " ".join(shlex.quote("/var/cache/company/bootstrap/stage/" + file) for file in files)
    output = ssh(name, "sudo -n rm -f -- " + paths + " && (sudo -n fstrim -v / 2>&1 || true)", timeout=120)
    after = disk.stat().st_blocks * 512
    with (ARTIFACTS / "staging-reclaim.jsonl").open("a") as stream:
        stream.write(
            json.dumps(
                {
                    "time": time.time(),
                    "node": name,
                    "before_allocated": before,
                    "after_allocated": after,
                    "trim_output": output,
                    "retained": "All package .9/.10, netplan and checksum inputs; host masters unchanged",
                }
            )
            + "\n"
        )
    log(f"{name}: verified installation copies removed; disk allocated {before} -> {after} bytes")


def init_control_plane():
    config = f"""apiVersion: kubeadm.k8s.io/v1beta3
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: {lab.VMS[CP]["ip"]}
  bindPort: 6443
nodeRegistration:
  name: {CP}
  criSocket: unix:///run/containerd/containerd.sock
  imagePullPolicy: Never
  # The host's 1,152 MiB is below kubeadm's 1,700 MB preflight minimum;
  # only that check is skipped.
  ignorePreflightErrors: [Mem]
---
apiVersion: kubeadm.k8s.io/v1beta3
kind: ClusterConfiguration
kubernetesVersion: v1.26.3
clusterName: {REGION}
controlPlaneEndpoint: {lab.VMS[CP]["ip"]}:6443
networking:
  podSubnet: {POD_SUBNET}
  serviceSubnet: {SERVICE_SUBNET}
etcd:
  local:
    imageTag: 3.5.7-0
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
"""
    path = ARTIFACTS / "kubeadm-init.yaml"
    path.write_text(config)
    push(CP, path, "/var/cache/company/bootstrap/kubeadm-init.yaml")
    if ssh(CP, "test -f /etc/kubernetes/admin.conf && echo yes || true").strip() != "yes":
        out = ssh(CP, "sudo -n kubeadm init --config /var/cache/company/bootstrap/kubeadm-init.yaml", timeout=900)
        (ARTIFACTS / "kubeadm-init.log").write_text(out)
    wait("API readyz", lambda: kubectl("get --raw=/readyz").strip() == "ok", 300)
    push(CP, STAGE / "helm.tar.gz", "/var/cache/company/bootstrap/stage/helm.tar.gz")
    push(CP, STAGE / "cilium-1.13.2.tgz", "/var/cache/company/bootstrap/stage/cilium-1.13.2.tgz")
    values = f"""k8sServiceHost: {lab.VMS[CP]["ip"]}
k8sServicePort: 6443
kubeProxyReplacement: disabled
operator:
  replicas: 1
ipam:
  mode: cluster-pool
  operator:
    clusterPoolIPv4PodCIDRList: ["{POD_SUBNET}"]
    clusterPoolIPv4MaskSize: 24
"""
    values += os.environ.get("SMART_METER_CILIUM_EXTRA_VALUES", "")
    (ARTIFACTS / "cilium-values.yaml").write_text(values)
    push(CP, ARTIFACTS / "cilium-values.yaml", "/var/cache/company/bootstrap/cilium-values.yaml")
    if "cilium" not in kubectl("-n kube-system get ds -o name", check=False):
        ssh(
            CP,
            'sudo -n sh -c "mkdir -p /var/cache/company/bootstrap/helm && '
            "tar -xzf /var/cache/company/bootstrap/stage/helm.tar.gz "
            "-C /var/cache/company/bootstrap/helm && KUBECONFIG=/etc/kubernetes/admin.conf HOME=/root "
            "/var/cache/company/bootstrap/helm/linux-amd64/helm install cilium "
            "/var/cache/company/bootstrap/stage/cilium-1.13.2.tgz "
            '--namespace kube-system --values /var/cache/company/bootstrap/cilium-values.yaml"',
            timeout=300,
        )
    wait(f"{CP} Ready", lambda: " Ready " in kubectl(f"get node {CP} --no-headers"), 600)
    # There is no recursive upstream on this isolated network. Retain the
    # Kubernetes zones and loop detection; external names return SERVFAIL.
    dns = json.loads(kubectl("-n kube-system get configmap coredns -o json"))
    corefile = dns["data"]["Corefile"]
    start = corefile.find("    forward . /etc/resolv.conf {")
    if start >= 0:
        end = corefile.index("    }", start) + len("    }\n")
        (ARTIFACTS / f"coredns-before-{REGION}.json").write_text(json.dumps(dns, indent=2) + "\n")
        corefile = corefile[:start] + corefile[end:]
        patch = json.dumps({"data": {"Corefile": corefile}})
        kubectl("-n kube-system patch configmap coredns --type merge -p " + shlex.quote(patch))
        kubectl("-n kube-system rollout restart deployment/coredns")


def join(name):
    if name in kubectl("get nodes -o name", check=False):
        log(f"{name}: already joined")
        return
    command = ssh(CP, "sudo -n kubeadm token create --print-join-command").split()
    token = command[command.index("--token") + 1]
    ca_hash = command[command.index("--discovery-token-ca-cert-hash") + 1]
    config = f"""apiVersion: kubeadm.k8s.io/v1beta3
kind: JoinConfiguration
discovery:
  bootstrapToken:
    apiServerEndpoint: {lab.VMS[CP]["ip"]}:6443
    token: {token}
    caCertHashes: [{ca_hash}]
nodeRegistration:
  name: {name}
  criSocket: unix:///run/containerd/containerd.sock
  imagePullPolicy: Never
"""
    path = ARTIFACTS / f"kubeadm-join-{name}.yaml"
    path.write_text(config)
    push(name, path, "/var/cache/company/bootstrap/kubeadm-join.yaml", "0600")
    path.unlink()
    out = ssh(name, "sudo -n kubeadm join --config /var/cache/company/bootstrap/kubeadm-join.yaml", timeout=600)
    (ARTIFACTS / f"kubeadm-join-{name}.log").write_text(out)
    wait(f"{name} Ready", lambda: " Ready " in kubectl(f"get node {name} --no-headers"), 600)
