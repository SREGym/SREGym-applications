#!/bin/bash
# Kubernetes node base for company worker provisioning, run as root in a fresh guest.
# Installs containerd 1.6.20, runc 1.1.5, CNI plugins 1.2.0, crictl 1.26.1 and
# kubeadm/kubelet/kubectl 1.26.3 from company-staged files verified against
# SHA256SUMS. Package inputs are verified local files; the guest has no archive access.
set -euo pipefail
STAGE=/var/cache/company/bootstrap
STATE=/var/lib/company/node-setup
[ "$(id -u)" = 0 ] || { echo "not root" >&2; exit 2; }

mkdir -p "$STATE"
exec > >(tee -a "$STATE/node-setup.log") 2>&1
log() { echo "$(date -u +%FT%T.%NZ) $*"; }

log "verifying staged files"
cd "$STAGE" && sha256sum --check --strict SHA256SUMS

if [ -d "$STAGE/systemd-pin" ] && [ ! -f "$STATE/pin-installed" ]; then
    log "installing the image's pinned systemd 249.11-0ubuntu3.9 (verified packages)"
    (cd "$STAGE/systemd-pin" && sha256sum --check --strict SHA256SUMS)
    DEBIAN_FRONTEND=noninteractive dpkg -i "$STAGE"/systemd-pin/*.deb
    log "installing the image's pinned netplan 0.105-0ubuntu2~22.04.3 (verified packages)"
    (cd "$STAGE/netplan-0.105" && sha256sum --check --strict SHA256SUMS)
    DEBIAN_FRONTEND=noninteractive dpkg -i "$STAGE"/netplan-0.105/*.deb
    touch "$STATE/pin-installed" "$STATE/reboot-required-for-pin"
fi

log "disabling image services a Kubernetes node does not use"
for unit in snapd.service snapd.socket snapd.seeded.service multipathd.service multipathd.socket \
            ModemManager.service packagekit.service; do
    systemctl disable --now "$unit" 2>/dev/null || true
done

if ! command -v conntrack >/dev/null || ! command -v socat >/dev/null || ! command -v ebtables >/dev/null; then
    log "installing kubeadm preflight packages from the verified local set (no archive access)"
    (cd "$STAGE/preflight-reused" && sha256sum --check --strict SHA256SUMS)
    DEBIAN_FRONTEND=noninteractive dpkg -i "$STAGE"/preflight-reused/*.deb
fi

log "installing runtime and Kubernetes binaries"
tar -C /usr/local -xzf "$STAGE/containerd-1.6.20-linux-amd64.tar.gz"
install -m 0755 "$STAGE/runc.amd64" /usr/local/sbin/runc
mkdir -p /opt/cni/bin /etc/cni/net.d
tar -C /opt/cni/bin -xzf "$STAGE/cni-plugins-linux-amd64-v1.2.0.tgz"
tar -C /usr/local/bin -xzf "$STAGE/crictl-v1.26.1-linux-amd64.tar.gz"
install -m 0755 "$STAGE/kubeadm" "$STAGE/kubelet" "$STAGE/kubectl" /usr/local/bin/

mkdir -p /etc/containerd
/usr/local/bin/containerd config default > /etc/containerd/config.toml
sed -i -e 's/SystemdCgroup = false/SystemdCgroup = true/' \
       -e 's#sandbox_image = .*#sandbox_image = "registry.k8s.io/pause:3.9"#' /etc/containerd/config.toml
cat > /etc/systemd/system/containerd.service <<'EOF'
[Unit]
Description=containerd container runtime
Documentation=https://containerd.io
After=network.target local-fs.target
[Service]
ExecStartPre=-/sbin/modprobe overlay
ExecStart=/usr/local/bin/containerd
Type=notify
Delegate=yes
KillMode=process
Restart=always
RestartSec=5
LimitNPROC=infinity
LimitCORE=infinity
LimitNOFILE=infinity
TasksMax=infinity
OOMScoreAdjust=-999
[Install]
WantedBy=multi-user.target
EOF
cat > /etc/crictl.yaml <<'EOF'
runtime-endpoint: unix:///run/containerd/containerd.sock
image-endpoint: unix:///run/containerd/containerd.sock
EOF
cat > /etc/systemd/system/kubelet.service <<'EOF'
[Unit]
Description=kubelet: The Kubernetes Node Agent
Documentation=https://kubernetes.io/docs/
Wants=network-online.target
After=network-online.target
[Service]
ExecStart=/usr/local/bin/kubelet
Restart=always
StartLimitInterval=0
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF
mkdir -p /etc/systemd/system/kubelet.service.d
cat > /etc/systemd/system/kubelet.service.d/10-kubeadm.conf <<'EOF'
[Service]
Environment="KUBELET_KUBECONFIG_ARGS=--bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubelet.conf --kubeconfig=/etc/kubernetes/kubelet.conf"
Environment="KUBELET_CONFIG_ARGS=--config=/var/lib/kubelet/config.yaml"
EnvironmentFile=-/var/lib/kubelet/kubeadm-flags.env
EnvironmentFile=-/etc/default/kubelet
ExecStart=
ExecStart=/usr/local/bin/kubelet $KUBELET_KUBECONFIG_ARGS $KUBELET_CONFIG_ARGS $KUBELET_KUBEADM_ARGS $KUBELET_EXTRA_ARGS
EOF
echo "KUBELET_EXTRA_ARGS=--node-ip=$(ip -4 -o addr show | awk '/ 10\.72\.0\./ {split($4,a,"/"); print a[1]}')" \
    > /etc/default/kubelet

cat > /etc/modules-load.d/k8s.conf <<'EOF'
overlay
br_netfilter
EOF
modprobe overlay
modprobe br_netfilter
cat > /etc/sysctl.d/90-k8s.conf <<'EOF'
net.bridge.bridge-nf-call-iptables = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward = 1
EOF
sysctl --system > /dev/null
# Approved image registry is mirrored locally.
grep -q 'company: offline mirror' /etc/hosts || echo '0.0.0.0 quay.io  # company: offline mirror' >> /etc/hosts

systemctl daemon-reload
systemctl enable --now containerd
systemctl enable kubelet
dpkg-query -W systemd netplan.io conntrack socat > "$STATE/packages.txt"
uname -r > "$STATE/kernel.txt"
touch "$STATE/node-base-done"
log "node base complete"
