#!/usr/bin/env bash
# Prepare a Linux host to run a saved Smart Meter world, then check that it can wake one.
# Installs packages on apt-based systems (Ubuntu 24.04, Debian 12 or newer); elsewhere install the same
# tools yourself and run this to check. SREGym's own requirements (Docker, kubectl, Helm, uv) are in its README.
set -euo pipefail
need_mem_gib=30   # MemTotal of a 32 GB host
need_disk_gib=60  # 10.2 GB package, 18 GB installed world, 30 GiB for a run
fail() { echo "setup_host: $*" >&2; exit 1; }

[ "$(uname -m)" = x86_64 ] || fail "needs an x86_64 host, this is $(uname -m)"
sudo -n true 2>/dev/null || fail "needs passwordless sudo (the grader reads frozen disks in a private mount namespace)"
if command -v apt-get >/dev/null; then
  sudo apt-get update -q
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q qemu-system-x86 qemu-utils openssh-client gzip zstd python3 curl
fi
for tool in qemu-system-x86_64 qemu-img ssh gzip zstd python3 docker; do
  command -v "$tool" >/dev/null || fail "missing $tool"
done
sudo usermod -aG kvm,docker "$USER"
sudo loginctl enable-linger "$USER"

[ -e /dev/kvm ] || fail "no /dev/kvm: enable virtualization (or nested virtualization on a cloud VM)"
version=$(qemu-system-x86_64 --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
# The saved world uses this processor model and machine type (QEMU 8.2 or newer). QEMU refuses at once when the
# host lacks one of its features; a probe still waiting after 5 s can wake the world. It runs through sudo
# because the kvm group above only applies from the next login.
set +e
probe=$(sudo -n timeout 5 qemu-system-x86_64 -enable-kvm -machine pc-q35-8.2 -cpu Westmere-v2,enforce -m 64 \
  -nodefaults -display none -S 2>&1)
rc=$?
set -e
[ $rc -eq 124 ] || fail "this host cannot wake the saved guests: ${probe:-QEMU $version exited with $rc}"

mem_gib=$(awk '/MemTotal/ {print int($2 / 1048576)}' /proc/meminfo)
disk_gib=$(df -BG --output=avail "$(dirname "$0")" | tail -1 | tr -dc 0-9)
[ "$mem_gib" -ge "$need_mem_gib" ] || fail "needs ${need_mem_gib} GiB of memory, has ${mem_gib}"
[ "$disk_gib" -ge "$need_disk_gib" ] || echo "setup_host: warning: ${disk_gib} GiB free here; the download, the installed world and one run need about ${need_disk_gib} GiB"
echo "setup_host: ready (QEMU $version, ${mem_gib} GiB memory, ${disk_gib} GiB free). Log in again for the kvm and docker groups."
