#!/usr/bin/env bash
set -euo pipefail

image=${1:?Usage: test-assets-image.sh IMAGE amd64|arm64}
arch=${2:?An architecture is required}
case "$arch" in
  amd64) machine=x86_64 ;;
  arm64) machine=aarch64 ;;
  *) echo "Unsupported architecture: $arch" >&2; exit 2 ;;
esac

test "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")" = "linux/$arch"
test "$(docker run --rm --network none "$image" uname -m)" = "$machine"

# Exercise the same copy operations as both init containers, without networking.
docker run --rm --network none "$image" sh -ec '
  ! command -v git
  test -z "$(find /opt/social-network -name .git -print)"
  for path in gen-lua lua-thrift nginx/lua-scripts nginx/pages media/lua-scripts keys; do
    test -n "$(find "/opt/social-network/$path" -type f -print -quit)"
    mkdir -p "/tmp/copied/$path"
    cp -r "/opt/social-network/$path/." "/tmp/copied/$path/"
    diff -r "/opt/social-network/$path" "/tmp/copied/$path"
  done
  test -s /tmp/copied/keys/server.key
  test -s /tmp/copied/keys/server.crt
  test -s /tmp/copied/keys/CA.pem
  echo "All runtime assets copied successfully"
'
