#!/bin/bash
# Company platform role: converge host settings from the platform repository.
set -eu
REPO_URL=${PLATFORM_REPO_URL:-}
SRC=/var/lib/company/platform-role/checkout
BOOTSTRAP=/var/lib/company/platform-role/bootstrap
EMPTY=/var/lib/company/platform-role/empty
log() { echo "platform-role: $*"; }
if [ -n "$REPO_URL" ]; then
    if [ -d "$SRC/.git" ]; then
        git -C "$SRC" pull -q --ff-only && log "checkout updated to $(git -C "$SRC" rev-parse --short HEAD)"
    else
        git clone -q "$REPO_URL" "$SRC" && log "checkout created at $(git -C "$SRC" rev-parse --short HEAD)"
    fi
fi
ROOT="$SRC"
[ -d "$ROOT/roles/platform" ] || ROOT="$BOOTSTRAP"
HOST=$(hostname -s)
PROFILE=$(python3 "$ROOT/fleet/resolve.py" "$ROOT/fleet" "$HOST" networkd_profile)
log "task: networkd settings"
mkdir -p /etc/systemd/networkd.conf.d "$EMPTY"
case "$PROFILE" in
    cluster) FROM="$ROOT/roles/platform/networkd.conf.d/" ;;
    *) FROM="$EMPTY/" ;;
esac
[ -d "$FROM" ] || FROM="$EMPTY/"
CHANGES=$(rsync -a --checksum --delete --itemize-changes "$FROM" /etc/systemd/networkd.conf.d/)
if [ -n "$CHANGES" ]; then
    log "changed: $(echo "$CHANGES" | tr '\n' ' ')"
else
    log "ok: networkd.conf.d unchanged"
fi
log "handler: reload systemd-networkd"
networkctl reload
log "done"
