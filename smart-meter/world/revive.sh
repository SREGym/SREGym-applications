#!/bin/sh
set -eu
exec /usr/bin/python3 "$(dirname "$(readlink -f "$0")")/revive.py" "$@"
