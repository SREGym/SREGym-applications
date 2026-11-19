#!/usr/bin/env python3
"""Serve only this company's current provisioning metadata, never lab seeds."""

import http.server
import urllib.parse

import lab
import world_policy


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        root = world_policy.config().get("seed_service_root", str(lab.SEED))
        super().__init__(*args, directory=root, **kwargs)

    def send_head(self):
        cfg = world_policy.config()
        names = set(cfg["memory_mib"]) | {"recovery-01"}
        names |= {name + "-rescue" for name in cfg["memory_mib"]}
        if cfg.get("construction_phase") == "ordinary-installation":
            names |= set(cfg.get("construction_seed_names", []))
        names = {name for name in names if lab.pid_of(lab.VM / (name + ".pid"))}
        parts = urllib.parse.unquote(urllib.parse.urlsplit(self.path).path).split("/")
        if (
            len(parts) != 3
            or parts[1] not in names
            or parts[2] not in {"user-data", "meta-data", "vendor-data", "network-config"}
        ):
            self.send_error(403, "Provisioning metadata path unavailable")
            return None
        return super().send_head()


if __name__ == "__main__":
    http.server.ThreadingHTTPServer(("127.0.0.1", lab.SEED_PORT), Handler).serve_forever()
