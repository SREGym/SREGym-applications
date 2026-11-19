"""Select device endpoints before any device session is created."""

from pathlib import Path

from build_context import OUT, lab, note, require_build


def install():
    require_build()
    assert not (OUT / "runtime-ready.json").exists()
    assert "inactive" in lab.ssh("ops-helper-01", "systemctl is-active private-devices-eu || true")
    source = Path(__file__).with_name("mqtt_router.py")
    lab.push("ops-helper-01", source, "/opt/client-routing/mqtt_router.py")
    unit = "[Unit]\nDescription=Regional device endpoint routing\nAfter=network-online.target\n[Service]\nUser=nobody\nGroup=nogroup\nRuntimeDirectory=mqtt-route\nLimitNOFILE=65536\nExecStartPre=+/bin/sh -ec 'iptables -t nat -C OUTPUT -p tcp -d 10.72.0.21 --dport 31883 -m owner ! --uid-owner 65534 -j REDIRECT --to-ports 31884 2>/dev/null || iptables -t nat -A OUTPUT -p tcp -d 10.72.0.21 --dport 31883 -m owner ! --uid-owner 65534 -j REDIRECT --to-ports 31884'\nExecStartPre=+/bin/sh -ec 'iptables -t nat -C OUTPUT -p tcp -d 10.72.0.20 --dport 8080 -j DNAT --to-destination 10.72.0.22:8080 2>/dev/null || iptables -t nat -A OUTPUT -p tcp -d 10.72.0.20 --dport 8080 -j DNAT --to-destination 10.72.0.22:8080'\nExecStart=/usr/bin/python3 /opt/client-routing/mqtt_router.py\nRestart=on-failure\nRestartSec=2\n[Install]\nWantedBy=multi-user.target\n"
    path = OUT / "initial-device-routing.service"
    path.write_text(unit)
    lab.push("ops-helper-01", path, "/etc/systemd/system/private-mqtt-route.service")
    lab.ssh("ops-helper-01", "sudo -n systemctl daemon-reload; sudo -n systemctl enable --now private-mqtt-route")
    note("device-routing-started", fraction_secondary=0.6)
