#!/usr/bin/env python3
"""Install the regional reporting service on each parent host."""

import lab
from world_boot import put


def install_parent(region):
    n = region + "-parent-01"
    ledger = region + "-work-0" + ("2" if region == "eu" else "1")
    lab.ssh(n, "sudo -n mkdir -p /opt/company/lib /var/lib/company")
    wheels = [
        "pg8000-1.29.8-py3-none-any.whl",
        "scramp-1.4.4-py3-none-any.whl",
        "asn1crypto-1.5.1-py2.py3-none-any.whl",
        "python_dateutil-2.8.2-py2.py3-none-any.whl",
        "six-1.16.0-py2.py3-none-any.whl",
        "kafka_python-2.0.2-py2.py3-none-any.whl",
    ]
    for w in wheels:
        lab.push(n, lab.ROOT / "stage" / w, "/var/lib/company/" + w)
        lab.ssh(n, "sudo -n python3 -m zipfile -e /var/lib/company/" + w + " /opt/company/lib")
    lab.push(n, lab.ROOT / "company_reporting.py", "/opt/company/reporting.py")
    bootstrap = ",".join(lab.VMS[region + "-work-0" + str(i)]["ip"] + ":" + str(31990 + i) for i in [1, 2, 3])
    unit = (
        "[Unit]\nAfter=network-online.target\n[Service]\nEnvironment=PYTHONPATH=/opt/company/lib\nEnvironment=REGION="
        + region
        + "\nEnvironment=PG_HOST="
        + lab.VMS[ledger]["ip"]
        + "\nEnvironment=KAFKA_BOOTSTRAP="
        + bootstrap
        + "\nExecStart=/usr/bin/python3 /opt/company/reporting.py\nRestart=on-failure\nRestartSec=5\n[Install]\nWantedBy=multi-user.target\n"
    )
    put(n, region + "-reporting.service", unit, "/etc/systemd/system/company-reporting.service")
    lab.ssh(n, "sudo -n systemctl daemon-reload && sudo -n systemctl enable --now company-reporting")
