"""Apply the measured Loki ingestion limit; collectors are unchanged."""

import json
import time
from pathlib import Path

import yaml
from build_context import OUT, lab, note, require_build


def main():
    require_build()
    baseline = json.loads((OUT / "log-baseline-current.json").read_text())
    folder = Path(baseline["folder"])
    frozen = json.loads((folder / "frozen-limit.json").read_text())
    assert frozen["actual_elapsed_seconds"] >= 900 and not frozen["normal_drops_observed"]
    assert frozen["ingestion_burst_size_mb"] * 2**20 >= frozen["max_promtail_batch_bytes"]
    old = lab.ssh("eu-work-02", "cat /var/lib/company/loki.yml")
    (folder / "loki-before-limit.yml").write_text(old)
    value = yaml.safe_load(old)
    limits = value.setdefault("limits_config", {})
    assert "ingestion_rate_mb" not in limits and "ingestion_burst_size_mb" not in limits
    limits.update(
        ingestion_rate_mb=frozen["ingestion_rate_mb"], ingestion_burst_size_mb=frozen["ingestion_burst_size_mb"]
    )
    path = folder / "loki-measured-limit.yml"
    path.write_text(yaml.safe_dump(value, sort_keys=False))
    lab.push("eu-work-02", path, "/var/lib/company/loki.yml")
    lab.ssh("eu-work-02", "sudo -n systemctl restart company-loki; systemctl is-active company-loki", timeout=45)
    result = dict(
        host_epoch=time.time(),
        baseline=str(folder),
        ingestion_rate_mb=frozen["ingestion_rate_mb"],
        ingestion_burst_size_mb=frozen["ingestion_burst_size_mb"],
    )
    (OUT / "measured-log-limit-applied.json").write_text(json.dumps(result, indent=2) + "\n")
    note("log-limit-applied", **result)


if __name__ == "__main__":
    main()
