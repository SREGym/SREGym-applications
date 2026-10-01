# Slack Spine

A polyglot Slack clone. Nine TypeScript roles (`svc-auth`, `svc-workspace`, `svc-channel`,
`svc-message`, `svc-thread`, `svc-file`, `svc-search`, `svc-notification`, `svc-platform`) sit
behind an Envoy edge. Go services handle real-time delivery and connections (`ws-gateway`,
`flannel`, `channel-rt`, `dispatcher`, `kafkagate`, `search-engine`). Python async workers consume
Redpanda, and PostgreSQL, Redis and MinIO hold state. About 40 pods in all.

Operational surfaces: every application role exposes `GET/PUT /admin/config`,
`POST /admin/reload`, `GET/PUT /admin/event` and `/admin/sequencer` on port 8000. The database
maintenance controller exposes `/v1/maintenance` on `db-maintenance:8081`.

Ported from [Incident Arena](https://github.com/abundant-ai/incident-arena) for SREGym.

## Using it in SREGym

SREGym installs the chart with a values overlay
(`sregym/service/apps/incident_arena/values/slack-spine.yaml` in the SREGym repo). The overlay
pins the task's published images, switches off the Harbor harness through the chart's own values
(the agent foothold `main` with its egress proxy and DNS filter, and the in-chart
observability stack) and turns on the `sregym.*` hooks. SREGym's cluster-wide Prometheus replaces
the in-chart observability.

```bash
helm install slack-spine ./slack-spine/chart -n slack-spine --create-namespace \
  -f <SREGym>/sregym/service/apps/incident_arena/values/slack-spine.yaml
```

### Load generator

The chart ships a seeded, open-loop load generator (`loadgen` Deployment). It writes one JSON
record per arrival to `/grader/loadgen.jsonl` in its own pod and offers no load until its
episode clock is pinned with an authenticated `POST /grader/episode-start` on port 9100 (header
`X-SRE-World-Grader-Access`, token in `/run/grader-access/token` inside the pod). SREGym's
`IncidentArenaLoadgen` workload manager does both: it pins the start and reads the ledger through
`kubectl exec`. Load profiles are selected with `loadgen.profile`, or defined inline through
`loadgen.profilesYaml`.

## Requirements

* Images are amd64-only.
* Incident Arena sized each task for 8 vCPU / 16 GB on its own. The app runs about 40 pods (about 4 vCPU of requests).
* The chart uses ReadWriteOnce volumes shared by several pods. That works with
  `openebs-hostpath` and kind's `local-path` (local PVs pin consumers to one node).

## Provenance

The chart comes from [abundant-ai/incident-arena](https://github.com/abundant-ai/incident-arena)
at commit `fba011e451653c4058c2dc80a97c5d260c036872` (task `018--slack-spine--09-I1-seq-lock-leak-0b7c2973`, plus `files/sequencer_config_broker.py` from task 007 and `templates/tier06.yaml` from task 008, so one chart covers all 13 Slack tasks). Its source of truth is
[abundant-ai/sre-world](https://github.com/abundant-ai/sre-world) (`substrates/slack (substrate/chart)/chart`).
Both are Apache-2.0; see [`LICENSE`](LICENSE).

The Incident Arena answer key (`ground-truth.yaml`), rendered `config-before.json` and egress
host list were removed from the chart directory. SREGym keeps the task contracts with its problems.

## SREGym edits

Every edit is inert unless `sregym.enabled` is true, so the chart still renders the original
Incident Arena task when it is false.

* `values.yaml` gains an `sregym:` block whose defaults do nothing.
* `templates/loadgen.yaml`:
  * adds `sregym.loadgenLabels` to every load generator resource. SREGym sets
    `app: load-generator`, which its agent visibility policy hides;
  * adds `sregym.loadgenEnv` to the load generator container's environment;
  * renders `sregym.neutralAnswerKey` as the load generator's answer key. The load generator
    refuses to start without one, and SREGym grades with its own oracles.
  * skips the verifier-only baseline capture init container and its ConfigMap.
* New `templates/sregym-toolbox.yaml`: an `ops-toolbox` Deployment running the task's `main`
  image. It provides the operator CLIs, with `submit_incident_report` and
  `declare_repair_complete` replaced by stubs.
