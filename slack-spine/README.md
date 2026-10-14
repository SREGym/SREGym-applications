# Slack Spine

A Slack-like application with nine TypeScript roles behind Envoy.
Go services provide real-time delivery and search. Python workers consume Redpanda events.
PostgreSQL, Redis, and MinIO store application state.

Application roles expose configuration, reload, event, and sequencer APIs on port 8000.
The database maintenance API is `/v1/maintenance` on `db-maintenance:8081`.

## SREGym integration

SREGym owns deployment and workload startup.
Its configuration is `sregym/service/apps/slack_spine/values.yaml` in the SREGym repository.
The pinned images require AMD64. The application contains about 40 pods and uses shared ReadWriteOnce volumes.

With `sregym.enabled=true`, the chart provides an operator toolbox and workload hooks instead of the original agent harness.
SREGym supplies observability and grading.
Each workload process uses a separate episode directory to preserve request-ID uniqueness after a restart.
See the [integration guide](https://github.com/SREGym/SREGym/blob/main/docs/incident-arena.md) for supported execution and evaluation.

## Provenance

The chart comes from [Incident Arena](https://github.com/abundant-ai/incident-arena)
commit `fba011e451653c4058c2dc80a97c5d260c036872`, task `018--slack-spine--09-I1-seq-lock-leak-0b7c2973`.
It also includes the sequencer broker from task 007 and `templates/tier06.yaml` from task 008.
Its original source is [sre-world](https://github.com/abundant-ai/sre-world).
The source uses Apache-2.0. See [LICENSE](LICENSE).

SREGym keeps the task answer keys and reference configuration outside the chart.
