# Frappe

[Frappe Framework](https://frappe.io/framework) serves one site through Gunicorn and nginx Desk APIs.
MariaDB stores application data. Redis provides a session cache and an RQ queue for long-running background jobs.

The chart wraps the upstream [Frappe Helm chart](https://github.com/frappe/helm)
and adds `frappe-admin` sidecars, database exporters, and the workload generator.
Repair tools include the MariaDB client, Desk API, and durable queue configuration through `frappe-admin`.

## SREGym integration

SREGym owns deployment and workload startup.
Its configuration is `sregym/service/apps/frappe/values.yaml` in the SREGym repository.
The first deployment creates the site before client traffic starts.
SREGym supplies pinned Linux/AMD64 and Linux/ARM64 images. The chart uses shared ReadWriteOnce volumes.

With `sregym.enabled=true`, the chart provides an operator toolbox and workload hooks instead of the original agent harness.
SREGym supplies observability and grading.
See the [integration guide](https://github.com/SREGym/SREGym/blob/main/docs/incident-arena.md) for supported execution and evaluation.

## Provenance

The chart comes from [Incident Arena](https://github.com/abundant-ai/incident-arena)
commit `fba011e451653c4058c2dc80a97c5d260c036872`, task `000--frappe--07-deletes-and-jobs-fail-194d9279`.
Its original source is [sre-world](https://github.com/abundant-ai/sre-world), under `substrates/frappe/chart`.
The source uses Apache-2.0. See [LICENSE](LICENSE).
The vendored ERPNext chart retains its [upstream license](chart/charts/erpnext/LICENSE-UPSTREAM.md).

SREGym keeps the task answer keys and reference configuration outside the chart.
