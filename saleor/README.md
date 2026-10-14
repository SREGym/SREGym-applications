# Saleor

[Saleor](https://saleor.io) provides a Django/GraphQL API and a Celery worker over PostgreSQL, Valkey, and RabbitMQ.
The workload generator sends storefront requests and checkouts.
Repair tools include `psql`, the GraphQL API, and RabbitMQ management.

## SREGym integration

SREGym owns deployment and workload startup.
Its configuration is `sregym/service/apps/saleor/values.yaml` in the SREGym repository.
The runner completes the migration and data-seeding Job before it starts the API and worker.
A direct Helm install does not reproduce this sequence.
The application URLs use the `saleor` namespace.
SREGym supplies pinned Linux/AMD64 and Linux/ARM64 images. The chart uses shared ReadWriteOnce volumes.

With `sregym.enabled=true`, the chart provides an operator toolbox and workload hooks instead of the original agent harness.
SREGym supplies observability and grading.
See the [integration guide](https://github.com/SREGym/SREGym/blob/main/docs/incident-arena.md) for supported execution and evaluation.

## Provenance

The chart comes from [Incident Arena](https://github.com/abundant-ai/incident-arena)
commit `fba011e451653c4058c2dc80a97c5d260c036872`, task `006--saleor-spine--10-T1-statement-timeout-canary-c7dcd6d4`.
Its original source is [sre-world](https://github.com/abundant-ai/sre-world), under `substrates/saleor/chart`.
The source uses Apache-2.0. See [LICENSE](LICENSE).

SREGym keeps the task answer keys and reference configuration outside the chart.
