# CodeHub operations

CodeHub is a deliberately bounded source-collaboration application. It stores
real relational customer data, immutable accepted operations, actual Git objects,
durable asynchronous work, search projections, build bundles and webhook attempts.
It supports MySQL 8.4. Authentication is opaque per-user API tokens with organization
roles; enterprise SSO, arbitrary shell CI, large-file extensions and full hosted
Git product features are outside this version's supported surface.

## Processes and startup

Each HTTP process listens on port 8080 by default (`PORT` overrides it):

```text
python -m codehub.api
python -m codehub.repository
python -m codehub.search
python -m codehub.delivery
python -m codehub.worker
```

Apply the explicit idempotent schema migration against each writable database
group before expecting readiness:

```text
python -m codehub.migrate /app/migrations/mysql/001_initial.sql
```

Process startup never drops/resets data or runs destructive schema migrations.
API/repository `/healthz` stays independent of database schema/availability;
`/readyz` requires SQL connectivity and schema. Search/delivery readiness tests
their actual durable SQLite store. Worker liveness reads the age of
`<data-dir>/worker-heartbeat`; a ready process is not evidence that its backlog
has drained. Keep a separate alert for pending age and failed attempts.

## Configuration

All application roles use the same locked Python package. Mount secrets as files
and set the corresponding `_FILE` variable; environment-value fallback is available
for local operation. Do not bake secrets into images.

| Configuration | Meaning |
| --- | --- |
| `CODEHUB_REGION` | Ordinary logical region identity, e.g. `region-a`. |
| `CODEHUB_MYSQL_HOST`, `_PORT`, `_DATABASE`, `_USER`, `_PASSWORD_FILE` | Writer connection (defaults host `mysql`, port 3306, database/user `codehub`). |
| `CODEHUB_MYSQL_READ_HOST`, `_READ_PORT` | Optional local entity-read connection. |
| `CODEHUB_DATABASE_ROUTES_FILE` | Inspectable JSON tenant-to-group routes; wildcard `*` fallback. |
| `CODEHUB_BOOTSTRAP_TOKEN_FILE` | API identity-provisioning credential. |
| `CODEHUB_SERVICE_TOKEN_FILE` | Internal HTTP role credential, separate from user/provisioning credentials. |
| `CODEHUB_REPOSITORY_URL` | Regional repository HTTP base URL. |
| `CODEHUB_REPOSITORY_PEERS` | Comma-separated ordinary regional peer base URLs for asynchronous Git replication. |
| `CODEHUB_SEARCH_URLS` | Comma-separated complete regional search replica inventory; every worker updates every declared copy before completion. API queries the first configured endpoint. |
| `CODEHUB_DELIVERY_URL` | Regional artifact/delivery HTTP base URL. |
| `CODEHUB_DELIVERY_URLS` | Comma-separated complete regional artifact-store inventory; every build publishes identical content to every declared copy before completion. Defaults to the local delivery URL for a single-region deployment. |
| `CODEHUB_BROKER_URL_FILE` | RabbitMQ AMQP connection URL with credentials. |
| `CODEHUB_QUEUE` | Regional durable quorum queue name, default `work`. |
| `CODEHUB_WEBHOOK_HOSTS` | Explicit comma-separated destination-host allowlist; default denies all. |
| `CODEHUB_DATA_DIR` | Durable role directory, default `/var/lib/codehub`. |
| `CODEHUB_READ_ONLY=1` | API-instance maintenance rejection of new operations. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | HTTP OTLP collector base URL, conventionally port 4318. |
| `CODEHUB_SERVICE_NAME` | Telemetry service name, defaults `codehub`. |

Example normal routing file (password paths are mounted per-role credentials):

```json
{
  "*":{"group":"group-0","writer_host":"mysql-writer","reader_host":"mysql-reader","port":3306,"database":"codehub","user":"codehub","password_file":"/run/secrets/mysql-password"}
}
```

Add exact tenant UUID entries to route separate groups. A group must have one
consistent route. Route files are reread for requests/work iterations, so normal
mounted-file updates take effect without embedding routing in accepted records.
Credential provisioning writes identical users into all configured groups;
customer operation transactions remain within one tenant's group. There are no
cross-group customer foreign keys or distributed SQL transactions.

## Persistence and durability

Use independent persistent regional Git, artifact/delivery and search volumes.
SQLite stores use WAL and `synchronous=FULL`; artifact publication checks content
hash and fsyncs the temporary file before atomic rename. Retain MySQL data and
binary logs independently; use GTIDs, unique server IDs, `sync_binlog=1` and
`innodb_flush_log_at_trx_commit=1`. Local durable acceptance does not imply a
synchronous regional replica or resilience to a shared physical host outage.

`journal` holds the original operation/actor and canonical digest, with unique
event ID and record/revision. `entities` holds the greatest accepted revision;
typed tables enforce ownership and relationships. `outbox` records event/effect/
destination plus durable retry state. `builds` and `artifacts` link actual commit
and artifact checksums. Never infer business completeness from a queue length,
outbox flag or healthy database process alone.

The publisher uses persistent Rabbit messages, quorum queues and publisher
confirms. A confirm updates SQL publication state; SQL work remains republishable
until completion. Consumers acknowledge only after the actual effect succeeds,
or after its failed-attempt/backoff state is durably retained in SQL. Delivery
attempts can repeat with the same idempotency key. Required work has no TTL or
overflow discard. Transient failures back off from a few seconds to five minutes;
the failed item remains inspectable and retryable, without a silent drop limit.

Search independently stores processed effect identities and greatest revision.
Older events remain processed history and cannot roll back a newer projection.
Builds consume actual Git archives and publish immutable content-addressed bundles.
Delivery records include requested body digest, destination, attempt count and
successful receiver status/response digest. These are operational records, not a
substitute for validating receiver business outcomes.

Git replication fetches actual objects/refs from configured peers into distinct
regional storage. The repository service credential permits fetch only. Published
branch refs follow SQL repository metadata, not wall-clock comparison of peers.
An unavailable peer or absent object creates real delayed replication/build work;
logs report the affected project without credentials or business payloads.

## Operational access and observability

Use a separate migration/administration account from the application account.
Grant the application only required database/volume/network access. Internal
HTTP credentials authorize ordinary workload services; they are not host or
cluster-administration credentials. Repository peer synchronization never accepts
service-token push. Expose public endpoints through your configured TLS gateway;
internal role addresses use the deployment's normal controlled service network.

HTTP roles expose `/metrics` for request counts and total latency and emit
structured region/method/route/status/duration logs, excluding tokens and bodies.
When HTTP OTLP is configured they export real request spans and latency metrics.
Observe SQL replication/role status, Rabbit quorum/queue age, pod CPU/memory/disk,
Git transfer lag and effect processing separately. Use resource limits, bounded
retention, request rates and storage alarms appropriate to the deployed workload.

Back up SQL with its consistent journal/typed/outbox state; retain necessary
binlog history and actual regional Git/artifact stores. Test restore and replica
replay using the documented API/Git journeys. Startup/readiness are not restore
validation. Any reconciliation must preserve accepted event identities and public
revision semantics, valid relationships, Git reachability and durable pending work.
