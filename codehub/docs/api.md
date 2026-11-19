# CodeHub API

CodeHub supports organizations, membership, projects, issues, comments, change
requests, reviews, Git repositories, source bundles, webhooks and eventual search.
The supported build profile validates Python source syntax and packages the
actual commit tree. It does not execute arbitrary user scripts or install
dependencies, and does not implement a complete hosted CI product.

HTTP JSON APIs use `Authorization: Bearer <user-api-token>`. Identity provisioning
uses a separate deployment credential. Tokens are opaque client-generated
secrets; only their SHA256 digests are stored in SQL. Choose at least 32 random
bytes and never put credentials in repository content or request logs.

## Identity and authorization

`POST /v1/identity/users`, authenticated with the provisioning credential:

```json
{"user_id":"00000000-0000-4000-8000-000000000001","username":"morgan","api_token":"<random-secret>"}
```

Repeating the same identity, username and token is idempotent. Reusing an
identity with different credentials or an existing username/token is rejected.
Provisioning retries every configured database group and only reports success
after all groups contain the requested identity. A partial outage can leave an
incomplete operation; repeat the identical request after restoring connectivity.

Organizations have owner/admin/developer/viewer memberships. Every authenticated
entity read requires membership. Writes require developer/admin/owner; membership
and webhook administration require admin/owner. Only an owner grants ownership.
The last owner cannot be demoted. Reviewers cannot approve their own changes.
Git push requires write permission, and fetch requires read permission.

## Durable operations

`POST /v1/operations` accepts the following envelope. All identities are canonical
lowercase UUID strings; `project_id` is null only for organization/membership
operations. The authenticated credential supplies the actor; clients cannot
override it.

```json
{
  "event_id":"00000000-0000-4000-8000-000000000002",
  "tenant_id":"00000000-0000-4000-8000-000000000003",
  "entity_id":"00000000-0000-4000-8000-000000000004",
  "project_id":"00000000-0000-4000-8000-000000000005",
  "client_revision":1,
  "kind":"issue.create",
  "payload":{"title":"Connection timeout","body":"Bound retry concurrency","state":"open"}
}
```

The client assigns an event UUID and monotonically increasing revision for each
record. One logical client writer owns a record. Create/new record operations
start at revision one. Updates contain the complete new public document, not a
patch. Lower revisions can be accepted as historical operations without replacing
a newer current record. Event reuse with changed content or identity, or revision
reuse with a different event, returns HTTP 409. Keep an event ID and payload
unchanged when resolving a timeout by retrying.

Success acknowledges one transaction containing the immutable operation journal,
current record when applicable, typed relational data and all applicable durable
outbox work. Asynchronous search/build/delivery completion happens afterward.
The response contains the operation, record key, authenticated actor, region,
operation digest and `replayed` indicator. The digest is SHA256 of UTF-8 JSON of
the envelope plus `actor_id`, with sorted keys, compact separators, unescaped
Unicode and no nonfinite numbers.

| Kind | Payload | Identity and relationship rules |
| --- | --- | --- |
| `organization.create` | `slug`, `name` | Entity ID equals tenant ID; authenticated actor becomes owner. |
| `membership.set` | `user_id`, `role` | Entity ID equals member user ID; user must already exist. |
| `project.create`, `project.update` | `slug`, `name`, `default_ref` | Entity ID equals project ID; slug unique within organization. |
| `issue.create`, `issue.update` | `title`, `body`, `state` | Existing project; state `open` or `closed`; body defaults empty. |
| `comment.create` | `issue_id`, `body` | Existing issue in the same project. |
| `change.create`, `change.update` | `title`, `head_sha`, `base_sha`, `head_ref`, `base_ref`, `state` | Actual repository commits/branch refs; state `open`, `closed`, `merged`. |
| `review.create` | `change_id`, `head_sha`, `verdict`, `body` | Open change in the same project; exact current head; verdict `approve`, `request_changes`, `comment`. |
| `repository.push` | `ref`, `commit_sha` | Actual pushed branch; independent stable entity ID for each branch. |
| `webhook.create`, `webhook.update` | `url`, `events`, `enabled` | HTTP(S) URL, subscribed operation kinds, boolean enabled (default true). |

Slugs use lowercase letters, digits and hyphens. Titles/names have at most 200
characters, issue/comment/review bodies at most 16,000. Unsupported envelope or
payload fields are rejected. A payload has at most 128 KiB of canonical JSON.
Project association and entity type never change. Update preserves the original
author. A merged change cannot be reopened. For merged state, the actual base
branch must equal `base_sha` and contain `head_sha`; the client performs the real
Git merge and push before recording that state.

## Customer Git and build workflow

1. Create an organization, memberships and project through authenticated operations.
2. Push actual commits using smart HTTP to `<repository-base>/git/<project-id>`.
   Use any HTTP Basic username and the user API token as the password.
3. Record `repository.push` with the branch ref and full 40-character commit SHA.
4. Push a feature branch, then create a change with actual head/base refs and SHAs.
5. A different authorized user records a review against the current head.
6. Merge and push through Git, then update the change with merged state/current base.
7. Fetch/clone Git normally, query builds and download the generated artifact.

Repository smart HTTP supports upload-pack/receive-pack and input up to 64 MiB.
Builds use `git archive` of the exact commit, reject unsafe paths/links, validate
`.py` files with the Python parser and generate a deterministic ZIP. The ZIP
contains original files and `CODEHUB-BUILD.json` with project/commit/builder and
per-file SHA256 provenance. Maximum build input is 32 MiB, 10,000 files and 2 MiB
per file. Invalid source/build inputs remain durable failed work for operational
inspection/retry; there is no arbitrary shell execution.

## Reads, search and effects

| Method/path | Result |
| --- | --- |
| `GET /v1/tenants/{tenant}/entities/{entity}` | Current typed document and revision. |
| `POST /v1/tenants/{tenant}/entities/lookup` | Bounded lookup of current documents by entity IDs. |
| `GET /v1/tenants/{tenant}/entities` | UUID-ordered pagination; `project_id`, `entity_type`, `after`, `limit` (max 1000). |
| `GET /v1/tenants/{tenant}/events` | Complete public accepted history, UUID-ordered `after` pagination (max 1000). |
| `GET /v1/tenants/{tenant}/search?q=...` | Tenant-filtered SQLite FTS query over projected text. |
| `GET /v1/projects/{project}/builds` | Most recent 100 build results with commit and artifact provenance. |
| `GET /v1/projects/{project}/artifacts/{sha256}` | Authorized artifact download with checksum ETag. |

UUID order is not acceptance-time order. For a changing dataset, paginate a
stable interval or repeat reads; random UUID insertion can precede a cursor.
Normal entity reads use the configured local read endpoint and can lag writes.
The event history uses the writer. Search is an independent asynchronous
projection and may temporarily lag either path.

Batch lookup accepts exactly `{"ids":["<entity-uuid>","<another-entity-uuid>"]}`
with 1..128 distinct canonical UUIDs and an encoded request body of at most 8 KiB.
It requires the same customer credential and organization read membership as a
single entity read, and uses that tenant's configured database reader. The result
is `{"items":[...]}` with the existing single-entity document shape, ordered by
entity UUID. IDs that do not exist in the organization are omitted, including
IDs belonging to another organization. No other response fields are added.
Each encoded stored document is limited to 256 KiB; exceeding this capacity
returns an error rather than a partial result. This includes room for metadata
added to the supported 128 KiB operation payloads.

Search services also expose
`POST /internal/tenants/{tenant}/entities/lookup` with the same request limits.
This internal endpoint requires the deployment's service credential and returns
`{"items":[...]}` using the existing search document shape. Missing projected
IDs are omitted. Search projections remain eventually consistent.

Project/issue/comment/change/review/repository operations create search work.
Repository pushes and change creations/updates create build work. Matching
enabled webhooks create delivery work. The committed business identity for each
effect is SHA256 of `<event-id>/<effect-kind>/<destination>`, with destination
entity ID for search, project ID for build and subscription ID for delivery.

Webhook HTTP requests contain `{effect_id,event_id,operation}`, use
`Idempotency-Key: <effect-id>` and `X-CodeHub-Event: <event-id>`. Receivers must
deduplicate the key while checking identical content. Network attempts can repeat
after a timeout or crash. HTTP 2xx completes a delivery attempt; redirects and
unlisted hosts are rejected. Content-sensitive receiver receipts remain the
receiver's responsibility, separate from CodeHub's local delivery records.

HTTP 403 indicates authorization/maintenance rejection, 404 an absent relationship,
409 an identity/revision/domain conflict, 422 invalid semantics and 503 unavailable
database/dependent services. Health/readiness endpoints describe process and
storage availability, not business-data completeness.
