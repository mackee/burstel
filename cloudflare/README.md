# burstel Cloudflare layer — Worker + ShotDO + SQLite IngestDO + R2

This directory contains the Phase 2 Cloudflare layer for **burstel**. It builds
on the Phase 1 Go recorder (`../cmd/burstel`, `../Dockerfile`); it does **not**
replace it.

```
                 public HTTPS                       Worker           coordinator / per-shot DO
 exporter ──► POST /ingest/v1/{traces,metrics,logs} ─► token→instance ─► IngestDO ─► R2 raw journal + ledger (durable ACK)
 rec client ─► POST /api/shots            ──► admin|member ────► IngestDO (register shot) + ShotDO
 rec client ─► POST /api/instances/{n}/tokens ─► admin|member ► IngestDO (issue/rotate 24h token)
 rec client ─► GET  /api/shots            ──► admin|member ────► IngestDO (shot registry, ?owner= filter)
 rec client ─► POST /api/shots/{id}/stop  ──► admin|member ────► ShotDO ─► GET /internal/stop
 rec client ─► POST /api/tokens           ──► admin ──────────► AuthDO (digest + scope + owner)
                                                                  │        GET /internal/artifact
                                                                  ▼
                                                        R2Bucket.put (streaming)
                                                        shots/{id}/telemetry.duckdb
                                                        shots/{id}/metadata.json
```

* `src/index.ts` — Worker entrypoint.
* `src/app.ts` — public HTTP routes + principal (admin/member/push) auth (whitelist only).
* `src/authz.ts` / `src/auth-core.ts` — principal resolution and the pure token registry (scopes, digests, revocation).
* `src/auth-do.ts` — singleton SQLite `AuthDO` holding the per-person token registry (digest + scope + attribution-only owner).
* `src/shot-do.ts` — `ShotDO extends DurableObject`, wires `ctx.storage.sql`,
  `ctx.container` and the `BUCKET` R2 binding into the core.

## Public HTTP API

Three credential scopes exist after Phase 13:

* **Admin scope** — `BURSTEL_API_TOKEN` (legacy, resolved in the Worker) or a
  registry token minted with `scope: "admin"`: every `/api/*` route including the
  token registry, `/mcp`, and the legacy per-shot ingest path.
* **Member scope** — a registry token minted with `scope: "member"`: the full
  *operational* capability set (create/stop shots, manage operational *instance*
  credentials, list/get/query/analyze/schema/compare) but **not** the *person*
  token registry. There is no per-owner isolation; `owner` is attribution only.
* **Push scope** — a 256-bit, admin/member-minted push-only *instance* token
  (valid exactly 24h from issue): `POST /ingest/v1/{traces,metrics,logs}` only,
  routed to a named instance and never to a shot. It is never accepted anywhere
  else and the admin/member tokens are never accepted for `/ingest`.

`GET /healthz` is unauthenticated.

| Method | Path | Scope | Description |
| --- | --- | --- | --- |
| `GET` | `/healthz` | public | Worker reachability |
| `POST` | `/api/tokens` | admin | Mint a per-person token (`scope` admin\|member, attribution-only `owner`); value shown once |
| `GET` | `/api/tokens` | admin | List token metadata (id/scope/owner/timestamps/revoked); never values |
| `DELETE` | `/api/tokens/{id}` | admin | Revoke a per-person token immediately (idempotent) |
| `POST` | `/api/instances` | admin\|member | Create/upsert a named instance; returns the immutable `instance_id` |
| `GET` | `/api/instances` | admin\|member | List instance metadata/status (never secrets) |
| `POST` | `/api/instances/{name}/tokens` | admin\|member | Issue a 256-bit 24h push-only token (shown once); `{"rotate":true,"overlap_seconds":N}` rotates |
| `GET` | `/api/instances/{name}/tokens` | admin\|member | List token metadata (id/timestamps/revoked); never values |
| `DELETE` | `/api/instances/{name}/tokens/{id}` | admin\|member | Revoke one instance token immediately |
| `POST` | `/api/ingest-token` | authenticated | **Retired** (spec 014). Always `410 Gone`; use a named instance token. |
| `POST` | `/api/shots` | admin\|member | Create a recording shot. Optional `owner` (attribution only). |
| `GET` | `/api/shots` | admin\|member | List shots newest first; optional `?owner=` filter and `?limit=`/`?offset=` pagination. |
| `GET` | `/api/shots/{id}` | admin\|member | Lifecycle / metadata / counters |
| `POST` | `/api/shots/{id}/ingest-token` | admin\|member | **Retired** (spec 014). Always `410 Gone`; use an instance token. |
| `POST` | `/api/shots/{id}/stop` | admin\|member | Idempotent per-shot freeze |
| `POST` | `/api/shots/{id}/query` | admin\|member | Constrained read-only SQL against the frozen snapshot |
| `POST` | `/api/shots/{id}/analyze` | admin\|member | SQL-backed analytical summaries |
| `GET` | `/api/shots/{id}/schema` | admin\|member | Documented queryable schema + safety policy |
| `POST` | `/api/shots/{id}/compare` | admin\|member | SQL-derived diff with another frozen shot (`{"other":"<id>"}`) |
| `POST` | `/mcp` | admin\|member | Remote MCP (Streamable HTTP) endpoint |
| `POST` | `/ingest/v1/traces` | instance | Continuous OTLP/HTTP ingest for the token's instance (durable R2 journal) |
| `POST` | `/ingest/v1/metrics` | instance | Continuous OTLP/HTTP ingest for the token's instance (durable R2 journal) |
| `POST` | `/ingest/v1/logs` | instance | Continuous OTLP/HTTP ingest for the token's instance (durable R2 journal) |
| `POST` | `/shots/{id}/v1/{traces,metrics,logs}` | admin\|member | **Deprecated** legacy per-shot path, kept for compatibility |

`POST /api/shots` and `GET /api/shots/{id}` return the stable `otlp_base_url`
plus explicit `otlp_endpoints` for the three signals, e.g.
`https://<host>/ingest/v1/traces`. The endpoint is identical for every instance:
one **instance token** (configured once, valid 24h) feeds continuous telemetry
regardless of shots. The deprecated per-shot admin paths are still advertised as
`legacy_otlp_base_url` / `legacy_otlp_endpoints` and continue to work with the
admin token.

The Go recorder's `/internal/*`, artifact and query endpoints are **never**
exposed. The Worker only forwards the whitelisted paths above to a Durable
Object, which reaches the container over its private TCP port.

### Authentication notes

* Tokens are read from `Authorization: Bearer …` first, then
  `X-Burstel-Token`. OTLP exporters can set either via
  `OTEL_EXPORTER_OTLP_HEADERS`.
* Tokens are compared in constant time, never logged, and never forwarded to
  the container. The container separately receives a **distinct**
  `BURSTEL_INTERNAL_TOKEN` so its `/internal/*` endpoints are also gated. The
  admin token is **never** used as the internal token: production requires a
  distinct, non-empty `BURSTEL_INTERNAL_TOKEN` and the DO fails closed
  (`500 internal_token_missing` / `internal_token_not_distinct`) rather than
  booting an unguarded container.
* Admin authentication **fails closed** when neither `BURSTEL_API_TOKEN` nor a
  matching registry token is presented. The only bypass is the explicit
  local-development flag `BURSTEL_DEV_INSECURE_AUTH=1` (or `true`/`yes`), which
  only applies when `BURSTEL_API_TOKEN` is unset and must never be set in
  production. There are no hardcoded credential defaults.
* Per-person registry tokens are stored only as SHA-256 digests in `AuthDO`,
  compared in constant time and never returned by `GET /api/tokens`. A revoked
  or expired token fails immediately (`401`).

## Continuous instance ingest and the durable raw journal (spec 014)

Ingestion is decoupled from shots. One named **instance** (`web-1`, `db-1`, …)
has a 256-bit push-only token valid for exactly **24h from issue**; the exporter
is configured once for the day and keeps sending across arbitrary shot events and
idle periods:

```
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_ENDPOINT=https://<host>/ingest
OTEL_EXPORTER_OTLP_HEADERS=X-Burstel-Token=<this instance's token>
```

```bash
# Create the instance (idempotent) and issue its token once.
curl -fsS -X POST https://<host>/api/instances \
  -H "Authorization: Bearer $BURSTEL_API_TOKEN" -d '{"name":"web-1"}'
curl -fsS -X POST https://<host>/api/instances/web-1/tokens \
  -H "Authorization: Bearer $BURSTEL_API_TOKEN"
# -> {"token":"bit_...","id":"...","instance_id":"...","issued_at":"...","expires_at":"...","ttl_seconds":86400}
```

* **Token → instance, never → shot.** A token is bound to exactly one named
  instance and resolves to its opaque `instance_id`; ingestion is never
  redirected, attached, discarded or switched at shot boundaries. Several named
  instances ingest simultaneously and independently.
* **Exactly 24h.** A token is accepted iff `now < expires_at` where
  `expires_at = issued_at + 86_400s`; at `expires_at` and later it is `401`.
  Expiry survives Worker/DO eviction (it is persisted) and is never extended by
  shot start/stop, queries or telemetry.
* **Durable ACK BEFORE 200 (hard requirement).** For every accepted export the
  Worker (1) authenticates the instance token, (2) validates the body, (3) writes
  the original, unmodified bytes to a **unique** R2 object under
  `journal/{instance_id}/{signal}/{received_at}-{id}.bin`, (4) commits a durable
  ledger row in the `IngestDO`, and only then (5) returns a valid OTLP
  `Export*ServiceResponse`. No runtime Container is involved in acknowledging
  intake.
* **Fail closed, never 200-for-discard.** If the R2 write or the ledger commit
  fails, the request returns a retryable `5xx` so the exporter retries. Distinct
  equal payloads are never merged. Duplicate delivery under upstream retry is
  bounded and documented: each accepted export is an immutable object and the
  ledger is idempotent on the object key. Reconciliation recovers an object whose
  ledger row was lost.
* **Trusted identity.** The server assigns immutable `burstel.instance_id` and
  `burstel.instance` as trusted *separate* fields (R2 metadata + ledger); the
  original OTLP bytes, encoding, signal and `request-received_at` are preserved
  and user OTel resource labels are never trusted or overwritten.
* **Validation.** Only `application/x-protobuf` and `application/json` are
  accepted; an optional `gzip` content-encoding is verified by decompressing.
  Malformed protobuf/gzip/JSON and empty bodies are rejected with `4xx` instead
  of being acknowledged. `MAX_INGEST_BYTES` (default 8 MiB) bounds the body.
* **Revocation and rotation.** `DELETE /api/instances/{name}/tokens/{id}` revokes
  a token immediately. `POST .../tokens` with `{"rotate":true,"overlap_seconds":N}`
  revokes the instance's previous active token(s) with a bounded grace (capped at
  1h) so an exporter can switch over; **no other instance is touched**.
* **Scope separation.** An instance token cannot call `/api/*`, `/mcp` or the
  legacy `/shots/*` path, and the admin/member credential cannot call `/ingest`.
  Failed authorization is distinguished (missing/invalid/expired/revoked).
* **No recording cap.** Spec 014 removed the application-level
  `MAX_CONCURRENT_RECORDING` rule; shots record concurrently, bounded only by the
  explicit backend container `max_instances` control.


## Shared multi-user access (spec 013, Part B)

A deployment can be shared by several people, each with their **own revocable
token**, without giving anyone the env admin secret. A new SQLite-backed
`AuthDO` stores only the SHA-256 **digest** of each token plus its scope, an
attribution-only owner label and timestamps.

```bash
# Admin mints a per-person token; the value is shown exactly once.
curl -fsS -X POST https://<host>/api/tokens \
  -H "Authorization: Bearer $BURSTEL_API_TOKEN" \
  -d '{"scope":"member","owner":"alice"}'          # -> {"token":"bta_...","id":"...","scope":"member",...}

# List metadata (id/scope/owner/timestamps/revoked); values are never returned.
curl -fsS https://<host>/api/tokens -H "Authorization: Bearer $BURSTEL_API_TOKEN"

# Revoke one person immediately, without rotating everyone else.
curl -fsS -X DELETE https://<host>/api/tokens/<id> -H "Authorization: Bearer $BURSTEL_API_TOKEN"
```

* **Scopes.** `admin` may manage the person-token registry and do everything;
  `member` has the full *operational* capability set (create/stop shots, manage
  operational **instance** credentials, list/get/query/analyze/schema/compare)
  but is denied `POST`/`GET`/`DELETE /api/tokens` and the MCP registry tools with
  `403`. The scope check happens before any DO work, so a denied call starts no
  container and mutates nothing.
* **No ownership isolation.** `owner` is attribution/audit only (plus the
  optional `GET /api/shots?owner=` convenience filter); it is never an
  authorization boundary. Any operational token may act on any shot.
* **Deny by default.** Unknown, revoked or expired tokens fail closed with
  `401`. Comparison against stored digests is constant time. The env
  `BURSTEL_API_TOKEN` remains the backward-compatible admin credential and is
  resolved in the Worker, never stored in the registry.
* **Instance tokens stay push-only.** An `/ingest` instance token is a distinct
  credential; it is rejected on `/api/*` and `/mcp`, and the admin/member tokens
  are rejected on `/ingest`.
* **MCP.** `/mcp` accepts any admin/member token. Both scopes get the full
  operational tool set (`shots_list`, `shot_start`, `shot_stop`, `shot_get`,
  `shot_query`, `shot_analyze`, `shot_schema`, `shot_compare` plus the instance
  credential tools `instance_create`, `instance_list`, `instance_token_issue`,
  `instance_token_list`, `instance_token_revoke`); only `admin` may call
  `token_mint`/`token_list`/`token_revoke`.
* **Migration.** The installation-wide and per-shot ingest tokens are gone:
  `POST`/`DELETE` `/api/ingest-token` and `POST /api/shots/{id}/ingest-token`
  return `410 Gone` pointing at named instances. Existing frozen R2 shot archives
  (`shots/…`) and the old analyzer are untouched; the raw journal lives under
  `journal/…`.

## Phase 3 — MCP analysis and comparison

The defining promise is **post-run analysis without the original recording
Container alive**. After `shot_stop`, the only copy of a shot is
`shots/{id}/telemetry.duckdb` in R2. Analysis never touches the recording
container:

```
MCP / REST ──► ShotDO (frozen)
                 │  start a *fresh* analysis container (same image, read-only role)
                 │  stream shots/{id}/telemetry.duckdb from R2 ──► POST /internal/restore
                 │  verify size + SHA-256, open DuckDB read-only
                 │  POST /internal/query | /internal/analyze | /internal/compare
                 ▼  bounded typed rows / summaries
```

The restored snapshot lives in the container's ephemeral disk and is torn down
when the container is idle (or on demand via `/internal/analysis/release`). R2
is always authoritative; the frozen object is never mutated.

### Remote MCP tools

`POST /mcp` speaks MCP over Streamable HTTP with JSON responses, implemented
with the maintained `@modelcontextprotocol/sdk`. The endpoint accepts the env
admin credential or any registered `admin`/`member` token, and each tool is a
thin wrapper over the same ShotDO/IngestDO boundaries, so MCP callers cannot
bypass REST authentication.

| Tool | Scope | Arguments | Result |
| --- | --- | --- | --- |
| `shots_list` | admin\|member | `owner?`, `limit?`, `offset?` | Shots newest first with state, owner, timings, size/SHA-256, counters |
| `shot_start` | admin\|member | `metadata?`, `owner?` | New shot id + stable OTLP endpoints |
| `shot_stop` | admin\|member | `shot_id` | Frozen state, size, SHA-256 |
| `shot_get` | admin\|member | `shot_id` | Lifecycle, metadata, counters, freeze info |
| `shot_query` | admin\|member | `shot_id`, `sql`, `max_rows?` | Bounded typed rows |
| `shot_analyze` | admin\|member | `shot_id`, `sections?`, `top_n?` | Latency p50/p95/p99, slow spans, errors, DB ops, log correlation, metrics |
| `shot_compare` | admin\|member | `left_shot_id`, `right_shot_id`, `top_n?` | SQL-derived deltas + sample sizes + disclosure |
| `shot_schema` | admin\|member | `shot_id` | Documented tables/columns/templates + safety policy |
| `instance_create` | admin\|member | `name` | Immutable instance metadata |
| `instance_list` | admin\|member | — | Instance metadata + token counts (never secrets) |
| `instance_token_issue` | admin\|member | `name`, `rotate?`, `overlap_seconds?` | New 24h push-only token (shown once) |
| `instance_token_list` | admin\|member | `name` | Token metadata (never values) |
| `instance_token_revoke` | admin\|member | `name`, `id` | Immediately revokes that instance token |
| `token_mint` | admin | `scope`, `owner?`, `expires_at?`, `ttl_seconds?` | New per-person token (shown once) |
| `token_list` | admin | — | Token metadata (never values) |
| `token_revoke` | admin | `id` | Immediately revokes a token |

A shot id is a capability: possession of the id (plus an operational token)
grants access to that shot. Tool errors are returned as MCP tool errors
(`isError: true`), not protocol errors, so a model can recover; a `member`
calling a registry tool gets a `403` tool error and no registry work happens.

### Query safety

DuckDB's `access_mode=read_only` is **not** sufficient on its own: a read-only
connection can still call `read_csv`/`read_parquet`/`glob`/`httpfs`, inspect
credentials and exhaust memory. Every query is therefore validated in the Go
analysis layer (where DuckDB actually runs) before execution:

* exactly one statement, and it must be `SELECT` or `WITH ... SELECT`;
* a table allowlist (`schema_meta`, `otlp_batches`, `spans`, `log_records`,
  `metric_points`); derived tables and `information_schema` are rejected;
* a function allowlist — unknown functions are rejected, which blocks file,
  network, extension and settings functions;
* a hard outer row limit, a query timeout, a memory limit, and a result/value
  byte budget;
* external access is additionally disabled at the DuckDB connection level.

See [`docs/adr-001-analysis-query-safety.md`](./docs/adr-001-analysis-query-safety.md)
for the full decision record.

## Lifecycle

```
created ──start──► recording ──stop──► stopping ──verified R2──► frozen
                       │           └── any error ──► failed (retryable)
                       └── container stopped ──► failed (lost, terminal)
```

* **Create** boots the Container (default scheduling policy), waits for
  `GET /readyz`, then calls the Go recorder's `POST /internal/start`. Only after
  the recorder accepts does the shot become `recording`.
* **Ingest** is admitted only while `recording`. `stop` sets `stopRequested`
  synchronously, so a request racing the stop gets `503` instead of being
  silently dropped. In-flight ingests are counted and drained before the
  recorder is told to stop.
* **Stop** marks `stopping`, drains in-flight requests, calls
  `POST /internal/stop`, streams `GET /internal/artifact` into R2, validates the
  stored size/checksum, writes `shots/{id}/metadata.json`, and only then marks
  `frozen` and destroys the Container.
* `created`, `recording`, `stopping`, `frozen`, and `failed` are persisted in
  the DO's SQLite storage, so a rehydrated DO restores its metadata and state.

### Keeping the recording container alive (spec 012)

* The platform stops a DO's container shortly after the DO goes idle unless an
  inactivity timeout is set, and forgets that timeout on every DO restart.
  Create therefore applies a 6h timeout (the platform maximum) **after**
  `/readyz` and before `recording`; if the platform refuses it the start fails
  (`503 container_keepalive`, retryable).
* While `recording`, a DO alarm fires every 60s. It re-applies the timeout,
  keeps the DO active and checks that the container still runs. A restarted DO
  re-applies the timeout and re-arms the alarm from its constructor. The
  heartbeat stops re-arming after 24h in `recording` so a forgotten shot cannot
  pin a container forever.
* **Container loss is terminal.** A stopped container is never restarted with
  its old disk, so when a recording shot's container is found stopped (ingest,
  status, stop, heartbeat or DO load), the shot becomes `failed` with
  `{phase:"recording", lost:true, retryable:false}`. Ingest then answers
  `409 shot_lost`, which the stable `/ingest` turns into an OTLP `200` discard
  labelled `X-Burstel-Disposition: ignored-shot-lost` (so exporters stop
  retrying), and `stop` answers `409 shot_lost` without touching any container
  and frees the stable endpoint for the next shot.
* Analysis containers use a 10 min timeout.

### Freeze durability and crash/retry semantics

* The Container disk is **ephemeral**. The only authoritative copy lives there
  until a verified R2 object exists, so a failed freeze **never destroys the
  container**.
* A failed freeze marks the shot `failed` with a retryable `{phase:"stop"}`
  record. Calling `POST /api/shots/{id}/stop` again retries the freeze; the
  artifact PUT is idempotent (same key) and multipart uploads are aborted on
  failure.
* A crash between "R2 object written" and "state persisted as frozen" is safe:
  the next stop re-uploads to the same key and re-derives the same size/checksum.
* `shots/{id}/metadata.json` carries a `freeze` object with
  `freeze_version` (currently `1`, distinct from the DuckDB `schema_version`).
  Readers must ignore unknown fields so new keys can be added without migration.

### Size limits

* Artifacts up to ~5 GiB are streamed with a single `R2Bucket.put`; the Worker
  never buffers the database in memory.
* Larger artifacts use a bounded multipart upload (64 MiB parts, one part in
  memory at a time) and only mark frozen after `complete()` reports the expected
  size.
* The account's concurrent Container-instance limit is reported to callers as a
  `503` with `retryable: true` (`code: "capacity"`).

## Local development and verification

Requirements: [Bun](https://bun.sh) for tests, Node.js for Wrangler, Docker for
building the container image.

```bash
bun install
bun run typecheck          # tsc for src (Worker/DO + MCP)
bun run typecheck:test     # tsc for the pure core + tests
bun test                   # lifecycle, ingest token, auth, R2, container, analysis, MCP
bun run dev                # wrangler dev (needs a local Docker/container engine)
bun run deploy:dry-run     # bundling dry-run, skips the container image build
```

For trusted local development only, `wrangler dev` can run without an admin
token by exporting `BURSTEL_DEV_INSECURE_AUTH=1` before `bun run dev`. In that
mode the DO also passes the same explicit opt-in to the container (never the
admin token) so the recorder's `/internal/*` routes are intentionally ungated.
This bypass is disabled by default and must never be set in production. See
[`docs/predeploy-auth-checklist.md`](./docs/predeploy-auth-checklist.md) for the
production secret requirements.

> **Docker/Container not verified here.** `wrangler deploy --dry-run` validates
> the Wrangler config and bundles the Worker, but this sandbox has no Docker
> daemon, so real Container scheduling, the injected `BURSTEL_INTERNAL_TOKEN`
> env and `/internal/*` gating are not exercised live.

### End-to-end quickstart (OTLP exporter -> start/stop/query/compare)

The Go analysis engine has a full local end-to-end test that needs no
Cloudflare account and no Docker:

```bash
go test ./internal/analysis/ -run TestFrozenShotRestoreQueryAnalyzeCompare -v
go test ./internal/server/  -run TestAnalysisHTTPEndToEnd -v
```

It records two synthetic benchmark shots with genuinely different latency and
error profiles, freezes them (closing the writer), then restores each from a
local R2-compatible directory, queries the first, compares both and asserts the
SQL-derived deltas. It also proves the raw OTLP bytes are unchanged.

Against a real `wrangler dev` (needs a container engine), the same flow is:

```bash
# 1. Create a named instance and issue a 24h push token (shown once).
curl -fsS -X POST https://<host>/api/instances \
  -H "Authorization: Bearer $TOKEN" -d '{"name":"web-1"}'
PUSH=$(curl -fsS -X POST https://<host>/api/instances/web-1/tokens \
  -H "Authorization: Bearer $TOKEN" | jq -r .token)

# 2. Configure the exporter once; ingestion is continuous and shot-independent.
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=https://<host>/ingest
export OTEL_EXPORTER_OTLP_HEADERS="X-Burstel-Token=$PUSH"

# 3. Start a shot around the benchmark window, then stop.
SHOT_ID=$(curl -fsS -X POST https://<host>/api/shots \
  -H "Authorization: Bearer $TOKEN" -d '{"owner":"alice","metadata":{"scenario":"bench"}}' | jq -r .shot_id)
# ... run the benchmark ...
curl -s -H "Authorization: Bearer $TOKEN" -X POST https://<host>/api/shots/$SHOT_ID/stop

# Shots may record concurrently (no application cap); list them with
#   curl -s -H "Authorization: Bearer $TOKEN" 'https://<host>/api/shots?owner=alice'

# 4. Query and analyze any frozen snapshot (no recording container is running).
curl -s -H "Authorization: Bearer $TOKEN" -X POST \
  -d '{"sql":"SELECT service_name, count(*) AS n FROM spans GROUP BY 1 ORDER BY n DESC"}' \
  https://<host>/api/shots/<id>/query

curl -s -H "Authorization: Bearer $TOKEN" -X POST -d '{"top_n":10}' \
  https://<host>/api/shots/<id>/analyze

# 5. Compare two shots.
curl -s -H "Authorization: Bearer $TOKEN" -X POST -d '{"other":"<id2>","top_n":10}' \
  https://<host>/api/shots/<id>/compare
```

MCP clients (for example ChatGPT connectors or the MCP Inspector) use the same
token against `https://<host>/mcp`; call `initialize`, then `tools/list`, then
`tools/call` with `{"name":"shot_query","arguments":{"shot_id":"...","sql":"..."}}`.

### What is verified without a Cloudflare account

* The full lifecycle state machine, idempotent stop, drain-vs-stop race,
  failure injection (R2 write failure cannot produce `frozen`), artifact length
  validation, and rehydration across a simulated DO restart — over real SQLite
  (`bun:sqlite`) and fakes.
* The continuous instance ingest flow (spec 014) with a **fake clock** and an
  in-memory R2 bucket: two named instances ingest traces/metrics/logs
  concurrently with **no shots** and across arbitrary shot events; issue is
  exactly 24h, `issue+23h59m59s` is accepted, `issue+24h` is `401`; immediate
  revocation and bounded, instance-scoped rotation; no cross-instance
  attribution; the raw bytes/encoding are preserved under
  `journal/{instance_id}/…`; the R2 write + ledger commit happen **before** the
  OTLP `200`; an R2 or ledger failure is a retryable `5xx`; and reconciliation
  recovers an unindexed object.
* Malformed protobuf/gzip/JSON and empty bodies are rejected with `4xx`, an
  oversized body with `413`, and only a lowercase SHA-256 **digest** — never the
  raw token — is persisted. Scope separation: an instance token is denied on
  `/api/*` and `/mcp`, and the admin/member credential is denied on `/ingest`.
* Concurrency without the application-level cap: many shots record and are
  listed at once; the retired `/api/ingest-token` and
  `/api/shots/{id}/ingest-token` return `410 Gone`; missing admin config fails
  closed and the explicit dev bypass works.
* IngestDO durability across a simulated DO restart (instance registry, token
  digests, expiry/revocation and the raw-journal ledger survive).
* Auth rejection, shot isolation, the legacy per-shot proxy response semantics,
  and route validation through the real Worker request handler.
* The shared multi-user registry (spec 013, Part B): the capability matrix over
  REST and MCP (a `member` token may operate on **any** shot and manage instance
  credentials but is denied the person-token registry with `403`; `admin` may
  mint/list/revoke), immediate revocation, optional expiry, digest-only
  persistence (never a value, never returned by list), and constant-time
  comparison.
* Single-PUT and multipart R2 streaming code paths against a fake R2 bucket.
* The real Go analysis engine over real DuckDB: read-only restore with
  size/SHA-256 verification, allowlisted SQL, p50/p95/p99 and error analysis,
  and a SQL-derived comparison — plus adversarial SQL rejection.
* The DO analysis flow over fakes: the frozen artifact is streamed from the
  shared R2-like bucket into a fresh analysis container, tampering is detected,
  and non-frozen/missing-artifact cases fail closed.
* MCP `initialize` / `tools/list` / `tools/call` envelopes, the full
  start/stop/query/analyze/compare tool flow and the admin-only registry tools
  through the official SDK.
* TypeScript typecheck and `wrangler deploy --dry-run` bundling (which lists the
  `ShotDO`, `IngestDO` and `AuthDO` bindings).

`wrangler deploy --dry-run` (without `--containers-rollout=none`) additionally
requires the Docker CLI because Wrangler must build the container image even for
a dry-run. This sandbox has no Docker daemon, so the dry-run used here is
`wrangler deploy --dry-run --containers-rollout=none`, which validates the
Wrangler configuration and bundles the Worker.

### Phase 9 — image build validation, preflight and smoke

The authoring sandbox has no Docker daemon, so the real `linux/amd64` image
build is validated by [`ci/github-actions/container-image.yml`](../ci/github-actions/container-image.yml)
(installed to `.github/workflows/` with `scripts/install-github-workflow.sh`; the managed
push App lacks the `workflows` permission) on an Ubuntu Docker runner:
`docker buildx build --platform linux/amd64 --load`
followed by [`scripts/container-smoke.sh`](../scripts/container-smoke.sh) (real
OTLP traces/metrics/logs, freeze size/SHA-256, DuckDB persistence across a
restart, nonroot uid 10001, libstdc++/cgo link). It uses no Cloudflare
credentials and deploys nothing.

Deployment is gated by a strictly read-only preflight and documented in
[`docs/cloudflare-deploy.md`](../docs/cloudflare-deploy.md):

```bash
scripts/cloudflare-preflight.sh --config-only   # config invariants, no credentials
scripts/cloudflare-preflight.sh                 # full read-only preflight (auth needed)
```

`cloudflare/src/preflight.ts` + `cloudflare/scripts/preflight.mjs` validate the
`wrangler.jsonc` invariants (ShotDO container, `standard-1`, bounded
`max_instances`, SQLite `ShotDO`/`IngestDO`/`AuthDO` exports,
`BUCKET -> burstel-shots`, no anonymous admin) and parse read-only
`wrangler whoami --json`, `wrangler secret list` (names only) and R2 bucket
listings. The live [`scripts/cloudflare-smoke.sh`](../scripts/cloudflare-smoke.sh)
is a bounded E2E
(two short shots) that never exposes a secret in argv/files/logs.

### Not verified here (needs a distinct authorized deployment job)

* Real Cloudflare Container scheduling/readiness and `ctx.container` behavior.
* Real R2 durability/checksums, real `wrangler deploy`, bucket creation, secret
  setup, migrations and account-level Container capacity.
* The live multi-user E2E (two concurrent shots plus two `member` tokens
  operating across each other's shots and the MCP token/analysis tools) is
  written into `scripts/cloudflare-multishot-smoke.sh` but **not executed here**:
  it needs the Cloudflare deploy host (a real `wrangler deploy` + Docker image),
  which this sandbox does not have.
* No Cloudflare API keys, R2 S3 keys or tokens are stored in this repository or
  image; `BURSTEL_API_TOKEN` is a Worker secret set at deploy time.
