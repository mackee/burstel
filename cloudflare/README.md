# burstel Phase 2 — Cloudflare Worker + ShotDO + R2

This directory contains the Phase 2 Cloudflare layer for **burstel**. It builds
on the Phase 1 Go recorder (`../cmd/burstel`, `../Dockerfile`); it does **not**
replace it.

```
                 public HTTPS                     Worker            per-shot DO
 exporter ──► POST /shots/{id}/v1/traces ──► auth + route ──► ShotDO ──► Container port
 rec client ─► POST /api/shots            ──► auth + route ──► ShotDO      (Go recorder)
 rec client ─► POST /api/shots/{id}/stop  ──► auth + route ──► ShotDO ──► GET /internal/stop
                                                                    │        GET /internal/artifact
                                                                    ▼
                                                          R2Bucket.put (streaming)
                                                          shots/{id}/telemetry.duckdb
                                                          shots/{id}/metadata.json
```

* `src/index.ts` — Worker entrypoint.
* `src/app.ts` — public HTTP routes + bearer auth (whitelist only).
* `src/shot-do.ts` — `ShotDO extends DurableObject`, wires `ctx.storage.sql`,
  `ctx.container` and the `BUCKET` R2 binding into the core.
* `src/core.ts` — race-safe lifecycle state machine (pure; fully unit tested).
* `src/store.ts` — SQLite-backed state store over a `SyncSql` seam.
* `src/artifacts.ts` — streaming single-PUT / bounded multipart R2 freeze.
* `src/container.ts` — wrapper around the Durable Object Container API.

## Public HTTP API

All control and ingest routes require `Authorization: Bearer <BURSTEL_API_TOKEN>`
(or `X-Burstel-Token`). `GET /healthz` is unauthenticated.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/healthz` | Worker reachability |
| `POST` | `/api/shots` | Create a shot (random unguessable id). Body: `{"metadata":{...}}` |
| `GET` | `/api/shots/{id}` | Lifecycle / metadata / counters |
| `POST` | `/api/shots/{id}/stop` | Idempotent freeze flow |
| `POST` | `/api/shots/{id}/query` | Constrained read-only SQL against the frozen snapshot |
| `POST` | `/api/shots/{id}/analyze` | SQL-backed analytical summaries |
| `GET` | `/api/shots/{id}/schema` | Documented queryable schema + safety policy |
| `POST` | `/api/shots/{id}/compare` | SQL-derived diff with another frozen shot (`{"other":"<id>"}`) |
| `POST` | `/mcp` | Remote MCP (Streamable HTTP) endpoint |
| `POST` | `/shots/{id}/v1/traces` | Authenticated OTLP/HTTP ingest (streamed) |
| `POST` | `/shots/{id}/v1/metrics` | Authenticated OTLP/HTTP ingest (streamed) |
| `POST` | `/shots/{id}/v1/logs` | Authenticated OTLP/HTTP ingest (streamed) |

`POST /api/shots` and `GET /api/shots/{id}` return `otlp_base_url` plus explicit
`otlp_endpoints` for the three signals, e.g.
`https://<host>/shots/<id>/v1/traces`.

The Go recorder's `/internal/*`, artifact and query endpoints are **never**
exposed. The Worker only forwards the whitelisted paths above to the ShotDO,
which reaches the container over its private TCP port.

### Authentication notes

* The token is read from `Authorization: Bearer …` first, then
  `X-Burstel-Token`. OTLP exporters can set either via
  `OTEL_EXPORTER_OTLP_HEADERS`.
* The token is compared in constant time, never logged, and never forwarded to
  the container. The container separately receives `BURSTEL_INTERNAL_TOKEN`
  (defaults to the API token) so its `/internal/*` endpoints are also gated.

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
with the maintained `@modelcontextprotocol/sdk`. The endpoint requires the same
bearer token as the REST API, and each tool is a thin wrapper over the same
per-shot ShotDO boundary, so MCP callers cannot bypass REST authentication.

| Tool | Arguments | Result |
| --- | --- | --- |
| `shot_start` | `metadata?` | New shot id + OTLP endpoints |
| `shot_stop` | `shot_id` | Frozen state, size, SHA-256 |
| `shot_get` | `shot_id` | Lifecycle, metadata, counters, freeze info |
| `shot_query` | `shot_id`, `sql`, `max_rows?` | Bounded typed rows |
| `shot_analyze` | `shot_id`, `sections?`, `top_n?` | Latency p50/p95/p99, slow spans, errors, DB ops, log correlation, metrics |
| `shot_compare` | `left_shot_id`, `right_shot_id`, `top_n?` | SQL-derived deltas + sample sizes + disclosure |
| `shot_schema` | `shot_id` | Documented tables/columns/templates + safety policy |

A shot id is a capability: possession of the id (plus the API token) grants
access to that shot only. Tool errors are returned as MCP tool errors
(`isError: true`), not protocol errors, so a model can recover.

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
                                   └── any error ──► failed (retryable)
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
bun test                   # lifecycle, auth, R2, container, analysis, MCP
bun run dev                # wrangler dev (needs a local Docker/container engine)
bun run deploy:dry-run     # bundling dry-run, skips the container image build
```

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
# 1. Start a shot and point an OTLP exporter at it.
curl -s -H "Authorization: Bearer $TOKEN" -X POST \
  -d '{"metadata":{"commit":"abc123","scenario":"bench"}}' \
  https://<host>/api/shots
# -> {"shot_id":"...","otlp_endpoints":{"traces":".../shots/<id>/v1/traces", ...}}

export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=https://<host>/shots/<id>
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $TOKEN"
# ... run the benchmark so the exporter sends traces/metrics/logs ...

# 2. Stop and freeze.
curl -s -H "Authorization: Bearer $TOKEN" -X POST https://<host>/api/shots/<id>/stop

# 3. Query and analyze the frozen snapshot (no recording container is running).
curl -s -H "Authorization: Bearer $TOKEN" -X POST \
  -d '{"sql":"SELECT service_name, count(*) AS n FROM spans GROUP BY 1 ORDER BY n DESC"}' \
  https://<host>/api/shots/<id>/query

curl -s -H "Authorization: Bearer $TOKEN" -X POST -d '{"top_n":10}' \
  https://<host>/api/shots/<id>/analyze

# 4. Compare two shots.
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
* Auth rejection, shot isolation, OTLP proxy response semantics, and route
  validation through the real Worker request handler.
* Single-PUT and multipart R2 streaming code paths against a fake R2 bucket.
* The real Go analysis engine over real DuckDB: read-only restore with
  size/SHA-256 verification, allowlisted SQL, p50/p95/p99 and error analysis,
  and a SQL-derived comparison — plus adversarial SQL rejection.
* The DO analysis flow over fakes: the frozen artifact is streamed from the
  shared R2-like bucket into a fresh analysis container, tampering is detected,
  and non-frozen/missing-artifact cases fail closed.
* MCP `initialize` / `tools/list` / `tools/call` envelopes and the full
  start/stop/query/analyze/compare tool flow through the official SDK.
* TypeScript typecheck and `wrangler deploy --dry-run` bundling.

`wrangler deploy --dry-run` (without `--containers-rollout=none`) additionally
requires the Docker CLI because Wrangler must build the container image even for
a dry-run. This sandbox has no Docker daemon, so the dry-run used here is
`wrangler deploy --dry-run --containers-rollout=none`, which validates the
Wrangler configuration and bundles the Worker.

### Not verified here (needs a distinct authorized deployment job)

* Real Cloudflare Container scheduling/readiness and `ctx.container` behavior.
* Real R2 durability/checksums, real `wrangler deploy`, bucket creation, secret
  setup, migrations and account-level Container capacity.
* No Cloudflare API keys, R2 S3 keys or tokens are stored in this repository or
  image; `BURSTEL_API_TOKEN` is a Worker secret set at deploy time.
