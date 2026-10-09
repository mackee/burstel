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
bun run typecheck          # tsc for src (Worker/DO)
bun run typecheck:test     # tsc for the pure core + tests
bun test                   # 25 mock-based tests: lifecycle, auth, R2, container
bun run dev                # wrangler dev (needs a local Docker/container engine)
bun run deploy:dry-run     # bundling dry-run, skips the container image build
```

`wrangler deploy --dry-run` (without `--containers-rollout=none`) additionally
requires the Docker CLI because Wrangler must build the container image even for
a dry-run. This sandbox has no Docker daemon, so the dry-run used for CI here is
`wrangler deploy --dry-run --containers-rollout=none`, which validates the
Wrangler configuration and bundles the Worker.

### What is verified without a Cloudflare account

* The full lifecycle state machine, idempotent stop, drain-vs-stop race,
  failure injection (R2 write failure cannot produce `frozen`), artifact length
  validation, and rehydration across a simulated DO restart — over real SQLite
  (`bun:sqlite`) and fakes.
* Auth rejection, shot isolation, OTLP proxy response semantics, and route
  validation through the real Worker request handler.
* Single-PUT and multipart R2 streaming code paths against a fake R2 bucket.
* TypeScript typecheck and `wrangler deploy --dry-run` bundling.

### Not verified here (needs a distinct authorized deployment job)

* Real Cloudflare Container scheduling/readiness and `ctx.container` behavior.
* Real R2 durability/checksums, real `wrangler deploy`, bucket creation, secret
  setup, migrations and account-level Container capacity.
* No Cloudflare API keys, R2 S3 keys or tokens are stored in this repository or
  image; `BURSTEL_API_TOKEN` is a Worker secret set at deploy time.
