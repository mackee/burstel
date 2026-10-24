# Spec 032 result — durable materialization driver + 031 staging fixes

- Implementation commit: `4d9d599cda0d63a27ab6f79a201c27e87e4ab2f7`
- Base: `origin/main` at `5b08636251957a7d7f09c859be3b3e1114419da3`
- Spec: `032-durable-materialization-driver.md`, revision 1
  (`bafa2ce60ab690e107a5a16e5bd3756e34d7bae05f65a70d94867032cad7e924`)
- Job: `3f819c85-db4d-43c4-9fcd-1e62f74b6088`
- Implementation: `369e0630-cf9c-412c-a996-b854a186f0e2`
- No deployment was performed and no Cloudflare resource was touched.

## What changed

### 1. Durable alarm-driven driver (R1)

- New `MaterializeDO` (`cloudflare/src/materialize-do.ts`), bound as
  `MATERIALIZE_DO` and addressed by shot id. It owns the durable run state and
  the alarm schedule.
- New pure `MaterializeDriver` (`cloudflare/src/materialize-driver.ts`): plans
  journal objects, splits them into byte-balanced parts
  (`materialize-part.ts`), fans out to `materializer-<shot>-<i>` containers, and
  persists per-part state.
- `shot_stop` now only calls the coordinator `POST /internal/materialize`; the
  Worker's `ctx.waitUntil` dependency is gone. The alarm is armed at
  `window.end + LATE_WRITE_LIMIT_MS`.
- Every part is claimed with `lease_owner`/`lease_expires_at`; a heartbeat
  refreshes running leases. An expired lease is retried by the next alarm, up to
  `MATERIALIZE_MAX_ATTEMPTS` (default 3). After the last attempt the run becomes
  terminal `failed` with a reason (never an indefinite `running`).
- Empty plans complete with zero parts; a plan that changes mid-run fails
  explicitly instead of silently mixing parts.
- New tunables (with defaults) in `wrangler.jsonc`:
  `MATERIALIZE_MAX_ATTEMPTS=3`, `MATERIALIZE_LEASE_TTL_MS=120000`,
  `MATERIALIZE_RETRY_MS=5000`, `MATERIALIZE_HEARTBEAT_MS=30000`.
- Preflight validates the new `MATERIALIZE_DO` binding/export.

### 2. Lazy analysis path (R2)

- `shot_analyze`/`shot_query`/`shot_compare` ask the coordinator to restart a
  stale run (`lease_expires_at` missing/expired, including pre-032 `running`
  rows) and then poll the durable record.
- A terminal `failed` run surfaces a `409 materialization_failed` with the
  reason and the retry hint instead of falling through to a misleading segment
  view.
- New `shot materialize <id>` action:
  - REST: `POST /api/shots/{id}/materialize` (`admin|member`), body
    `{force?: boolean, wait?: boolean}`.
  - MCP: `shot_materialize` tool (`admin|member`).
  - CLI: `shot materialize <id> [--no-force] [--wait]`.
- `windowShotInfo` exposes part leases/attempts/heartbeat and the run state.

### 3. Truthful coverage (R3)

- `coverage.complete` is forced false whenever the materialization state is not
  `complete`; this applies on create, stop, analysis coverage writes and every
  materialization mirror.
- Coverage now carries `materialization_state`,
  `materialization_parts_done`, `materialization_parts_total` and
  `materialization_error`.

### 4. Loadgen flag/env precedence (R4)

- `-compression`, `-batch-size` and `-queue-size` default to *unset*.
- Resolution order is explicit flag → standard env var
  (`OTEL_EXPORTER_OTLP_COMPRESSION`, `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`,
  `OTEL_BSP_MAX_QUEUE_SIZE`) → SDK default (512/2048).
- The JSON report keeps `client_settings` (effective values) and adds
  `client_settings_sources` with `flag`/`env`/`default` per field.

### 5. Observability (R5)

The driver logs `materialize.run.scheduled/start/retry/complete/failed`,
`materialize.plan`, `materialize.part.start/finish/failure/retry` (duration,
spans, bytes) and mirror failures.

## Tests added or updated

- `cloudflare/test/materialize-driver.test.ts`: alarm completes after the
  `shot_stop` request is gone; stale part lease retry; terminal failure with a
  reason after bounded attempts; truthful coverage while pending/running and
  after completion; coordinator restart of a stale run.
- `cloudflare/test/app-window.test.ts`: REST `POST /api/shots/{id}/materialize`
  records the coordinator command and returns truthful coverage.
- `cloudflare/test/window.test.ts`: stop leaves coverage intentionally
  incomplete until materialization completes.
- `cmd/burstel-loadgen/main_test.go`: defaults, env-when-unset, flag-wins,
  bad-env and `parseFlags` provenance.
- `cmd/burstel-cli/main_test.go`: `shot materialize` wire contract and flags.

## Verification (all mandatory gates green)

Run against implementation commit `4d9d599`:

```text
gofmt -l cmd internal                                              exit 0 (no output)
go build ./...                                                     exit 0
go vet ./...                                                       exit 0
go test -count=1 ./...                                             exit 0
cd cloudflare && bun run typecheck                                 exit 0
cd cloudflare && bun run typecheck:test                            exit 0
cd cloudflare && bun test                                          exit 0 (248 pass, 4 skip)
cd cloudflare && bun run deploy:dry-run                            exit 0
scripts/cloudflare-preflight.sh --config-only --allow-placeholder-kv exit 0
scripts/check-public.sh                                            exit 0
```

No deploy, no `wrangler deploy`, and no Cloudflare API mutation was run.
