# Spec 033 result — materializer pool, egress, prewarm and analysis latency

- Implementation commit: `66d7f4aef28ef8f57565fba70ebf32c21f338190`
- Base: `origin/main` at `58ca29c` (spec 032 result)
- Spec: `033-materializer-pool-and-latency.md`, revision 1
  (`ed44c0e0c1eef637314bb77ddd5df3f40aedc95b8f8b9e7933a4d7de78e5db89`)
- Job: `552c289e-49c6-416b-ac94-6b7b319df4e1`
- Implementation: `354c683c-86bc-45b4-a6a3-1a196d282a54`
- No deployment was performed and no Cloudflare resource was touched.

## What changed

### 1. Container egress is per class (R1)

- `createContainerControl` accepts `enableInternet` and passes it in
  `container.start({ enableInternet, env })`.
- `containerEgressFor("CompactorDO")` is `true`; `containerEgressFor("ShotDO")`
  is `false`. `CompactorDO` opts in, `ShotDO` (recorder and analysis) stays
  isolated.
- Unit tests assert the per-class policy and that the start options carry the
  flag.

### 2. Fixed materializer pool with queueing and backoff (R2)

- Materializer DO names are the fixed pool `materializer-0..P-1`
  (`materializerPoolName`), never `materializer-<shot>-<i>`.
- `MATERIALIZE_PARALLELISM=3` and the CompactorDO `max_instances=4`, leaving
  one instance for `compactor-primary`. Preflight enforces
  `P <= max_instances - 1`.
- `MaterializeDriver` leases a free pool member per part and queues parts.
  `MATERIALIZE_PARTS_PER_MEMBER=2` splits a plan into more parts than members,
  so a freed member immediately takes a queued part (and R2 GET concurrency
  doubles).
- A platform "There is no container instance that can be provided to this
  Durable Object" response is detected as transient: the part returns to
  `pending`, its attempt is refunded, and the next alarm runs after
  `MATERIALIZE_CAPACITY_BACKOFF_MS` (10 s). It never consumes `max_attempts`.
- After a part finishes, the member receives `/internal/keepalive` and keeps
  its container warm for `MATERIALIZE_KEEPALIVE_MS` (10 min) for the next shot.

### 3. Prewarm via DO alarms (R3)

- `shot_start` (window) calls `POST /internal/prewarm` on the ShotDO and the
  MaterializeDO. Both requests only write a durable flag and arm the DO alarm;
  no `waitUntil` and no container start on the request path.
- The ShotDO alarm starts the analysis container (`prewarmAnalysis`); the
  MaterializeDO alarm starts every pool member through
  `materializer-<i>/internal/prewarm`.
- A failed prewarm is retried by a later alarm with the capacity backoff.

### 4. Analysis latency (R4)

- The analysis container caches downloaded parts in a durable per-container
  part cache keyed by object key plus SHA-256 sidecar. `/internal/window/open`
  returns `cached_keys`; the Worker skips the R2 GET and segment upload for
  those keys. Releasing a window session no longer deletes the cached bytes.
- `WindowStore` exposes `OpenWindowWithCache` and `CommitWithTimings`.
  `BuildScopedEngine` records `attach_ms`, `materialize_ms`, `checkpoint_ms`,
  `open_ms`, `coverage_ms` and `total_ms`.
- `Engine.Analyze` records `raw_counts_ms`, per-section `sections_ms` and
  `total_ms` in the `analyze` response.
- The Worker logs `analysis.container.start`, `analysis.window.open`,
  `analysis.part.download` (with cached/streamed counts and download time),
  `analysis.window.commit`/attach timing and `analysis.window.run` timings.

### 5. Materialize latency (R5)

- `Materialize` records `objects`, `get_ms` (summed R2 GET), `decode_ms`,
  `append_ms`, `index_ms`, `checkpoint_ms`, `hash_ms`, `put_ms` and `total_ms`
  in the response and a structured container log line.
- `SegmentInfo` building has a `BuildPartProfile` variant; existing
  `BuildSegment`/`BuildPart` callers are unchanged.
- Splitting into `2 x P` parts raises GET concurrency and keeps the pool busy.

### 6. 180 s client-drop investigation (R6)

- `burstel-loadgen` now keeps per-exporter counters, reports
  `signals.*.dropped`/`drop_pct`, `per_exporter_signals`, and a per-second
  `client_drop.per_exporter[*].samples` series inferred from
  `generated - exported` (the SDK exposes no queue-drop counter).
- `docs/isucon-runbook.md` and `cloudflare/README.md` document the measured
  60 s 0 % / 180 s ≈10 % result and the 180 s target settings: gzip, batch
  8192, queue 262144, two exporters per host at a 15k spans/s host target, and
  shutdown ≥ 20 s.

## Tests added or updated

- `cloudflare/test/container.test.ts`: egress policy per class and start
  options.
- `cloudflare/test/materialize-driver.test.ts`: fixed pool names, queueing
  with maximum concurrency, no-capacity backoff that refunds attempts,
  keepalive after a finished part, and prewarm request→alarm split.
- `cloudflare/test/materialize.test.ts`: direct path uses pool member names.
- `cloudflare/test/app-window.test.ts`, `cloudflare/test/mcp-window.test.ts`:
  default window `shot_start` prewarms only; no recording shot state.
- `internal/analysis/window_test.go`: cached part reuse across sessions,
  `cached_keys`, idempotent re-add and non-negative phase timings.
- `internal/compaction/materialize_test.go`: per-phase timings present.

## Verification (all mandatory gates green)

Run against implementation commit `66d7f4a`:

```text
gofmt -l cmd internal                                              exit 0 (no output)
go build ./...                                                     exit 0
go vet ./...                                                       exit 0
go test -count=1 ./...                                             exit 0
cd cloudflare && bun run typecheck                                 exit 0
cd cloudflare && bun run typecheck:test                            exit 0
cd cloudflare && bun test                                          exit 0 (254 pass, 4 skip)
cd cloudflare && bun run deploy:dry-run                            exit 0
scripts/cloudflare-preflight.sh --config-only --allow-placeholder-kv exit 0
scripts/check-public.sh                                            exit 0
```

No deploy, no `wrangler deploy`, and no Cloudflare API mutation was run.

## Staging procedure (operator, separate authorized deployment job)

Do not run this from the implementation job. It requires the normal deploy
authorization and the R2/bucket secrets already documented in
`docs/cloudflare-deploy.md`.

1. Deploy the Worker at `66d7f4a` and confirm the effective vars include
   `MATERIALIZE_PARALLELISM=3`, `MATERIALIZE_MAX_INSTANCES=4`,
   `MATERIALIZE_KEEPALIVE_MS=600000`, `MATERIALIZE_PARALLELISM` below the
   CompactorDO `max_instances=4`, and `MATERIALIZE_PARTS_PER_MEMBER=2`.
2. Probe health and the serving colo:
   `curl -fsS https://<host>/healthz` and
   `curl -fsS https://<host>/cdn-cgi/trace | grep '^colo='`.
3. Create the instance and issue its push token
   (`POST /api/instances`, `POST /api/instances/{name}/tokens`).
4. **Run 1 (180 s):** start a window shot; run
   `burstel-loadgen -endpoint https://<host> -variant b1 -token "$PUSH" \
   -exporters 6 -spans-per-second 7500 -duration 180s \
   -compression gzip -batch-size 8192 -queue-size 262144 \
   -shot-id <shot-id> -api-token "$BURSTEL_API_TOKEN"`;
   stop the shot immediately after the run.
5. Record from the loadgen JSON: `client_settings`,
   `signals.traces.generated/exported/dropped`, `per_exporter_signals`, and the
   final `client_drop.per_exporter[*].samples` point. Requirement: traces
   `dropped == 0` for the full 180 s.
6. Record from `shot_get <shot-id>`: `materialization.finished_at -
   stopped_at` must be **≤ 60 s**, and `materialization.spans == exported`.
   Run `shot analyze <shot-id>` and require **≤ 15 s**; confirm the Worker logs
   contain `analysis.window.open`, `analysis.part.download` (cached parts > 0
   on a repeated call), `analysis.window.commit` and `analysis.window.run`.
7. **Run 2 (180 s), immediately after Run 1:** repeat steps 4–6 with a new
   shot. The pool containers should still be warm (`cached_parts > 0` on
   analyze if reusing the same shot; no
   `There is no container instance` errors in `wrangler tail`), and all
   thresholds must hold again.
8. If a threshold fails, collect the per-part `materialize.part.*` logs and the
   analysis timing blocks before changing tuning; do not lower
   `MATERIALIZE_PARALLELISM` above `max_instances - 1`.
