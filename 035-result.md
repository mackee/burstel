# Spec 035 result — zero-copy part analysis

- Implementation commit: `2c151cebb0cd69b86ff9d4cb6df13da063bc336c`
- Base: `origin/main` at `54d7679` (spec 034 result)
- Spec: `035-zero-copy-analysis.md`, revision 1
  (`3c008177c8d2918b54cafe0b78c498ecdb1b9144356716b7c14af47873fc5c50`)
- Job: `cd20e3e6-9a03-4a36-94ba-64bb68df5757`
- Implementation: `9436c9d1-a302-4336-9565-aab855eeea70`
- Provenance schema: checkpoint schema version 1, sequence 1
- No deployment was performed and no Cloudflare resource was touched.

## What changed

### 1. Zero-copy window analysis (R1)

- `internal/analysis/scope_view.go` adds the default `ScopedModeView` build:
  every verified part is `ATTACH ... (READ_ONLY)` and the five logical
  projections are created as `UNION ALL` views with the same predicates as the
  pre-035 copy path (`spans` start-in-window, log event/observed time, metric
  time, `otlp_batches` window widened by ingest lag, private pre-window
  cumulative baseline). No row is copied into a combined database, so
  `timings.materialize_ms` is zero.
- The old build is retained as `ScopedModeCopy` and is the reference the view
  path is tested against (`TestScopedViewMatchesCopyOnMultiPartFixture`:
  identical rows for spans, logs, metrics, `otlp_batches` and every analysis
  section on a three-part fixture).
- After ATTACH the connection runs `SET enable_external_access=false` (and
  extension autoload off, configuration locked). Attached catalogs stay
  readable in place, but new file/network access is refused, preserving the
  pre-035 defense-in-depth posture; the SQL table/function allowlist remains
  the primary boundary.
- A repeated open of the same shot with the same window/catalog reuses the
  attached session: `OpenWindowWithCache` returns every selected key without a
  re-download, and `CommitWithTimings` returns `timings.reused=true` without a
  re-ATTACH.

### 2. Direct R2 part download (R2)

- `internal/r2s3` adds streaming `GetTo`; `WindowStore.downloadSelected`
  fetches missing parts with bounded parallelism (`FetchParallelism`, default
  16, `ANALYSIS_FETCH_PARALLELISM` in Wrangler), verifies the declared size and
  SHA-256 while writing, and installs the cache file atomically.
- `cloudflare/src/analysis-env.ts` resolves the bucket-scoped credential. The
  analysis container gets `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`,
  `R2_ACCOUNT_ID` and `R2_BUCKET` plus container egress only when all four are
  configured. Without them the Worker keeps the legacy streaming path and the
  container never gets egress.
- When enabled, the Worker sends `download: true` on `/internal/window/open`
  and streams no part bytes (`analysis.part.download` logs `streamed=0`). The
  Go server also exposes `/internal/window/preload` (open + download + commit)
  and fails closed with `503` when direct download is not configured.

### 3. Preload on materialization complete (R3)

- `MaterializeDriver` gains `preloadAnalysis`: after a run reaches `complete`
  (same DO alarm, never `waitUntil`) it posts the window and parts to the
  ShotDO `/internal/analysis/preload` endpoint, with bounded retries recorded
  as `analysis_preload_attempts`; the run result is never changed by a
  preload failure.
- The ShotDO stores the request durably and arms its own alarm. The alarm opens
  the shot session, downloads the parts and commits the views
  (`analysis.session.preload`), so `shot analyze` only runs queries. A shot
  that was never preloaded still works: analyze opens with `download: true`.

### 4. Capacity (R4)

- `ShotDO` (recorder/analysis container) is now `standard-4`.
- `MATERIALIZE_PARALLELISM=5`, `MATERIALIZE_MAX_INSTANCES=6`, CompactorDO
  `max_instances=6`; preflight enforces `P <= max_instances - 1` so one slot
  stays available for continuous compaction.
- Cost is documented in `docs/cloudflare-deploy.md` §8 and
  `cloudflare/README.md`: both container classes bill at `standard-4`
  (~4× `standard-1`), the 10 min materializer keepalive dominates intermittent
  spend, and the analysis container now needs egress only when the R2 credential
  is configured.

### 5. Timeout UX (R5)

- `burstel-cli shot analyze` uses a 180 s default HTTP timeout and accepts
  `--timeout` (Go duration, e.g. `240s`). A `202` preparing response prints the
  retry hint to stderr.
- `POST /api/shots/{id}/analyze` returns
  `202 {"state":"preparing","retry_after_ms":5000,...}` while a window shot's
  materialization is `pending`/`running`, instead of blocking past 60 s. A
  `failed` run keeps the existing error path and a pre-031 backlog shot falls
  through to the segment path.

## Tests added or updated

- `internal/analysis/scope_view_test.go`: view-vs-copy equality on a
  multi-part fixture, direct-download SHA-256 rejection then verified cache,
  session reuse without re-download/re-ATTACH, bounded download parallelism.
- `internal/analysis/window_test.go`: updated open signature; cached-part
  reuse still asserted.
- `internal/server/window_test.go`: `/internal/window/preload` downloads from a
  fake R2 endpoint, commits the session and answers queries; fails closed
  without a credential.
- `cmd/burstel-cli/main_test.go`: 180 s default timeout, `--timeout` override
  (client timeout) and duration validation.
- `cloudflare/test/analysis-env.test.ts`: credential/egress resolution, no
  partial-secret leakage.
- `cloudflare/test/analysis.test.ts`: preload starts the analysis container
  with egress and the R2 env, posts `download:true`, streams no bytes; no-op
  without the credential.
- `cloudflare/test/analysis-direct-download.test.ts`: analyze passes
  `download:true` and sends no `/internal/window/segment`; the legacy path
  still requires the bucket binding.
- `cloudflare/test/materialize-driver.test.ts`: complete run notifies the
  ShotDO preload exactly once and retries a failed notification.
- `cloudflare/test/app-window.test.ts`: analyze returns `202 preparing` while
  materialization runs.
- `cloudflare/test/preflight.test.ts`: both containers `standard-4`,
  `max_instances=6` with `P=5`, `P <= max-1` rejection and
  `ANALYSIS_FETCH_PARALLELISM` bounds.

## Verification (mandatory gates, all green)

Run against implementation commit `2c151ce`:

```text
gofmt -l cmd internal                                              exit 0 (no output)
go build ./...                                                     exit 0
go vet ./...                                                       exit 0
go test -count=1 ./...                                             exit 0
cd cloudflare && bun run typecheck                                 exit 0
cd cloudflare && bun run typecheck:test                            exit 0
cd cloudflare && bun test                                          exit 0 (277 pass, 4 skip)
cd cloudflare && bun run deploy:dry-run                            exit 0
scripts/cloudflare-preflight.sh --config-only --allow-placeholder-kv exit 0
scripts/check-public.sh                                            exit 0
```

No `wrangler deploy`, no Cloudflare API mutation and no R2/DO resource change
was performed.

## Staging procedure (operator, separate authorized deployment job)

Do not run this from the implementation job. It requires the normal deploy
authorization and the R2/bucket secrets documented in
`docs/cloudflare-deploy.md`.

1. Deploy the Worker at `2c151ce` (do not run this from the implementation
   job) and confirm the effective vars: `MATERIALIZE_PARALLELISM=5`,
   `MATERIALIZE_MAX_INSTANCES=6`, `ANALYSIS_FETCH_PARALLELISM=16`,
   `MATERIALIZE_PART_TARGET_SPANS=500000`, `MATERIALIZE_SPANS_PER_KIB=40`,
   `MATERIALIZE_MAX_PART_BYTES=1073741824`, and that **both** the `ShotDO`
   and `CompactorDO` containers are `standard-4` with CompactorDO
   `max_instances=6`. Confirm `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and
   `R2_ACCOUNT_ID` are present as Worker secrets (values are never printed).
2. `scripts/cloudflare-preflight.sh` (full read-only preflight) must pass,
   including `materialize_parallelism_bounded` and
   `analysis_fetch_parallelism_bounded`.
3. Probe health and the serving colo:
   `curl -fsS https://<host>/healthz` and
   `curl -fsS https://<host>/cdn-cgi/trace | grep '^colo='`.
4. Create the instance and issue its push token (`POST /api/instances`,
   `POST /api/instances/{name}/tokens`).
5. **Run 1 (180 s, 6 exporters × 7,500 spans/s = 45k spans/s):** start a window
   shot; run
   `burstel-loadgen -endpoint https://<host> -variant b1 -token "$PUSH" \
   -exporters 6 -spans-per-second 7500 -duration 180s \
   -compression gzip -batch-size 8192 -queue-size 262144 \
   -shot-id <shot-id> -api-token "$BURSTEL_API_TOKEN"`;
   stop the shot immediately after the run.
6. Record from the loadgen JSON: `client_settings`,
   `signals.traces.generated/exported/dropped`, `per_exporter_signals`, and the
   final `client_drop.per_exporter[*].samples` point. Requirement: traces
   `dropped == 0` for the full 180 s.
7. Record from `shot_get <shot-id>`:
   - `materialization.finished_at - stopped_at` must be **≤ 60 s**;
   - `materialization.spans == exported` and `stored == exported`
     (7,998,966+ spans in the spec-035 staging shape);
   - every part is projection-only (~20 MiB per 500k spans; no
     multi-hundred-MB part).
8. Immediately run `burstel-cli shot analyze <shot-id>` (the default 180 s
   timeout) and require **≤ 15 s after materialization complete**. In
   `wrangler tail` require:
   - `analysis.session.preload` (parts downloaded by the ShotDO alarm before
     analyze) with `cached_parts == parts`;
   - `analysis.window.open` with `direct_download: true` and
     `cached_parts == parts`;
   - `analysis.part.download` with `streamed=0`;
   - `analysis.window.commit` with `materialize_ms=0` (and `reused: true` when
     analyze re-opens the preloaded session);
   - `analysis.window.run` completing the sections.
   A repeated `shot analyze` of the same shot must show `reused=true` and no
   new R2 GETs for its parts.
9. **Run 2 (180 s), immediately after Run 1:** repeat steps 5–8 with a new
   shot. Pool containers should still be warm (no `There is no container
   instance` errors) and every threshold must hold again.
10. Negative check: query analyze of a shot whose materialization is still
    running (start a shot and call analyze before its run completes) must
    return `202 {"state":"preparing","retry_after_ms":5000}` rather than a
    request that hangs past 60 s.
11. If a threshold fails, collect the per-part `materialize.part.*` logs, the
    `analysis.session.preload`/`analysis.window.*` timing blocks and the
    effective var/instance list before changing tuning. Do not raise
    `MATERIALIZE_PARALLELISM` above `max_instances - 1`, and do not disable the
    bucket-scoped credential to "fix" analysis latency.
