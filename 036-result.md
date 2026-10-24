# Spec 036 result — analysis session correctness

- Implementation commit: `82d55d75a6335f2b9260a0c6109bc5e9aefe1dc1`
- Base: `origin/main` at `de905ab` (spec 035 result)
- Spec: `036-analysis-session-correctness.md`, revision 1
  (`dba96e33ca702ee3a754360ab013ab519badc6f341d41e25e791a9679c2637d5`)
- Job: `3668ce86-7659-4c44-8ba4-bb003264741b`
- Implementation: `bab9addc-deb4-4001-97a4-a222d0784516`
- Provenance schema: checkpoint schema version 1, sequence 1
- No deployment was performed and no Cloudflare resource was touched.

## What changed

### 1. Commit invariant (R1)

- `internal/analysis/window_store.go` adds `OpenWindowWithManifest` and
  `CommitWithManifest`. Commit now builds the snapshot **only** from selected
  parts that are present locally with a verified SHA-256 (`cachedSegment`
  requires the digest sidecar and, when the manifest declares one, an exact
  match). A missing/corrupt selected part refuses the commit with
  `*IncompleteError` (`code: analysis_incomplete`, HTTP 409), discards the
  session (`Release`) and never serves rows.
- `WindowCommit` (also the commit/preload response) reports `attached_parts`,
  per-part `part_rows` (`spans`, `log_records`, `metric_points`, `batches`),
  `attached_spans` (sum of the part counts), `view_spans`
  (`SELECT count(*) FROM spans` over the committed views),
  `expected_parts`/`expected_spans` and `verified`.
- Per-part counts come from the attached part catalogs
  (`countParts` in `internal/analysis/scope.go`), before any window predicate,
  so they are comparable with the materialization manifest.
- The Worker (`cloudflare/src/tools.ts`) passes
  `expected_parts`/`expected_spans` on open and commit and independently
  compares `attached_spans` with `materialization.spans` (and the part count).
  Any mismatch throws `HttpError(409, "analysis_incomplete")` before the
  analyze call, releases the session, and records coverage `complete=false`
  through `syncCoverage`.
- A window-session 404 is now `window session not found; open the session
  again` (`ErrWindowSessionNotFound`); the frozen-restore message can no longer
  appear on a window path.

### 2. Synchronous direct download (R2)

- `/internal/window/open` with `download: true` already fetched parts inside
  the request; spec 036 makes the success contract explicit: after
  `downloadSelected` the store verifies that **every** selected part attached
  with a good digest. Any gap fails the open with `analysis_incomplete` and
  discards the session, so no caller can commit a partially downloaded
  session behind a 200. `download_ms: 0` now means "already cached and
  verified", never "download still running".

### 3. Single-flight per shot session (R3)

- `ShotCore` (`cloudflare/src/core.ts`) adds a per-session-id flight chain:
  open, segment upload, commit, preload, query, analyze, compare and release
  for the same session never interleave. Container boot is single-flight too.
- `preloadAnalysis` marks the session as preloading before it queues; an
  analyze/query/commit that arrives while the preload is in flight waits up to
  `DEFAULT_PRELOAD_WAIT_MS` (20 s) and then either runs on the committed
  session or returns the documented
  `202 {"state":"preparing","retry_after_ms":5000}` body.
- `window_not_committed` is a retryable 409 from the container and is mapped by
  the Worker to the same `202 preparing` body, so a lifecycle race can never
  surface as `400`.
- `app.ts` returns 202 when query/analyze/compare yields a preparing body
  (materialization pending/running keeps the existing spec-035 path).

### 4. Preload path (R4)

- The ShotDO preload remains the window-session path
  (`/internal/window/preload`: open + direct download + commit). It never
  consults the shot row's freeze metadata or `/internal/restore`.
- `MaterializeDO` now includes `spans` and `parts_total` in the preload
  notification; `ShotDO` persists them with the durable preload flag and
  `ShotCore.preloadAnalysis` forwards them as `expected_spans` /
  `expected_parts`. Tested with an empty artifact sink: the preload succeeds
  with no frozen artifact and no `/internal/restore` request.

### 5. Self-check (R5)

- Commit runs the span-count self-check over the views and reports
  `view_spans`/`view_matches_manifest`; a view-count divergence caused by a
  span whose start precedes the window is disclosed (`start-in-shot`
  predicate) rather than treated as a missing part.
- Both the analyze path (`tools.ts`) and the preload path (`ShotCore`) log
  `analysis.session.verified` with `verified`, `attached_parts` /
  `expected_parts`, `attached_spans` / `expected_spans`, `view_spans` and
  `view_matches_manifest`.

## Tests added or updated

- `internal/analysis/window_commit_test.go`: commit refused with a missing
  selected part (session discarded, query → session-not-found), span-count
  self-check mismatch refused, direct download synchronous with per-part
  counts and `attached == view == expected`, failed download discards the
  session.
- `internal/server/window_test.go`: `/internal/window/commit` →
  `409 analysis_incomplete` with `missing_parts`/`attached_parts`, session
  discarded (query → 404 window-session error, never the frozen-shot message);
  preload self-check mismatch refused and the correct manifest verifies with
  per-part rows.
- `cloudflare/test/analysis-session.test.ts`: preload uses the window path and
  needs no frozen artifact; analyze waits out an in-flight preload and runs
  after it; bounded wait returns `202 preparing` and never calls analyze;
  worker commit verification passes manifest totals; a shortened attachment
  set fails 409 with no analyze/release ordering violated and coverage
  `complete=false`; `window_not_committed` maps to 202.
- `cloudflare/test/app-window.test.ts`: REST analyze returns 202 preparing
  when the ShotDO session is still being prepared.
- `cloudflare/test/analysis-direct-download.test.ts`, `test/fakes.ts`: fake
  commit/preload responses carry the spec-036 verification fields.

## Verification (mandatory gates, all green)

Run against implementation commit `82d55d7` (clean worktree):

```text
gofmt -l cmd internal                                              exit 0 (no output)
go build ./...                                                     exit 0
go vet ./...                                                       exit 0
go test -count=1 ./...                                             exit 0
cd cloudflare && bun run typecheck                                 exit 0
cd cloudflare && bun run typecheck:test                            exit 0
cd cloudflare && bun test                                          exit 0 (284 pass, 4 skip)
cd cloudflare && bun run deploy:dry-run                            exit 0
scripts/cloudflare-preflight.sh --config-only --allow-placeholder-kv exit 0
scripts/check-public.sh                                            exit 0
```

No `wrangler deploy`, no Cloudflare API mutation and no R2/DO resource change
was performed by this job. The worktree was pushed only through the trusted
`git:push:burstel` capability (`de905ab` → `82d55d7`).

## Staging procedure (operator, separate authorized deployment job)

Do not run this from the implementation job. It requires the normal deploy
authorization and the R2/bucket secrets documented in
`docs/cloudflare-deploy.md`.

1. Deploy the Worker at `82d55d7` (authorized deployment job only). Confirm the
   effective vars from spec 035 are unchanged (`MATERIALIZE_PARALLELISM=5`,
   `MATERIALIZE_MAX_INSTANCES=6`, `ANALYSIS_FETCH_PARALLELISM=16`,
   `MATERIALIZE_PART_TARGET_SPANS=500000`, `MATERIALIZE_SPANS_PER_KIB=40`) and
   that `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and `R2_ACCOUNT_ID` are
   Worker secrets (values are never printed). Both containers stay
   `standard-4`.
2. `scripts/cloudflare-preflight.sh` (full read-only preflight) must pass.
3. Probe `GET /healthz` and `GET /cdn-cgi/trace` (`colo=`).
4. Create the instance and issue its push token (`POST /api/instances`,
   `POST /api/instances/{name}/tokens`).
5. **Run 1 (180 s, 6 exporters × 7,500 spans/s = 45k spans/s):** start a window
   shot, then
   `burstel-loadgen -endpoint https://<host> -variant b1 -token "$PUSH" \
   -exporters 6 -spans-per-second 7500 -duration 180s \
   -compression gzip -batch-size 8192 -queue-size 262144 \
   -shot-id <shot-id> -api-token "$BURSTEL_API_TOKEN"`;
   stop the shot immediately after the run.
6. Record `client_settings`, `signals.traces.generated/exported/dropped`,
   `per_exporter_signals` and the final `client_drop.per_exporter[*].samples`
   point. Requirement: traces `dropped == 0` for the full run.
7. From `shot_get <shot-id>` require:
   - `materialization.finished_at - stopped_at ≤ 60 s`;
   - `materialization.spans == exported` **and** `stored == exported`
     (`stored` is the `SELECT count(*) FROM spans` the analysis self-check
     reports; they must be equal for both runs);
   - every part is projection-only (~20 MiB per 500k spans).
8. Immediately run `burstel-cli shot analyze <shot-id>` (default 180 s
   timeout) and require **≤ 15 s after materialization complete** with no
   error on the immediate call. In `wrangler tail` require:
   - `analysis.session.preload` with `cached_parts == parts`;
   - `analysis.session.verified` with `verified: true`,
     `attached_parts == expected_parts`, `attached_spans == expected_spans`,
     and `view_spans` equal to the preload's attached count unless
     `view_matches_manifest` is explicitly false (boundary-crossing spans);
   - `analysis.window.open` with `direct_download: true` and
     `cached_parts == parts`;
   - `analysis.part.download` with `streamed=0`;
   - `analysis.window.commit` with `materialize_ms=0` (and `reused: true`
     when analyze re-opens the preloaded session);
   - `analysis.window.run` completing the sections;
   - no `analysis.session.preload.failure` and no
     `409 analysis_incomplete`.
9. **Run 2 (180 s), immediately after Run 1:** repeat steps 5–8 with a new
   shot. All thresholds must hold again and `stored == exported` again.
10. Negative checks:
    - Query analyze while materialization is `pending`/`running` → must be
      `202 {"state":"preparing","retry_after_ms":5000}`.
    - Analyze immediately after a preload is triggered (within its download
      window) → a correct result or `202 preparing`; never
      `400 window_not_committed` and never a row set smaller than the
      exported span count. A `GET /api/shots/{id}` coverage must not report
      `complete: true` for a refused/incomplete session.
    - Re-run analyze of the same shot → `reused=true` and no new part GETs.
11. If a threshold fails, collect the per-part `materialize.part.*` logs, the
    `analysis.session.preload` / `analysis.session.verified` /
    `analysis.window.*` timing blocks and the effective var/instance list
    before changing tuning. A `409 analysis_incomplete` is an integrity
    signal: compare the manifest `materialization.parts` with the parts
    actually present in R2; do not retry it as a latency problem.
