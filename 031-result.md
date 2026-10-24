# Spec 031 result — shot-time parallel materialization

- Implementation commit: `0d120976feabfdb79567b49031af2d59ba706d47`
- Base: fresh `origin/main` at `5dbb467` (post-`git filter-repo` history)
- Spec: `031-shot-time-parallel-materialization.md`, revision 1
  (`d4facc66fc58bcaf8d29a7309269f426f51e1e314fd87998a45d12a872e46f7b`)
- No deployment was performed and no Cloudflare resource was touched.

## What changed

- **D1** — bucket-scoped R2 S3 credential (`R2_ACCESS_KEY_ID`,
  `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`) is injected only into the
  compactor/materializer container env (`compactor-env.ts`). The preflight
  requires the three secret names; the deploy runbook documents creating a
  single-bucket, read/write token. The ShotDO recorder path is unchanged.
- **D2** — `BuildSegment` now writes through DuckDB Appenders on one
  transaction/connection, keeps the raw batch + projections atomic, and builds
  secondary indexes once after the bulk append.
- **D3** — shot materialization: the ControlDO enumerates journal keys in
  `[start − 5 s, end + LATE_WRITE_LIMIT_MS]`, the Worker splits them into
  byte-balanced contiguous chunks (`MATERIALIZE_PARALLELISM`, default 4,
  bounded by container `max_instances`), and each chunk runs on a distinct
  `CompactorDO` name (`materializer-<shot>-<i>`). The Go service reads
  `journal/`, decodes, CHECKPOINTs and PUTs `shots/<shot_id>/part-<i>.duckdb`
  with a SHA-256. The shot record stores the part manifest; analysis ATTACHes
  every part. Excluded (revoked / late-write) keys are filtered both in the plan
  and in the Go materializer. Continuous compaction yields while a run is
  active (TTL-bounded row in `materialization_runs`).
- **D4** — `LATE_WRITE_LIMIT_MS` defaults to 10 s. `shot_stop` returns
  immediately and starts materialization in the background; `shot_get` exposes
  `materialization {state, parts_done, parts_total, spans, started_at,
  finished_at, parts}`; `shot_analyze`/`query`/`compare` lazily starts or waits
  up to `MATERIALIZE_WAIT_MS` (default 90 s).
- **D5** — part keys are deterministic and re-materialization replaces the same
  keys; parts are built from the exact stored wire bytes, so stored and exported
  spans reconcile by construction.

## Local measured build throughput

Environment: this sandbox (Linux amd64, 4 visible AMD EPYC vCPUs), `CGO_ENABLED=1`
with the repository toolchain. This is **not** a Cloudflare standard-1 number.

```bash
. .scratch/toolchain.env
gofmt -w cmd internal
go build ./...
go vet ./...
go test ./...

go test ./internal/compaction -run '^$' \
  -bench BenchmarkBuildSegmentAppender -benchtime=3x -count=1
```

Result (50 000 spans per build, 25 × 2 000-span raw batches, including
schema creation, appender flush, index build, CHECKPOINT, close and SHA-256):

```text
BenchmarkBuildSegmentAppender-4   3   553053949 ns/op   90407 spans/s
```

So the measured local Appender build throughput is **≈90.4k spans/s** on this
4-vCPU sandbox. The spec's ≥1M spans/s target is a per-container standard-1
target; it was not reachable here and is measured on staging by the procedure
below (the benchmark records whatever the host actually does, it does not assert
the target).

Other local gates (all green):

```text
go build ./...                         exit 0
go vet ./...                           exit 0
gofmt -w cmd internal                  (no diff)
go test ./...                          exit 0
cd cloudflare && bun run typecheck     exit 0
cd cloudflare && bun run typecheck:test exit 0
cd cloudflare && bun test              exit 0 (240 pass, 4 skipped)
cd cloudflare && bun run deploy:dry-run exit 0
scripts/cloudflare-preflight.sh --config-only --allow-placeholder-kv  exit 0
scripts/check-public.sh                exit 0
```

## Exact staging benchmark procedure (operator-run; this job does not deploy)

Prerequisites (once): the Worker is deployed from this commit, the operator has
created the single-bucket R2 token and set the three `R2_*` Worker secrets (see
`docs/cloudflare-deploy.md` §2a), and `MATERIALIZE_PARALLELISM=4` /
`CompactorDO.max_instances=4` are live.

1. Build the load generator on the Linux benchmark client from this commit:

   ```bash
   go build -o /tmp/burstel-loadgen ./cmd/burstel-loadgen
   go build -o /tmp/burstel-cli ./cmd/burstel-cli
   ```

2. Create the named instance and issue its push-only 24 h token (shown once,
   never commit it):

   ```bash
   export BURSTEL_TOKEN=<admin-or-member-token>
   /tmp/burstel-cli instance create web-1 --base-url https://<host> --token "$BURSTEL_TOKEN"
   /tmp/burstel-cli instance token issue web-1 --base-url https://<host> \
     --token "$BURSTEL_TOKEN" --out /tmp/push-token
   PUSH=$(cat /tmp/push-token)
   ```

3. Open a server-clock window shot immediately before the run:

   ```bash
   SHOT=$(/tmp/burstel-cli shot start --name spec031 --instances web-1 \
     --base-url https://<host> --token "$BURSTEL_TOKEN" | jq -r .shot_id)
   ```

4. Run the spec-027 client settings for exactly 180 s (3 exporters × 15k
   spans/s, gzip, 8192 batch, 131072 queue), recording the run id:

   ```bash
   RUN=spec031-$(date +%s)
   /tmp/burstel-loadgen \
     -endpoint https://<host> -variant b1 -token "$PUSH" \
     -exporters 3 -spans-per-second 15000 -duration 180s \
     -compression gzip -batch-size 8192 -queue-size 131072 \
     -shot-id "$SHOT" -run-id "$RUN" \
     -api-url https://<host> -api-token "$BURSTEL_TOKEN" \
     > "/tmp/${RUN}.json"
   ```

   The load generator must call every provider `Shutdown` with a ≥20 s budget
   (its default shutdown grace is 60 s); otherwise the tail export is lost.

5. Stop the shot and record wall-clock times:

   ```bash
   date +%s%3N > /tmp/shot-stop-start.ms
   /tmp/burstel-cli shot stop "$SHOT" --base-url https://<host> --token "$BURSTEL_TOKEN"
   date +%s%3N > /tmp/shot-stop-end.ms

   # Poll until materialization is terminal.
   until /tmp/burstel-cli shot get "$SHOT" --base-url https://<host> --token "$BURSTEL_TOKEN" \
       | jq -e '.materialization.state == "complete" or .materialization.state == "failed"' >/dev/null; do
     sleep 1
   done
   date +%s%3N > /tmp/materialized.ms
   ```

6. Analyze all sections and time it:

   ```bash
   /usr/bin/time -f '%e s' /tmp/burstel-cli shot analyze "$SHOT" \
     --base-url https://<host> --token "$BURSTEL_TOKEN" > "/tmp/${RUN}-analyze.json"
   ```

7. Compare stored vs exported for the run id (the loadgen report contains the
   generated/exported counts; the stored count comes from analysis or a bounded
   query):

   ```bash
   jq '{run_id, generated_spans, exported_spans, client_settings, shutdown_ms}' "/tmp/${RUN}.json"
   /tmp/burstel-cli shot query "$SHOT" \
     --sql "SELECT count(*) AS stored_spans FROM spans WHERE trace_id IS NOT NULL" \
     --base-url https://<host> --token "$BURSTEL_TOKEN"
   ```

Acceptance measurement:

- `world_end = stop time` (or the loadgen's reported end), and
  `T_materialize = /tmp/materialized.ms - /tmp/shot-stop-end.ms`; require
  `T_materialize ≤ 60 s`.
- `T_analyze` from step 6; require `≤ 15 s` after materialization completes.
- `stored_spans == exported_spans` for `run_id`.

Record all three numbers in the next revision of this file. A failed or
truncated journal enumeration sets `materialization.state = failed`; it must
never be reported as complete.

## Tests added

- `internal/compaction/segment_appender_test.go`: appender row coverage,
  transaction rollback on a duplicate source key, empty part support, and the
  `BenchmarkBuildSegmentAppender` throughput benchmark.
- `internal/compaction/materialize_test.go`: direct-R2 part build, checksummed
  manifest, deterministic re-materialization, exclusion handling.
- `internal/r2s3/r2s3_test.go`: SigV4 signing + GET/PUT against an httptest S3.
- `internal/analysis/parts_test.go`: ATTACH/UNION ALL analysis over multiple
  materialization parts.
- `cloudflare/test/materialize.test.ts`: plan bounds, byte chunking,
  exclusions, distinct materializer DO names, part manifest, idempotent
  re-run, lazy wait, and compaction yield.
- `cloudflare/test/compactor.test.ts`: compaction yields during an active
  materialization and resumes afterwards.
