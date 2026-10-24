# Spec 034 result — materializer throughput

- Implementation commit: `43ec65531c6d04d0026268a6c20b4349bfa2ce14`
- Base: `origin/main` at `60c1847` (spec 033 result)
- Spec: `034-materializer-throughput.md`, revision 1
  (`5589c81097c02ad633658c8e0ef07d8277ac0c27acfe9f4855686fecc9e6f3cb`)
- Job: `cc20aa3e-a765-4d52-9c64-2392e7b463b3`
- Implementation: `7736f7be-bd71-46c7-a249-27d19fd40d21`
- No deployment was performed and no Cloudflare resource was touched.

## What changed

### 1. Projection-only parts (R1)

- `internal/compaction/part.go` adds a dedicated part builder. A shot part now
  contains `spans`, `log_records`, `metric_points`, `schema_meta`/`segment_meta`
  and a small `part_meta` table with one provenance row per journal object
  (`instance_id`, `source_key`, `request_id`, `signal`, `received_at`,
  `content_encoding`, `wire_sha256`, `payload_sha256`, `record_count`,
  `payload_length`, `resource_summary`).
- No `wire_payload`/`payload` BLOBs and **no secondary indexes** are created for
  a part. Continuous compaction segments keep their existing raw layout
  (`BuildSegment` unchanged).
- The part build is streamed: `JournalRecord` carries the wire/payload hashes and
  drops both blobs after decode, so a part never holds hundreds of MiB of raw
  payload in memory.
- Analysis is unchanged for callers. `BuildScopedEngine` inspects each attached
  file's tables (`information_schema.tables`) and, for a part, synthesizes the
  `otlp_batches` projection from `part_meta` (payload columns are empty BLOBs).
  Coverage, batch counts and `record_count` stay exact.
- `SegmentReader.Counts`/`WireHashes` fall back to `part_meta` for parts.

### 2. Instance size (R2)

- `CompactorDO` (continuous compaction + the fixed materializer pool) now uses
  `instance_type: standard-4`, configurable in `wrangler.jsonc`. `ShotDO` stays
  on `standard-1`.
- Preflight validates `container_compactor_instance_type`
  (`expectedCompactorInstanceType`, default `standard-4`).
- Cost is documented in `docs/cloudflare-deploy.md` §8: `standard-4` is 4 vCPU /
  12 GiB and bills at ~4× `standard-1` per second while a pool member is alive;
  the 10 min `MATERIALIZE_KEEPALIVE_MS` warm window dominates spend for
  intermittent shots, so operators can lower it (or the pool size) to trade
  latency for cost.

### 3. Parallel pipeline in Go (R3)

- R2 GETs are bounded (`MATERIALIZE_CONCURRENCY`, default 32) and verify size
  and SHA-256 before decode.
- Gunzip + protobuf decode run on GOMAXPROCS workers
  (`MATERIALIZE_DECODE_WORKERS`, default GOMAXPROCS), feeding an in-order reorder
  buffer so the appenders see deterministic, receive-time-ordered objects while
  peak memory stays proportional to the in-flight window, not the whole part.
- Each table is appended by exactly one goroutine/appender on its own DuckDB
  connection (configurable via `MATERIALIZE_APPEND_SHARDS` for future sizing;
  the default is 1 after measuring that DuckDB serializes writes per database).
  Rows are written from one contiguous `driver.Value` buffer per object.
- No secondary indexes are created during the build.

### 4. Row-based part sizing (R4)

- `cloudflare/src/materialize-part.ts` plans by **estimated spans**
  (`size / 1024 * spansPerKiB`) with `MATERIALIZE_PART_TARGET_SPANS=500000` and
  `MATERIALIZE_SPANS_PER_KIB=40` (measured: 196 MB gzip → ~8.0M spans). It still
  creates at least `MATERIALIZE_PARALLELISM × MATERIALIZE_PARTS_PER_MEMBER`
  contiguous parts for pool occupancy and caps at the Go service's 64 parts.
- `chunkMaterializationPlan` keeps the same signature plus an options argument;
  chunks now report `bytes` and `spans`.
- The part size ceiling is separate from segments:
  `MATERIALIZE_MAX_PART_BYTES` (default 1 GiB), read by the Go service and
  forwarded into the container env.

### 5. Timeouts (R5)

- `MaterializerCore` computes the request deadline per part:
  `MATERIALIZE_PART_TIMEOUT_MS` (120 s) +
  `MATERIALIZE_PART_TIMEOUT_PER_MIB_MS` (500 ms/MiB of input), capped by
  `MATERIALIZE_MAX_PART_TIMEOUT_MS` (10 min). It logs
  `materialize.part.timeout` with `input_bytes` and `timeout_ms`.
- The DO lease TTL default is raised to 300 s so a part can never outlive its
  lease and be rebuilt concurrently.
- The Go service write timeout is raised to 15 min.

### 6. Benchmark + docs (R6)

- `internal/compaction/materialize_bench_test.go`:
  `BenchmarkMaterializePartThroughput` builds a 96,000-span part with the ISUCON
  span shape (1 HTTP server root + 5 DB child spans per trace, `db.system`),
  gzip journal input and the in-memory store, reporting `spans/s` and the phase
  breakdown. `BenchmarkPartBuildLayout` compares the projection-only build with
  the pre-034 raw+indexes build.
- `cloudflare/README.md` documents the part layout, planning/tuning variables
  and timeout behaviour.

## Tests added or updated

- `internal/compaction/materialize_test.go`: projections-only schema (no
  `otlp_batches`, `part_meta` provenance, no payload columns, no indexes),
  configurable part size limit, and parallel-decode-vs-serial row equality.
- `internal/analysis/parts_test.go`: analysis over a projection-only part
  (`otlp_batches` synthesized from `part_meta`, full `Analyze` run).
- `cloudflare/test/materialize.test.ts`: row-based split (500k target, 64-part
  cap, custom spans/KiB).
- `cloudflare/test/materializer-core.test.ts`: timeout scaling base/per-MiB/cap
  and the logged effective timeout.
- `cloudflare/test/compactor-token.test.ts`: container env forwarding of the
  spec-034 tuning variables.
- `cloudflare/test/preflight.test.ts`: `standard-4` materializer instance and
  part limit bounds.

## Verification (mandatory gates, all green)

Run against implementation commit `43ec655`:

```text
gofmt -l cmd internal                                              exit 0 (no output)
go build ./...                                                     exit 0
go vet ./...                                                       exit 0
go test -count=1 ./...                                             exit 0
cd cloudflare && bun run typecheck                                 exit 0
cd cloudflare && bun run typecheck:test                            exit 0
cd cloudflare && bun test                                          exit 0 (265 pass, 4 skip)
cd cloudflare && bun run deploy:dry-run                            exit 0
scripts/cloudflare-preflight.sh --config-only --allow-placeholder-kv exit 0
scripts/check-public.sh                                            exit 0
```

No `wrangler deploy`, no Cloudflare API mutation and no R2/DO resource change
was performed.

## Measured throughput

Environment: 4 vCPU (`nproc=4`, `GOMAXPROCS=4`), AMD EPYC shared sandbox,
`go test -bench BenchmarkMaterializePartThroughput -benchtime 5x -cpu 4`.

| Metric | Value |
| --- | --- |
| Part input | 96,000 spans, 32 gzip journal objects, 978 KB gzip |
| Part output | ~3.85 MiB projection-only DuckDB |
| `spans/s` | **106k – 121k per container** (median ≈ 110k) in the memory-backed build benchmark |
| Build phases (96k spans) | GET ~0 ms, decode ~600–900 ms summed over 4 workers, append+flush ~520–840 ms, checkpoint ~4 ms, hash ~5 ms, total ~0.85 s |

`BenchmarkPartBuildLayout` (pure build, 48k spans, no R2):

| Layout | spans/s |
| --- | --- |
| projection-only, no indexes, 4 table appenders | **110,524** |
| pre-034 raw wire+payload + 3 secondary indexes | 84,920 |

Honest gap: the R6 target of **≥ 300k spans/s per container was not reached on
this 4-vCPU host**. Profiling shows the cost is dominated by DuckDB's row-wise
appender (`appendPartChunk` flush: ~3–4 µs per 25-column span row, serialized
inside one database) plus OTLP decode (~6 µs CPU per span, parallelized to
~1.5 µs wall). The projection-only layout removed the dominant extra work of the
old part (raw BLOBs + ART index maintenance) and lifted the build from the
spec-033 evidence of ~30k spans/s to ~110k/s, i.e. ~3.7×; it does not reach 300k
with the `database/sql` appender. The next lever would be a bulk/columnar ingest
path (Arrow / multi-threaded writer) or a narrower per-part projection, not part
sizing. At the pool level the current number is still sufficient for the shot
target: 8.0M spans / (3 × 110k/s) ≈ **24 s** of container wall time, which fits
the ≤ 60 s materialize window with margin, and a 500k-span part is ~20 MiB, so
16 parts use ~320 MiB of analysis download (fits the ≤ 15 s analyze target).

Reproduce:

```bash
scripts/dev-toolchain.sh --write && . .scratch/toolchain.env
go test -run '^$' -bench BenchmarkMaterializePartThroughput -benchtime 5x -cpu 4 ./internal/compaction/
go test -run '^$' -bench BenchmarkPartBuildLayout -cpu 4 ./internal/compaction/
```

## Staging procedure (operator, separate authorized deployment job)

Do not run this from the implementation job. It requires the normal deploy
authorization and the R2/bucket secrets documented in
`docs/cloudflare-deploy.md`.

1. Deploy the Worker at `43ec655` and confirm the effective vars:
   `MATERIALIZE_PARALLELISM=3`, `MATERIALIZE_MAX_INSTANCES=4`,
   `MATERIALIZE_PARTS_PER_MEMBER=2`, `MATERIALIZE_PART_TARGET_SPANS=500000`,
   `MATERIALIZE_SPANS_PER_KIB=40`, `MATERIALIZE_MAX_PART_BYTES=1073741824`,
   `MATERIALIZE_PART_TIMEOUT_MS=120000`,
   `MATERIALIZE_PART_TIMEOUT_PER_MIB_MS=500`,
   `MATERIALIZE_MAX_PART_TIMEOUT_MS=600000`, `MATERIALIZE_CONCURRENCY=32`,
   `MATERIALIZE_LEASE_TTL_MS=300000`, and that the `CompactorDO` container is
   `standard-4` while `ShotDO` stays `standard-1`.
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
   final `client_drop.per_exporter[*].samples` point. Requirement:
   traces `dropped == 0` for the full 180 s.
6. Record from `shot_get <shot-id>`:
   - `materialization.finished_at - stopped_at` must be **≤ 60 s**;
   - `materialization.spans == exported` and `materialization.parts_total`
     must be ≤ 64 with the largest part under the 500k-span target (check the
     `materialize.plan` log's `estimated_spans`/`max_part_spans`);
   - every part must be projection-only: its size stays near the modelled
     ~20 MiB per 500k spans (no multi-hundred-MB part) and the Worker/DuckDB log
     shows no `exceeds the ... limit` or `/v1/materialize timed out` errors.
   Run `shot analyze <shot-id>` and require **≤ 15 s**; confirm `wrangler tail`
   contains `materialize.plan`, `materialize.part.timeout`,
   `materialize.part.finish`, `analysis.window.open`,
   `analysis.part.download`, `analysis.window.commit` and
   `analysis.window.run`.
7. **Run 2 (180 s), immediately after Run 1:** repeat steps 4–6 with a new
   shot. Pool containers should still be warm (`cached_parts > 0` when
   re-analyzing a shot, no `There is no container instance` errors) and every
   threshold must hold again.
8. If a threshold fails, collect the per-part `materialize.part.*` logs
   (including `input_bytes`/`timeout_ms` and the phase timings) and the analysis
   timing block before changing tuning. Do not raise
   `MATERIALIZE_PARALLELISM` above `max_instances - 1`, and do not lower
   `MATERIALIZE_MAX_PART_BYTES` below the observed largest part.
