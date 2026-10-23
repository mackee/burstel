# Standalone ISUCON-style field trial (no Cloudflare)

Task **004** validates burstel's Go recorder + DuckDB analyzer as a standalone
tool: a real OpenTelemetry Go SDK workload exports traces, metrics and
correlated logs over OTLP/HTTP protobuf to a loopback `burstel` process, which
freezes a DuckDB shot. A separate process then restores the frozen file and runs
the real read-only analysis API.

No Cloudflare Worker, R2 bucket, remote MCP server, Docker or root privileges
are involved. This is an **operational trial with real numbers**, not a mock
unit test.

## Commands / how to reproduce

Host toolchain was located through the BuildHost Nix store (see `AGENTS.md`);
`scripts/local-trial.sh` detects `go`/`gcc` on `PATH` first and falls back to the
store paths.

```bash
# from the repository root
export PATH=/nix/store/<hash>-go-1.25.5/bin:/nix/store/<hash>-gcc-wrapper-15.2.0/bin:$PATH
export CC=/nix/store/<hash>-gcc-wrapper-15.2.0/bin/gcc
export CXX=/nix/store/<hash>-gcc-wrapper-15.2.0/bin/g++
export CGO_ENABLED=1

# fast smoke (~30 s wall, 10 s + 20 s shots)
scripts/local-trial.sh --smoke

# the reported 60 s baseline + 180 s regressed trial
scripts/local-trial.sh --baseline 60 --regressed 180 --rate 15 --keep
```

`scripts/local-trial.sh` builds `cmd/burstel`, `cmd/burstel-trial`,
`cmd/burstel-inspect` and `cmd/otlpsend`, starts a loopback-only recorder,
records each shot in a fresh recorder process, force-flushes the SDK, stops the
shot, kills the recorder, verifies the PID is gone, runs a two-recorder
isolation check, then analyses the frozen artifacts in a brand-new process.
Outputs land in `.trial/out/` (gitignored) as JSON, text and CSV.

### Manual quickstart (analyse an existing frozen shot locally)

```bash
go build -o /tmp/burstel ./cmd/burstel
go build -o /tmp/burstel-inspect ./cmd/burstel-inspect

# 1. record a shot (loopback only)
BURSTEL_INTERNAL_TOKEN=dev-secret /tmp/burstel -addr 127.0.0.1:4318 -data-dir /tmp/burstel-data &
curl -sf -X POST -H 'Authorization: Bearer dev-secret' -H 'Content-Type: application/json' \
  -d '{"shot_id":"demo"}' http://127.0.0.1:4318/internal/start

# 2. run the real OTel SDK workload against it
go run ./cmd/burstel-trial -burstel-url http://127.0.0.1:4318 -duration 60s -rate 15 -mode baseline

# 3. freeze, then stop the recorder
curl -sf -X POST -H 'Authorization: Bearer dev-secret' -H 'Content-Type: application/json' \
  -d '{"shot_id":"demo"}' http://127.0.0.1:4318/internal/stop
kill %1

# 4. analyse the frozen DuckDB in an independent process
/tmp/burstel-inspect -left /tmp/burstel-data/demo/telemetry.duckdb -left-id demo -top 10
```

`burstel-inspect` starts its own loopback analysis server, streams the frozen
file into `POST /internal/restore` with `X-Burstel-Size` / `X-Burstel-Sha256`
headers, and then calls the real `/internal/query`, `/internal/analyze` and
`/internal/compare` endpoints. It does not reimplement the analytics.

## Host / toolchain baseline

| Item | Value |
| --- | --- |
| Kernel | `Linux vmi3091988 6.12.111+deb13-cloud-amd64 x86_64` |
| CPU | 4 vCPU (`nproc=4`) |
| RAM | 8,138,616 kB total (~7.8 GiB) |
| Go | `go1.26.0 linux/amd64` (module `go 1.26.0`) |
| C compiler | `gcc (GCC) 15.2.0`, cgo enabled |
| DuckDB | `github.com/duckdb/duckdb-go/v2 v2.10506.0` (prebuilt static lib) |
| OTel Go SDK | `go.opentelemetry.io/otel* v1.47.0`, `otlploghttp v0.23.0`, `otelhttp v0.72.0` |
| Repo revision | `c07f57b5cb9d050e3b61e3e9242a924751cde5e6` (verified `origin/main`; Phase 3 baseline) |
| Clock | `CLK_TCK=100` |

## What was actually built

- **`cmd/burstel-trial`** — in-process instrumented HTTP application plus a
  bounded load generator. It exports with the official OTel Go SDK:
  - **Traces**: `sdktrace` with `AlwaysSample` (no intentional drops). Server
    spans come from `otelhttp.NewHandler`; every items request fans out into
    `db.query items` nested CLIENT spans (N+1). A deliberate `/api/error` route
    produces an ERROR SERVER span and a failing CLIENT span.
  - **Metrics**: `sdkmetric` periodic reader; explicit request count/duration and
    simulated-DB count/duration instruments, plus the `otelhttp` HTTP metrics.
  - **Logs**: `sdklog` batch processor + `otlploghttp`. The SDK attaches the
    active span's trace/span id from the context, so logs correlate to traces
    without bespoke plumbing.
  - All three exporters share one counting `http.RoundTripper`, so the report
    contains real export request counts, per-signal HTTP status counts and
    transport errors (retries are extra round trips).
  - The simulated database is explicitly labelled (`db.system=burstel.simulated-db`,
    `db.simulated=true`) and is **not** a real database.
- **`cmd/burstel-inspect`** — independent analysis process driving the real
  internal REST analysis surface.
- **`scripts/local-trial.sh`** — full loopback orchestration with cleanup,
  free-port selection, resource sampling and failure probes.
- **`internal/server/failure_modes_test.go`** — integration checks for invalid
  payloads, ingest-after-stop, double stop, corrupt restore, dangerous SQL and
  two isolated recorders.

### Workload parameters

| Parameter | baseline | regressed |
| --- | --- | --- |
| Duration | 60 s | 180 s |
| Target rate | 15 req/s | 15 req/s |
| N+1 DB fan-out per items request | 4 | 16 |
| Simulated DB latency per query | 1 ms | 5 ms |
| Deliberate error-route weight | 2 % | 5 % |
| `/api/items/{id}` / `/api/summary` weight | 73 % / 25 % | 70 % / 25 % |
| Sampling | AlwaysSample | AlwaysSample |
| Metric export interval | 5 s | 5 s |

## Raw results (full 60 s + 180 s run)

All values below are from the real, completed run. Raw JSON is under
`.trial/out/` (not committed).

### Workload and exporter

| Metric | baseline | regressed |
| --- | --- | --- |
| Wall-clock workload duration | 60.001 s | 180.021 s |
| Application requests attempted | 899 | 2,699 |
| Application 2xx success | 892 | 2,547 |
| Deliberate 500 (`/api/error`) | 7 | 152 |
| Unexpected errors | 0 | 0 |
| Transport errors (load client) | 0 | 0 |
| App-side latency p50 / p95 / p99 (ms) | 5.733 / 8.295 / 14.504 | 89.439 / 96.472 / 108.207 |
| Server spans emitted | 899 | 2,699 |
| DB CLIENT spans emitted | 3,203 | 31,748 |
| Log records emitted | 899 | 2,699 |
| SDK metric measurements recorded | 8,204 | 68,894 |
| OTLP traces export requests (all HTTP 200) | 30 | 91 |
| OTLP metrics export requests (all 200) | 14 | 38 |
| OTLP logs export requests (all 200) | 30 | 90 |
| OTLP transport errors | 0 | 0 |

App-side latency uses nearest-rank percentiles (ceil(p·n)). DuckDB
`quantile_cont` values below are the authoritative engine-side numbers.

### Frozen artifact, ingest and resource use

| Metric | baseline | regressed |
| --- | --- | --- |
| Frozen size (`telemetry.duckdb`) | 5,517,312 B (5.26 MiB) | 35,926,016 B (34.26 MiB) |
| SHA-256 | `e6ac4351473392d4…81a209` | `e0225c8faade2ff8…17e469` |
| Freeze latency (stop → frozen) | 164 ms | 371 ms |
| Recorder peak RSS | 106.12 MiB | 191.17 MiB |
| Recorder CPU time | 8.17 s | 54.34 s |
| Stored OTLP batches (traces/logs/metrics) | 30 / 30 / 14 | 91 / 90 / 38 |
| Stored spans | 4,102 | 34,447 |
| Stored log records | 899 | 2,699 |
| Stored metric point rows | 238 | 646 |
| Stored trace records (`records`) | 4,102 | 34,447 |
| Ingest records/s (batches' `record_count`) | 87.32 | 209.93 |
| Ingest batches/s | 1.23 | 1.22 |
| OTLP payload bytes received | 1,584,678 | 10,961,028 |
| Ingest payload throughput | ≈ 26.4 KiB/s | ≈ 60.9 KiB/s |
| Disk used by shot dir | 5.52 MB | 35.93 MB |

Formulas: `records/s = Σ batch.record_count / wall_seconds`;
`payload throughput = Σ otlp_batches.payload_length / wall_seconds`;
`freeze latency` = wall time of `POST /internal/stop`.

### Emitted vs stored (DuckDB is the source of truth)

| Signal | baseline emitted / stored | regressed emitted / stored | Agreement |
| --- | --- | --- | --- |
| Server spans | 899 / 899 | 2,699 / 2,699 | exact |
| DB CLIENT spans | 3,203 / 3,203 | 31,748 / 31,748 | exact |
| Total spans | 4,102 / 4,102 | 34,447 / 34,447 | exact |
| Log records | 899 / 899 | 2,699 / 2,699 | exact |
| Metric rows | 8,204 measurements / 238 rows | 68,894 measurements / 646 rows | expected mismatch, see caveat |

No exporter drops or retries were observed at 15 req/s: every accepted batch
was committed to the frozen file, and all OTLP responses were `200`. Metric
measurements are not directly comparable to stored rows: OTLP metrics are
exported with cumulative temporality, so each 5 s export re-serialises the
current series state and the row count reflects exported series-instants, not
individual measurements.

### Analysis output (from `cmd/burstel-inspect`)

Sample sizes: baseline spans 4,102 (899 traces); regressed spans 34,447
(2,699 traces). Summed span-seconds: 8.662 s vs 359.482 s.

Comparison (right vs left), sorted by |Δp95|:

| service / operation | n left→right | p50 (ms) | p95 (ms) | p99 (ms) | Δp95 |
| --- | --- | --- | --- | --- | --- |
| `GET /api/items/{id}` | 706 → 1,893 | 5.142 → 89.643 | 7.393 → 97.254 | 11.239 → 110.319 | **+1215.5 %** |
| `GET /api/summary` | 186 → 654 | 2.620 → 11.277 | 4.595 → 12.758 | 6.726 → 17.193 | +177.7 % |
| `GET /api/error` | 7 → 152 | 1.359 → 5.646 | 1.707 → 6.436 | 1.722 → 7.298 | +277.0 % |
| `db.query failing` (CLIENT) | 7 → 152 | 1.250 → 5.446 | 1.545 → 6.114 | 1.591 → 6.615 | +295.7 % |
| `db.query items` (CLIENT) | 3,196 → 31,596 | 1.196 → 5.485 | 1.806 → 6.263 | 3.211 → 7.965 | +246.8 % |

The N+1 fan-out is visible: the maximum number of CLIENT spans in a single
trace is 4 (baseline) versus 16 (regressed). Error concentration is visible on
both the SERVER (`GET /api/error`) and CLIENT (`db.query failing`) spans
(7 → 152 errors, 100 % of those operations). Logs correlate by trace id
(`log_correlation` sample 899 / 2,699 records) and severity split is
INFO 892 / ERROR 7 (baseline) and INFO 2,547 / ERROR 152 (regressed). Metrics
are queryable per instrument, type and aggregations (`metrics` section, 7
instrument groups each).

The analyzer correctly diagnoses the deliberately regressed shot as slower on
both HTTP and DB-like child spans, with real numbers and no misleading
prioritisation. The `compare` disclosure correctly warns that sample sizes
differ and that deltas are observations, not causes.

### Failure-mode probes

| Probe | Result |
| --- | --- |
| Invalid export payload while recording | HTTP 400 |
| Ingest after stop | HTTP 503 |
| Double stop idempotent (Go test) | same size/SHA |
| Corrupt / wrong-size / wrong-SHA restore (Go test) | HTTP 400 with stable code |
| Dangerous read-only SQL: file/network/mutation/unknown table (Go test) | HTTP 400 |
| Two independent recorder processes, different shots | both frozen |

> The post-stop row records the behavior at trial time. Since spec 022 (rev 2)
> the standalone recorder answers exports outside an active shot with `200` +
> OTLP `partial_success` (`"no active shot; data discarded"`) and counts them in
> `/internal/metadata` as `dropped_outside_shot`, so a default-BSP exporter never
> enters a retry/backoff storm after `shot stop`.

## Test commands

```text
$ go vet ./...
EXIT 0

$ go test -count=1 ./...
ok  github.com/mackee/burstel/internal/analysis   0.566s
ok  github.com/mackee/burstel/internal/otlp       0.016s
ok  github.com/mackee/burstel/internal/recorder   0.082s
ok  github.com/mackee/burstel/internal/server     1.985s
ok  github.com/mackee/burstel/cmd/burstel-trial   0.022s
(remaining packages: no test files)

$ (cd cloudflare && bun install --frozen-lockfile && bun run typecheck && bun run typecheck:test && bun test)
tsc --noEmit          : exit 0
tsc -p tsconfig.test  : exit 0
bun test              : 39 pass, 0 fail, 192 expect() calls
```

Docker was **not** run: there is no Docker/Podman on this host and no root, so
the `Dockerfile` is not validated here. Cloudflare deploy was **not** run and no
Cloudflare credentials were used.

## What demonstrably worked

- Real OTel Go SDK exporter traffic (traces + metrics + logs, OTLP/HTTP
  protobuf) reaches the standalone recorder and lands in DuckDB with exact
  emitted-vs-stored counts and no loss at the tested rate.
- Nested SERVER → CLIENT spans, trace correlation, error status and log
  correlation survive the round trip.
- Freeze (CHECKPOINT + SHA-256) completes in sub-second time for 5–36 MB shots.
- A fresh, independent process can restore by streaming bytes with size + SHA-256
  and run the real read-only query/analyze/compare engine on the frozen file
  after the recorder PID is gone.
- The deliberately regressed shot is detected and quantified, and the N+1
  pattern is visible.

## What failed / was not proven

- The database is **simulated in-process**, not a real DuckDB/SQLite workload.
  The N+1 shape is synthetic; real driver/semconv instrumentation was not
  exercised.
- Analysis in this trial goes through burstel's local Go REST endpoints. The
  remote Cloudflare MCP path is **not** proven by this trial.
- The workload is not a real ISUCON application; route mix and latency are
  synthetic.
- Rate was bounded at 15 req/s on a 4-vCPU host. Exporter loss/retry behaviour
  under saturation was not measured.
- Metric row counts are cumulative re-exports and must not be read as
  measurement counts.
- Only one shot per recorder process is possible; the script starts a fresh
  recorder per shot. This is existing Phase 1 behaviour, documented here.

## Triage list of findings

1. **One shot per process.** `POST /internal/start` returns `409` after a shot is
   frozen, so sequential shots require a process restart. Acceptable for the
   Cloudflare per-shot container model, but surprising for standalone use;
   candidate follow-up: allow an explicit reset/close-shot endpoint.
2. **Freeze cost scales with file size** (164 ms at 5.5 MB, 371 ms at 35.9 MB),
   dominated by `CHECKPOINT` + full-file SHA-256. For multi-GB shots this may
   exceed the analysis/write timeout; a streaming/incremental checksum or
   reported-size-only mode could help.
3. **Metric semantics.** Stored `metric_points` rows are export instants, not
   measurements. The analysis `metrics` section should document temporality so
   users do not read cumulative sums as per-shot totals.
4. **Simulated DB labels are good** (`db.system`, `db.operation.name`,
   `db.simulated`) and the `db_operations` section found them, but the section
   matches any CLIENT span; a real DB would also carry `db.system` from semconv
   so this is forward-compatible.
5. **No per-row loss detection.** The trial proves count agreement by comparing
   emitted counters to DuckDB, but does not decode every raw batch back to the
   SDK's emitted set (the recorder already has a fixture-level round-trip test).
   A payload-level reconciliation would strengthen future trials.
6. **`burstel-inspect` starts an embedded HTTP server.** This reuses the real
   API but is a slightly unusual CLI shape; a direct engine mode could be added
   without changing the API.

## Feasibility judgment (standalone ISUCON use)

| Dimension | Verdict | Basis |
| --- | --- | --- |
| Ingestion fidelity | **PASS** | All three signals accepted; emitted == stored for spans/logs; all OTLP responses 200; 0 transport errors. |
| Stable 3-minute operation | **PASS** | 180 s regressed shot completed; recorder peak RSS 191 MiB, CPU 54 s, 36 MB disk; no drops/restarts. |
| Latency overhead | **WARN** | The recorder is not in the request path (exporter is async), but recording adds 8–54 s CPU and 106–191 MiB RSS over 60–180 s. Not proven under high load. |
| Freeze / restore | **PASS** | Freeze 164–371 ms; independent restore verified size + SHA-256 and analysed after recorder PID exit. |
| Analysis usefulness | **PASS** | Correctly found the injected regression (HTTP p95 +1215 %, DB p95 +247 %), N+1 (4→16), error concentration and log correlation, with sample-size disclosure. |
| Usability | **WARN** | `scripts/local-trial.sh` is one command, but one shot per process, simulated DB and local-only analysis (no remote MCP) limit real-world parity. |

**Overall: PASS with caveats** — standalone ingestion, freezing, restore and
analysis are demonstrably useful and correct for the tested bounded workload.
The simulated database, the bounded 15 req/s rate, the 4-vCPU host and the lack
of a remote-MCP/real-ISUCON run are explicit limitations, not hidden ones.
