#!/usr/bin/env bash
#
# local-trial.sh - reproducible standalone field trial for burstel.
#
# Runs, entirely on loopback and without Cloudflare/R2/Docker/root:
#   1. a standalone burstel recorder (127.0.0.1, temp data dir)
#   2. a real OTel-Go-SDK instrumented HTTP workload (cmd/burstel-trial)
#      exporting traces + metrics + correlated logs over OTLP/HTTP protobuf
#   3. two bounded shots: baseline and a deliberately regressed shot. One fresh
#      recorder process per shot (a recorder process records a single shot).
#   4. a hard recorder shutdown, then an independent analysis process
#      (cmd/burstel-inspect) that restores the frozen DuckDB by streaming its
#      bytes with size + SHA-256 headers and calls the real read-only
#      query/analyze/compare endpoints.
#
# Usage:
#   scripts/local-trial.sh [--baseline 60] [--regressed 180] [--rate 15]
#                          [--root .trial] [--smoke] [--keep]
#
# Everything is bounded; the recorder is killed on exit and artifacts live
# under --root (gitignored).
set -euo pipefail

BASELINE_SECONDS=60
REGRESSED_SECONDS=180
RATE=15
ROOT=".trial"
SMOKE=0
KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --baseline) BASELINE_SECONDS="$2"; shift 2 ;;
    --regressed) REGRESSED_SECONDS="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --root) ROOT="$2"; shift 2 ;;
    --smoke) SMOKE=1; shift ;;
    --keep) KEEP=1; shift ;;
    -h|--help)
      sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ "$SMOKE" = "1" ]; then
  BASELINE_SECONDS=10
  REGRESSED_SECONDS=20
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
ROOT="$(mkdir -p "$ROOT" && cd "$ROOT" && pwd)"
BIN="$ROOT/bin"
DATA="$ROOT/data"
LOGS="$ROOT/logs"
OUT="$ROOT/out"
rm -rf "$BIN" "$DATA" "$LOGS" "$OUT"
mkdir -p "$BIN" "$DATA" "$LOGS" "$OUT"

# ---------------------------------------------------------------------------
# Toolchain detection (Go+CGO may live in a local store rather than on PATH).
# ---------------------------------------------------------------------------
find_tool() {
  local name="$1" fallback_glob="$2"
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return
  fi
  # shellcheck disable=SC2086
  ls -d $fallback_glob 2>/dev/null | sort -V | tail -1
}
GO_BIN="$(find_tool go '/nix/store/*go-1*/bin/go')"
CC_BIN="$(find_tool gcc '/nix/store/*gcc-wrapper*/bin/gcc')"
CXX_BIN="$(find_tool g++ '/nix/store/*gcc-wrapper*/bin/g++')"
if [ -z "$GO_BIN" ] || [ -z "$CC_BIN" ]; then
  echo "local-trial: could not locate go/gcc (set PATH or CC/GO explicitly)" >&2
  exit 1
fi
export CC="$CC_BIN" CXX="$CXX_BIN" CGO_ENABLED=1

# ---------------------------------------------------------------------------
# Ports: choose free loopback ports to avoid collisions.
# ---------------------------------------------------------------------------
free_port() {
  python3 - "$1" <<'PY'
import socket, sys
start = int(sys.argv[1])
for p in range(start, start + 200):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", p))
        s.close()
        print(p)
        raise SystemExit(0)
    except OSError:
        s.close()
raise SystemExit(1)
PY
}
PORT="$(free_port 15318)"
CHECK_PORT="$(free_port $((PORT + 1)))"
TOKEN="trial-$(date +%s)-$$"
export BURSTEL_INTERNAL_TOKEN="$TOKEN"
CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# ---------------------------------------------------------------------------
# Build.
# ---------------------------------------------------------------------------
echo "== building (go=$(basename "$GO_BIN"), cc=$(basename "$CC_BIN")) =="
"$GO_BIN" build -o "$BIN/burstel" ./cmd/burstel
"$GO_BIN" build -o "$BIN/burstel-trial" ./cmd/burstel-trial
"$GO_BIN" build -o "$BIN/burstel-inspect" ./cmd/burstel-inspect
"$GO_BIN" build -o "$BIN/otlpsend" ./cmd/otlpsend

REC_PID=""
SAMPLER_PID=""
cleanup() {
  stop_recorder >/dev/null 2>&1 || true
  if [ "$KEEP" != "1" ]; then
    rm -rf "$DATA" "$BIN"
  else
    echo "kept artifacts under $ROOT"
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Resource sampler: peak RSS and CPU seconds of a recorder process.
# ---------------------------------------------------------------------------
sample_resources() {
  local pid="$1" out="$2"
  echo "epoch_ms,rss_kb,cpu_ticks" > "$out"
  while kill -0 "$pid" 2>/dev/null; do
    local rss ticks
    rss="$(awk '/VmRSS/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)"
    ticks="$(awk '{print $14+$15}' "/proc/$pid/stat" 2>/dev/null || echo 0)"
    printf '%s,%s,%s\n' "$(date +%s%3N)" "${rss:-0}" "${ticks:-0}" >> "$out"
    sleep 0.5
  done
}

start_recorder() {
  local tag="$1"
  "$BIN/burstel" -addr "127.0.0.1:$PORT" -data-dir "$DATA" >"$LOGS/recorder.$tag.log" 2>&1 &
  REC_PID=$!
  sample_resources "$REC_PID" "$OUT/recorder.$tag.resources.csv" &
  SAMPLER_PID=$!
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && break
    sleep 0.1
  done
  if ! curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
    echo "local-trial: recorder ($tag) did not become healthy" >&2
    exit 1
  fi
  echo "   recorder up on 127.0.0.1:$PORT (pid $REC_PID)"
}

stop_recorder() {
  if [ -n "$SAMPLER_PID" ]; then
    kill "$SAMPLER_PID" 2>/dev/null || true
    wait "$SAMPLER_PID" 2>/dev/null || true
    SAMPLER_PID=""
  fi
  if [ -n "$REC_PID" ] && kill -0 "$REC_PID" 2>/dev/null; then
    kill -TERM "$REC_PID" 2>/dev/null || true
    for _ in $(seq 1 100); do kill -0 "$REC_PID" 2>/dev/null || break; sleep 0.1; done
    kill -KILL "$REC_PID" 2>/dev/null || true
    wait "$REC_PID" 2>/dev/null || true
  fi
  REC_PID=""
}

curl_internal() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -o /dev/null -w '%{http_code}' -X "$method" \
      -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
      --data "$body" "http://127.0.0.1:$PORT$path"
  else
    curl -sS -o /dev/null -w '%{http_code}' -X "$method" \
      -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$PORT$path"
  fi
}

# ---------------------------------------------------------------------------
# One shot: start -> real workload -> force flush (in trial) -> stop.
# ---------------------------------------------------------------------------
run_shot() {
  local name="$1" seconds="$2" mode="$3"
  echo "== shot $name ($mode, ${seconds}s, target ${RATE} req/s) =="

  local start_code
  start_code="$(curl_internal POST /internal/start "{\"shot_id\":\"$name\",\"metadata\":{\"mode\":\"$mode\",\"seconds\":$seconds,\"rate\":$RATE}}")"
  if [ "$start_code" != "200" ]; then
    echo "local-trial: start $name returned $start_code" >&2
    exit 1
  fi

  # Invalid export payload must be rejected while recording, not accepted.
  local invalid_code
  invalid_code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/x-protobuf' --data-binary $'\xff\x00\xff\x00\xff\x00' \
    "http://127.0.0.1:$PORT/v1/traces")"
  printf '%s\n' "$invalid_code" > "$OUT/${name}.invalid_payload_status"

  local t0 t1 t2 freeze_ms
  t0="$(date +%s%3N)"
  "$BIN/burstel-trial" \
    -burstel-url "http://127.0.0.1:$PORT" \
    -duration "${seconds}s" -rate "$RATE" -mode "$mode" \
    -report "$OUT/${name}.trial.json" \
    >"$OUT/${name}.trial.stdout" 2>"$LOGS/${name}.trial.log"
  t1="$(date +%s%3N)"

  local stop_json
  stop_json="$(curl -sS -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    --data "{\"shot_id\":\"$name\"}" "http://127.0.0.1:$PORT/internal/stop")"
  t2="$(date +%s%3N)"
  freeze_ms=$((t2 - t1))
  echo "$stop_json" > "$OUT/${name}.stop.json"

  curl -sS -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$PORT/internal/metadata" \
    > "$OUT/${name}.metadata.json"

  # Ingest after stop must be refused. Empty body is a valid empty
  # ExportLogsServiceRequest, so the 503 reflects recorder state, not decode.
  local after_stop_code
  after_stop_code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/x-protobuf' --data-binary '' \
    "http://127.0.0.1:$PORT/v1/logs")"
  printf '%s\n' "$after_stop_code" > "$OUT/${name}.ingest_after_stop_status"

  local artifact="$DATA/$name/telemetry.duckdb"
  local size sha
  size="$(python3 -c "import json;print(json.load(open('$OUT/${name}.stop.json'))['size_bytes'])")"
  sha="$(python3 -c "import json;print(json.load(open('$OUT/${name}.stop.json'))['sha256'])")"
  printf '%s\n' "$freeze_ms" > "$OUT/${name}.freeze_ms"
  printf '%s\n' "$artifact" > "$OUT/${name}.artifact_path"
  printf '%s\n' "$(( (t1 - t0) / 1000 ))" > "$OUT/${name}.workload_seconds"
  du -sb "$DATA/$name" > "$OUT/${name}.du_bytes" 2>/dev/null || true
  echo "   workload=$(( (t1 - t0) / 1000 ))s freeze=${freeze_ms}ms artifact_bytes=$size sha=${sha:0:12} invalid_payload=$invalid_code ingest_after_stop=$after_stop_code"
}

start_recorder baseline
run_shot baseline "$BASELINE_SECONDS" baseline
stop_recorder

start_recorder regressed
run_shot regressed "$REGRESSED_SECONDS" regressed
stop_recorder

# Compute per-recorder peak RSS and CPU seconds.
for tag in baseline regressed; do
  python3 - "$OUT/recorder.$tag.resources.csv" "$CLK_TCK" > "$OUT/recorder.$tag.resources.json" <<'PY'
import csv, json, sys
rows = list(csv.DictReader(open(sys.argv[1])))
clk = float(sys.argv[2])
rss = [int(r["rss_kb"]) for r in rows] or [0]
ticks = [int(r["cpu_ticks"]) for r in rows] or [0]
print(json.dumps({
    "samples": len(rows),
    "peak_rss_kb": max(rss),
    "peak_rss_mib": round(max(rss)/1024, 2),
    "cpu_seconds": round((max(ticks)-min(ticks))/clk, 3),
}))
PY
done
echo "   recorder baseline: $(cat "$OUT/recorder.baseline.resources.json")"
echo "   recorder regressed: $(cat "$OUT/recorder.regressed.resources.json")"

# ---------------------------------------------------------------------------
# Two independent recorder processes, different shots, must both freeze.
# ---------------------------------------------------------------------------
echo "== two-recorder isolation check =="
D2="$ROOT/data2"
mkdir -p "$D2"
start_recorder two-a
"$BIN/burstel" -addr "127.0.0.1:$CHECK_PORT" -data-dir "$D2" >"$LOGS/recorder2.log" 2>&1 &
REC2_PID=$!
for _ in $(seq 1 100); do curl -sf "http://127.0.0.1:$CHECK_PORT/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
"$BIN/otlpsend" -url "http://127.0.0.1:$PORT" -shot iso-a -token "$TOKEN" >"$OUT/iso-a.log" 2>&1 || true
"$BIN/otlpsend" -url "http://127.0.0.1:$CHECK_PORT" -shot iso-b -token "$TOKEN" >"$OUT/iso-b.log" 2>&1 || true
kill -TERM "$REC2_PID" 2>/dev/null || true
wait "$REC2_PID" 2>/dev/null || true
stop_recorder
ISO_A="missing"; ISO_B="missing"
[ -f "$DATA/iso-a/telemetry.duckdb" ] && ISO_A="frozen"
[ -f "$D2/iso-b/telemetry.duckdb" ] && ISO_B="frozen"
printf 'recorder_a=%s\nrecorder_b=%s\n' "$ISO_A" "$ISO_B" > "$OUT/two_recorders.txt"
echo "   recorder A iso-a: $ISO_A ; recorder B iso-b: $ISO_B (want frozen/frozen)"

# ---------------------------------------------------------------------------
# Independent analysis process: restore + query + analyze + compare.
# ---------------------------------------------------------------------------
BASE_ART="$(cat "$OUT/baseline.artifact_path")"
REG_ART="$(cat "$OUT/regressed.artifact_path")"
echo "== independent analysis (cmd/burstel-inspect, new process) =="
"$BIN/burstel-inspect" \
  -left "$BASE_ART" -left-id baseline \
  -right "$REG_ART" -right-id regressed \
  -top 10 -out "$OUT/analysis.json" \
  > "$OUT/analysis.txt" 2> "$LOGS/inspect.log"
echo "   analysis written to $OUT/analysis.txt and $OUT/analysis.json"

# ---------------------------------------------------------------------------
# Consolidated summary from the raw outputs.
# ---------------------------------------------------------------------------
python3 - "$OUT" > "$OUT/summary.txt" <<'PY'
import json, os, sys
out = sys.argv[1]
def load(name):
    p = os.path.join(out, name)
    try:
        return json.load(open(p))
    except Exception:
        return None
def read(name):
    try:
        return open(os.path.join(out, name)).read().strip()
    except Exception:
        return None
lines = []
for name in ("baseline", "regressed"):
    trial = load(name + ".trial.json") or {}
    stop = load(name + ".stop.json") or {}
    meta = load(name + ".metadata.json") or {}
    res = load("recorder." + name + ".resources.json") or {}
    counters = meta.get("counters", {})
    seconds = float(trial.get("duration_actual_seconds") or 0)
    batches = counters.get("records", {})
    exp = trial.get("otlp_export", {})
    lines.append(f"[{name}]")
    lines.append(f"  workload_seconds={seconds:.3f} target_rps={trial.get('target_rps')}")
    lines.append(f"  app_attempted={trial.get('attempted')} success={trial.get('success')} deliberate_errors={trial.get('deliberate_errors')} transport_errors={trial.get('transport_errors')}")
    lines.append(f"  app_latency_ms p50={trial.get('latency',{}).get('p50_ms')} p95={trial.get('latency',{}).get('p95_ms')} p99={trial.get('latency',{}).get('p99_ms')}")
    lines.append(f"  emitted={trial.get('emitted')}")
    lines.append(f"  exporter_requests={exp.get('requests_by_path')} statuses={exp.get('status_by_path')} transport_errors={exp.get('transport_errors')}")
    lines.append(f"  frozen size_bytes={stop.get('size_bytes')} sha256={stop.get('sha256')}")
    lines.append(f"  freeze_ms={read(name + '.freeze_ms')} du_bytes={read(name + '.du_bytes')}")
    lines.append(f"  stored counters: batches={counters.get('batches')} records={counters.get('records')} spans={counters.get('spans')} log_records={counters.get('log_records')} metric_points={counters.get('metric_points')}")
    lines.append(f"  recorder_resources={res}")
    if seconds > 0:
        lines.append(f"  ingest_records_per_s={sum(batches.values())/seconds:.2f} batches_per_s={sum(counters.get('batches',{}).values())/seconds:.2f}")
    lines.append(f"  invalid_payload_status={read(name + '.invalid_payload_status')} ingest_after_stop_status={read(name + '.ingest_after_stop_status')}")
lines.append("[two_recorders]")
lines.append("  " + (read("two_recorders.txt") or "").replace("\n", " "))
print("\n".join(lines))
PY
echo
echo "== summary =="
cat "$OUT/summary.txt"
echo
echo "== done =="
echo "outputs: $OUT"
if [ "$KEEP" = "1" ]; then
  echo "artifacts kept: $DATA/baseline/telemetry.duckdb $DATA/regressed/telemetry.duckdb"
fi
