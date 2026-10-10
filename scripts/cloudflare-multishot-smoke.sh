#!/usr/bin/env bash
#
# cloudflare-multishot-smoke.sh - bounded live two-shot isolation E2E against a
# deployed burstel Worker + Containers + R2 deployment (spec 013, Part A).
#
# It proves that two shots recording at the same time, each addressed by its own
# per-shot ingest token on the stable /ingest endpoint, keep strictly isolated
# DuckDB state:
#
#   1. create two shots concurrently (both `recording`);
#   2. push the OTLP fixtures with token A only;
#   3. assert shot A's counters advanced while shot B's stayed at zero;
#   4. push with token B and assert both advanced;
#   5. freeze both and assert independent frozen artifacts;
#   6. assert scope separation (/ingest rejects the admin token; /api/*
#      rejects a per-shot token).
#
# Secret confidentiality: tokens are read from the environment, passed to curl
# via `--config` process substitution (never argv) and to otlpsend via
# BURSTEL_PUSH_TOKEN, never echoed, never written to the repository, logs, URLs
# or result files.
#
# This script was NOT executed by the implementing agent: it needs the live
# Cloudflare deploy host (a real `wrangler deploy`) and Docker-built container.
#
# Usage:
#   BURSTEL_BASE_URL=https://burstel.example \
#   BURSTEL_API_TOKEN=... \
#   scripts/cloudflare-multishot-smoke.sh
#
# Optional: OTLPSEND_BIN (otherwise cmd/otlpsend is built with go).
set -euo pipefail

BASE="${BURSTEL_BASE_URL:-}"
ADMIN_TOKEN="${BURSTEL_API_TOKEN:-}"
[ -n "$BASE" ] || { echo "cloudflare-multishot-smoke: BURSTEL_BASE_URL is required" >&2; exit 2; }
[ -n "$ADMIN_TOKEN" ] || { echo "cloudflare-multishot-smoke: BURSTEL_API_TOKEN is required" >&2; exit 2; }
BASE="${BASE%/}"
case "$BASE" in
  https://*) ;;
  http://127.0.0.1*|http://localhost*) ;;
  *) echo "cloudflare-multishot-smoke: refusing a non-HTTPS base URL ($BASE) unless loopback" >&2; exit 2 ;;
esac

command -v curl >/dev/null 2>&1 || { echo "cloudflare-multishot-smoke: curl is required" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "cloudflare-multishot-smoke: python3 is required" >&2; exit 2; }

WORK="$(mktemp -d "${TMPDIR:-/tmp}/burstel-multishot.XXXXXX")"
chmod 700 "$WORK"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

BODY_FILE="$WORK/body"
die() { printf 'cloudflare-multishot-smoke: %s\n' "$*" >&2; exit 1; }

req() {
  local method="$1" path="$2" body="${3:-}" auth="${4:-}"
  local cfg
  cfg="$( {
    printf 'silent\nshow-error\nmax-time = 30\nrequest = "%s"\n' "$method"
    if [ -n "$auth" ]; then printf 'header = "%s"\n' "$auth"; fi
    if [ -n "$body" ]; then printf 'header = "Content-Type: application/json"\ndata-binary = "@-"\n'; fi
  } )"
  if [ -n "$body" ]; then
    HTTP_CODE="$(printf '%s' "$body" | curl --config <(printf '%s' "$cfg") -o "$BODY_FILE" -w '%{http_code}' "$BASE$path")"
  else
    HTTP_CODE="$(curl --config <(printf '%s' "$cfg") -o "$BODY_FILE" -w '%{http_code}' "$BASE$path")"
  fi
  BODY="$(cat "$BODY_FILE")"
}

jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print(d$1)" 2>/dev/null; }
expect_code() { [ "$HTTP_CODE" = "$1" ] || die "$2 returned HTTP $HTTP_CODE, want $1: $BODY"; }

ADMIN="Authorization: Bearer $ADMIN_TOKEN"

echo "== healthz ==" >&2
req GET /healthz
expect_code 200 "healthz"

echo "== create two concurrent shots ==" >&2
req POST /api/shots "{\"owner\":\"e2e-a\",\"metadata\":{\"source\":\"multishot-a\",\"ts\":\"$(date +%s)\"}}" "$ADMIN"
expect_code 201 "create shot A"
SHOT_A="$(printf '%s' "$BODY" | jget "['shot_id']")"
PUSH_A="$(printf '%s' "$BODY" | jget "['ingest_token']")"
[ -n "$SHOT_A" ] || die "no shot A id"
[ -n "$PUSH_A" ] || die "shot A did not disclose a per-shot token"

req POST /api/shots "{\"owner\":\"e2e-b\",\"metadata\":{\"source\":\"multishot-b\",\"ts\":\"$(date +%s)\"}}" "$ADMIN"
expect_code 201 "create shot B"
SHOT_B="$(printf '%s' "$BODY" | jget "['shot_id']")"
PUSH_B="$(printf '%s' "$BODY" | jget "['ingest_token']")"
[ -n "$SHOT_B" ] || die "no shot B id"
[ -n "$PUSH_B" ] || die "shot B did not disclose a per-shot token"
[ "$SHOT_A" != "$SHOT_B" ] || die "shots share an id"
[ "$PUSH_A" != "$PUSH_B" ] || die "shots share an ingest token"

echo "== both shots are recording simultaneously ==" >&2
for id in "$SHOT_A" "$SHOT_B"; do
  req GET "/api/shots/$id" "" "$ADMIN"
  expect_code 200 "status $id"
  [ "$(printf '%s' "$BODY" | jget "['state']")" = "recording" ] || die "shot $id is not recording: $BODY"
done

echo "== GET /api/shots lists both with attribution ==" >&2
req GET "/api/shots?owner=e2e-a" "" "$ADMIN"
expect_code 200 "list owner=e2e-a"
printf '%s' "$BODY" | grep -q "$SHOT_A" || die "list did not return shot A"
printf '%s' "$BODY" | grep -q "$SHOT_B" && die "owner filter leaked shot B"
req GET "/api/shots?owner=e2e-b" "" "$ADMIN"
expect_code 200 "list owner=e2e-b"
printf '%s' "$BODY" | grep -q "$SHOT_B" || die "list did not return shot B"

OTLPSEND_BIN="${OTLPSEND_BIN:-}"
if [ -z "$OTLPSEND_BIN" ]; then
  command -v go >/dev/null 2>&1 || die "go not found and OTLPSEND_BIN unset"
  OTLPSEND_BIN="$WORK/otlpsend"
  (cd "$(dirname "$0")/.." && go build -o "$OTLPSEND_BIN" ./cmd/otlpsend)
fi

echo "== token A only: shot A advances, shot B stays at zero ==" >&2
BURSTEL_PUSH_TOKEN="$PUSH_A" "$OTLPSEND_BIN" -ingest "$BASE" >/dev/null 2>"$WORK/a.err" \
  || die "otlpsend A failed: $(cat "$WORK/a.err")"

req GET "/api/shots/$SHOT_A" "" "$ADMIN"
expect_code 200 "status A after A-only push"
SPANS_A="$(printf '%s' "$BODY" | jget "['counters']['spans']")"
req GET "/api/shots/$SHOT_B" "" "$ADMIN"
expect_code 200 "status B after A-only push"
SPANS_B="$(printf '%s' "$BODY" | jget "['counters']['spans']")"
[ "$SPANS_A" -gt 0 ] || die "shot A recorded no spans ($SPANS_A)"
[ "$SPANS_B" = "0" ] || die "isolation breach: shot B saw $SPANS_B spans from token A"

echo "== token B: shot B advances too ==" >&2
BURSTEL_PUSH_TOKEN="$PUSH_B" "$OTLPSEND_BIN" -ingest "$BASE" >/dev/null 2>"$WORK/b.err" \
  || die "otlpsend B failed: $(cat "$WORK/b.err")"
req GET "/api/shots/$SHOT_B" "" "$ADMIN"
expect_code 200 "status B after B push"
SPANS_B2="$(printf '%s' "$BODY" | jget "['counters']['spans']")"
[ "$SPANS_B2" -gt 0 ] || die "shot B recorded no spans ($SPANS_B2)"

echo "== freeze both; independent artifacts ==" >&2
req POST "/api/shots/$SHOT_A/stop" "" "$ADMIN"
expect_code 200 "stop A"
SHA_A="$(printf '%s' "$BODY" | jget "['sha256']")"
SIZE_A="$(printf '%s' "$BODY" | jget "['size_bytes']")"
[ "$(printf '%s' "$BODY" | jget "['state']")" = "frozen" ] || die "shot A did not freeze"
req POST "/api/shots/$SHOT_B/stop" "" "$ADMIN"
expect_code 200 "stop B"
SHA_B="$(printf '%s' "$BODY" | jget "['sha256']")"
SIZE_B="$(printf '%s' "$BODY" | jget "['size_bytes']")"
[ "$(printf '%s' "$BODY" | jget "['state']")" = "frozen" ] || die "shot B did not freeze"
printf '%s' "$SHA_A" | grep -Eq '^[0-9a-f]{64}$' || die "shot A sha256 is not 64 hex chars"
printf '%s' "$SHA_B" | grep -Eq '^[0-9a-f]{64}$' || die "shot B sha256 is not 64 hex chars"
[ "$SIZE_A" -gt 0 ] || die "shot A size_bytes is not positive"
[ "$SIZE_B" -gt 0 ] || die "shot B size_bytes is not positive"

echo "== scope separation ==" >&2
req GET "/api/shots/$SHOT_A" "" "X-Burstel-Token: $PUSH_A"
expect_code 401 "per-shot token on admin route"
req POST /ingest/v1/logs '{}' "$ADMIN"
expect_code 401 "admin token on /ingest"

echo "== retired installation-wide token endpoint ==" >&2
req POST /api/ingest-token "" "$ADMIN"
expect_code 410 "retired ingest-token endpoint"

cat <<JSON
{"base_url":"$BASE","shot_a":"$SHOT_A","shot_b":"$SHOT_B","isolation":"ok","concurrent_recording":"ok","frozen_sha256_a":"$SHA_A","frozen_sha256_b":"$SHA_B","owner_filter":"ok","scope_separation":"ok","installation_token_retired":true}
JSON
echo "== cloudflare multishot smoke passed (no secret was printed) ==" >&2
