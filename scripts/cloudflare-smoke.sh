#!/usr/bin/env bash
#
# cloudflare-smoke.sh - bounded live end-to-end smoke against a deployed burstel
# Worker + Containers + R2 deployment.
#
# It is deliberately small (two short shots, seconds not minutes) and exercises:
#   * GET /healthz
#   * anonymous/wrong admin auth is rejected
#   * POST /api/shots, GET /api/shots, GET /api/shots/{id}, POST /api/shots/{id}/stop
#   * per-shot ingest tokens disclosed once at shot creation (exactly 24h)
#   * real OTLP traces + metrics + logs through the fixed /ingest endpoint
#   * REST query/analyze/schema/compare on the frozen R2 snapshot
#   * remote MCP tools/list + tools/call
#   * push-token / admin-token scope separation and revocation
#
# Secret confidentiality: tokens are read from the environment, passed to curl
# via `--config` process substitution (never argv) and to otlpsend via
# BURSTEL_PUSH_TOKEN, never echoed, never written to the repository, logs, URLs
# or result files. This does NOT upload anything to an ISUCON host.
#
# Usage:
#   BURSTEL_BASE_URL=https://burstel.example \
#   BURSTEL_API_TOKEN=... \
#   scripts/cloudflare-smoke.sh
#
# Optional: BURSTEL_INTERNAL_TOKEN (presence/distinctness check only),
#           OTLPSEND_BIN (otherwise cmd/otlpsend is built with go).
set -euo pipefail

BASE="${BURSTEL_BASE_URL:-}"
ADMIN_TOKEN="${BURSTEL_API_TOKEN:-}"
[ -n "$BASE" ] || { echo "cloudflare-smoke: BURSTEL_BASE_URL is required" >&2; exit 2; }
[ -n "$ADMIN_TOKEN" ] || { echo "cloudflare-smoke: BURSTEL_API_TOKEN is required" >&2; exit 2; }
BASE="${BASE%/}"
case "$BASE" in
  https://*) ;;
  http://127.0.0.1*|http://localhost*) ;;
  *) echo "cloudflare-smoke: refusing a non-HTTPS base URL ($BASE) unless loopback" >&2; exit 2 ;;
esac

if [ -n "${BURSTEL_INTERNAL_TOKEN:-}" ] && [ "${BURSTEL_INTERNAL_TOKEN}" = "$ADMIN_TOKEN" ]; then
  echo "cloudflare-smoke: BURSTEL_INTERNAL_TOKEN must differ from BURSTEL_API_TOKEN" >&2
  exit 2
fi

command -v curl >/dev/null 2>&1 || { echo "cloudflare-smoke: curl is required" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "cloudflare-smoke: python3 is required" >&2; exit 2; }

WORK="$(mktemp -d "${TMPDIR:-/tmp}/burstel-smoke.XXXXXX")"
chmod 700 "$WORK"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

BODY_FILE="$WORK/body"
die() { printf 'cloudflare-smoke: %s\n' "$*" >&2; exit 1; }

# curl via a process-substituted config: secrets never appear in argv.
req() {
  local method="$1" path="$2" body="${3:-}" auth="${4:-}"
  local cfg
  # Build the whole curl config in ONE command substitution. Each `printf`
  # keeps its own trailing newline, so `request`, `header` and `data-binary`
  # stay on separate lines. Chaining `cfg+=` command substitutions would strip
  # each trailing newline and glue the `request` line to the following
  # `header` line, silently dropping the Authorization header.
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

expect_code() { # want label
  [ "$HTTP_CODE" = "$1" ] || die "$2 returned HTTP $HTTP_CODE, want $1: $BODY"
}

ADMIN="Authorization: Bearer $ADMIN_TOKEN"

echo "== healthz ==" >&2
req GET /healthz
expect_code 200 "healthz"
[ "$(printf '%s' "$BODY" | jget "['status']")" = "ok" ] || die "unexpected healthz body: $BODY"

echo "== auth boundaries ==" >&2
req POST /api/shots '{"metadata":{"source":"smoke"}}'
expect_code 401 "anonymous admin"
req POST /api/shots '{"metadata":{"source":"smoke"}}' "Authorization: Bearer wrong-token"
expect_code 401 "wrong admin token"

echo "== shot A: create -> OTLP -> freeze ==" >&2
req POST /api/shots "{\"metadata\":{\"source\":\"cloudflare-smoke\",\"ts\":\"$(date +%s)\"}}" "$ADMIN"
expect_code 201 "create shot A"
SHOT_A="$(printf '%s' "$BODY" | jget "['shot_id']")"
OTLP_BASE="$(printf '%s' "$BODY" | jget "['otlp_base_url']")"
PUSH="$(printf '%s' "$BODY" | jget "['ingest_token']")"
TTL="$(printf '%s' "$BODY" | jget "['ingest_token_ttl_seconds']")"
[ -n "$SHOT_A" ] || die "no shot_id returned"
[ -n "$PUSH" ] || die "shot A did not disclose a per-shot ingest token"
[ "$TTL" = "86400" ] || die "per-shot token ttl_seconds=$TTL, want 86400"
case "$OTLP_BASE" in */ingest) ;; *) die "otlp_base_url=$OTLP_BASE is not the fixed /ingest endpoint" ;; esac

# Send the three real fixtures to the fixed /ingest endpoint. The token goes
# through the environment, never argv.
OTLPSEND_BIN="${OTLPSEND_BIN:-}"
if [ -z "$OTLPSEND_BIN" ]; then
  command -v go >/dev/null 2>&1 || die "go not found and OTLPSEND_BIN unset"
  OTLPSEND_BIN="$WORK/otlpsend"
  (cd "$(dirname "$0")/.." && go build -o "$OTLPSEND_BIN" ./cmd/otlpsend)
fi
BURSTEL_PUSH_TOKEN="$PUSH" "$OTLPSEND_BIN" -ingest "$BASE" >"$WORK/otlpsend.out" 2>"$WORK/otlpsend.err" \
  || die "otlpsend failed: $(cat "$WORK/otlpsend.err")"

# Scope separation: the push token is denied on admin routes, the admin token is
# denied on /ingest.
req GET "/api/shots/$SHOT_A" "" "X-Burstel-Token: $PUSH"
expect_code 401 "push token on admin route"
req POST /ingest/v1/logs '{}' "$ADMIN"
expect_code 401 "admin token on /ingest"

req GET "/api/shots/$SHOT_A" "" "$ADMIN"
expect_code 200 "status shot A"
[ "$(printf '%s' "$BODY" | jget "['state']")" = "recording" ] || die "shot A is not recording: $BODY"

req POST "/api/shots/$SHOT_A/stop" "" "$ADMIN"
expect_code 200 "stop shot A"
SHA_A="$(printf '%s' "$BODY" | jget "['sha256']")"
SIZE_A="$(printf '%s' "$BODY" | jget "['size_bytes']")"
[ "$(printf '%s' "$BODY" | jget "['state']")" = "frozen" ] || die "shot A did not freeze: $BODY"
printf '%s' "$SHA_A" | grep -Eq '^[0-9a-f]{64}$' || die "shot A sha256 is not 64 hex chars"
[ "$SIZE_A" -gt 0 ] || die "shot A size_bytes is not positive"

echo "== REST analysis on the frozen R2 snapshot ==" >&2
req POST "/api/shots/$SHOT_A/query" '{"sql":"SELECT 1 AS n","max_rows":1}' "$ADMIN"
expect_code 200 "query shot A"
[ "$(printf '%s' "$BODY" | jget "['row_count']")" = "1" ] || die "query row_count != 1: $BODY"

req POST "/api/shots/$SHOT_A/analyze" '{"top_n":5}' "$ADMIN"
expect_code 200 "analyze shot A"
[ "$(printf '%s' "$BODY" | jget "['schema_version']")" = "1" ] || die "analyze schema_version != 1: $BODY"

req GET "/api/shots/$SHOT_A/schema" "" "$ADMIN"
expect_code 200 "schema shot A"

echo "== shot B + compare ==" >&2
req POST /api/shots "{\"metadata\":{\"source\":\"cloudflare-smoke-b\",\"ts\":\"$(date +%s)\"}}" "$ADMIN"
expect_code 201 "create shot B"
SHOT_B="$(printf '%s' "$BODY" | jget "['shot_id']")"
PUSH_B="$(printf '%s' "$BODY" | jget "['ingest_token']")"
[ -n "$SHOT_B" ] || die "no shot B id returned"
[ -n "$PUSH_B" ] || die "shot B did not disclose a per-shot ingest token"
BURSTEL_PUSH_TOKEN="$PUSH_B" "$OTLPSEND_BIN" -ingest "$BASE" >/dev/null 2>"$WORK/otlpsend-b.err" \
  || die "otlpsend (shot B) failed: $(cat "$WORK/otlpsend-b.err")"
req POST "/api/shots/$SHOT_B/stop" "" "$ADMIN"
expect_code 200 "stop shot B"
req POST "/api/shots/$SHOT_A/compare" "{\"other\":\"$SHOT_B\",\"top_n\":5}" "$ADMIN"
expect_code 200 "compare A/B"

echo "== MCP tools/list + tools/call ==" >&2
mcp() {
  local body="$1"
  local cfg
  cfg="$(printf 'silent\nshow-error\nmax-time = 30\nrequest = "POST"\nheader = "%s"\nheader = "Content-Type: application/json"\nheader = "Accept: application/json, text/event-stream"\ndata-binary = "@-"\n' "$ADMIN")"
  HTTP_CODE="$(printf '%s' "$body" | curl --config <(printf '%s' "$cfg") -o "$BODY_FILE" -w '%{http_code}' "$BASE/mcp")"
  BODY="$(cat "$BODY_FILE")"
}
mcp '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
expect_code 200 "MCP tools/list"
printf '%s' "$BODY" | grep -q 'shot_get' || die "MCP tools/list missing shot_get"
mcp "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"shot_get\",\"arguments\":{\"shot_id\":\"$SHOT_A\"}}}"
expect_code 200 "MCP tools/call shot_get"
printf '%s' "$BODY" | grep -q "$SHOT_A" || die "MCP shot_get did not return shot A"

echo "== retired installation-wide token endpoint ==" >&2
req POST /api/ingest-token "" "$ADMIN"
expect_code 410 "retired ingest-token endpoint"

cat <<JSON
{"base_url":"$BASE","healthz":"ok","shot_a":"$SHOT_A","shot_b":"$SHOT_B","frozen_sha256_a":"$SHA_A","per_shot_token_ttl_seconds":$TTL,"otlp_traces_metrics_logs":"sent","rest_query_analyze_compare":"ok","mcp_tools_list_and_call":"ok","scope_separation":"ok","installation_token_retired":true}
JSON
echo "== cloudflare smoke passed (no secret was printed) ==" >&2
