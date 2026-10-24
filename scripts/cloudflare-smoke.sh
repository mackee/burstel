#!/usr/bin/env bash
#
# cloudflare-smoke.sh - bounded live end-to-end smoke against a deployed burstel
# Worker + R2 + (optionally) compactor deployment.
#
# Spec 024 R3: the pre-016 per-shot `ingest_token` / `state: recording` model is
# gone. This script exercises the current model only:
#
#   * GET /healthz
#   * anonymous / wrong admin auth is rejected
#   * POST /api/instances + POST /api/instances/{name}/tokens (24h `bit_` token)
#   * POST /api/shots defaults to a logical window shot (no per-shot token)
#   * real OTLP traces + metrics + logs through POST /ingest/v1/*
#   * listing-based journal occupancy via GET /api/ingest/status
#   * the compactor container token gate (POST /api/compaction/run must not
#     report `compactor API disabled`, spec 028)
#   * scope separation (instance token is not an admin token and vice versa)
#   * KV backfill endpoint, freeze/stop of the window marker, MCP tools/list
#
# Secret confidentiality: tokens are read from the environment and passed to
# curl via `--config` process substitution (never argv), and to the load
# generator via BURSTEL_PUSH_TOKEN. They are never echoed or written to the
# repository. No deploy, no Cloudflare resource mutation.
#
# Usage:
#   BURSTEL_BASE_URL=https://burstel.example \
#   BURSTEL_API_TOKEN=... \
#   scripts/cloudflare-smoke.sh
#
# Optional:
#   LOADGEN_BIN       prebuilt cmd/burstel-loadgen (otherwise built with go)
#   BURSTEL_INTERNAL_TOKEN  presence/distinctness check only
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
STAMP="$(date +%s)"
INSTANCE="smoke-$STAMP"

echo "== healthz ==" >&2
req GET /healthz
expect_code 200 "healthz"
[ "$(printf '%s' "$BODY" | jget "['status']")" = "ok" ] || die "unexpected healthz body: $BODY"

echo "== auth boundaries ==" >&2
req POST /api/shots '{"metadata":{"source":"smoke"}}'
expect_code 401 "anonymous admin"
req POST /api/shots '{"metadata":{"source":"smoke"}}' "Authorization: Bearer wrong-token"
expect_code 401 "wrong admin token"

echo "== named instance + push-only instance token ==" >&2
req POST /api/instances "{\"name\":\"$INSTANCE\"}" "$ADMIN"
expect_code 201 "create instance"
req POST "/api/instances/$INSTANCE/tokens" '{}' "$ADMIN"
expect_code 201 "issue instance token"
INSTANCE_ID="$(printf '%s' "$BODY" | jget "['instance_id']")"
PUSH="$(printf '%s' "$BODY" | jget "['token']")"
TTL="$(printf '%s' "$BODY" | jget "['ttl_seconds']")"
[ -n "$INSTANCE_ID" ] || die "instance creation returned no instance_id"
[ -n "$PUSH" ] || die "token issuance returned no token"
[ "$TTL" = "86400" ] || die "instance token ttl_seconds=$TTL, want 86400"

echo "== default window shot (no per-shot token) ==" >&2
req POST /api/shots "{\"metadata\":{\"source\":\"cloudflare-smoke\",\"ts\":\"$STAMP\"},\"instances\":[\"$INSTANCE\"]}" "$ADMIN"
expect_code 201 "create window shot"
SHOT="$(printf '%s' "$BODY" | jget "['shot_id']")"
KIND="$(printf '%s' "$BODY" | jget "['kind']")"
SHOT_STATE="$(printf '%s' "$BODY" | jget "['state']")"
[ -n "$SHOT" ] || die "no shot_id returned"
[ "$KIND" = "window" ] || die "POST /api/shots defaulted to kind=$KIND, want window"
[ "$SHOT_STATE" = "open" ] || die "new window shot state=$SHOT_STATE, want open"
if printf '%s' "$BODY" | grep -q 'ingest_token'; then
  die "window shot unexpectedly disclosed a per-shot ingest token"
fi

echo "== real OTLP through /ingest/v1/{traces,metrics,logs} ==" >&2
LOADGEN_BIN="${LOADGEN_BIN:-}"
if [ -z "$LOADGEN_BIN" ]; then
  command -v go >/dev/null 2>&1 || die "go not found and LOADGEN_BIN unset"
  LOADGEN_BIN="$WORK/burstel-loadgen"
  (cd "$(dirname "$0")/.." && go build -o "$LOADGEN_BIN" ./cmd/burstel-loadgen)
fi
BURSTEL_PUSH_TOKEN="$PUSH" "$LOADGEN_BIN" \
  -endpoint "$BASE" -variant b1 \
  -exporters 1 -spans-per-second 50 -duration 2s -db-spans 2 \
  >"$WORK/loadgen.out" 2>"$WORK/loadgen.err" \
  || die "burstel-loadgen failed: $(cat "$WORK/loadgen.err")"

echo "== listing-based journal occupancy ==" >&2
req GET /api/ingest/status "" "$ADMIN"
expect_code 200 "ingest status"
OBJECTS="$(printf '%s' "$BODY" | jget "['objects']")"
[ -n "$OBJECTS" ] || die "ingest status returned no objects count: $BODY"
[ "$OBJECTS" -ge 1 ] || die "no acknowledged export is visible in the journal ($OBJECTS objects)"

echo "== scope separation + KV backfill ==" >&2
req GET "/api/shots/$SHOT" "" "X-Burstel-Token: $PUSH"
expect_code 401 "instance token on admin route"
req POST /ingest/v1/logs '{}' "$ADMIN"
expect_code 401 "admin token on /ingest"
req POST /api/instance-tokens/backfill "" "$ADMIN"
expect_code 200 "KV backfill"
printf '%s' "$BODY" | grep -q '"backfilled"' || die "backfill response missing backfilled: $BODY"

echo "== compactor container token gate (spec 028) ==" >&2
# A secure deployment must never surface the Go compactor's fail-closed
# `compactor API disabled: BURSTEL_INTERNAL_TOKEN is not configured` error.
# `run` may be 200 or 503 (a tick can legitimately fail/skip for other
# reasons), so inspect the report body instead of requiring success.
req POST /api/compaction/run '{}' "$ADMIN"
case "$HTTP_CODE" in
  200|503) ;;
  *) die "POST /api/compaction/run returned HTTP $HTTP_CODE: $BODY" ;;
esac
if printf '%s' "$BODY" | grep -q 'compactor API disabled'; then
  die "compactor container is missing BURSTEL_INTERNAL_TOKEN (no segment can ever be published): $BODY"
fi
if printf '%s' "$BODY" | grep -q 'internal_token_unconfigured'; then
  die "compactor coordinator has no distinct internal token configured: $BODY"
fi

echo "== stop the window marker ==" >&2
req GET "/api/shots/$SHOT" "" "$ADMIN"
expect_code 200 "status window shot"
req POST "/api/shots/$SHOT/stop" "" "$ADMIN"
expect_code 200 "stop window shot"
[ "$(printf '%s' "$BODY" | jget "['kind']")" = "window" ] || die "stop did not return a window shot: $BODY"
FINAL_STATE="$(printf '%s' "$BODY" | jget "['state']")"
case "$FINAL_STATE" in ready|incomplete) ;; *) die "window shot stopped in state=$FINAL_STATE" ;; esac

echo "== MCP tools/list ==" >&2
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
mcp "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"shot_get\",\"arguments\":{\"shot_id\":\"$SHOT\"}}}"
expect_code 200 "MCP tools/call shot_get"
printf '%s' "$BODY" | grep -q "$SHOT" || die "MCP shot_get did not return the window shot"

echo "== retired installation-wide token endpoint ==" >&2
req POST /api/ingest-token "" "$ADMIN"
expect_code 410 "retired ingest-token endpoint"

cat <<JSON
{"base_url":"$BASE","healthz":"ok","instance":"$INSTANCE","shot":"$SHOT","kind":"window","shot_state":"$FINAL_STATE","instance_token_ttl_seconds":$TTL,"otlp_traces_metrics_logs":"sent","journal_objects":$OBJECTS,"kv_backfill":"ok","scope_separation":"ok","compactor_token_gate":"ok","mcp_tools_list_and_call":"ok","installation_token_retired":true}
JSON
echo "== cloudflare smoke passed (no secret was printed) ==" >&2
