#!/usr/bin/env bash
#
# cloudflare-multishot-smoke.sh - bounded live two-instance isolation E2E.
#
# Spec 024 R3: rewritten for the current named-instance model. There are no
# per-shot ingest tokens and no recording containers on the ingest path.
#
# It proves:
#   1. two named instances each get a distinct push-only `bit_` token;
#   2. POST /api/shots defaults to logical window shots (kind=window) and each
#      shot can be scoped to one instance;
#   3. /ingest/v1/* attributes each export to the token's instance (checked from
#      the x-burstel-instance response header, never a cross-instance leak);
#   4. owner filtering on GET /api/shots is isolated;
#   5. revocation of one instance token is immediate (401) without affecting the
#      other instance;
#   6. /ingest rejects admin/member credentials, and window markers stop to a
#      ready/incomplete state.
#
# Secret confidentiality: tokens are read from the environment and passed to
# curl via `--config` process substitution (never argv), and are never echoed.
# No deploy and no Cloudflare resource mutation.
#
# Usage:
#   BURSTEL_BASE_URL=https://burstel.example \
#   BURSTEL_API_TOKEN=... \
#   scripts/cloudflare-multishot-smoke.sh
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
  local method="$1" path="$2" body="${3:-}" auth="${4:-}" extra="${5:-}"
  local cfg
  cfg="$( {
    printf 'silent\nshow-error\nmax-time = 30\nrequest = "%s"\n' "$method"
    if [ -n "$auth" ]; then printf 'header = "%s"\n' "$auth"; fi
    if [ -n "$extra" ]; then printf 'header = "%s"\n' "$extra"; fi
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
INSTANCE_A="multi-a-$STAMP"
INSTANCE_B="multi-b-$STAMP"

# Sends one minimal valid OTLP protobuf export with an instance token and
# returns the attributed instance from the response header (stdout).
push_signal() { # token signal
  local token="$1" signal="$2" cfg code
  cfg="$(printf 'silent\nshow-error\nmax-time = 30\nrequest = "POST"\nheader = "%s"\nheader = "Content-Type: application/x-protobuf"\ndata-binary = "@-"\n' "X-Burstel-Token: $token")"
  code="$(printf '\x0a\x00' | curl --config <(printf '%s' "$cfg") -D "$WORK/headers" -o /dev/null -w '%{http_code}' "$BASE/ingest/v1/$signal")"
  [ "$code" = "200" ] || die "/ingest/v1/$signal returned HTTP $code, want 200"
  awk 'tolower($1) == "x-burstel-instance:" { print $2 }' "$WORK/headers" | tr -d '\r'
}

push_expect() { # token signal want_code label
  local token="$1" signal="$2" want="$3" label="$4" cfg code
  cfg="$(printf 'silent\nshow-error\nmax-time = 30\nrequest = "POST"\nheader = "%s"\nheader = "Content-Type: application/x-protobuf"\ndata-binary = "@-"\n' "X-Burstel-Token: $token")"
  code="$(printf '\x0a\x00' | curl --config <(printf '%s' "$cfg") -o /dev/null -w '%{http_code}' "$BASE/ingest/v1/$signal")"
  [ "$code" = "$want" ] || die "$label returned HTTP $code, want $want"
}

echo "== healthz ==" >&2
req GET /healthz
expect_code 200 "healthz"

echo "== two named instances + distinct push tokens ==" >&2
req POST /api/instances "{\"name\":\"$INSTANCE_A\"}" "$ADMIN"
expect_code 201 "create instance A"
INSTANCE_ID_A="$(printf '%s' "$BODY" | jget "['instance_id']")"
req POST "/api/instances/$INSTANCE_A/tokens" '{}' "$ADMIN"
expect_code 201 "issue token A"
PUSH_A="$(printf '%s' "$BODY" | jget "['token']")"
TOKEN_ID_A="$(printf '%s' "$BODY" | jget "['id']")"
[ -n "$PUSH_A" ] || die "no token for instance A"

req POST /api/instances "{\"name\":\"$INSTANCE_B\"}" "$ADMIN"
expect_code 201 "create instance B"
INSTANCE_ID_B="$(printf '%s' "$BODY" | jget "['instance_id']")"
req POST "/api/instances/$INSTANCE_B/tokens" '{}' "$ADMIN"
expect_code 201 "issue token B"
PUSH_B="$(printf '%s' "$BODY" | jget "['token']")"
[ -n "$PUSH_B" ] || die "no token for instance B"
[ "$PUSH_A" != "$PUSH_B" ] || die "instances share a token"
[ "$INSTANCE_ID_A" != "$INSTANCE_ID_B" ] || die "instances share an id"

echo "== two default (window) shots, one per instance ==" >&2
req POST /api/shots "{\"owner\":\"e2e-a\",\"instances\":[\"$INSTANCE_A\"],\"metadata\":{\"source\":\"multishot-a\",\"ts\":\"$STAMP\"}}" "$ADMIN"
expect_code 201 "create shot A"
SHOT_A="$(printf '%s' "$BODY" | jget "['shot_id']")"
[ "$(printf '%s' "$BODY" | jget "['kind']")" = "window" ] || die "shot A is not a window shot: $BODY"
SHOT_A_INST="$(printf '%s' "$BODY" | jget "['window']['instances'][0]")"
[ "$SHOT_A_INST" = "$INSTANCE_A" ] || die "shot A window instance=$SHOT_A_INST, want $INSTANCE_A"

req POST /api/shots "{\"owner\":\"e2e-b\",\"instances\":[\"$INSTANCE_B\"],\"metadata\":{\"source\":\"multishot-b\",\"ts\":\"$STAMP\"}}" "$ADMIN"
expect_code 201 "create shot B"
SHOT_B="$(printf '%s' "$BODY" | jget "['shot_id']")"
[ "$(printf '%s' "$BODY" | jget "['kind']")" = "window" ] || die "shot B is not a window shot: $BODY"
[ "$SHOT_A" != "$SHOT_B" ] || die "shots share an id"

echo "== /ingest attributes each token to its own instance ==" >&2
GOT_A="$(push_signal "$PUSH_A" traces)"
[ "$GOT_A" = "$INSTANCE_ID_A" ] || die "token A was attributed to $GOT_A, want $INSTANCE_ID_A"
GOT_B="$(push_signal "$PUSH_B" logs)"
[ "$GOT_B" = "$INSTANCE_ID_B" ] || die "token B was attributed to $GOT_B, want $INSTANCE_ID_B"

echo "== owner filtering ==" >&2
req GET "/api/shots?owner=e2e-a" "" "$ADMIN"
expect_code 200 "list owner=e2e-a"
printf '%s' "$BODY" | grep -q "$SHOT_A" || die "list did not return shot A"
if printf '%s' "$BODY" | grep -q "$SHOT_B"; then die "owner filter leaked shot B"; fi
req GET "/api/shots?owner=e2e-b" "" "$ADMIN"
expect_code 200 "list owner=e2e-b"
printf '%s' "$BODY" | grep -q "$SHOT_B" || die "list did not return shot B"

echo "== admin and instance credential scopes stay separate ==" >&2
req GET "/api/shots/$SHOT_A" "" "X-Burstel-Token: $PUSH_A"
expect_code 401 "instance token on admin route"
req POST /ingest/v1/logs '{}' "$ADMIN"
expect_code 401 "admin token on /ingest"

echo "== revoking A is immediate and does not affect B ==" >&2
req DELETE "/api/instances/$INSTANCE_A/tokens/$TOKEN_ID_A" "" "$ADMIN"
expect_code 200 "revoke token A"
push_expect "$PUSH_A" traces 401 "revoked token A"
GOT_B_AFTER="$(push_signal "$PUSH_B" traces)"
[ "$GOT_B_AFTER" = "$INSTANCE_ID_B" ] || die "token B attribution changed after revoking A"

echo "== stop both window markers ==" >&2
for id in "$SHOT_A" "$SHOT_B"; do
  req POST "/api/shots/$id/stop" "" "$ADMIN"
  expect_code 200 "stop $id"
  case "$(printf '%s' "$BODY" | jget "['state']")" in
    ready|incomplete) ;;
    *) die "window shot $id stopped in an unexpected state: $BODY" ;;
  esac
done

echo "== MCP tools/list ==" >&2
req POST /mcp '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' \
  "$ADMIN" "Accept: application/json, text/event-stream"
expect_code 200 "MCP tools/list"
printf '%s' "$BODY" | grep -q '"shot_start"' || die "MCP tools/list missing shot_start"
printf '%s' "$BODY" | grep -q '"instance_token_issue"' || die "MCP tools/list missing instance_token_issue"

cat <<JSON
{"base_url":"$BASE","instance_a":"$INSTANCE_A","instance_b":"$INSTANCE_B","shot_a":"$SHOT_A","shot_b":"$SHOT_B","model":"named-instances+window-shots","isolation":"ok","owner_filter":"ok","scope_separation":"ok","revocation":"ok","window_stop":"ok","mcp_tools_list":"ok"}
JSON
echo "== cloudflare multishot smoke passed (no secret was printed) ==" >&2
