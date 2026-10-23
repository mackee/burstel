#!/usr/bin/env bash
#
# cloudflare-preflight.sh - read-only preflight for a burstel Cloudflare deploy.
#
# It NEVER deploys, never enrolls/upgrades a plan and never creates or
# overwrites a Cloudflare resource. It only reads:
#
#   * account identity/plan eligibility (wrangler whoami --json, and the
#     Cloudflare API when CLOUDFLARE_API_TOKEN is available)
#   * existing R2 bucket lookup (never created here)
#   * Worker/DO collision lookup (never recreated here)
#   * effective secret PRESENCE via `wrangler secret list` (names only, never
#     values; `wrangler auth token` is never used)
#   * docker/buildx compatibility for the linux/amd64 image build
#   * the checked-in wrangler.jsonc invariants (port/DO/R2/cost/auth)
#
# It halts before any `wrangler deploy` and exits non-zero on a failed required
# check, including "no image-build mechanism".
#
# Usage:
#   scripts/cloudflare-preflight.sh                 # full read-only preflight
#   scripts/cloudflare-preflight.sh --config-only   # no credentials needed
#   scripts/cloudflare-preflight.sh --config cloudflare/wrangler.jsonc
#   scripts/cloudflare-preflight.sh --config-only --allow-placeholder-kv
#       (structural lint only; the real preflight rejects the all-zero id)
#
# Credentials (optional, read-only):
#   CLOUDFLARE_API_TOKEN   API token used for the read-only account lookups
#   CLOUDFLARE_ACCOUNT_ID  optional; otherwise taken from `wrangler whoami`
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CF="$REPO_ROOT/cloudflare"
CONFIG="$CF/wrangler.jsonc"
MODE="full"
MAX_INSTANCES=10
ALLOW_PLACEHOLDER_KV=0

while [ $# -gt 0 ]; do
  case "$1" in
    --config-only) MODE="config"; shift ;;
    --config) CONFIG="$2"; shift 2 ;;
    --max-instances) MAX_INSTANCES="$2"; shift 2 ;;
    --allow-placeholder-kv) ALLOW_PLACEHOLDER_KV=1; shift ;;
    -h|--help) sed -n '2,36p' "$0"; exit 0 ;;
    *) echo "cloudflare-preflight: unknown argument: $1" >&2; exit 2 ;;
  esac
done

TMP="$(mktemp -d "${TMPDIR:-/tmp}/burstel-preflight.XXXXXX")"
chmod 700 "$TMP"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

die() { printf 'cloudflare-preflight: %s\n' "$*" >&2; exit 2; }

command -v bun >/dev/null 2>&1 || die "bun is required to run the preflight validator"
RUNNER=(bun "$CF/scripts/preflight.mjs")
CONFIG_ARGS=(--config "$CONFIG" --max-instances "$MAX_INSTANCES")
if [ "$ALLOW_PLACEHOLDER_KV" = "1" ]; then CONFIG_ARGS+=(--allow-placeholder-kv); fi

if [ "$MODE" = "config" ]; then
  echo "cloudflare-preflight: config-only (no credentials, no deploy)" >&2
  "${RUNNER[@]}" "${CONFIG_ARGS[@]}" --config-only
  exit $?
fi

# --------------------------------------------------------------- wrangler
wrangler() {
  if [ -n "${WRANGLER_BIN:-}" ]; then "$WRANGLER_BIN" "$@"; return; fi
  if command -v wrangler >/dev/null 2>&1; then command wrangler "$@"; return; fi
  local entry="$CF/node_modules/wrangler/bin/wrangler.js"
  if [ -f "$entry" ]; then
    if command -v node >/dev/null 2>&1; then node "$entry" "$@"; return; fi
    if command -v bun >/dev/null 2>&1; then bun "$entry" "$@"; return; fi
  fi
  if command -v npx >/dev/null 2>&1; then npx --yes wrangler "$@"; return; fi
  return 127
}

echo "cloudflare-preflight: read-only checks; no deploy, no plan change, no resource mutation" >&2

# ------------------------------------------------------- account identity
if wrangler whoami --json >"$TMP/whoami.json" 2>"$TMP/whoami.err"; then
  echo "cloudflare-preflight: wrangler whoami OK" >&2
else
  echo "cloudflare-preflight: wrangler whoami failed (not authenticated?); see the report" >&2
fi

ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID:-}"
if [ -z "$ACCOUNT_ID" ] && [ -s "$TMP/whoami.json" ] && command -v python3 >/dev/null 2>&1; then
  ACCOUNT_ID="$(python3 - "$TMP/whoami.json" <<'PY' 2>/dev/null || true
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
acct = d.get("account") if isinstance(d, dict) else None
if isinstance(acct, dict) and acct.get("id"):
    print(acct["id"]); sys.exit(0)
for a in (d.get("accounts") or []) if isinstance(d, dict) else []:
    if isinstance(a, dict) and a.get("id"):
        print(a["id"]); sys.exit(0)
PY
)"
fi

# ------------------------------------------------------ effective secrets
wrangler secret list --name burstel --format json >"$TMP/secrets.json" 2>/dev/null || true

# --------------------------------------------------- R2 bucket existence
if [ -n "${CLOUDFLARE_API_TOKEN:-}" ] && [ -n "$ACCOUNT_ID" ]; then
  if curl -fsS --max-time 20 \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      "https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/r2/buckets" \
      | python3 -c 'import sys,json; d=json.load(sys.stdin); print(json.dumps([{"name":b.get("name")} for b in (d.get("result") or [])]))' \
      >"$TMP/r2.json" 2>/dev/null; then
    :
  else
    echo '[]' >"$TMP/r2.json"
  fi
elif command -v wrangler >/dev/null 2>&1 || [ -f "$CF/node_modules/wrangler/bin/wrangler.js" ]; then
  if wrangler r2 bucket list >"$TMP/r2.raw" 2>/dev/null && grep -q 'burstel-shots' "$TMP/r2.raw"; then
    echo '[{"name":"burstel-shots"}]' >"$TMP/r2.json"
  else
    echo '[]' >"$TMP/r2.json"
  fi
else
  echo '[]' >"$TMP/r2.json"
fi

# ---------------------------- plan eligibility + worker collision lookup
WORKER_EXISTS=""
CONTAINERS_PAID=""
PLAN_NAME=""
if [ -n "${CLOUDFLARE_API_TOKEN:-}" ] && [ -n "$ACCOUNT_ID" ]; then
  # Worker collision: 200 => exists, 404 => absent.
  code="$(curl -sS -o "$TMP/worker.json" -w '%{http_code}' --max-time 20 \
    -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
    "https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/workers/services/burstel" || true)"
  if [ "$code" = "200" ]; then WORKER_EXISTS=1; elif [ "$code" = "404" ]; then WORKER_EXISTS=0; fi

  # Workers Paid / Containers eligibility: look for a paid workers subscription.
  if curl -fsS --max-time 20 \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      "https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/subscriptions" \
      >"$TMP/subs.json" 2>/dev/null; then
    CONTAINERS_PAID="$(python3 - "$TMP/subs.json" <<'PY' 2>/dev/null || true
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
paid = False; name = ""
for s in (d.get("result") or []):
    rp = (s.get("rate_plan") or {}) if isinstance(s, dict) else {}
    rid = str(rp.get("id", "")).lower()
    if "workers" in rid:
        name = rid
        if "paid" in rid or "standard" in rid or "unbound" in rid:
            paid = True
print(("1" if paid else "0") + "\n" + name)
PY
)"
    if [ -n "$CONTAINERS_PAID" ]; then
      PLAN_NAME="$(printf '%s\n' "$CONTAINERS_PAID" | tail -n1)"
      CONTAINERS_PAID="$(printf '%s\n' "$CONTAINERS_PAID" | head -n1)"
    fi
  fi
fi

# ---------------------------------------------------- docker compatibility
DOCKER_CLI=0; DOCKER_DAEMON=0; DOCKER_BUILDX=0; DOCKER_PLATFORM=0
if command -v docker >/dev/null 2>&1; then
  DOCKER_CLI=1
  if docker info >/dev/null 2>&1; then DOCKER_DAEMON=1; fi
  if docker buildx version >/dev/null 2>&1; then
    DOCKER_BUILDX=1
    # Capture buildx output before grepping. Piping buildx directly into
    # `grep -q` lets grep close the pipe early, which kills buildx with SIGPIPE
    # and, under `set -o pipefail`, fails this check even though linux/amd64 is
    # supported (a verified false negative on a working Docker+QEMU host).
    buildx_platforms="$(docker buildx inspect --bootstrap 2>/dev/null || true)"
    if printf '%s' "$buildx_platforms" | grep -q 'linux/amd64'; then DOCKER_PLATFORM=1; fi
  fi
fi

# ------------------------------------------------------------ run report
ARGS=(
  "${CONFIG_ARGS[@]}"
  --whoami "$TMP/whoami.json"
  --secrets "$TMP/secrets.json"
  --r2 "$TMP/r2.json"
  --docker-cli "$DOCKER_CLI"
  --docker-daemon "$DOCKER_DAEMON"
  --docker-buildx "$DOCKER_BUILDX"
  --docker-platform-linux-amd64 "$DOCKER_PLATFORM"
)
if [ -n "$WORKER_EXISTS" ]; then ARGS+=(--worker-exists "$WORKER_EXISTS"); fi
if [ -n "$CONTAINERS_PAID" ]; then ARGS+=(--containers-paid "$CONTAINERS_PAID"); fi
if [ -n "$PLAN_NAME" ]; then ARGS+=(--plan-name "$PLAN_NAME"); fi

set +e
"${RUNNER[@]}" "${ARGS[@]}"
RC=$?
set -e

echo >&2
echo "cloudflare-preflight: report above; exit=$RC" >&2
if [ "$RC" -ne 0 ]; then
  echo "cloudflare-preflight: BLOCKED - resolve the failed required checks before any 'wrangler deploy'." >&2
else
  echo "cloudflare-preflight: OK - safe to proceed to the documented deploy sequence." >&2
fi
exit "$RC"
