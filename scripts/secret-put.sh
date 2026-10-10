#!/usr/bin/env bash
#
# secret-put.sh - generate a strong Worker secret and put it in one step.
#
# Generates random bytes with `openssl rand -hex` and feeds the value to
# `wrangler secret put` over stdin. The value is never passed on the command
# line and never committed. Because a Worker secret cannot be read back after
# it is set, the value is saved to a mode-600 file by default so the operator
# can retrieve it; pass --no-save if you really do not need it.
#
# Usage:
#   scripts/secret-put.sh <SECRET_NAME> [options]
#
# Options:
#   --name <worker>   Worker name (default: the name in cloudflare/wrangler.jsonc)
#   --out <file>      Save the value to <file> (mode 600) instead of .secrets/<NAME>
#   --no-save         Do not save the generated value anywhere
#   --show            Also print the generated value to stdout (shown once)
#   -h, --help        Show this help
#
# Environment:
#   WRANGLER   wrangler command (default: "bunx wrangler@4.149.0")
#   BYTES      random bytes to generate (default: 32 -> 64 lowercase hex chars)
#
# Examples:
#   scripts/secret-put.sh BURSTEL_API_TOKEN
#   scripts/secret-put.sh BURSTEL_INTERNAL_TOKEN --show
set -euo pipefail

usage() { sed -n '2,30p' "$0"; }

name=""
worker=""
out=""
save=1
show=0

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --name) worker="${2:?--name needs a value}"; shift 2 ;;
    --out) out="${2:?--out needs a value}"; shift 2 ;;
    --no-save) save=0; shift ;;
    --show) show=1; shift ;;
    -*) echo "secret-put: unknown option: $1" >&2; exit 2 ;;
    *) name="$1"; shift ;;
  esac
done

[ -n "$name" ] || { echo "secret-put: SECRET_NAME is required" >&2; usage >&2; exit 2; }
if ! [[ "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "secret-put: invalid secret name (use [A-Za-z_][A-Za-z0-9_]*): $name" >&2
  exit 2
fi

command -v openssl >/dev/null 2>&1 || { echo "secret-put: openssl not found" >&2; exit 1; }

root="$(cd "$(dirname "$0")/.." && pwd)"
cf="$root/cloudflare"
wrangler_cmd="${WRANGLER:-bunx wrangler@4.149.0}"
bytes="${BYTES:-32}"
read -r -a wrangler_argv <<<"$wrangler_cmd"

value="$(openssl rand -hex "$bytes")"
[ -n "$value" ] || { echo "secret-put: failed to generate a value" >&2; exit 1; }

if [ "$save" = "1" ]; then
  [ -n "$out" ] || out="$root/.secrets/$name"
  mkdir -p "$(dirname "$out")"
  ( umask 077; printf '%s\n' "$value" >"$out" )
  chmod 600 "$out" 2>/dev/null || true
fi

put_args=(secret put "$name")
[ -n "$worker" ] && put_args+=(--name "$worker")

echo "secret-put: setting $name (${#value} chars) via: $wrangler_cmd" >&2
( cd "$cf" && printf '%s' "$value" | "${wrangler_argv[@]}" "${put_args[@]}" )

if [ "$save" = "1" ]; then
  echo "secret-put: saved to $out (mode 600; keep it out of git)" >&2
fi
if [ "$show" = "1" ]; then
  printf '%s\n' "$value"
else
  echo "secret-put: value not printed (pass --show to print it once)" >&2
fi
