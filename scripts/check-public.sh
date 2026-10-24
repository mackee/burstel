#!/usr/bin/env bash
#
# check-public.sh - fail if tracked files still contain deployment/operator
# identifiers that must never appear in the public repository.
#
# The patterns are assembled at runtime so this guard does not match itself.
#
# Usage:
#   scripts/check-public.sh              # scan this repository's tracked files
#   scripts/check-public.sh --root DIR   # scan another git checkout
set -euo pipefail

ROOT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --root) ROOT="${2:?--root needs a directory}"; shift 2 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "check-public: unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "check-public: $ROOT is not a git work tree" >&2
  exit 2
fi

# Keep the literals split so the guard file itself stays clean.
PATTERN="triple""point|""melt""hills|""Melt""Hills|/nix/store/[a-z0-9]{32}"

if matches="$(git grep -nE -e "$PATTERN" -- . 2>/dev/null)" && [ -n "$matches" ]; then
  echo "check-public: found identifiers that must be removed before publishing:" >&2
  printf '%s\n' "$matches" >&2
  exit 1
fi

echo "check-public: OK - no deployment/operator identifiers in tracked files"
