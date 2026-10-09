#!/usr/bin/env bash
#
# container-smoke.sh - real linux/amd64 container smoke for the burstel image.
#
# Builds nothing. It exercises an already-built image (the GitHub Actions
# workflow builds it with `docker buildx build --platform linux/amd64 --load`):
#
#   * /healthz and /readyz
#   * /internal/* fails closed (401) when a token is configured
#   * the Dockerfile's nonroot user (uid 10001) runs the process
#   * real OTLP traces + metrics + logs are accepted through /v1/*
#   * stop -> freeze reports size and SHA-256
#   * the frozen DuckDB is present on the container disk, matches the reported
#     size/checksum, and survives a container restart
#   * libstdc++ is linked (cgo DuckDB runtime dependency)
#
# Usage:
#   scripts/container-smoke.sh [IMAGE]
#
# Requirements: docker (with a working daemon), curl, and `go` unless
# OTLPSEND_BIN points at a prebuilt cmd/otlpsend binary. The generated token is
# ephemeral and is never written to the repository, a result file or CI
# artifacts.
set -euo pipefail

IMAGE="${1:-burstel:ci}"
PORT="${BURSTEL_SMOKE_PORT:-14318}"
NAME="burstel-smoke-$$"
SHOT="smoke-$$"
HEALTH_TIMEOUT_SECONDS="${BURSTEL_SMOKE_HEALTH_TIMEOUT:-90}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/burstel-smoke.XXXXXX")"

log() { printf '%s\n' "$*" >&2; }
die() { printf 'container-smoke: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"

# Ephemeral, non-secret token: generated locally and only ever passed through
# the container env / the otlpsend env, never through argv or a committed file.
TOKEN="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
export BURSTEL_INTERNAL_TOKEN="$TOKEN"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

log "== building OTLP smoke client (cmd/otlpsend) =="
OTLPSEND_BIN="${OTLPSEND_BIN:-}"
if [ -z "$OTLPSEND_BIN" ]; then
  command -v go >/dev/null 2>&1 || die "go not found and OTLPSEND_BIN is unset"
  OTLPSEND_BIN="$WORK/otlpsend"
  (cd "$(dirname "$0")/.." && go build -o "$OTLPSEND_BIN" ./cmd/otlpsend)
fi
[ -x "$OTLPSEND_BIN" ] || die "OTLPSEND_BIN is not executable: $OTLPSEND_BIN"

log "== image metadata =="
PLATFORM="$(docker image inspect "$IMAGE" --format '{{.Os}}/{{.Architecture}}')"
[ "$PLATFORM" = "linux/amd64" ] || die "image is $PLATFORM, expected linux/amd64"
NONROOT_USER="$(docker image inspect "$IMAGE" --format '{{.Config.User}}')"
log "   image=$IMAGE platform=$PLATFORM configured_user=${NONROOT_USER:-<root>}"

log "== running the container nonroot on 127.0.0.1:$PORT =="
docker run -d --name "$NAME" \
  -p "127.0.0.1:${PORT}:4318" \
  -e BURSTEL_INTERNAL_TOKEN="$TOKEN" \
  "$IMAGE" >/dev/null

wait_health() {
  local deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS -o /dev/null --max-time 2 "http://127.0.0.1:${PORT}/healthz"; then
      return 0
    fi
    sleep 1
  done
  return 1
}
wait_health || { docker logs "$NAME" >&2 || true; die "container did not become healthy"; }

HEALTHZ="$(curl -fsS --max-time 5 "http://127.0.0.1:${PORT}/healthz")"
READYZ_CODE="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${PORT}/readyz")"
NOAUTH_CODE="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${PORT}/internal/metadata")"
WRONGAUTH_CODE="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 \
  -H 'Authorization: Bearer definitely-wrong' "http://127.0.0.1:${PORT}/internal/metadata")"
[ "$READYZ_CODE" = "200" ] || die "/readyz returned $READYZ_CODE"
[ "$NOAUTH_CODE" = "401" ] || die "/internal/metadata without a token returned $NOAUTH_CODE, expected 401"
[ "$WRONGAUTH_CODE" = "401" ] || die "/internal/metadata with a wrong token returned $WRONGAUTH_CODE, expected 401"

log "== sending real OTLP traces + metrics + logs, then freezing =="
"$OTLPSEND_BIN" -url "http://127.0.0.1:${PORT}" -shot "$SHOT" >"$WORK/otlpsend.out"
cat "$WORK/otlpsend.out" >&2
REPORTED_SIZE="$(sed -n 's/.* size=\([0-9][0-9]*\) .*/\1/p' "$WORK/otlpsend.out" | tail -1)"
REPORTED_SHA="$(sed -n 's/.* sha256=\([0-9a-f]\{64\}\).*/\1/p' "$WORK/otlpsend.out" | tail -1)"
[ -n "$REPORTED_SIZE" ] || die "otlpsend did not report a frozen size"
[ -n "$REPORTED_SHA" ] || die "otlpsend did not report a frozen sha256"

ARTIFACT="/data/${SHOT}/telemetry.duckdb"
DISK_SIZE="$(docker exec "$NAME" stat -c %s "$ARTIFACT")"
DISK_SHA="$(docker exec "$NAME" sha256sum "$ARTIFACT" | awk '{print $1}')"
DISK_OWNER="$(docker exec "$NAME" stat -c %U "$ARTIFACT")"
[ "$DISK_SIZE" = "$REPORTED_SIZE" ] || die "on-disk size $DISK_SIZE != reported $REPORTED_SIZE"
[ "$DISK_SHA" = "$REPORTED_SHA" ] || die "on-disk sha256 $DISK_SHA != reported $REPORTED_SHA"

log "== restart: the frozen DuckDB must persist on the container disk =="
docker restart "$NAME" >/dev/null
wait_health || die "container did not become healthy after restart"
PERSIST_SHA="$(docker exec "$NAME" sha256sum "$ARTIFACT" | awk '{print $1}')"
[ "$PERSIST_SHA" = "$REPORTED_SHA" ] || die "artifact changed across restart"

log "== nonroot + cgo libstdc++ checks =="
RUNTIME_UID="$(docker exec "$NAME" id -u)"
[ "$RUNTIME_UID" = "10001" ] || die "container runs as uid $RUNTIME_UID, expected 10001"
LDD_OUT="$(docker run --rm --entrypoint /usr/bin/ldd "$IMAGE" /usr/local/bin/burstel 2>&1 || true)"
case "$LDD_OUT" in
  *libstdc++*) ;;
  *) die "burstel binary is not linked against libstdc++: $LDD_OUT" ;;
esac

cat <<JSON
{"image":"$IMAGE","platform":"$PLATFORM","configured_user":"${NONROOT_USER:-}","runtime_uid":$RUNTIME_UID,"healthz":$HEALTHZ,"readyz_status":$READYZ_CODE,"internal_unauth_status":$NOAUTH_CODE,"otlp_sent":["traces","metrics","logs"],"frozen_size":$REPORTED_SIZE,"sha256_match":true,"persisted_after_restart":true,"libstdcxx_linked":true}
JSON
log "== container smoke passed =="
