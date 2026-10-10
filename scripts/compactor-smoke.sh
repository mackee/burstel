#!/usr/bin/env bash
#
# compactor-smoke.sh - real linux/amd64 smoke for the spec-018 compactor image.
#
# Builds nothing. It exercises an already-built `burstel-compact` image (the CI
# workflow builds it with `docker buildx build --platform linux/amd64 --load`):
#
#   * /healthz and /readyz
#   * /v1/segments fails closed (401/503) without the internal token
#   * a bounded build request produces a real DuckDB segment whose downloaded
#     SHA-256 matches the advertised checksum
#   * the Dockerfile's nonroot user (uid 10001) runs the process
#   * libstdc++ is linked (cgo DuckDB runtime dependency)
#
# Usage:
#   scripts/compactor-smoke.sh [IMAGE]
#
# Requirements: docker (with a working daemon), curl. The generated token is
# ephemeral and is never written to the repository, a result file or CI
# artifacts.
set -euo pipefail

IMAGE="${1:-burstel-compactor:ci}"
PORT="${BURSTEL_COMPACTOR_SMOKE_PORT:-18080}"
NAME="burstel-compactor-smoke-$$"
HEALTH_TIMEOUT_SECONDS="${BURSTEL_SMOKE_HEALTH_TIMEOUT:-90}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/burstel-compactor-smoke.XXXXXX")"
EMPTY_SHA="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

log() { printf '%s\n' "$*" >&2; }
die() { printf 'compactor-smoke: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"

TOKEN="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

log "== image metadata =="
PLATFORM="$(docker image inspect "$IMAGE" --format '{{.Os}}/{{.Architecture}}')"
[ "$PLATFORM" = "linux/amd64" ] || die "image is $PLATFORM, expected linux/amd64"
NONROOT_USER="$(docker image inspect "$IMAGE" --format '{{.Config.User}}')"
log "   image=$IMAGE platform=$PLATFORM configured_user=${NONROOT_USER:-<root>}"

log "== running the compactor nonroot on 127.0.0.1:$PORT =="
docker run -d --name "$NAME" \
  -p "127.0.0.1:${PORT}:8080" \
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
NOAUTH_CODE="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 \
  -X POST -H 'content-type: application/json' -d '{}' "http://127.0.0.1:${PORT}/v1/segments")"
WRONGAUTH_CODE="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 \
  -X POST -H 'Authorization: Bearer definitely-wrong' -H 'content-type: application/json' -d '{}' \
  "http://127.0.0.1:${PORT}/v1/segments")"
[ "$READYZ_CODE" = "200" ] || die "/readyz returned $READYZ_CODE"
[ "$NOAUTH_CODE" = "401" ] || die "/v1/segments without a token returned $NOAUTH_CODE, expected 401"
[ "$WRONGAUTH_CODE" = "401" ] || die "/v1/segments with a wrong token returned $WRONGAUTH_CODE, expected 401"

log "== building a real bounded segment and verifying the download checksum =="
REQ='{"instance_id":"smoke","instance":"smoke","objects":[{"key":"journal/smoke/traces/1-a.bin","signal":"traces","received_at":1,"size":0,"sha256":"'"$EMPTY_SHA"'","content_type":"application/x-protobuf","body_base64":""}]}'
curl -fsS --max-time 60 -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d "$REQ" "http://127.0.0.1:${PORT}/v1/segments" >"$WORK/build.json"
BUILD_SHA="$(sed -n 's/.*"sha256":"\([0-9a-f]\{64\}\)".*/\1/p' "$WORK/build.json" | head -1)"
BUILD_TOKEN="$(sed -n 's/.*"download_token":"\([^"]*\)".*/\1/p' "$WORK/build.json" | head -1)"
[ -n "$BUILD_SHA" ] || die "build response had no sha256: $(cat "$WORK/build.json")"
[ -n "$BUILD_TOKEN" ] || die "build response had no download_token"

curl -fsS --max-time 60 -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:${PORT}/v1/segments/${BUILD_TOKEN}" -o "$WORK/segment.duckdb"
DOWNLOAD_SHA="$(sha256sum "$WORK/segment.duckdb" | awk '{print $1}')"
[ "$DOWNLOAD_SHA" = "$BUILD_SHA" ] || die "downloaded segment sha $DOWNLOAD_SHA != advertised $BUILD_SHA"

log "== nonroot + cgo libstdc++ checks =="
RUNTIME_UID="$(docker exec "$NAME" id -u)"
[ "$RUNTIME_UID" = "10001" ] || die "container runs as uid $RUNTIME_UID, expected 10001"
LDD_OUT="$(docker run --rm --entrypoint /usr/bin/ldd "$IMAGE" /usr/local/bin/burstel-compact 2>&1 || true)"
case "$LDD_OUT" in
  *libstdc++*) ;;
  *) die "burstel-compact binary is not linked against libstdc++: $LDD_OUT" ;;
esac

cat <<JSON
{"image":"$IMAGE","platform":"$PLATFORM","configured_user":"${NONROOT_USER:-}","runtime_uid":$RUNTIME_UID,"healthz":$HEALTHZ,"readyz_status":$READYZ_CODE,"unauth_status":$NOAUTH_CODE,"wrongauth_status":$WRONGAUTH_CODE,"segment_sha256_match":true,"libstdcxx_linked":true}
JSON
log "== compactor smoke passed =="
