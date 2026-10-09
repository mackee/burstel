# Predeployment auth checklist (spec 008)

This checklist must be satisfied before any production `wrangler deploy`. It is
reviewable without a Cloudflare account; no deployment is performed by the code
change that introduced it.

## Credentials are distinct and explicit

| Secret | Scope | Where it lives | Never |
| --- | --- | --- | --- |
| `BURSTEL_API_TOKEN` | admin: `/api/*`, `/mcp`, legacy per-shot `/shots/{id}` | Worker secret | sent to a container, logged, committed |
| `BURSTEL_INTERNAL_TOKEN` | recorder `/internal/*` only (defense in depth) | Worker secret **and** injected into the container env by the DO | reused as the admin token, logged, committed |
| push-only token | `POST /ingest/v1/{traces,metrics,logs}` only, exactly 24h | minted by `POST /api/ingest-token`, returned once | stored raw, logged, used for admin routes |

Requirements, all **fail closed** when violated:

1. `BURSTEL_INTERNAL_TOKEN` is non-empty and **different** from
   `BURSTEL_API_TOKEN`. An equal token is refused (`internal_token_not_distinct`).
2. A missing internal token is refused (`internal_token_missing`). There is no
   `BURSTEL_API_TOKEN` fallback: the admin token is never forwarded into the
   container environment or its request headers.
3. The push-only token is independent, random (>= 256 bits), and never used as
   either the admin or the internal token.
4. The recorder itself refuses `/internal/*` when `BURSTEL_INTERNAL_TOKEN` is
   empty, so a directly-reachable container is not left open.

```bash
# Generate and set distinct secrets (do not paste into logs or tickets).
wrangler secret put BURSTEL_API_TOKEN
wrangler secret put BURSTEL_INTERNAL_TOKEN
```

## No default or bypass in production

* `BURSTEL_DEV_INSECURE_AUTH` must **not** be set (or must be unset/`0`). It is
  the only way to run admin routes without `BURSTEL_API_TOKEN` and `/internal/*`
  without `BURSTEL_INTERNAL_TOKEN`, and exists solely for trusted local
  `wrangler dev`.
* There are no hardcoded credential defaults anywhere in the Worker, the DO, the
  recorder or the image.
* Tokens are compared in constant time and never logged. The DO stores only the
  push token's one-way SHA-256 digest.

## Legacy admin per-shot endpoint

* `POST /shots/{id}/v1/{traces,metrics,logs}` is **deprecated** and remains
  admin-scoped only (`BURSTEL_API_TOKEN`); the push token is rejected there.
* Prefer the stable `POST /ingest/v1/{traces,metrics,logs}` with a 24h push
  token. Reconfigure exporters once per day, not once per shot.
* A shot id is a capability: it grants access to that shot only, and every
  request still requires the admin token.

## Stop race and retry semantics

* A request rejected because the shot is stopping/not-recording is *provably
  uncommitted*; the stable `/ingest` forwarder answers a valid OTLP
  `ExportResponse` `200` with `X-Burstel-Disposition: ignored-no-active-shot`.
* Genuine failures — storage write error, unavailable/down container, request
  write timeout, capacity exhaustion, crash, or any unknown outcome — stay a
  retryable `503` and are **never** acknowledged as success.
* In-flight, previously admitted writes are drained before a shot is frozen; no
  accepted batch is lost and no late batch is attributed to the next shot.
