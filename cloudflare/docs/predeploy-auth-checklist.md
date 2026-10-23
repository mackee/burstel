# Predeployment auth checklist (spec 008)

This checklist must be satisfied before any production `wrangler deploy`. It is
reviewable without a Cloudflare account; no deployment is performed by the code
change that introduced it.

## Credentials are distinct and explicit

| Secret | Scope | Where it lives | Never |
| --- | --- | --- | --- |
| `BURSTEL_API_TOKEN` | admin: `/api/*`, `/mcp`, legacy per-shot `/shots/{id}` | Worker secret | sent to a container, logged, committed |
| `BURSTEL_INTERNAL_TOKEN` | recorder `/internal/*` only (defense in depth) | Worker secret **and** injected into the container env by the DO | reused as the admin token, logged, committed |
| instance push token | `POST /ingest/v1/{traces,metrics,logs}` only, exactly 24h, routes to its named instance | minted by `POST /api/instances/{name}/tokens` (admin\|member), returned once; positive grants cached in KV | stored raw (only its digest lives in `ControlDO`/KV), logged, used for admin routes |
| per-person token (`admin`/`member`) | `/api/*` and `/mcp`; `member` is full operational but never registry; `admin` may manage the registry | minted by `POST /api/tokens` (admin only), returned once | stored raw (only its digest lives in `AuthDO`), logged, used as a push/internal token |

Requirements, all **fail closed** when violated:

1. `BURSTEL_INTERNAL_TOKEN` is non-empty and **different** from
   `BURSTEL_API_TOKEN`. An equal token is refused (`internal_token_not_distinct`).
2. A missing internal token is refused (`internal_token_missing`). There is no
   `BURSTEL_API_TOKEN` fallback: the admin token is never forwarded into the
   container environment or its request headers.
3. The instance push-only token is independent, random (>= 256 bits), and never
   used as either the admin or the internal token. Ingestion authenticates it
   (memory -> KV `INSTANCE_TOKENS` -> authoritative `ControlDO`) before any
   bucket write and only acknowledges (`200`) after the raw bytes are durable in
   R2; a write failure or a late write is a retryable `5xx`. Negative admission
   results are never cached. See ADR-007 for the exact expiry/revocation
   boundaries.
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
* Tokens are compared in constant time and never logged. The control plane and
  KV store only the one-way SHA-256 digest of an instance push token.
* Per-person registry tokens (spec 013, Part B) store only a SHA-256 digest in
  `AuthDO`; `GET /api/tokens` never returns a value, and a revoked/expired token
  fails immediately (`401`). Token issuance is the trust boundary: hand out one
  `member` token per person and revoke individually rather than sharing the
  admin secret.

## Legacy admin per-shot endpoint

* `POST /shots/{id}/v1/{traces,metrics,logs}` is **deprecated** and remains
  admin-scoped only (`BURSTEL_API_TOKEN`); an instance push token is rejected
  there.
* Prefer the stable `POST /ingest/v1/{traces,metrics,logs}` with a 24h
  **instance** token. Reconfigure exporters once per day, not once per shot.
* A shot id is a capability: it grants access to that shot only, and every
  request still requires the admin token.

## Durable intake and retry semantics

* The stable instance endpoint authenticates first, writes the original bytes to
  a unique R2 object, and only then answers OTLP `200`. Every acknowledged
  export is either included in a verified segment or explicitly reported as
  `excluded_revoked`; it is never silently missing. A valid export is **never**
  discarded for lack of an active shot, and an R2 failure or a late write is a
  retryable `5xx`.
* The legacy per-shot path (`/shots/{id}/v1/*`) has no durability guarantee of
  its own: it proxies to the shot's recorder, so a request rejected because the
  shot is stopping/not-recording may be answered with a valid OTLP `200` and
  `X-Burstel-Disposition: ignored-no-active-shot`, while genuine failures stay a
  retryable `503`.
* Revocation is absolute for analysis but eventually consistent at the edge
  (nominal KV propagation <= 60 s). Compaction excludes objects written at/after
  the token's effective revocation instant and deletes them.
* A recording shot whose container has **stopped** is terminally lost (spec 012):
  the shot becomes `failed` (`lost: true`). This affects only the legacy path; it
  has no effect on instance intake.
* In-flight, previously admitted writes are drained before a shot is frozen; no
  accepted batch is lost and no late batch is attributed to the next shot.
