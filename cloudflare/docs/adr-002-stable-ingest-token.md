# ADR-002: Stable OTLP `/ingest` with a 24h push-only token

Status: accepted (spec 006)

## Context

In ISUCON an operator runs many 1–3 minute benchmark shots per day. The Phase
2/3 design required a collector to be reconfigured with a shot-specific
`/shots/{id}` endpoint (and the global `BURSTEL_API_TOKEN`) for every shot.
That is impractical and risks leaking the full admin token onto benchmark hosts.

## Decision

1. **Stable receiver.** Standard OTel HTTP exporters always target
   `https://<host>/ingest` (`/ingest/v1/{traces,metrics,logs}`). The URL is
   identical for every shot.
2. **Push-only token.** `POST /api/ingest-token` (admin-authenticated) issues a
   CSPRNG token of at least 256 bits. It is returned raw exactly once and is
   accepted only at `POST /ingest/v1/{traces,metrics,logs}`. It is *not* the
   admin token and cannot reach `/api/*`, `/mcp`, query/stop or the legacy
   per-shot path.
3. **Exact 24h TTL.** `expires_at = issued_at + 86_400s`. A request is admitted
   iff `now < expires_at`; at `expires_at` or later the server replies `401` and
   proxies nothing. Expiry concerns the **token**, never the shot: recording,
   freezing, analysis and R2 retention are unaffected, and frozen artifacts do
   not expire.
4. **SQLite-backed coordinator.** A singleton, non-container `IngestDO`
   (SQLite storage) stores only the SHA-256 digest of the token plus
   `issued_at`/`expires_at`/`revoked` and the active-shot mapping. The raw
   secret is never persisted, logged or placed in R2/git/environment. Digest
   comparison is length-folded constant time.
5. **Atomic rotation, bounded semantics.** Issuing a replacement atomically
   revokes the prior digest. Rotation is refused while a shot is recording so a
   benchmark cannot be abruptly cut off. There is no silent infinite renewal.
6. **Active-shot routing.** `shot_start` activates its shot once it is
   recording; `/ingest` forwards to that shot, which enforces `recording`.
   `shot_stop` detaches after the freeze completes. A recording shot cannot be
   silently displaced: a second `shot_start` returns `409`.
7. **Background data is consciously dropped.** With no active shot, the server
   returns a valid OTLP/HTTP `200` (empty proto message, or `{}` for JSON) with
   `X-Burstel-Disposition: ignored-no-active-shot` and counts the discard. This
   avoids exporter retry storms and unbounded queues. Nothing is stored.

## Cross-DO preconditions and failure recovery

There is no transactional atomicity across DOs. The coordinator provides
*ordering per admission*, with documented preconditions:

* **Token rotation vs. requests.** Issuance and admission are serialized by the
  `IngestDO`. A request admitted immediately before rotation is allowed to
  complete (in-flight tolerance); requests arriving after the replacement see
  the new digest and are `invalid`. This is acceptable and does not resurrect a
  revoked token.
* **Start vs. stop races.** `ingest_active` is set to `recording` on activation
  and cleared on detach. If a stop wins the race, in-flight requests already
  admitted to the old shot are drained/frozen with it; new requests observe no
  active shot (discard) or a `409 not_recording`, which the Worker converts to a
  discard and detaches.
* **DO restart / stale mapping.** `shot_start` reconciles a persisted active
  mapping whose shot is no longer recording (for example a crash between freeze
  and detach) by detaching it before activating the new shot. Ingest likewise
  self-heals on a `409`. No request is ever attributed to the wrong shot.
* **Rollback.** If activation loses a concurrent race, the freshly created shot
  is best-effort stopped so it cannot linger as an unreachable recording orphan.

## Consequences

* The legacy `/shots/{id}/v1/*` path and `BURSTEL_API_TOKEN` ingest remain for
  backwards compatibility, but are documented as deprecated; the admin token is
  never accepted at `/ingest` and the push token is never accepted elsewhere.
* Admin authentication fails closed when no `BURSTEL_API_TOKEN` is configured.
  The only bypass is the explicit local-development flag
  `BURSTEL_DEV_INSECURE_AUTH=1`.
* `GET /api/ingest-token` exposes status (`expires_at`, `remaining_seconds`,
  sub-hour warning) and safe counters (missing/invalid/expired/revoked/
  accepted/discarded) without ever returning the secret or digest.
* No production deployment, R2 bucket creation or secret provisioning happens in
  this change; the new `IngestDO` namespace is a configuration/code change only.
