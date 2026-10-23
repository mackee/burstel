# ADR-007: DO-free ingest hot path, KV admission and compaction-time aggregation

Status: accepted (spec 023). Amends the intake guarantees of ADR-003 (spec 014).

## Context

The spec-014 intake path performed, serially for every OTLP export:

1. `POST /internal/instance/admit` to the singleton `IngestDO`;
2. one R2 PUT of the raw bytes;
3. `POST /internal/journal/commit` to the same `IngestDO`.

All three target one Durable Object, so throughput is bounded by a single DO's
serialized request latency. A standalone field trial (spec 022) showed silent
client-side drops once the exporter's BSP ceiling was exceeded. The Cloudflare
side had never been measured.

## Decision

### D1. Token admission via KV

Instance tokens are admitted from:

1. **isolate memory** — positive entries only, `<= 30 s`;
2. **Workers KV `INSTANCE_TOKENS`** — key `sha256(token)`, value
   `{instance_id, instance_name, token_id, expires_at}`, `cacheTtl = 60 s`;
3. the **authoritative ControlDO** on a miss (`/internal/instance/admit`).

A successful authoritative check backfills KV and memory. **Negative results are
never cached**, so a newly issued token works immediately.

Expiry is evaluated locally from `expires_at` and is exact: accepted iff
`now < expires_at`. Issuance and rotation write KV; revocation and rotation
delete the affected digests. Existing `bit_` tokens keep working; a one-shot
`POST /api/instance-tokens/backfill` populates KV for every live token.

### D2. Ledger-free journal write

After admission the Worker validates the body, PUTs the original bytes to
`journal/{instance_id}/{signal}/{received_at}-{rand}.bin` with custom metadata
`{token_id, instance, instance_id, signal, received_at, sha256,
content_encoding, content_type}` and returns OTLP 200. There is no
`journal_ledger` write and no `/internal/journal/commit` on the hot path.

**Late-write guard.** If the PUT completes later than
`received_at + LATE_WRITE_LIMIT_MS` (default `COMPACTION_GRACE_MS / 2`), the
object is deleted and the request is answered with a retryable `503` so the
exporter resends with a fresh `received_at`.

Ingest counters (accepted, rejected-by-reason, bytes, `received_at`,
duration) are emitted as Workers Analytics Engine data points
(`INGEST_ANALYTICS`). A missing/failing dataset never fails an ingest.

### D3. Compaction-time aggregation

The compactor discovers work by **listing R2**, not by reading a ledger:

* only objects with `received_at < now - COMPACTION_GRACE_MS` are finalized;
* every run re-lists a **lookback window >= 2 x grace** from
  `min(discovery_cursor, now - lookback)`, so a late object that lands after a
  previous run's cursor is picked up;
* objects already covered by a verified segment are de-duplicated by key;
* an object whose `token_id` was revoked at/before `received_at` is excluded
  from every segment (`excluded_revoked` in the compaction report), recorded in
  `excluded_journal`, and deleted from R2 as soon as it is proven final;
* segments aggregate one receive-time bucket **across instances**; the synthetic
  manifest scope is `all` while each DuckDB row keeps its real `instance_id`
  column, and window analysis filters by instance;
* `pendingJournalInRange` / coverage disclosure is computed from the R2 listing
  of the window range.

Rotation grace is handled correctly: a rotated token's effective revocation
instant is `max(revoked_at, grace_until)`, so objects accepted during the
documented overlap are not excluded.

### D4. ControlDO (deploy-safe `IngestDO` export)

The code-level control-plane role is `ControlDO`, but the **exported Wrangler
storage class keeps the legacy name `IngestDO`** (spec 024 R1). It owns the
instance registry, authoritative tokens, shots, segment manifest, compaction
lease/pins and discovery cursor, and is not on the ingest hot path.

Keeping the class name is deliberate: a real rename requires a
`renamed_classes` migration, which this repo's `exports`-style Wrangler config
cannot declare. Deploying a new class name without it would create empty
storage and orphan all authoritative state. `preflight` enforces
`INGEST_DO.class_name == "IngestDO"` and refuses a separate `ControlDO` export.

## Revised guarantees (replacing the corresponding spec-014 wording)

1. **Nothing durable before authentication** still holds. Authentication may be
   served from a positive KV/memory grant; the authoritative ControlDO is the
   source of truth and is consulted on any cache miss.
2. **Expiry is exact.** A token is accepted iff `now < expires_at`, including
   when the grant came from cache.
3. **Revocation is absolute for analysis, eventually consistent at the edge.**
   Revocation takes effect at the edge within the KV propagation window
   (nominally `<= 60 s`: 30 s memory + 60 s KV, overlapping). Data written after
   a token's effective revocation instant is excluded from every segment and
   from pending counts, and is retained in place for the configured audit
   window (default 7 days, spec 024 R5) before deletion, so it can never appear
   in analysis.
4. **The durable ledger guarantee becomes:** every `200`-acknowledged export is
   a durable R2 object, and is either included in a verified segment or
   explicitly reported as `excluded_revoked` — never silently missing.
   Duplicate upstream retries remain bounded (distinct immutable objects,
   de-duplicated at compaction by key and checksum).
5. **Revocation lookup is fail-closed at compaction.** If the authoritative
   revocation set cannot be read, the tick fails and retries; it never compacts
   objects whose token state is unknown (spec 024 R7).

## Consequences

* The ingest hot path is "authenticate from cache + one R2 PUT" on a hit.
* The cost of revocation convergence moves from an RPC to a bounded cache
  window; the correctness guarantee is preserved by D3.2.
* Compaction must list R2 prefixes; the key layout embeds a fixed-width
  `received_at` so discovery seeks with `startAfter` instead of scanning the
  whole bucket.
* The DO storage class stays `IngestDO`; no deployment-time rename migration is
  needed (spec 024 R1). No deployment is performed here.
