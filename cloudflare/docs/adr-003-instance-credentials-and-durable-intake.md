# ADR-003: Named instances and durable continuous OTLP intake

Status: accepted (spec 014). Superseded in part by ADR-007 (spec 023): the
per-request ledger and the immediate-at-the-edge revocation wording are
replaced by KV token admission, one R2 object per acknowledged export, and
compaction-time revocation exclusion. Named instances, token -> instance
routing and the 24 h token lifetime are unchanged. It still supersedes the
per-shot ingest-token and token→shot ingest-routing design from spec 013 and
the installation-wide token from spec 006, while preserving spec 013's
per-person `admin`/`member` credentials and the existing frozen-shot
read/compare behavior.

## Context

Collectors were reconfigured per shot: a per-shot push token routed the stable
`/ingest` endpoint to exactly one recording shot. That couples a day of
telemetry to shot boundaries — a batch that arrives while no shot is recording
is consciously discarded, and every shot needs a fresh exporter configuration.
It also imposed an application-level `MAX_CONCURRENT_RECORDING` rule.

Real deployments capture continuously: one host (`web-1`, `db-1`, …) runs an
exporter all day regardless of any benchmark window. Telemetry must be durably
retained even when no shot is active, and a shot later selects a time window.

## Decision

1. **Named instances.** A logical `instance` (`POST /api/instances`) has an
   explicit, normalized name and a server-assigned immutable, opaque
   `instance_id`. `admin` and `member` may manage instance credentials; only
   `admin` may manage *person* tokens.

2. **Instance tokens, token → instance.** `POST /api/instances/{name}/tokens`
   issues a 256-bit push-only bearer token valid for exactly 24h from issue,
   persisted only as a SHA-256 digest in the SQLite `IngestDO`, compared in
   constant time and returned raw exactly once. Tokens are individually
   revocable (`DELETE …/tokens/{id}`) and rotatable with a bounded overlap
   (`{"rotate":true,"overlap_seconds":N}`, capped at 1h); rotation never affects
   another instance. A request is admitted iff `now < expires_at`.

3. **Continuous durable intake.** `POST /ingest/v1/{traces,metrics,logs}`
   authenticates the instance token, then: (a) validates the body
   (protobuf/JSON, optional gzip); (b) writes the original, unmodified bytes to
   a **unique** R2 object under
   `journal/{instance_id}/{signal}/{received_at}-{id}.bin`; (c) commits a durable
   ledger row in the `IngestDO`; and only then (d) returns the signal-specific
   OTLP `Export*ServiceResponse`. **The ACK happens only after both durable
   writes.** If either fails the request is a retryable `5xx`; a valid export is
   never answered with a discard while no shot is active. Distinct equal
   payloads are never merged; duplicate delivery under upstream retry is bounded
   and documented (the ledger is idempotent on the object key, and
   reconciliation recovers an object whose ledger row was lost). No runtime
   Container is required to acknowledge intake.

   > **Amended by ADR-007 / spec 023.** Step (c) is gone: the ACK now depends on
   > one durable R2 object only, and compaction discovers it by listing. Every
   > 200 is a durable object that is either in a verified segment or explicitly
   > reported as `excluded_revoked`. See ADR-007 for the late-write guard and
   > the revised revocation guarantee.

4. **Trusted identity.** The server assigns `burstel.instance_id` and
   `burstel.instance` as trusted, separate fields in R2 metadata and the ledger.
   The original bytes, encoding, signal and `request-received_at` are preserved;
   user OTel resource labels are never trusted or overwritten.

5. **No recording cap.** The `MAX_CONCURRENT_RECORDING` application rule is
   removed; concurrency is bounded only by the explicit backend container
   `max_instances` control.

6. **Compatibility.** Person `admin`/`member` credentials (spec 013, Part B) are
   unchanged. `POST /api/ingest-token` and `POST /api/shots/{id}/ingest-token`
   return `410 Gone`. The legacy admin path `/shots/{id}/v1/*`, frozen R2 shot
   archives (`shots/…`) and the old analyzer are untouched; the raw journal
   lives under a separate `journal/…` prefix, so there is zero silent
   misattribution between old and new data.

## Consequences

* A collector is configured once per day and keeps sending across shots and idle
  periods. A later spec (015/016) selects a time window from the raw journal.
* Per-request cost includes one R2 PUT and one durable ledger commit; both are
  bounded and documented as the price of the ACK-before-200 guarantee. Request
  size is bounded by `MAX_INGEST_BYTES` (default 8 MiB) and backpressure is
  surfaced as retryable `5xx`.
* The intake path has no Container dependency, so a shot's container lifecycle
  can never discard or redirect accepted telemetry.
