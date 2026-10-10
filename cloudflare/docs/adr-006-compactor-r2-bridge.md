# ADR-006 — Worker/DO-mediated R2 ↔ Go compactor bridge (spec 018)

Status: accepted (spec 018, code only; no deploy)

## Context

Spec 014 durably journals every acknowledged OTLP export to `journal/*` in R2
and records a ledger row in the singleton `IngestDO`. Spec 015's Go compactor
(`internal/compaction`) turns that journal into immutable DuckDB segments, but
its production entrypoint only accepted a filesystem mirror of the bucket
(`-store-dir`) plus a local DuckDB manifest. Nothing in production selected
uncovered journal, ran the compactor and published a *verified* segment to the
`IngestDO` — so spec 016 window analysis had no real segments to read.

The bucket binding (`env.BUCKET`) is available to Workers and Durable Objects,
but **not** automatically inside a Container. Two integration shapes were
possible:

1. inject an R2 S3 API credential into the Container and let the Go process
   read/write R2 directly; or
2. keep the Container credential-free and have a Durable Object mediate every
   R2 read/write over a private, bounded HTTP contract.

## Decision

Use a **private, authenticated DO-mediated bridge**.

* A new independent `CompactorDO` (Container DO, `max_instances: 1`) owns the
  short-lived `burstel-compact` service image (`Dockerfile.compactor`).
* The DO is the only component with the `BUCKET` binding. It selects uncovered,
  finalized journal keys from the `IngestDO`, streams their exact bytes into the
  container, and streams the resulting segment bytes straight back into R2.
* No account-wide R2/S3 credential is ever created, stored, rotated or injected
  into an instance. The only secret the container sees is a distinct
  `BURSTEL_INTERNAL_TOKEN`, never the admin or an instance token.
* The Go service never talks to R2. It reuses `internal/compaction.BuildSegment`
  and the OTLP decoder/projections unchanged; it exposes only
  `GET /healthz`, `GET /readyz`, `POST /v1/segments` and a single-use
  `GET/DELETE /v1/segments/{token}`.

### Durability and correctness

* **Coordinator source of truth is SQLite `IngestDO`**, never a local DuckDB
  manifest. `planCompaction` returns uncovered, grace-finalized objects grouped
  into receive-time windows; `publishSegment` records the manifest + coverage.
* **Fenced, single-active lease.** `compaction_lease` is unique per partition
  with `(holder, fencing, expires_at)`. A live lease is never stolen; an expired
  lease is taken over with an incremented fence. A stale holder's
  `recordCompaction` result is ignored.
* **R2 is the integrity boundary.** The DO passes the advertised SHA-256 to
  `R2.put`, which rejects a mismatch; the DO then HEAD-verifies size/checksum
  and the `IngestDO` independently re-verifies the stored object before it
  advertises a segment (closing the "believe an unverified client hash" gap).
* **Idempotent retries.** The segment key is deterministic over the sorted
  source keys, and coverage is unique on `journal_key`, so a crash after the R2
  PUT but before the publish is repaired by the next tick without
  double-counting.
* **Bounded work.** Per-tick time budget, per-batch object/byte caps, a bounded
  segment size and single-use downloads bound memory and cost well below the
  15-minute scheduled/alarm wall limit.

### Scheduling and retention

* A Cron `scheduled` trigger is the backstop; the DO's own alarm is the primary
  scheduler and re-arms after every tick.
* GC (deleting covered raw journal) is **disabled by default** (`COMPACTION_GC`
  defaults to `0`) and only ever deletes objects attributed to a *verified*
  segment. Logical window shots auto-pin their receive-time range with a
  TTL-bounded pin so a shot protects the data it needs without accumulating
  permanent protection.

## Consequences

* The Container stays a small, credential-free, short-lived CPU worker; the
  attack surface is a reserved `/internal/*` DO surface plus a token-gated,
  size-bounded `/v1/*` container surface.
* Window analysis needs no manual `publish` step: once the cron/alarm publishes
  a verified segment, `shot_query`/`shot_analyze`/`shot_compare` stream it from
  R2 automatically, and a window with un-compacted journal is explicitly
  `incomplete` (with `pending_journal`) rather than silently empty.
* A live Cloudflare E2E, image build and Container scheduling are **not**
  exercised by this code-only job; see the spec-018 rollout runbook.
