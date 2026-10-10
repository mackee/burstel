# ADR-004: Segmented R2 compaction and crash recovery

Status: accepted (spec 015). Builds on ADR-003 (durable continuous intake).
No production deployment in spec 015.

## Context

Since spec 014, every acknowledged OTLP export is durably journaled to R2
(`journal/{instance_id}/{signal}/{received_at}-{id}.bin`) and indexed in the
`IngestDO` ledger before the OTLP `200`. Nothing yet turns that raw journal into
something queryable, and querying raw protobuf objects directly is neither
bounded nor safe.

## Decision

1. **Immutable DuckDB segments.** A bounded compactor groups finalized raw
   journal objects into short receive-time windows (default 60 s, configurable;
   additionally bounded by `MaxWindowBytes`/`MaxWindowObjects`) and writes one
   immutable DuckDB segment per window per instance. Each segment carries the
   raw **wire bytes byte-for-byte**, the decompressed payload, and the existing
   Go OTLP projections (`spans`, `log_records`, `metric_points`), plus the
   trusted `instance_id` and the source journal key as provenance. Unsupported
   metric variants keep a row *and* their raw bytes, so nothing is discarded.

2. **Verify before advertise.** The compactor writes the segment, hashes the
   file, `CHECKPOINT`s and closes it, PUTs the object with its size and SHA-256,
   then **re-reads and re-hashes** the stored object before committing a manifest
   row. A partial, truncated or corrupt object is never advertised.

3. **Exactly-once, idempotent publication.** The manifest is unique on
   `segment_key` and every journal object is attributed to at most one segment
   (`segment_coverage.journal_key` is the primary key). The segment key is
   deterministic from the instance and the sorted source keys, so a retried
   attempt reuses an abandoned upload instead of duplicating it. The R2 bucket is
   the source of truth; the manifest and coverage are rebuildable by listing it.

4. **Gap-aware recovery.** `Reconcile` re-verifies every advertised segment
   against the bucket and records explicit gaps (`missing_segment`,
   `checksum_mismatch`) rather than skipping them. `Repair` rebuilds a
   missing/damaged segment from the still-present raw journal objects. A range is
   reported **incomplete** while it has a missing segment, an unresolved gap, a
   pending raw object, or a late event.

5. **Watermarked finalization and late data.** Only objects with
   `received_at <= now - grace` (default 90 s) are compacted, so a segment is
   never finalized before its window is unambiguous. An object that arrives
   after its receive time was already finalized is compacted into a new,
   `late`-tagged segment; the `late_events` counter and the range completeness
   flag make the reopened window visible.

6. **GC after verification, retention, pinning.** A raw journal object is
   deleted only once its segment is verified and manifested. Default
   unreferenced retention is a configurable 24 h rolling window (retention `<= 0`
   means unlimited). A shot pins the receive-time ranges it needs; GC never
   deletes a pinned segment's objects. Counters, bytes, object counts and
   approximate R2 PUT cost are surfaced.

7. **Read-only restore.** A segment is restored by downloading the object,
   verifying its size and SHA-256, writing it read-only and opening it with
   DuckDB `access_mode=read_only`, independent of the compactor process. Table
   names match the existing analysis schema, so the analysis engine can query a
   segment unchanged.

## Consequences

* Compaction is at-least-once but produces no duplicate logical rows or
  coverage; a crash before upload, after upload but before the manifest, or in
  the middle of GC is recoverable.
* A short-lived compactor Container (or on-demand trigger) is enough; no
  container needs to stay up for the retention window.
* A real bucket is reached by supplying an R2-backed `ObjectStore`; the engine,
  manifest and recovery logic do not change.
