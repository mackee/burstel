# ADR-005 — A shot is an immutable logical time window, not an ingest target

Status: accepted (spec 016)

## Context

Spec 014 made ingestion continuous and instance-scoped: a named instance
(`web-1`, `db-1`, …) authenticates OTLP with a 24h push-only token, and the raw
export is journaled to R2 before it is acknowledged. Spec 015 compacts that
journal into immutable, checksummed DuckDB **segments** described by a durable
manifest. Nothing in that pipeline needs or benefits from a per-shot recording
container, and a per-shot ingest token forces the exporter to be reconfigured
between benchmark runs.

The benchmark unit we actually want is a labelled *time window* over those
continuous streams: "the 3 minutes around this deploy" or "the 2 minutes of the
load phase".

## Decision

A **shot** is an immutable, half-open logical time window `[start, end)` over a
set of named instances, plus durable metadata. It is a query/analysis label, not
an ingest target.

* **Half-open semantics.** `start` is included, `end` is excluded, for every
  signal. Span default is start-in-shot; a span that crosses an edge is counted
  by its start and reported in the crossing diagnostics.
* **Provenance.** `server` windows come from the burstel clock
  (`shot_start` / `shot_stop`). `external` windows are supplied by a client
  (`shot_create`) and are retained as **untrusted client-clock** values; the
  trust class is derived from provenance and cannot be forged. External
  timestamps are validated (ordered, bounded to 1h, retained for 7d, ≤5min
  clock skew).
* **No per-shot credentials or containers.** No `shot_ingest_token`, no
  recording container, no token rotation, no ingest detach. `stop` is
  idempotent. The retired `/api/ingest-token` and
  `/api/shots/{id}/ingest-token` return `410 Gone` with migration guidance.
* **Instance scope.** A window may select a subset of instances; an empty set
  means every appropriate registered instance. Windows may overlap freely;
  overlapping does not duplicate ingestion.

## Analysis

Analysis is **segment-backed and read-only**:

1. Candidate segments are selected from the durable manifest. A segment is a
   candidate when its receive-time range overlaps `[start − ingestLag,
   end + ingestLag)`, **or** when its event min/max overlaps the half-open
   window (catching a late export whose receive time is far after the window).
   Advertised-but-unverified segments are reported as missing and counted in
   `expected`, so coverage can never silently under-report.
2. The Worker streams the selected verified segment objects from R2 into a
   throwaway window session on the analysis container. There the rows are
   materialized into a fresh scratch DuckDB file, filtered per signal on its
   default time basis (span start / metric time / log event time with observed
   fallback), then reopened **read-only** with external access disabled.
3. User SQL runs only against the shot-scoped logical tables
   (`schema_meta`, `otlp_batches`, `spans`, `log_records`, `metric_points`).
   There is no raw instance/global table to reach. The existing read-only SQL
   policy (single statement, table/function allowlists, row/value/result caps)
   is reused and additionally rejects `ATTACH`, table functions, `COPY`,
   filesystem/network access and unsafe PRAGMAs. A regex `WHERE` is never
   appended to arbitrary SQL; scoping is structural.
4. **Cumulative counters.** A cumulative monotonic sum cannot be read as a
   per-shot value. A pre-window baseline is materialized into a private table
   that is *not* in the query policy allowlist; the `metric_cumulative_delta`
   section reads it only to derive `last(window) − last(baseline)` and never
   exposes pre-window rows.
5. **Comparison.** `shot_compare` builds independent scopes for both windows,
   uses consistent time bases, and always discloses differences in duration,
   instance set and coverage. It never asserts a causal conclusion.

## Coverage and honesty

Every analysis result carries instance identity, sample counts, units, coverage
and known incompleteness. Missing segments, late arrivals (batches received
after the window that may carry in-window events), log observed-time fallbacks
and crossing spans are labelled, never dropped.

## Compatibility

Legacy frozen recording shots (pre-016) remain readable: their `shot_id` still
dispatches through the original restore-and-query path, and a mixed comparison
is rejected with an explicit error rather than a wrong result.

## Tooling

MCP exposes `shots_list`, `shot_create`, `shot_start` (with `mode=window`),
`shot_stop`, `shot_get`, `shot_query`, `shot_analyze`, `shot_compare`,
`shot_schema`, `instances_list`, `instance_token_mint`/`_list`/`_revoke`.
Operational instance and shot tools are available to `admin` and `member`;
the per-person `token_mint`/`token_list`/`token_revoke` registry stays
`admin`-only. Read/list/query tools never return instance credentials.

## Operator workflow

```bash
burstel-cli instance create web-1
burstel-cli instance create db-1
burstel-cli instance token issue web-1 --out .secrets/web-1   # once per day
burstel-cli instance token issue db-1  --out .secrets/db-1    # once per day
# exporters send continuously; no exporter change between shots
burstel-cli shot create --name load --instances web-1,db-1 \
  --started-at 2026-01-01T12:00:00Z --ended-at 2026-01-01T12:03:00Z
burstel-cli shot analyze <shot-id> --sections latency,metric_cumulative_delta
burstel-cli shot compare <shot-id> --other <neighbour-shot-id>
```

## Consequences

* Exporters are configured once per day and never per shot.
* Empty windows, early/late data, clock skew and pending compaction are
  diagnosable from the returned coverage/diagnostics rather than from silence.
* The analysis container is ephemeral and read-only; destroying it loses
  nothing because R2 remains authoritative.
* Segment selection must be conservative; over-selecting is safe (extra rows
  are filtered structurally) while under-selecting would hide data, which is
  why the event-range fallback exists.
