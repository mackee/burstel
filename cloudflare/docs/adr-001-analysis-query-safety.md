# ADR-001: Analysis query safety and frozen storage format

Status: accepted (Phase 3)

## Context

Phase 3 adds post-run analysis of a frozen shot without the original recording
Container. A frozen shot is a single DuckDB file in R2
(`shots/{id}/telemetry.duckdb`) plus `shots/{id}/metadata.json`. Callers want to
run SQL against it, and an LLM-driven MCP client is one of the callers.

Two risks dominate:

1. **Query safety.** DuckDB's `access_mode=read_only` prevents writes to *the
   database*, but a read-only connection can still read arbitrary local files
   (`read_csv`, `read_parquet`, `glob`, `read_blob`), reach the network
   (`httpfs`), inspect settings/credentials (`getenv`, `duckdb_settings`),
   install extensions, and exhaust memory. "Read-only" is not a security
   boundary for untrusted SQL.
2. **Immutability of the frozen artifact.** The recording container is gone.
   If analysis opened the R2 object (or a shared cached copy) read-write, a bug
   or hostile query could corrupt the only copy of the shot.

## Decision

### Storage format

* Keep the Phase 1 DuckDB layout. `otlp_batches.payload` remains the lossless
  source of truth; `spans`, `log_records` and `metric_points` remain
  convenience projections. Analysis reads projections for speed and can still
  reach the raw batches for anything not projected.
* Analysis never opens the R2 object. A ShotDO streams the frozen bytes from R2
  into a **fresh analysis container**, which verifies `size` and `sha256`
  against the freeze record before opening the file `access_mode=read_only`.
  R2 stays authoritative; the restored copy is ephemeral and disposable.
* The restored snapshot is reused for the lifetime of the analysis container
  (bounded by the container inactivity timeout and `/internal/analysis/release`).
  It is never written back.

### Query safety

Enforce, in the Go process that owns DuckDB (not only at the Worker edge):

* **One statement**, and it must be `SELECT` or `WITH ... SELECT`. `COPY`,
  `ATTACH`, `DETACH`, `INSTALL`, `LOAD`, `EXPORT`, `IMPORT`, `PRAGMA`, `SET`,
  `CREATE`, `DROP`, `ALTER`, `INSERT`, `UPDATE`, `DELETE`, `BEGIN`/`COMMIT` and
  friends are rejected as bare tokens.
* **Table allowlist**: `schema_meta`, `otlp_batches`, `spans`, `log_records`,
  `metric_points`. Unknown tables (including `information_schema`), derived
  tables and table functions are rejected.
* **Function allowlist**: only deterministic, non-I/O scalar/aggregate/window
  functions are permitted. Any unknown identifier used as a function call is
  rejected, which blocks `read_csv`, `read_parquet`, `glob`, `httpfs`,
  `read_blob`, `getenv`, `duckdb_settings`, `query_table` and similar.
* **Resource bounds**: a hard outer `LIMIT` (default 1000), a query timeout
  (default 15s), a DuckDB `memory_limit` (default 256MB, 2 threads), a maximum
  column count, and result/value byte budgets. String and BLOB values are
  truncated, and BLOBs are reported as `<blob N bytes>` rather than dumped.
* **Defense in depth**: the connection also sets `enable_external_access=false`
  (plus extension autoloading disabled). If that statement is unavailable the
  engine fails closed rather than running with external access on.
* The tokenizer preserves string literals and quoted identifiers so a `;` or a
  forbidden word inside a string cannot split a statement or bypass the
  allowlists.

### Result shape

`shot_query` returns bounded typed rows. 64-bit integers that cannot be
represented exactly as an IEEE-754 double are emitted as decimal strings, so
nanosecond timestamps survive a JSON round-trip through a JavaScript client.
`shot_analyze` returns SQL-backed sections with an explicit `observed` flag and
sample sizes; `shot_compare` returns query-derived deltas, sample sizes, units
and a `disclosure` list. Hypotheses are kept separate from observations and are
never presented as measured facts.

## Alternatives considered

* **Arbitrary SQL with only `access_mode=read_only`** — rejected: file, network
  and settings functions remain reachable.
* **DuckDB `enable_external_access=false` alone** — rejected: it is defense in
  depth, not a parser; it also does not bound cost or result size.
* **A third-party SQL parser** — deferred: the token-based allowlist validator
  is small, auditable and sufficient for the five known tables. A parser can be
  adopted later without changing the wire contract.
* **Querying a shared long-lived analysis database** — rejected: it couples
  shots and risks cross-shot leakage.

## Consequences

* Models can inspect the schema and run useful read-only queries, but cannot
  read local files, reach the network, mutate state, or run unbounded work.
* Analysis availability depends on starting a container per frozen shot, which
  is bounded by the container inactivity timeout.
* The SQL surface is intentionally narrower than full DuckDB; unsupported but
  useful functions can be added to the allowlist with a review.
