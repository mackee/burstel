package analysis

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// This file builds the *shot-scoped* read-only DuckDB snapshot for a logical
// window shot.
//
// Security model (spec 016):
//
//   - User SQL never runs against an instance/global table or a segment file
//     directly. It runs against a freshly materialized scratch database that
//     contains only the window's rows, projected into the same five logical
//     tables the frozen-shot engine exposes (schema_meta, otlp_batches, spans,
//     log_records, metric_points).
//   - The scratch database is then reopened through OpenReadOnly, which disables
//     external file/network access, so even a policy hole cannot reach another
//     segment or the network.
//   - Pre-window rows needed to derive a cumulative delta are materialized into
//     a private table (`_burstel_baseline_metrics`) that is deliberately NOT in
//     the query policy's allowlist, so a caller can never read pre-window rows
//     through the shot-scoped query surface.

// SegmentRef points at one verified segment file on local disk.
type SegmentRef struct {
	Key      string
	Instance string
	Path     string
}

// ScopedOptions configures the scoped snapshot build.
type ScopedOptions struct {
	// WorkDir is the parent directory for the ephemeral scratch database.
	WorkDir string
	// BaselineHorizon bounds how far before the window a cumulative-counter
	// baseline point may be read. Only the latest pre-window point is used.
	BaselineHorizon time.Duration
	// SpanPolicy selects the span crossing policy (default start-in-shot).
	SpanPolicy SpanBoundaryPolicy
	// Open configures the read-only engine.
	Open OpenOptions
}

// DefaultBaselineHorizon is how far before a window a cumulative baseline may
// be read. It is generous so a slowly reported cumulative counter still has a
// pre-window point.
const DefaultBaselineHorizon = 24 * time.Hour

// ScopedCoverage extends CoverageSummary with window-specific diagnostics.
type ScopedCoverage struct {
	CoverageSummary
	// SegmentsRestored is the number of segment files materialized.
	SegmentsRestored int `json:"segments_restored"`
	// LateBatches counts raw batches received after the window end yet included
	// because they may carry in-window events.
	LateBatches int64 `json:"late_batches"`
	// SpanCrossing counts spans that start before the window and end inside or
	// after it (reported regardless of the default counted set).
	SpanCrossing int64 `json:"span_crossing"`
	// SelectionJustification records why these segments were chosen.
	SelectionJustification string `json:"selection_justification,omitempty"`
}

// ScopedTimings is the per-phase latency breakdown of a window snapshot build
// (spec 033). All values are milliseconds.
type ScopedTimings struct {
	AttachMS      int64 `json:"attach_ms"`
	MaterializeMS int64 `json:"materialize_ms"`
	CheckpointMS  int64 `json:"checkpoint_ms"`
	OpenMS        int64 `json:"open_ms"`
	CoverageMS    int64 `json:"coverage_ms"`
	TotalMS       int64 `json:"total_ms"`
}

// ScopedEngine is a read-only window snapshot plus its coverage diagnostics.
type ScopedEngine struct {
	Engine   *Engine
	Window   TimeWindow
	Coverage ScopedCoverage
	Timings  ScopedTimings
	dir      string
}

// Close releases the engine and removes the ephemeral scratch database.
func (s *ScopedEngine) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.Engine != nil {
		err = s.Engine.Close()
	}
	if s.dir != "" {
		if rerr := os.RemoveAll(s.dir); rerr != nil && err == nil {
			err = rerr
		}
	}
	return err
}

// ScopedPolicy returns the query policy for a scoped window snapshot. The
// baseline table is never queryable.
func ScopedPolicy(base Policy) Policy {
	p := base
	if p.MaxRows == 0 {
		p = DefaultPolicy()
	}
	p.Tables = map[string]struct{}{
		"schema_meta":   {},
		"otlp_batches":  {},
		"spans":         {},
		"log_records":   {},
		"metric_points": {},
	}
	return p
}

// BuildScopedEngine materializes a window's rows into an immutable read-only
// DuckDB snapshot and returns it with coverage diagnostics.
func BuildScopedEngine(ctx context.Context, window TimeWindow, refs []SegmentRef, opts ScopedOptions) (*ScopedEngine, error) {
	startedAt := time.Now()
	var timings ScopedTimings
	workDir := opts.WorkDir
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, fmt.Errorf("analysis: window workdir: %w", err)
	}
	dir, err := os.MkdirTemp(workDir, "window-")
	if err != nil {
		return nil, fmt.Errorf("analysis: window tempdir: %w", err)
	}
	scratch := filepath.Join(dir, "window.duckdb")

	horizon := opts.BaselineHorizon
	if horizon <= 0 {
		horizon = DefaultBaselineHorizon
	}
	policy := opts.SpanPolicy
	if policy == "" {
		policy = SpanStartInShot
	}

	db, err := sql.Open("duckdb", scratch)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("analysis: open window scratch: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}

	if err := materialize(ctx, db, window, refs, horizon, policy, &timings); err != nil {
		cleanup()
		return nil, err
	}
	checkpointStart := time.Now()
	if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
		cleanup()
		return nil, fmt.Errorf("analysis: checkpoint window: %w", err)
	}
	timings.CheckpointMS = time.Since(checkpointStart).Milliseconds()
	if err := db.Close(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("analysis: close window scratch: %w", err)
	}

	openStart := time.Now()
	engine, err := OpenReadOnly(ctx, scratch, opts.Open)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	timings.OpenMS = time.Since(openStart).Milliseconds()
	engine.windowStartNS = window.StartNS()
	engine.windowEndNS = window.EndNS()
	engine.scoped = true

	coverageStart := time.Now()
	cov, err := computeScopedCoverage(ctx, engine, window, refs, policy)
	if err != nil {
		_ = engine.Close()
		_ = os.RemoveAll(dir)
		return nil, err
	}
	timings.CoverageMS = time.Since(coverageStart).Milliseconds()
	timings.TotalMS = time.Since(startedAt).Milliseconds()
	return &ScopedEngine{Engine: engine, Window: window, Coverage: cov, Timings: timings, dir: dir}, nil
}

func materialize(ctx context.Context, db *sql.DB, window TimeWindow, refs []SegmentRef, horizon time.Duration, policy SpanBoundaryPolicy, timings *ScopedTimings) error {
	if _, err := db.ExecContext(ctx, scopedSchemaDDL); err != nil {
		return fmt.Errorf("analysis: window schema: %w", err)
	}
	startNS, endNS := window.StartNS(), window.EndNS()
	lag := DefaultIngestLag.Milliseconds()
	loMS, hiMS := window.Start.UnixMilli()-lag, window.End.UnixMilli()+lag
	baseFromNS := startNS - horizon.Nanoseconds()

	attachStart := time.Now()
	available := make([]map[string]bool, len(refs))
	for i, ref := range refs {
		if err := validateSegmentPath(ref.Path); err != nil {
			return err
		}
		alias := fmt.Sprintf("seg%d", i)
		if _, err := db.ExecContext(ctx, "ATTACH "+quoteLiteral(ref.Path)+" AS "+alias+" (READ_ONLY)"); err != nil {
			return fmt.Errorf("analysis: attach segment %s: %w", ref.Key, err)
		}
		tables, err := attachedTableSet(ctx, db, alias)
		if err != nil {
			return fmt.Errorf("analysis: inspect segment %s: %w", ref.Key, err)
		}
		available[i] = tables
	}
	if timings != nil {
		timings.AttachMS = time.Since(attachStart).Milliseconds()
	}
	materializeStart := time.Now()
	defer func() {
		if timings != nil {
			timings.MaterializeMS = time.Since(materializeStart).Milliseconds()
		}
	}()

	tableSpecs := []struct {
		table string
		where func(alias string) string
		// selectSQL returns the source projection for one attached ref. Spec 034
		// parts carry `part_meta` instead of `otlp_batches`; the batch projection
		// is synthesized from it (without the raw payload blobs) so analysis and
		// coverage keep working unchanged.
		selectSQL func(alias string, tables map[string]bool) (string, bool)
	}{
		{"otlp_batches", func(a string) string {
			return fmt.Sprintf("WHERE epoch_ms(%s.received_at) >= %d AND epoch_ms(%s.received_at) < %d", a, loMS, a, hiMS)
		}, func(a string, tables map[string]bool) (string, bool) {
			if tables["otlp_batches"] {
				return "SELECT * FROM " + a + ".main.otlp_batches AS t ", true
			}
			if tables["part_meta"] {
				return "SELECT instance_id, source_key, request_id, signal, received_at, content_encoding, " +
					"wire_sha256, payload_sha256, record_count, payload_length, resource_summary, " +
					"CAST('' AS BLOB) AS wire_payload, CAST('' AS BLOB) AS payload " +
					"FROM " + a + ".main.part_meta AS t ", true
			}
			return "", false
		}},
		{"spans", func(a string) string {
			return spanWindowWhere(a, startNS, endNS, policy)
		}, func(a string, tables map[string]bool) (string, bool) {
			if !tables["spans"] {
				return "", false
			}
			return "SELECT * FROM " + a + ".main.spans AS t ", true
		}},
		{"log_records", func(a string) string {
			return fmt.Sprintf("WHERE COALESCE(NULLIF(%[1]s.time_unix_nano, 0), %[1]s.observed_time_unix_nano) >= %[2]d AND COALESCE(NULLIF(%[1]s.time_unix_nano, 0), %[1]s.observed_time_unix_nano) < %[3]d", a, startNS, endNS)
		}, func(a string, tables map[string]bool) (string, bool) {
			if !tables["log_records"] {
				return "", false
			}
			return "SELECT * FROM " + a + ".main.log_records AS t ", true
		}},
		{"metric_points", func(a string) string {
			return fmt.Sprintf("WHERE %s.time_unix_nano >= %d AND %s.time_unix_nano < %d", a, startNS, a, endNS)
		}, func(a string, tables map[string]bool) (string, bool) {
			if !tables["metric_points"] {
				return "", false
			}
			return "SELECT * FROM " + a + ".main.metric_points AS t ", true
		}},
	}
	for _, ts := range tableSpecs {
		var b strings.Builder
		b.WriteString("INSERT INTO ")
		b.WriteString(ts.table)
		b.WriteString(" SELECT * FROM (")
		sources := 0
		for i := range refs {
			selectSQL, ok := ts.selectSQL(fmt.Sprintf("seg%d", i), available[i])
			if !ok {
				continue
			}
			if sources > 0 {
				b.WriteString(" UNION ALL ")
			}
			b.WriteString(selectSQL)
			b.WriteString(ts.where("t"))
			sources++
		}
		if sources == 0 {
			b.WriteString("SELECT * FROM " + ts.table + " WHERE 1=0")
		}
		b.WriteString(")")
		if _, err := db.ExecContext(ctx, b.String()); err != nil {
			return fmt.Errorf("analysis: materialize %s: %w", ts.table, err)
		}
	}

	// Pre-window cumulative baseline: kept private and never queryable.
	var bb strings.Builder
	bb.WriteString("INSERT INTO _burstel_baseline_metrics SELECT * FROM (")
	baselineSources := 0
	for i := range refs {
		if !available[i]["metric_points"] {
			continue
		}
		if baselineSources > 0 {
			bb.WriteString(" UNION ALL ")
		}
		alias := fmt.Sprintf("seg%d", i)
		fmt.Fprintf(&bb, "SELECT * FROM %s.main.metric_points AS t WHERE t.aggregation_temporality = 2 AND t.time_unix_nano < %d AND t.time_unix_nano >= %d", alias, startNS, baseFromNS)
		baselineSources++
	}
	if baselineSources == 0 {
		bb.WriteString("SELECT * FROM metric_points WHERE 1=0")
	}
	bb.WriteString(")")
	if _, err := db.ExecContext(ctx, bb.String()); err != nil {
		return fmt.Errorf("analysis: materialize baseline: %w", err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO schema_meta (key, value) VALUES
		('schema_version', '1'),
		('window_start_ns', ?),
		('window_end_ns', ?),
		('window_provenance', ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
		fmt.Sprint(startNS), fmt.Sprint(endNS), string(window.Provenance),
	); err != nil {
		return fmt.Errorf("analysis: window meta: %w", err)
	}
	return nil
}

func spanWindowWhere(alias string, startNS, endNS int64, _ SpanBoundaryPolicy) string {
	// The default counted set is always "start-in-shot" for spans. Crossing
	// diagnostics are reported separately in coverage, never by changing the
	// counted set.
	return fmt.Sprintf("WHERE %s.start_time_unix_nano >= %d AND %s.start_time_unix_nano < %d", alias, startNS, alias, endNS)
}

func computeScopedCoverage(ctx context.Context, e *Engine, window TimeWindow, refs []SegmentRef, policy SpanBoundaryPolicy) (ScopedCoverage, error) {
	cov := ScopedCoverage{SegmentsRestored: len(refs)}
	instances := map[string]struct{}{}
	for _, r := range refs {
		if r.Instance != "" {
			instances[r.Instance] = struct{}{}
		}
	}
	for inst := range instances {
		cov.Instances = append(cov.Instances, inst)
	}
	cov.ExpectedSegments = len(refs)
	cov.AvailableSegments = len(refs)
	cov.Complete = true

	endMS := window.End.UnixMilli()
	if err := e.db.QueryRowContext(ctx, `SELECT count(*) FROM otlp_batches WHERE epoch_ms(received_at) >= ?`, endMS).Scan(&cov.LateBatches); err != nil {
		return cov, err
	}
	if err := e.db.QueryRowContext(ctx, `SELECT count(*) FROM log_records WHERE time_unix_nano IS NULL OR time_unix_nano = 0`).Scan(&cov.LogObservedFallbacks); err != nil {
		return cov, err
	}
	cov.LateArrivals = cov.LateBatches
	if err := e.db.QueryRowContext(ctx, `SELECT count(*) FROM spans WHERE start_time_unix_nano < ? AND coalesce(end_time_unix_nano, start_time_unix_nano) > ?`,
		window.StartNS(), window.EndNS()).Scan(&cov.SpanCrossing); err != nil {
		return cov, err
	}
	_ = policy
	return cov, nil
}

func validateSegmentPath(path string) error {
	if path == "" {
		return fmt.Errorf("analysis: empty segment path")
	}
	if strings.ContainsAny(path, "'\x00") {
		return fmt.Errorf("analysis: invalid segment path")
	}
	return nil
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// attachedTableSet lists the tables a verified attached file exposes. A
// compacted segment has the raw `otlp_batches` tables; a projection-only shot
// part (spec 034) has `part_meta` plus the analysis projections instead.
func attachedTableSet(ctx context.Context, db *sql.DB, alias string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_catalog = ?`, alias)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables[name] = true
	}
	return tables, rows.Err()
}

// scopedSchemaDDL mirrors the compacted segment schema. It is created empty and
// then filled only with in-window rows.
const scopedSchemaDDL = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key   VARCHAR PRIMARY KEY,
  value VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS otlp_batches (
  instance_id      VARCHAR NOT NULL,
  source_key       VARCHAR NOT NULL,
  request_id       VARCHAR NOT NULL,
  signal           VARCHAR NOT NULL,
  received_at      TIMESTAMP NOT NULL,
  content_encoding VARCHAR,
  wire_sha256      VARCHAR NOT NULL,
  payload_sha256   VARCHAR NOT NULL,
  record_count     BIGINT  NOT NULL,
  payload_length   BIGINT  NOT NULL,
  resource_summary VARCHAR,
  wire_payload     BLOB    NOT NULL,
  payload          BLOB    NOT NULL
);
CREATE TABLE IF NOT EXISTS spans (
  instance_id              VARCHAR NOT NULL,
  source_key               VARCHAR NOT NULL,
  ordinal                  BIGINT  NOT NULL,
  trace_id                 VARCHAR,
  span_id                  VARCHAR,
  parent_span_id           VARCHAR,
  trace_state              VARCHAR,
  name                     VARCHAR,
  kind                     VARCHAR,
  start_time_unix_nano     BIGINT,
  end_time_unix_nano       BIGINT,
  status_code              VARCHAR,
  status_message           VARCHAR,
  service_name             VARCHAR,
  scope_name               VARCHAR,
  scope_version            VARCHAR,
  resource_attributes      VARCHAR,
  scope_attributes         VARCHAR,
  attributes               VARCHAR,
  flags                    BIGINT,
  dropped_attributes_count BIGINT,
  dropped_events_count     BIGINT,
  dropped_links_count      BIGINT,
  events_count             BIGINT,
  links_count              BIGINT
);
CREATE TABLE IF NOT EXISTS log_records (
  instance_id              VARCHAR NOT NULL,
  source_key               VARCHAR NOT NULL,
  ordinal                  BIGINT  NOT NULL,
  time_unix_nano           BIGINT,
  observed_time_unix_nano  BIGINT,
  severity_number          VARCHAR,
  severity_text            VARCHAR,
  body                     VARCHAR,
  service_name             VARCHAR,
  scope_name               VARCHAR,
  scope_version            VARCHAR,
  resource_attributes      VARCHAR,
  scope_attributes         VARCHAR,
  attributes               VARCHAR,
  trace_id                 VARCHAR,
  span_id                  VARCHAR,
  event_name               VARCHAR,
  flags                    BIGINT,
  dropped_attributes_count BIGINT
);
CREATE TABLE IF NOT EXISTS metric_points (
  instance_id             VARCHAR NOT NULL,
  source_key              VARCHAR NOT NULL,
  ordinal                 BIGINT  NOT NULL,
  metric_name             VARCHAR,
  metric_type             VARCHAR,
  metric_unit             VARCHAR,
  metric_description      VARCHAR,
  service_name            VARCHAR,
  scope_name              VARCHAR,
  scope_version           VARCHAR,
  resource_attributes     VARCHAR,
  scope_attributes        VARCHAR,
  attributes              VARCHAR,
  start_time_unix_nano    BIGINT,
  time_unix_nano          BIGINT,
  flags                   BIGINT,
  value_double            DOUBLE,
  value_int               BIGINT,
  aggregation_temporality INTEGER,
  is_monotonic            BOOLEAN,
  count                   BIGINT,
  sum                     DOUBLE,
  min                     DOUBLE,
  max                     DOUBLE,
  bucket_counts           VARCHAR,
  explicit_bounds         VARCHAR,
  detail                  VARCHAR
);
CREATE TABLE IF NOT EXISTS _burstel_baseline_metrics (
  instance_id             VARCHAR NOT NULL,
  source_key              VARCHAR NOT NULL,
  ordinal                 BIGINT  NOT NULL,
  metric_name             VARCHAR,
  metric_type             VARCHAR,
  metric_unit             VARCHAR,
  metric_description      VARCHAR,
  service_name            VARCHAR,
  scope_name              VARCHAR,
  scope_version           VARCHAR,
  resource_attributes     VARCHAR,
  scope_attributes        VARCHAR,
  attributes              VARCHAR,
  start_time_unix_nano    BIGINT,
  time_unix_nano          BIGINT,
  flags                   BIGINT,
  value_double            DOUBLE,
  value_int               BIGINT,
  aggregation_temporality INTEGER,
  is_monotonic            BOOLEAN,
  count                   BIGINT,
  sum                     DOUBLE,
  min                     DOUBLE,
  max                     DOUBLE,
  bucket_counts           VARCHAR,
  explicit_bounds         VARCHAR,
  detail                  VARCHAR
);
`
