package analysis

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

// This file implements spec 035's zero-copy window analysis.
//
// The pre-035 path copied every in-window row from every attached part into one
// scratch DuckDB (`materialize_ms` 47-89 s for 16 parts). The view path instead
// ATTACHes each verified part read-only and creates the five logical tables as
// UNION ALL views with the same window predicates. Queries then read the part
// files in place, so opening a shot is an ATTACH (milliseconds) rather than a
// copy proportional to the shot size.
//
// Security: a view over an attached database must be evaluated with external
// access enabled (the attached files are external). After ATTACH and view
// creation the connection sets `enable_external_access=false`, exactly like the
// frozen-shot engine: DuckDB keeps already-attached catalogs readable but
// blocks every new file/network access. The SQL policy's table/function
// allowlist remains the primary boundary (ATTACH, read_csv and friends are
// rejected before reaching DuckDB).

// buildScopedView ATTACHes the window's parts and exposes them through views.
func buildScopedView(ctx context.Context, window TimeWindow, refs []SegmentRef, opts ScopedOptions) (*ScopedEngine, error) {
	startedAt := time.Now()
	var timings ScopedTimings
	workDir := opts.WorkDir
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, fmt.Errorf("analysis: window workdir: %w", err)
	}
	dir, err := os.MkdirTemp(workDir, "window-view-")
	if err != nil {
		return nil, fmt.Errorf("analysis: window tempdir: %w", err)
	}

	horizon := opts.BaselineHorizon
	if horizon <= 0 {
		horizon = DefaultBaselineHorizon
	}
	policy := opts.SpanPolicy
	if policy == "" {
		policy = SpanStartInShot
	}
	mem := opts.Open.MemoryLimit
	if mem == "" {
		// The analysis container is standard-4 (4 vCPU / 12 GiB); give the
		// in-place part scan a larger buffer pool than the tiny copy path.
		mem = "1GB"
	}
	threads := opts.Open.Threads
	if threads <= 0 {
		threads = 4
	}

	// An in-memory catalog holds only views and the tiny schema_meta table; the
	// part bytes stay on disk and are read in place.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("analysis: open window view catalog: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}

	for _, stmt := range []string{
		fmt.Sprintf("SET memory_limit='%s'", mem),
		fmt.Sprintf("SET threads=%d", threads),
		"SET allow_community_extensions=false",
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			cleanup()
			return nil, fmt.Errorf("analysis: configure window views: %w", err)
		}
	}

	attachStart := time.Now()
	available := make([]map[string]bool, len(refs))
	for i, ref := range refs {
		if err := validateSegmentPath(ref.Path); err != nil {
			cleanup()
			return nil, err
		}
		alias := fmt.Sprintf("seg%d", i)
		if _, err := db.ExecContext(ctx, "ATTACH "+quoteLiteral(ref.Path)+" AS "+alias+" (READ_ONLY)"); err != nil {
			cleanup()
			return nil, fmt.Errorf("analysis: attach segment %s: %w", ref.Key, err)
		}
		tables, err := attachedTableSet(ctx, db, alias)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("analysis: inspect segment %s: %w", ref.Key, err)
		}
		available[i] = tables
	}
	timings.AttachMS = time.Since(attachStart).Milliseconds()

	if err := createScopedViews(ctx, db, window, refs, available, horizon, policy); err != nil {
		cleanup()
		return nil, err
	}

	// No new file or network access from here on. Already-attached catalogs
	// remain readable, so the views keep working while read_csv/glob/ATTACH are
	// refused even if the SQL policy were bypassed.
	for _, stmt := range []string{
		"SET enable_external_access=false",
		"SET allow_community_extensions=false",
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET lock_configuration=true",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			cleanup()
			return nil, fmt.Errorf("analysis: harden window views: %w", err)
		}
	}

	openStart := time.Now()
	engine := &Engine{db: db, path: ":memory:", scoped: true}
	engine.windowStartNS = window.StartNS()
	engine.windowEndNS = window.EndNS()
	var schema string
	if err := engine.db.QueryRowContext(ctx, "SELECT value FROM schema_meta WHERE key = 'schema_version'").Scan(&schema); err == nil {
		engine.schema = schema
	}
	timings.OpenMS = time.Since(openStart).Milliseconds()

	coverageStart := time.Now()
	cov, err := computeScopedCoverage(ctx, engine, window, refs, policy)
	if err != nil {
		_ = engine.Close()
		_ = os.RemoveAll(dir)
		return nil, err
	}
	timings.CoverageMS = time.Since(coverageStart).Milliseconds()
	timings.TotalMS = time.Since(startedAt).Milliseconds()

	return &ScopedEngine{
		Engine:   engine,
		Window:   window,
		Coverage: cov,
		Timings:  timings,
		dir:      dir,
	}, nil
}

// createScopedViews creates the five logical projections. A projection with no
// source part is created as an empty table with the documented schema so the
// analysis templates can always query it.
func createScopedViews(
	ctx context.Context,
	db *sql.DB,
	window TimeWindow,
	refs []SegmentRef,
	available []map[string]bool,
	horizon time.Duration,
	policy SpanBoundaryPolicy,
) error {
	ddl := map[string]string{
		"otlp_batches":  otlpBatchesDDL,
		"spans":         spansDDL,
		"log_records":   logRecordsDDL,
		"metric_points": metricPointsDDL,
	}
	for _, spec := range scopedTableSpecs(window, policy) {
		var b strings.Builder
		sources := 0
		var union strings.Builder
		for i := range refs {
			selectSQL, ok := spec.selectSQL(fmt.Sprintf("seg%d", i), available[i])
			if !ok {
				continue
			}
			if sources > 0 {
				union.WriteString(" UNION ALL ")
			}
			union.WriteString(selectSQL)
			union.WriteString(spec.where("t"))
			sources++
		}
		if sources == 0 {
			if _, err := db.ExecContext(ctx, ddl[spec.table]); err != nil {
				return fmt.Errorf("analysis: empty view %s: %w", spec.table, err)
			}
			continue
		}
		b.WriteString("CREATE VIEW ")
		b.WriteString(spec.table)
		b.WriteString(" AS SELECT * FROM (")
		b.WriteString(union.String())
		b.WriteString(")")
		if _, err := db.ExecContext(ctx, b.String()); err != nil {
			return fmt.Errorf("analysis: create view %s: %w", spec.table, err)
		}
	}

	// Pre-window cumulative baseline: private, never in the query allowlist.
	startNS := window.StartNS()
	baseFromNS := startNS - horizon.Nanoseconds()
	var baseline strings.Builder
	baselineSources := 0
	for i := range refs {
		source, ok := baselineSourceSQL(fmt.Sprintf("seg%d", i), startNS, baseFromNS, available[i])
		if !ok {
			continue
		}
		if baselineSources > 0 {
			baseline.WriteString(" UNION ALL ")
		}
		baseline.WriteString(source)
		baselineSources++
	}
	if baselineSources == 0 {
		if _, err := db.ExecContext(ctx, baselineMetricsDDL); err != nil {
			return fmt.Errorf("analysis: empty baseline view: %w", err)
		}
	} else if _, err := db.ExecContext(ctx, "CREATE VIEW _burstel_baseline_metrics AS SELECT * FROM ("+baseline.String()+")"); err != nil {
		return fmt.Errorf("analysis: create baseline view: %w", err)
	}

	if _, err := db.ExecContext(ctx, schemaMetaDDL); err != nil {
		return fmt.Errorf("analysis: window meta table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO schema_meta (key, value) VALUES
		('schema_version', '1'),
		('window_start_ns', ?),
		('window_end_ns', ?),
		('window_provenance', ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
		fmt.Sprint(startNS), fmt.Sprint(window.EndNS()), string(window.Provenance),
	); err != nil {
		return fmt.Errorf("analysis: window meta: %w", err)
	}
	return nil
}
