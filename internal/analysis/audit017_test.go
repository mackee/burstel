package analysis_test

// Independent acceptance audit for spec 017 (Go side).
//
// It composes the acceptance contract that the unit suites exercise piecemeal:
// a continuous two-instance stream with idle gaps is compacted into immutable
// segments, then a *backdated external* window and an *overlapping* window are
// selected and analyzed from the durable segment metadata alone. It pins:
//   - half-open [start,end) boundaries;
//   - per-instance isolation and idle telemetry that must NOT be counted;
//   - cumulative-counter deltas from a private pre-window baseline;
//   - adversarial SQL rejection on the scoped snapshot;
//   - overlapping windows sharing a boundary segment without cross-contamination;
//   - explicit coverage (late arrivals / crossing spans) disclosure.
//
// It uses the same real DuckDB segment builder and scoped engine as production.

import (
	"context"
	"testing"
	"time"

	"github.com/mackee/burstel/internal/analysis"
	"github.com/mackee/burstel/internal/otlp"
)

func TestAudit017BackdatedOverlappingWindowEndToEnd(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

	// A backdated external window [now-3m, now-1m) over web-1 + db-1, and an
	// overlapping window [now-2m, now) over web-1 only.
	window := analysis.NewExternalWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1", "db-1"}, "backdated")
	if err := window.Validate(now, analysis.DefaultWindowBounds()); err != nil {
		t.Fatalf("backdated window rejected: %v", err)
	}
	overlap := analysis.NewExternalWindow(now.Add(-2*time.Minute), now, []string{"web-1"}, "overlap")
	if err := overlap.Validate(now, analysis.DefaultWindowBounds()); err != nil {
		t.Fatalf("overlap window rejected: %v", err)
	}
	if !window.Overlaps(overlap) {
		t.Fatal("expected the two windows to overlap")
	}

	startNS, endNS := window.StartNS(), window.EndNS()
	dir := t.TempDir()

	// web-1: in-window spans plus two cumulative metric points (one pre-window
	// baseline, one in-window) so the delta is derivable but never exposes the
	// baseline through the query surface.
	web := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 3,
		Spans: []otlp.Span{
			spanAt("web-1", "w-at-start", startNS, startNS+1000, ""),
			spanAt("web-1", "w-inside", startNS+1_000_000_000, startNS+2_000_000_000, ""),
			// Starts exactly at the exclusive end: must be excluded.
			spanAt("web-1", "w-at-end", endNS, endNS+1000, ""),
		},
	}
	webMetrics := &otlp.Decoded{
		Signal:      otlp.Metrics,
		RecordCount: 2,
		MetricPoints: []otlp.MetricPoint{
			{MetricName: "http.requests", MetricType: "sum", ServiceName: "web-1", TimeUnixNano: uint64(startNS - int64(time.Minute)), AggregationTemporality: 2, Sum: floatPtr(100), IsMonotonic: true},
			{MetricName: "http.requests", MetricType: "sum", ServiceName: "web-1", TimeUnixNano: uint64(startNS + int64(30*time.Second)), AggregationTemporality: 2, Sum: floatPtr(250), IsMonotonic: true},
		},
	}
	_, webPath := buildSegment(t, dir, "journal/web-1/traces/1000-w.bin", "inst-web", "web-1", now.Add(-2*time.Minute).UnixMilli(), web)
	_, webMetricPath := buildSegment(t, dir, "journal/web-1/metrics/1001-wm.bin", "inst-web", "web-1", now.Add(-2*time.Minute).UnixMilli(), webMetrics)

	// db-1: one in-window span. It must never leak into the web-1-only overlap.
	db := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 1,
		Spans:       []otlp.Span{spanAt("db-1", "d-inside", startNS+500_000_000, startNS+600_000_000, "")},
	}
	_, dbPath := buildSegment(t, dir, "journal/db-1/traces/1002-d.bin", "inst-db", "db-1", now.Add(-2*time.Minute).UnixMilli(), db)

	// Idle telemetry between shots: a batch received after the window end whose
	// events are entirely outside the window. It may be selected by receive lag
	// but must contribute zero counted rows.
	idle := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 1,
		Spans:       []otlp.Span{spanAt("web-1", "idle-after", endNS+int64(time.Minute), endNS+int64(time.Minute)+1000, "")},
	}
	_, idlePath := buildSegment(t, dir, "journal/web-1/traces/1003-idle.bin", "inst-web", "web-1", now.Add(-30*time.Second).UnixMilli(), idle)

	// A much older batch outside the ingest lag and with no in-window events:
	// never selected.
	old := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 1,
		Spans:       []otlp.Span{spanAt("web-1", "old", startNS-int64(10*time.Minute), startNS-int64(10*time.Minute)+1000, "")},
	}
	_, _ = buildSegment(t, dir, "journal/web-1/traces/1004-old.bin", "inst-web", "web-1", now.Add(-5*time.Minute).UnixMilli(), old)

	meta := func(key, inst string, firstMS, lastMS int64) analysis.SegmentMeta {
		return analysis.SegmentMeta{Key: key, Instance: inst, Status: "verified", FirstReceivedAt: firstMS, LastReceivedAt: lastMS}
	}
	receivedAt := now.Add(-2 * time.Minute).UnixMilli()
	catalog := []analysis.SegmentMeta{
		meta("w", "web-1", receivedAt, receivedAt),
		meta("wm", "web-1", receivedAt, receivedAt),
		meta("d", "db-1", receivedAt, receivedAt),
		meta("idle", "web-1", now.Add(-30*time.Second).UnixMilli(), now.Add(-30*time.Second).UnixMilli()),
		meta("old", "web-1", now.Add(-5*time.Minute).UnixMilli(), now.Add(-5*time.Minute).UnixMilli()),
	}

	sel := analysis.SelectSegments(window, catalog, analysis.SelectionOptions{IngestLag: time.Minute})
	selected := map[string]bool{}
	for _, s := range sel.Selected {
		selected[s.Key] = true
	}
	if !selected["w"] || !selected["wm"] || !selected["d"] || !selected["idle"] {
		t.Fatalf("expected w/wm/d/idle selected, got %v", selected)
	}
	if selected["old"] {
		t.Fatalf("old batch must not be selected: %v", selected)
	}
	if sel.Expected != len(sel.Selected) {
		t.Fatalf("expected no missing segments, got %+v", sel)
	}

	refs := []analysis.SegmentRef{
		{Key: "w", Instance: "web-1", Path: webPath},
		{Key: "wm", Instance: "web-1", Path: webMetricPath},
		{Key: "d", Instance: "db-1", Path: dbPath},
		{Key: "idle", Instance: "web-1", Path: idlePath},
	}
	eng, err := analysis.BuildScopedEngine(ctx, window, refs, analysis.ScopedOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("BuildScopedEngine: %v", err)
	}
	defer eng.Close()

	// Half-open boundaries: exactly the two in-window web spans (start included,
	// end excluded) and the db span; idle/old rows excluded.
	res, err := eng.Engine.Query(ctx, analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{
		SQL: "SELECT service_name, count(*) FROM spans GROUP BY service_name ORDER BY service_name",
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected two service groups, got %v", res.Rows)
	}
	counts := map[string]int64{}
	for _, row := range res.Rows {
		counts[row[0].(string)] = row[1].(int64)
	}
	if counts["web-1"] != 2 || counts["db-1"] != 1 {
		t.Fatalf("per-instance isolation wrong: %v", counts)
	}
	if counts["web-1"] != 2 {
		t.Fatalf("idle/old/at-end rows leaked into the window: %v", counts)
	}

	// Cumulative delta uses the private pre-window baseline.
	analyzed, err := eng.Engine.Analyze(ctx, analysis.ScopedPolicy(analysis.DefaultPolicy()), "audit-window", analysis.AnalyzeRequest{Sections: []string{"metric_cumulative_delta"}})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	var delta float64 = -1
	for _, sec := range analyzed.Sections {
		if sec.Name == "metric_cumulative_delta" && len(sec.Rows) == 1 {
			delta = sec.Rows[0][5].(float64)
		}
	}
	if delta != 150 {
		t.Fatalf("expected cumulative delta 150, got %v", delta)
	}
	if _, err := eng.Engine.Query(ctx, analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{SQL: "SELECT * FROM _burstel_baseline_metrics"}); err == nil {
		t.Fatal("private baseline table must not be queryable")
	}

	// Adversarial SQL must be rejected on the scoped snapshot.
	for _, hostile := range []string{
		"SELECT * FROM information_schema.tables",
		"SELECT * FROM read_csv('/etc/passwd')",
		"SELECT * FROM spans; DROP TABLE spans",
		"ATTACH '/tmp/evil.db' AS evil; SELECT * FROM evil.spans",
		"SELECT getenv('HOME')",
	} {
		if _, err := eng.Engine.Query(ctx, analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{SQL: hostile}); err == nil {
			t.Fatalf("hostile query allowed: %s", hostile)
		}
	}

	// Coverage discloses the late-received idle batch.
	if eng.Coverage.LateBatches < 1 {
		t.Fatalf("expected late batch disclosure, got %+v", eng.Coverage)
	}

	// The overlapping window shares the boundary segment but must see only its
	// own instance and its own half-open range (web-1 spans starting >= now-2m).
	overlapRefs := []analysis.SegmentRef{{Key: "w", Instance: "web-1", Path: webPath}}
	overlapEng, err := analysis.BuildScopedEngine(ctx, overlap, overlapRefs, analysis.ScopedOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("BuildScopedEngine overlap: %v", err)
	}
	defer overlapEng.Close()
	ores, err := overlapEng.Engine.Query(ctx, analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{SQL: "SELECT count(*) FROM spans"})
	if err != nil {
		t.Fatalf("overlap query: %v", err)
	}
	// web spans: at-start (now-3m) excluded, inside (now-3m+1s) excluded,
	// at-end (now-1m) included -> exactly one.
	if got := ores.Rows[0][0].(int64); got != 1 {
		t.Fatalf("overlapping window count = %d, want 1", got)
	}
	if ores.RowCount == 0 {
		t.Fatal("overlap window returned no rows")
	}
}

func floatPtr(v float64) *float64 { return &v }
