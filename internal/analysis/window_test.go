package analysis_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mackee/burstel/internal/analysis"
	"github.com/mackee/burstel/internal/compaction"
	"github.com/mackee/burstel/internal/otlp"
)

// buildSegment writes a real immutable segment containing the given decoded
// records and returns its metadata plus its on-disk path.
func buildSegment(t *testing.T, dir, key, instanceID, instanceName string, receivedMS int64, d *otlp.Decoded) (compaction.SegmentInfo, string) {
	t.Helper()
	path := filepath.Join(dir, compactionSegmentFileName(key))
	rec := compaction.JournalRecord{
		Object: compaction.JournalObject{
			Key:          key,
			InstanceID:   instanceID,
			Instance:     instanceName,
			Signal:       d.Signal,
			ReceivedAt:   time.UnixMilli(receivedMS).UTC(),
			ReceivedAtMS: receivedMS,
			RequestID:    key,
		},
		Wire:    []byte("wire-" + key),
		Payload: []byte("payload-" + key),
		Decoded: d,
	}
	info, err := compaction.BuildSegment(context.Background(), path, key, instanceID, instanceName, []compaction.JournalRecord{rec})
	if err != nil {
		t.Fatalf("BuildSegment(%s): %v", key, err)
	}
	return info, path
}

func compactionSegmentFileName(key string) string {
	repl := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return repl.Replace(key) + ".duckdb"
}

func spanAt(service, name string, startNS, endNS int64, status string) otlp.Span {
	return otlp.Span{
		TraceID:           "trace-1",
		SpanID:            name,
		Name:              name,
		ServiceName:       service,
		StartTimeUnixNano: uint64(startNS),
		EndTimeUnixNano:   uint64(endNS),
		StatusCode:        status,
	}
}

func TestSelectSegmentsHalfOpenAndLag(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	window := analysis.NewServerWindow(start, start.Add(2*time.Minute), []string{"web-1", "db-1"}, "bench")
	startNS, endNS := window.StartNS(), window.EndNS()
	startMS, endMS := window.Start.UnixMilli(), window.End.UnixMilli()

	evMin, evMax := startNS-10, startNS+10
	segments := []analysis.SegmentMeta{
		// In-window events, received on time.
		{Key: "a", Instance: "web-1", Status: "verified", FirstReceivedAt: startMS, LastReceivedAt: endMS, EventMinNS: &evMin, EventMaxNS: &evMax},
		// Received far after the window but carries in-window events: selected by
		// the event-time test even though receive time is outside the lag.
		{Key: "late", Instance: "web-1", Status: "verified", FirstReceivedAt: endMS + 10*60*1000, LastReceivedAt: endMS + 11*60*1000, EventMinNS: &evMin, EventMaxNS: &evMax},
		// Received inside the lag window: selected by receive-time.
		{Key: "lagged", Instance: "db-1", Status: "verified", FirstReceivedAt: endMS + 30_000, LastReceivedAt: endMS + 30_000},
		// Too old and too new with no event range: excluded.
		{Key: "old", Instance: "db-1", Status: "verified", FirstReceivedAt: startMS - 10*60*1000, LastReceivedAt: startMS - 9*60*1000},
		// A different instance is filtered out.
		{Key: "other", Instance: "cache-1", Status: "verified", FirstReceivedAt: startMS, LastReceivedAt: endMS},
		// Advertised but missing: expected but not selectable.
		{Key: "missing", Instance: "web-1", Status: "missing", FirstReceivedAt: startMS, LastReceivedAt: endMS},
	}
	sel := analysis.SelectSegments(window, segments, analysis.SelectionOptions{IngestLag: time.Minute})
	keys := map[string]bool{}
	for _, s := range sel.Selected {
		keys[s.Key] = true
	}
	if !keys["a"] || !keys["late"] || !keys["lagged"] {
		t.Fatalf("expected a/late/lagged selected, got %v", keys)
	}
	if keys["old"] || keys["other"] {
		t.Fatalf("unexpected selection %v", keys)
	}
	if len(sel.Missing) != 1 || sel.Missing[0] != "missing" {
		t.Fatalf("expected missing segment reported, got %v", sel.Missing)
	}
	if sel.Expected != 4 {
		t.Fatalf("expected 4 expected segments (missing counts), got %d", sel.Expected)
	}
	if endNS <= startNS {
		t.Fatal("sanity")
	}
}

func TestWindowHalfOpenEdges(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	window := analysis.NewServerWindow(start, start.Add(time.Minute), nil, "")
	startNS, endNS := window.StartNS(), window.EndNS()

	d := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 4,
		Spans: []otlp.Span{
			spanAt("web-1", "before", startNS-1, startNS+10, ""),
			spanAt("web-1", "at-start", startNS, startNS+10, ""),
			spanAt("web-1", "before-end", endNS-1, endNS+10, ""),
			spanAt("web-1", "at-end", endNS, endNS+10, ""),
		},
	}
	dir := t.TempDir()
	_, path := buildSegment(t, dir, "journal/web-1/traces/1-a.bin", "inst-web", "web-1", start.UnixMilli(), d)

	eng, err := analysis.BuildScopedEngine(context.Background(), window, []analysis.SegmentRef{{Key: "a", Instance: "web-1", Path: path}}, analysis.ScopedOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("BuildScopedEngine: %v", err)
	}
	defer eng.Close()

	res, err := eng.Engine.Query(context.Background(), analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{
		SQL: "SELECT name FROM spans ORDER BY start_time_unix_nano",
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected exactly the two in-window spans, got %d rows: %v", res.RowCount, res.Rows)
	}
	if res.Rows[0][0] != "at-start" || res.Rows[1][0] != "before-end" {
		t.Fatalf("half-open edges wrong: %v", res.Rows)
	}
}

func TestLateArrivalCountedAndDisclosed(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	window := analysis.NewServerWindow(start, start.Add(time.Minute), nil, "")
	startNS := window.StartNS()

	d := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 1,
		Spans:       []otlp.Span{spanAt("web-1", "late", startNS+5, startNS+100, "")},
	}
	dir := t.TempDir()
	// Received thirty seconds after the window end (inside the ingest lag).
	_, path := buildSegment(t, dir, "journal/web-1/traces/2-b.bin", "inst-web", "web-1", window.End.Add(30*time.Second).UnixMilli(), d)

	eng, err := analysis.BuildScopedEngine(context.Background(), window, []analysis.SegmentRef{{Key: "b", Instance: "web-1", Path: path}}, analysis.ScopedOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("BuildScopedEngine: %v", err)
	}
	defer eng.Close()

	res, err := eng.Engine.Query(context.Background(), analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{SQL: "SELECT count(*) FROM spans"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := res.Rows[0][0]; got != int64(1) {
		t.Fatalf("late in-window span should be counted, got %v", got)
	}
	if eng.Coverage.LateBatches != 1 {
		t.Fatalf("expected 1 late batch disclosed, got %d", eng.Coverage.LateBatches)
	}
}

func TestLogObservedFallback(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	window := analysis.NewServerWindow(start, start.Add(time.Minute), nil, "")
	startNS := window.StartNS()

	d := &otlp.Decoded{
		Signal:      otlp.Logs,
		RecordCount: 2,
		LogRecords: []otlp.LogRecord{
			// No event time: falls back to observed time inside the window.
			{TimeUnixNano: 0, ObservedTimeUnixNano: uint64(startNS + 10), ServiceName: "web-1", Body: "hello"},
			// Event time outside the window but observed inside: excluded.
			{TimeUnixNano: uint64(startNS - 100), ObservedTimeUnixNano: uint64(startNS + 20), ServiceName: "web-1", Body: "outside"},
		},
	}
	dir := t.TempDir()
	_, path := buildSegment(t, dir, "journal/web-1/logs/3-c.bin", "inst-web", "web-1", start.UnixMilli(), d)

	eng, err := analysis.BuildScopedEngine(context.Background(), window, []analysis.SegmentRef{{Key: "c", Instance: "web-1", Path: path}}, analysis.ScopedOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("BuildScopedEngine: %v", err)
	}
	defer eng.Close()

	res, err := eng.Engine.Query(context.Background(), analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{SQL: "SELECT count(*) FROM log_records"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := res.Rows[0][0]; got != int64(1) {
		t.Fatalf("expected only the observed-fallback log, got %v", got)
	}
	if eng.Coverage.LogObservedFallbacks != 1 {
		t.Fatalf("expected 1 observed fallback, got %d", eng.Coverage.LogObservedFallbacks)
	}
}

func TestCumulativeDeltaUsesPrivateBaseline(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	window := analysis.NewServerWindow(start, start.Add(time.Minute), nil, "")
	startNS := window.StartNS()

	sumBefore := 100.0
	sumIn := 250.0
	d := &otlp.Decoded{
		Signal:      otlp.Metrics,
		RecordCount: 2,
		MetricPoints: []otlp.MetricPoint{
			{MetricName: "http.requests", MetricType: "sum", ServiceName: "web-1", TimeUnixNano: uint64(startNS - int64(time.Minute)), AggregationTemporality: 2, Sum: &sumBefore, IsMonotonic: true},
			{MetricName: "http.requests", MetricType: "sum", ServiceName: "web-1", TimeUnixNano: uint64(startNS + int64(30*time.Second)), AggregationTemporality: 2, Sum: &sumIn, IsMonotonic: true},
		},
	}
	dir := t.TempDir()
	_, path := buildSegment(t, dir, "journal/web-1/metrics/4-d.bin", "inst-web", "web-1", start.UnixMilli(), d)

	eng, err := analysis.BuildScopedEngine(context.Background(), window, []analysis.SegmentRef{{Key: "d", Instance: "web-1", Path: path}}, analysis.ScopedOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("BuildScopedEngine: %v", err)
	}
	defer eng.Close()

	// The window query must only see the in-window point.
	res, err := eng.Engine.Query(context.Background(), analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{SQL: "SELECT count(*) FROM metric_points"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := res.Rows[0][0]; got != int64(1) {
		t.Fatalf("expected 1 in-window metric point, got %v", got)
	}

	analyzed, err := eng.Engine.Analyze(context.Background(), analysis.ScopedPolicy(analysis.DefaultPolicy()), "shot-1", analysis.AnalyzeRequest{Sections: []string{"metric_cumulative_delta"}})
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

	// Pre-window rows are not exposed through the query surface.
	_, err = eng.Engine.Query(context.Background(), analysis.ScopedPolicy(analysis.DefaultPolicy()), analysis.QueryRequest{SQL: "SELECT * FROM _burstel_baseline_metrics"})
	if err == nil {
		t.Fatal("expected the private baseline table to be denied")
	}
}

func TestHostileQueriesRejected(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	window := analysis.NewServerWindow(start, start.Add(time.Minute), nil, "")
	d := &otlp.Decoded{Signal: otlp.Traces, RecordCount: 0}
	dir := t.TempDir()
	_, path := buildSegment(t, dir, "journal/web-1/traces/5-e.bin", "inst-web", "web-1", start.UnixMilli(), d)

	eng, err := analysis.BuildScopedEngine(context.Background(), window, []analysis.SegmentRef{{Key: "e", Instance: "web-1", Path: path}}, analysis.ScopedOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("BuildScopedEngine: %v", err)
	}
	defer eng.Close()

	policy := analysis.ScopedPolicy(analysis.DefaultPolicy())
	hostile := []string{
		"SELECT * FROM _burstel_baseline_metrics",
		"SELECT * FROM information_schema.tables",
		"SELECT * FROM duckdb_tables()",
		"SELECT * FROM read_csv('secret.csv')",
		"SELECT * FROM spans; DROP TABLE spans",
		"COPY spans TO '/tmp/out.csv'",
		"ATTACH 'evil.db' AS evil; SELECT * FROM evil.spans",
		"WITH x AS (SELECT * FROM glob('/etc/*')) SELECT * FROM x",
		"SELECT * FROM (SELECT * FROM spans)",
	}
	for _, sql := range hostile {
		if _, err := eng.Engine.Query(context.Background(), policy, analysis.QueryRequest{SQL: sql}); err == nil {
			t.Fatalf("hostile query was allowed: %s", sql)
		}
	}

	// CTE/JOIN/UNION over the scoped tables is legitimate and must still work.
	if _, err := eng.Engine.Query(context.Background(), policy, analysis.QueryRequest{
		SQL: "WITH s AS (SELECT service_name FROM spans) SELECT s.service_name FROM s JOIN spans ON spans.service_name = s.service_name UNION SELECT service_name FROM spans",
	}); err != nil {
		t.Fatalf("legitimate scoped query was rejected: %v", err)
	}
}

func TestWindowValidationBounds(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	bounds := analysis.DefaultWindowBounds()

	ok := analysis.NewServerWindow(now.Add(-time.Minute), now, []string{"web-1"}, "")
	if err := ok.Validate(now, bounds); err != nil {
		t.Fatalf("valid window rejected: %v", err)
	}
	bad := []analysis.TimeWindow{
		analysis.NewExternalWindow(now, now, nil, ""),
		analysis.NewExternalWindow(now, now.Add(-time.Minute), nil, ""),
		analysis.NewExternalWindow(now.Add(-time.Hour-1), now, nil, ""),
		analysis.NewExternalWindow(now, now.Add(24*time.Hour), nil, ""),
		analysis.NewExternalWindow(now.Add(-time.Minute), now, []string{"BAD name"}, ""),
	}
	for i, w := range bad {
		if err := w.Validate(now, bounds); err == nil {
			t.Fatalf("invalid window %d accepted", i)
		}
	}
	if !analysis.ProvenanceExternal.Valid() || analysis.Provenance("bogus").Valid() {
		t.Fatal("provenance validity wrong")
	}
}

func TestWindowHalfOpenHelpers(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	w := analysis.NewServerWindow(start, start.Add(time.Minute), nil, "")
	if !w.ContainsNS(w.StartNS()) {
		t.Fatal("start must be included")
	}
	if w.ContainsNS(w.EndNS()) {
		t.Fatal("end must be excluded")
	}
	other := analysis.NewServerWindow(start.Add(30*time.Second), start.Add(2*time.Minute), nil, "")
	if !w.Overlaps(other) {
		t.Fatal("neighbouring windows should overlap")
	}
	adjacent := analysis.NewServerWindow(start.Add(time.Minute), start.Add(2*time.Minute), nil, "")
	if w.Overlaps(adjacent) {
		t.Fatal("adjacent half-open windows must not overlap")
	}
}

func TestWindowStoreCachesPartsAcrossSessions(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	start := now.Add(-3 * time.Minute)
	end := now.Add(-time.Minute)
	window := analysis.NewServerWindow(start, end, []string{"web-1"}, "cached")

	d := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 2,
		Spans: []otlp.Span{
			spanAt("web-1", "a", window.StartNS()+10, window.StartNS()+20, "STATUS_CODE_OK"),
			spanAt("web-1", "b", window.StartNS()+30, window.StartNS()+40, "STATUS_CODE_ERROR"),
		},
	}
	dir := t.TempDir()
	info, path := buildSegment(t, dir, "shots/shot-1/part-0.duckdb", "inst-web", "web-1", now.Add(-time.Minute).UnixMilli(), d)
	meta := analysis.SegmentMeta{
		Key: "shots/shot-1/part-0.duckdb", Instance: "web-1", Status: "verified",
		Size: info.Size, SHA256: info.SHA256,
		FirstReceivedAt: info.FirstReceivedAt, LastReceivedAt: info.LastReceivedAt,
	}
	store := analysis.NewWindowStore(filepath.Join(dir, "windows"), analysis.WindowStoreOptions{})
	defer store.Close()

	if _, err := store.OpenWindow("s1", window, []analysis.SegmentMeta{meta}); err != nil {
		t.Fatalf("OpenWindow: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddSegment(ctx, "s1", analysis.SegmentRef{Key: meta.Key, Instance: "web-1"}, f, info.Size, info.SHA256); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	f.Close()
	if _, err := store.Commit(ctx, "s1"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	store.Release("s1")

	// A second session for the same shot must reuse the cached part and report
	// its key so the Worker can skip the R2 download entirely.
	_, cached, err := store.OpenWindowWithCache("s2", window, []analysis.SegmentMeta{meta})
	if err != nil {
		t.Fatalf("OpenWindowWithCache: %v", err)
	}
	if len(cached) != 1 || cached[0] != meta.Key {
		t.Fatalf("cached keys = %v, want [%s]", cached, meta.Key)
	}
	// Re-adding the same part is idempotent: it must not duplicate rows.
	f2, _ := os.Open(path)
	if err := store.AddSegment(ctx, "s2", analysis.SegmentRef{Key: meta.Key, Instance: "web-1"}, f2, info.Size, info.SHA256); err != nil {
		t.Fatalf("AddSegment cached: %v", err)
	}
	f2.Close()
	cov, timings, err := store.CommitWithTimings(ctx, "s2")
	if err != nil {
		t.Fatalf("CommitWithTimings: %v", err)
	}
	if !cov.Complete {
		t.Fatalf("expected complete cached coverage, got %+v", cov)
	}
	if timings.TotalMS < 0 || timings.AttachMS < 0 {
		t.Fatalf("timings must be non-negative: %+v", timings)
	}
	res, err := store.Query(ctx, "s2", analysis.QueryRequest{SQL: "SELECT count(*) FROM spans"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := res.Rows[0][0]; got != int64(2) && got != float64(2) && got != "2" {
		t.Fatalf("expected 2 cached spans, got %v (%T)", got, got)
	}
}

func TestWindowStoreSessionAndCompare(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	start := now.Add(-3 * time.Minute)
	end := now.Add(-time.Minute)
	window := analysis.NewServerWindow(start, end, []string{"web-1"}, "run")

	d := &otlp.Decoded{
		Signal:      otlp.Traces,
		RecordCount: 2,
		Spans: []otlp.Span{
			spanAt("web-1", "a", window.StartNS()+10, window.StartNS()+20, "STATUS_CODE_OK"),
			spanAt("web-1", "b", window.StartNS()+30, window.StartNS()+40, "STATUS_CODE_ERROR"),
		},
	}
	dir := t.TempDir()
	info, path := buildSegment(t, dir, "journal/web-1/traces/store.bin", "inst-web", "web-1", now.Add(-time.Minute).UnixMilli(), d)
	meta := analysis.SegmentMeta{
		Key: "store", Instance: "web-1", Status: "verified",
		FirstReceivedAt: info.FirstReceivedAt, LastReceivedAt: info.LastReceivedAt,
		EventMinNS: info.EventMinNS, EventMaxNS: info.EventMaxNS,
	}

	store := analysis.NewWindowStore(filepath.Join(dir, "windows"), analysis.WindowStoreOptions{})
	defer store.Close()

	sel, err := store.OpenWindow("s1", window, []analysis.SegmentMeta{meta})
	if err != nil {
		t.Fatalf("OpenWindow: %v", err)
	}
	if sel.Expected != 1 {
		t.Fatalf("expected 1 candidate, got %d", sel.Expected)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddSegment(ctx, "s1", analysis.SegmentRef{Key: "store", Instance: "web-1"}, f, info.Size, info.SHA256); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	f.Close()
	cov, err := store.Commit(ctx, "s1")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !cov.Complete {
		t.Fatalf("expected complete coverage, got %+v", cov)
	}
	res, err := store.Query(ctx, "s1", analysis.QueryRequest{SQL: "SELECT name FROM spans ORDER BY name"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected 2 spans, got %d", res.RowCount)
	}

	// A neighbouring window over the same segment.
	neighbour := analysis.NewServerWindow(now.Add(-2*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "after")
	if _, err := store.OpenWindow("s2", neighbour, []analysis.SegmentMeta{meta}); err != nil {
		t.Fatalf("OpenWindow neighbour: %v", err)
	}
	f2, _ := os.Open(path)
	if err := store.AddSegment(ctx, "s2", analysis.SegmentRef{Key: "store", Instance: "web-1"}, f2, info.Size, info.SHA256); err != nil {
		t.Fatalf("AddSegment neighbour: %v", err)
	}
	f2.Close()
	if _, err := store.Commit(ctx, "s2"); err != nil {
		t.Fatalf("Commit neighbour: %v", err)
	}
	cmp, err := store.Compare(ctx, "s1", "s2", 10)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	joined := strings.Join(cmp.Disclosure, " | ")
	if !strings.Contains(joined, "left:") || !strings.Contains(joined, "right:") {
		t.Fatalf("compare disclosure missing window identity: %s", joined)
	}
	if cmp.SampleSizes.Left["spans"] == 0 {
		t.Fatalf("expected left sample size, got %+v", cmp.SampleSizes)
	}
}
