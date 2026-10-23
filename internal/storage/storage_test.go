package storage_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/storage"
)

// openDB gives tests a read handle on the same file after the writer closed.
func openDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func count(t *testing.T, db *sql.DB, table string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func spanBatch(shotID string, n int) *storage.Batch {
	spans := make([]otlp.Span, 0, n)
	for i := 0; i < n; i++ {
		start := uint64(time.Now().UnixNano())
		spans = append(spans, otlp.Span{
			TraceID:            fmt.Sprintf("%032x", i),
			SpanID:             fmt.Sprintf("%016x", i),
			Name:               fmt.Sprintf("span-%d", i),
			Kind:               "SPAN_KIND_SERVER",
			StartTimeUnixNano:  start,
			EndTimeUnixNano:    start + uint64(time.Millisecond),
			ServiceName:        "bench",
			ResourceAttributes: `{"service.name":"bench"}`,
			ScopeAttributes:    `{}`,
			Attributes:         `{"http.status_code":200}`,
		})
	}
	return &storage.Batch{
		ShotID:          shotID,
		Signal:          otlp.Traces,
		ReceivedAt:      time.Now().UTC(),
		ResourceSummary: `{"service_names":["bench"],"resource_count":1}`,
		Payload:         []byte("raw-otlp-payload"),
		Spans:           spans,
	}
}

// TestInsertBatchRoundTrip proves the raw payload row and the projection rows
// are both committed, and that the payload bytes are stored unchanged.
func TestInsertBatchRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "roundtrip.duckdb")
	st, err := storage.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	if err := st.InsertBatch(ctx, spanBatch("shot-1", 3)); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := st.InsertBatch(ctx, spanBatch("shot-1", 3)); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if err := st.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db := openDB(t, path)
	for table, want := range map[string]int64{
		"otlp_batches":  2,
		"spans":         6,
		"log_records":   0,
		"metric_points": 0,
	} {
		if got := count(t, db, table); got != want {
			t.Errorf("%s rows = %d, want %d", table, got, want)
		}
	}
	var payload string
	if err := db.QueryRow(`SELECT CAST(payload AS VARCHAR) FROM otlp_batches ORDER BY ordinal LIMIT 1`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "raw-otlp-payload" {
		t.Errorf("payload = %q, want the raw bytes unchanged", payload)
	}
}

// TestInsertBatchMixedSignals covers the log and metric appender paths.
func TestInsertBatchMixedSignals(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mixed.duckdb")
	st, err := storage.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	logs := &storage.Batch{
		ShotID:     "shot-1",
		Signal:     otlp.Logs,
		ReceivedAt: time.Now().UTC(),
		Payload:    []byte("logs"),
		LogRecords: []otlp.LogRecord{
			{TimeUnixNano: 1, SeverityText: "INFO", Body: `"hello"`, ServiceName: "bench"},
			{TimeUnixNano: 2, SeverityText: "ERROR", Body: `"boom"`, ServiceName: "bench"},
		},
	}
	metrics := &storage.Batch{
		ShotID:     "shot-1",
		Signal:     otlp.Metrics,
		ReceivedAt: time.Now().UTC(),
		Payload:    []byte("metrics"),
		MetricPoints: []otlp.MetricPoint{
			{MetricName: "http_requests", MetricType: "sum", TimeUnixNano: 3, ServiceName: "bench"},
		},
	}
	if err := st.InsertBatch(ctx, logs); err != nil {
		t.Fatalf("insert logs: %v", err)
	}
	if err := st.InsertBatch(ctx, metrics); err != nil {
		t.Fatalf("insert metrics: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	db := openDB(t, path)
	if got := count(t, db, "log_records"); got != 2 {
		t.Errorf("log_records = %d, want 2", got)
	}
	if got := count(t, db, "metric_points"); got != 1 {
		t.Errorf("metric_points = %d, want 1", got)
	}
	if got := count(t, db, "otlp_batches"); got != 2 {
		t.Errorf("otlp_batches = %d, want 2", got)
	}
}

// BenchmarkInsertBatch512 measures the 512-span OTLP export commit that spec
// 022 Part B targets (<= 10 ms p50 on the development machine).
func BenchmarkInsertBatch512(b *testing.B) {
	ctx := context.Background()
	path := filepath.Join(b.TempDir(), "bench.duckdb")
	st, err := storage.Open(ctx, path)
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.InsertBatch(ctx, spanBatch("shot-bench", 512)); err != nil {
			b.Fatalf("insert batch: %v", err)
		}
	}
}

// TestInsertBatch512Latency records the p50/p95 of a 512-span commit so a
// regression in the ingest path is visible in plain `go test` output too.
func TestInsertBatch512Latency(t *testing.T) {
	if testing.Short() {
		t.Skip("latency measurement skipped in -short mode")
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "latency.duckdb")
	st, err := storage.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	const runs = 60
	samples := make([]time.Duration, 0, runs)
	batch := spanBatch("shot-latency", 512)
	for i := 0; i < runs; i++ {
		start := time.Now()
		if err := st.InsertBatch(ctx, batch); err != nil {
			t.Fatalf("insert batch: %v", err)
		}
		samples = append(samples, time.Since(start))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50 := samples[len(samples)/2]
	p95 := samples[(len(samples)*95)/100]
	t.Logf("512-span commit: p50=%s p95=%s min=%s max=%s (spec 022 target: p50 <= 10ms)",
		p50, p95, samples[0], samples[len(samples)-1])
	// The 10ms target is a benchmark on a quiet machine; under `go test ./...`
	// several cgo/DuckDB packages run concurrently, so only a gross regression
	// (a return to the ~300ms row-at-a-time path) fails the gate here.
	if p50 > 50*time.Millisecond {
		t.Errorf("512-span commit p50 = %s, want <= 50ms (row-at-a-time regression; target is 10ms)", p50)
	}
}
