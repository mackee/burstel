package compaction

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mackee/burstel/internal/otlp"
)

func testRecord(t *testing.T, key, signal string, at int64, wire []byte) JournalRecord {
	t.Helper()
	decoded, err := otlp.Decode(otlp.Signal(signal), wire)
	if err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	return JournalRecord{
		Object: JournalObject{
			Key:          key,
			InstanceID:   "inst-1",
			Instance:     "node-1",
			Signal:       otlp.Signal(signal),
			ReceivedAt:   time.UnixMilli(at).UTC(),
			ReceivedAtMS: at,
			RequestID:    "req",
		},
		Wire:    wire,
		Payload: wire,
		Decoded: decoded,
	}
}

func TestBuildSegmentAppenderWritesAllRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	records := []JournalRecord{
		testRecord(t, "journal/inst-1/traces/1700000000000-a.bin", "traces", 1_700_000_000_000, spanBatch("svc", 7, 1_700_000_000_000_000_000)),
		testRecord(t, "journal/inst-1/logs/1700000000001-b.bin", "logs", 1_700_000_000_001, logBatch("svc", 4, 1_700_000_000_000_000_000)),
	}
	path := filepath.Join(dir, "segment.duckdb")
	info, err := BuildSegment(ctx, path, "segments/test/a.duckdb", "all", "", records)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if info.SpanCount != 7 || info.LogCount != 4 || info.RequestCount != 2 {
		t.Fatalf("unexpected info: %+v", info)
	}
	reader, err := OpenSegmentReadOnly(ctx, path, info.SHA256)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer reader.Close()
	spans, logs, metrics, batches, err := reader.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if spans != 7 || logs != 4 || metrics != 0 || batches != 2 {
		t.Fatalf("unexpected counts %d/%d/%d/%d", spans, logs, metrics, batches)
	}
}

func TestBuildSegmentAppenderRollsBackOnDuplicateKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	wire := spanBatch("svc", 3, 1_700_000_000_000_000_000)
	records := []JournalRecord{
		testRecord(t, "journal/inst-1/traces/1-dup.bin", "traces", 1_700_000_000_000, wire),
		testRecord(t, "journal/inst-1/traces/1-dup.bin", "traces", 1_700_000_000_000, wire),
	}
	path := filepath.Join(dir, "segment.duckdb")
	if _, err := BuildSegment(ctx, path, "segments/test/dup.duckdb", "all", "", records); err == nil {
		t.Fatal("expected the duplicate source key to fail the build")
	}
	reader, err := OpenSegmentReadOnly(ctx, path, "")
	if err != nil {
		t.Fatalf("open after rollback: %v", err)
	}
	defer reader.Close()
	spans, logs, metrics, batches, err := reader.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if spans != 0 || logs != 0 || metrics != 0 || batches != 0 {
		t.Fatalf("failed build left partial rows: %d/%d/%d/%d", spans, logs, metrics, batches)
	}
}

func TestBuildPartAllowsEmptyChunk(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "part.duckdb")
	info, err := BuildPart(ctx, path, "shots/shot-1/part-3.duckdb", "", "", nil)
	if err != nil {
		t.Fatalf("empty part: %v", err)
	}
	if info.Size <= 0 || info.SHA256 == "" {
		t.Fatalf("empty part has no checksum: %+v", info)
	}
}

func BenchmarkBuildSegmentAppender(b *testing.B) {
	const spansPerBatch = 2_000
	const batches = 25
	wire := spanBatch("svc", spansPerBatch, 1_700_000_000_000_000_000)
	records := make([]JournalRecord, 0, batches)
	for i := 0; i < batches; i++ {
		key := fmt.Sprintf("journal/inst-1/traces/%d-%d.bin", 1_700_000_000_000+i, i)
		decoded, err := otlp.Decode(otlp.Traces, wire)
		if err != nil {
			b.Fatal(err)
		}
		records = append(records, JournalRecord{
			Object: JournalObject{
				Key: key, InstanceID: "inst-1", Instance: "node-1", Signal: otlp.Traces,
				ReceivedAt:   time.UnixMilli(1_700_000_000_000 + int64(i)).UTC(),
				ReceivedAtMS: 1_700_000_000_000 + int64(i), RequestID: "req",
			},
			Wire: wire, Payload: wire, Decoded: decoded,
		})
	}
	b.ResetTimer()
	var spans int64
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		info, err := BuildSegment(context.Background(), filepath.Join(dir, "segment.duckdb"), "segments/bench/x.duckdb", "all", "", records)
		if err != nil {
			b.Fatal(err)
		}
		spans += info.SpanCount
	}
	b.ReportMetric(float64(spans)/b.Elapsed().Seconds(), "spans/s")
}
