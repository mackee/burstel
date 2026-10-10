package compaction

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	colllogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
)

// testClock is a deterministic, mutable clock for the trials.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock(t time.Time) *testClock { return &testClock{now: t} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *testClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// testEnv bundles a fake bucket and a manifest-backed compactor.
type testEnv struct {
	t        testing.TB
	dir      string
	store    *FakeR2
	manifest *Manifest
	comp     *Compactor
	clock    *testClock
}

func newTestEnv(t testing.TB, cfg Config) *testEnv {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFakeR2(filepath.Join(dir, "r2"))
	if err != nil {
		t.Fatalf("fake r2: %v", err)
	}
	manifest, err := OpenManifest(context.Background(), filepath.Join(dir, "manifest.duckdb"))
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	t.Cleanup(func() { _ = manifest.Close() })
	clock := newTestClock(time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC))
	if cfg.WorkDir == "" {
		cfg.WorkDir = filepath.Join(dir, "work")
	}
	comp := New(store, manifest, cfg)
	comp.SetClock(clock.Now)
	return &testEnv{t: t, dir: dir, store: store, manifest: manifest, comp: comp, clock: clock}
}

// spanBatch builds a genuine OTLP trace export with n spans.
func spanBatch(service string, n int, base uint64) []byte {
	spans := make([]fixtures.SpanProfile, 0, n)
	for i := 0; i < n; i++ {
		traceID := make([]byte, 16)
		spanID := make([]byte, 8)
		traceID[0] = byte(i)
		traceID[1] = byte(n)
		spanID[0] = byte(i + 1)
		spans = append(spans, fixtures.SpanProfile{
			Name:          fmt.Sprintf("op-%d", i),
			Kind:          tracepb.Span_SPAN_KIND_SERVER,
			Service:       service,
			StartNanos:    base + uint64(i)*1_000_000,
			DurationNanos: 5_000_000,
			TraceID:       traceID,
			SpanID:        spanID,
		})
	}
	raw, _ := proto.Marshal(fixtures.TraceExportProfiled(spans))
	return raw
}

// logBatch builds a genuine OTLP log export with n records.
func logBatch(service string, n int, base uint64) []byte {
	logs := make([]fixtures.LogProfile, 0, n)
	for i := 0; i < n; i++ {
		logs = append(logs, fixtures.LogProfile{
			TimeNanos:    base + uint64(i)*1_000_000,
			SeverityText: "INFO",
			Body:         fmt.Sprintf("log-%d", i),
			Service:      service,
		})
	}
	raw, _ := proto.Marshal(fixtures.LogsExportProfiled(logs))
	return raw
}

// metricBatch builds a genuine OTLP metric export with a fixed number of points.
func metricBatch() []byte {
	raw, _ := proto.Marshal(fixtures.MetricsExport())
	return raw
}

func gzipBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func shaHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ingestSignals writes one traces, one logs and one metrics export for an
// instance at time at, returning the expected wire hashes.
func ingestSignals(t *testing.T, env *testEnv, instanceID, service string, at time.Time, tick int, spansPer, logsPer int) map[string]string {
	t.Helper()
	ctx := context.Background()
	expect := map[string]string{}
	base := uint64(at.UnixNano())

	tracesWire := spanBatch(service, spansPer, base)
	if _, err := WriteJournal(ctx, env.store, instanceID, instanceID, otlp.Traces, at, fmt.Sprintf("t%d-traces", tick), tracesWire, ""); err != nil {
		t.Fatalf("write traces: %v", err)
	}
	expect[JournalKeyFor(instanceID, otlp.Traces, at, fmt.Sprintf("t%d-traces", tick))] = shaHex(tracesWire)

	logsWire := gzipBytes(t, logBatch(service, logsPer, base))
	if _, err := WriteJournal(ctx, env.store, instanceID, instanceID, otlp.Logs, at, fmt.Sprintf("t%d-logs", tick), logsWire, "gzip"); err != nil {
		t.Fatalf("write logs: %v", err)
	}
	expect[JournalKeyFor(instanceID, otlp.Logs, at, fmt.Sprintf("t%d-logs", tick))] = shaHex(logsWire)

	metricsWire := metricBatch()
	if _, err := WriteJournal(ctx, env.store, instanceID, instanceID, otlp.Metrics, at, fmt.Sprintf("t%d-metrics", tick), metricsWire, ""); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	expect[JournalKeyFor(instanceID, otlp.Metrics, at, fmt.Sprintf("t%d-metrics", tick))] = shaHex(metricsWire)
	return expect
}

// coverageKeys reconstructs source_key -> wire SHA from all restored segments.
func coverageKeys(t *testing.T, env *testEnv, segments []SegmentManifest) map[string]string {
	t.Helper()
	ctx := context.Background()
	out := map[string]string{}
	for _, seg := range segments {
		reader, _, err := RestoreSegment(ctx, env.store, env.manifest, seg.Key, filepath.Join(env.dir, "restore"))
		if err != nil {
			t.Fatalf("restore %s: %v", seg.Key, err)
		}
		hashes, err := reader.RawPayloadHashes(ctx)
		if err != nil {
			t.Fatalf("raw hashes: %v", err)
		}
		for k, v := range hashes {
			if prev, ok := out[k]; ok && prev != v {
				t.Fatalf("key %s covered with conflicting bytes", k)
			}
			out[k] = v
		}
		_ = reader.Close()
	}
	return out
}

// expectedCounts tracks exactly how much was ingested.
type expectedCounts struct {
	PerInstance map[string]struct{ Spans, Logs, Metrics, Batches int64 }
	Keys        map[string]string
}

func newExpected() *expectedCounts {
	return &expectedCounts{
		PerInstance: map[string]struct{ Spans, Logs, Metrics, Batches int64 }{},
		Keys:        map[string]string{},
	}
}

func (e *expectedCounts) add(instanceID string, spans, logs, metrics int, hashes map[string]string) {
	c := e.PerInstance[instanceID]
	c.Spans += int64(spans)
	c.Logs += int64(logs)
	c.Metrics += int64(metrics)
	c.Batches += int64(len(hashes))
	e.PerInstance[instanceID] = c
	for k, v := range hashes {
		e.Keys[k] = v
	}
}

func allSegments(t *testing.T, env *testEnv) []SegmentManifest {
	t.Helper()
	segs, err := env.comp.allSegments(context.Background())
	if err != nil {
		t.Fatalf("allSegments: %v", err)
	}
	return segs
}

func protoUnmarshalLogs(t *testing.T, raw []byte) *colllogs.ExportLogsServiceRequest {
	t.Helper()
	var req colllogs.ExportLogsServiceRequest
	if err := proto.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal logs: %v", err)
	}
	return &req
}

var (
	_ = colltrace.ExportTraceServiceRequest{}
	_ = collmetrics.ExportMetricsServiceRequest{}
)
