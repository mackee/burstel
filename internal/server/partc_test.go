package server_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/proto"

	colllogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/server"
)

type cappedHarness struct {
	*harness
}

func newHarnessWith(t *testing.T, cfg config.Config) *harness {
	t.Helper()
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	if cfg.InternalToken == "" {
		cfg.InternalToken = testToken
	}
	if cfg.MaxRequestBytes == 0 {
		cfg.MaxRequestBytes = 8 << 20
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	rec := recorder.New(cfg)
	ts := httptest.NewServer(server.New(cfg, rec).Handler())
	t.Cleanup(ts.Close)
	return &harness{t: t, ts: ts, dir: cfg.DataDir, rec: rec, file: filepath.Join(cfg.DataDir, "shot-1", recorder.ArtifactName)}
}

func (h *harness) droppedOutsideShot() int64 {
	h.t.Helper()
	resp := h.get("/internal/metadata", testToken)
	defer resp.Body.Close()
	var meta recorder.Metadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		h.t.Fatalf("decode metadata: %v", err)
	}
	return meta.DroppedOutsideShot
}

// TestOutsideShotExportIsAcknowledgedAndDiscarded covers spec 022 Part C1:
// exports before start and after stop return 200 with an OTLP partial_success,
// bump dropped_outside_shot and record nothing.
func TestOutsideShotExportIsAcknowledgedAndDiscarded(t *testing.T) {
	h := newHarness(t)
	trace := mustMarshal(t, fixtures.TraceExport())
	traceCount := int64(len(fixtures.TraceExport().GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()))

	// Before start.
	before := h.postOTLP("/v1/traces", "application/x-protobuf", trace, "")
	bodyBefore := readAll(t, before)
	if before.StatusCode != http.StatusOK {
		t.Fatalf("pre-start export = %d, want 200", before.StatusCode)
	}
	var traceResp colltrace.ExportTraceServiceResponse
	if err := proto.Unmarshal(bodyBefore, &traceResp); err != nil {
		t.Fatalf("decode trace response: %v", err)
	}
	if traceResp.GetPartialSuccess() == nil {
		t.Fatal("pre-start export: expected OTLP partial_success")
	}
	if got := traceResp.GetPartialSuccess().GetRejectedSpans(); got != traceCount {
		t.Errorf("rejected_spans = %d, want %d", got, traceCount)
	}
	if msg := traceResp.GetPartialSuccess().GetErrorMessage(); msg != "no active shot; data discarded" {
		t.Errorf("error_message = %q", msg)
	}
	if got := h.droppedOutsideShot(); got != traceCount {
		t.Errorf("dropped_outside_shot = %d, want %d", got, traceCount)
	}

	// Inside the shot: a normal empty partial_success.
	if status := h.start("shot-1", nil); status != http.StatusOK {
		t.Fatalf("start = %d", status)
	}
	inside := h.postOTLP("/v1/traces", "application/x-protobuf", trace, "")
	bodyInside := readAll(t, inside)
	if inside.StatusCode != http.StatusOK {
		t.Fatalf("in-shot export = %d", inside.StatusCode)
	}
	var insideResp colltrace.ExportTraceServiceResponse
	if err := proto.Unmarshal(bodyInside, &insideResp); err != nil {
		t.Fatalf("decode in-shot response: %v", err)
	}
	if insideResp.GetPartialSuccess() != nil {
		t.Errorf("in-shot export must be fully accepted, got %v", insideResp.GetPartialSuccess())
	}
	if status, _ := h.stop("shot-1"); status != http.StatusOK {
		t.Fatalf("stop = %d", status)
	}

	// After stop: logs and metrics are also acknowledged and discarded.
	logsBefore := h.droppedOutsideShot()
	logs := h.postOTLP("/v1/logs", "application/x-protobuf", mustMarshal(t, fixtures.LogsExport()), "")
	logsBody := readAll(t, logs)
	if logs.StatusCode != http.StatusOK {
		t.Fatalf("post-stop log export = %d, want 200", logs.StatusCode)
	}
	var logResp colllogs.ExportLogsServiceResponse
	if err := proto.Unmarshal(logsBody, &logResp); err != nil {
		t.Fatalf("decode log response: %v", err)
	}
	if logResp.GetPartialSuccess() == nil || logResp.GetPartialSuccess().GetRejectedLogRecords() == 0 {
		t.Fatalf("post-stop log export: expected rejected_log_records > 0, got %v", logResp.GetPartialSuccess())
	}

	metrics := h.postOTLP("/v1/metrics", "application/x-protobuf", mustMarshal(t, fixtures.MetricsExport()), "")
	metricsBody := readAll(t, metrics)
	if metrics.StatusCode != http.StatusOK {
		t.Fatalf("post-stop metric export = %d, want 200", metrics.StatusCode)
	}
	var metricResp collmetrics.ExportMetricsServiceResponse
	if err := proto.Unmarshal(metricsBody, &metricResp); err != nil {
		t.Fatalf("decode metric response: %v", err)
	}
	if metricResp.GetPartialSuccess() == nil || metricResp.GetPartialSuccess().GetRejectedDataPoints() == 0 {
		t.Fatalf("post-stop metric export: expected rejected_data_points > 0")
	}
	if got := h.droppedOutsideShot(); got <= logsBefore {
		t.Errorf("dropped_outside_shot did not increase: %d -> %d", logsBefore, got)
	}

	// Nothing from the discarded exports was recorded: only the in-shot batch.
	db, err := sql.Open("duckdb", h.file+"?access_mode=read_only")
	if err != nil {
		t.Fatalf("open frozen shot: %v", err)
	}
	defer db.Close()
	var batches, spans int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM otlp_batches`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM spans`).Scan(&spans); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || spans != traceCount {
		t.Errorf("frozen shot has batches=%d spans=%d, want 1/%d (discarded data must not be recorded)", batches, spans, traceCount)
	}
}

// TestOverloadResponsesCarryRetryAfter covers spec 022 Part C2: a real
// transient overload stays 503 and now advertises Retry-After.
func TestOverloadResponsesCarryRetryAfter(t *testing.T) {
	h := newHarnessWith(t, config.Config{MaxInflight: 0, MaxQueueWait: 0})
	if status := h.start("shot-1", nil); status != http.StatusOK {
		t.Fatalf("start = %d", status)
	}
	resp := h.postOTLP("/v1/traces", "application/x-protobuf", mustMarshal(t, fixtures.TraceExport()), "")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("overloaded export = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
}

// TestShutdownAfterStopIsPrompt covers spec 022 Part C3: after stop the server
// accepts and discards, so an exporter with default BSP settings can Shutdown
// promptly instead of retrying until its 30s deadline.
func TestShutdownAfterStopIsPrompt(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test skipped in -short mode")
	}
	h := newHarness(t)
	if status := h.start("shot-1", nil); status != http.StatusOK {
		t.Fatalf("start = %d", status)
	}

	counting := &statusCountingTransport{base: http.DefaultTransport}
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(strings.TrimPrefix(h.ts.URL, "http://")),
		otlptracehttp.WithURLPath("/v1/traces"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithHTTPClient(&http.Client{Transport: counting}),
	)
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter)) // default BSP settings
	emitSpans(tp, 2000)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tp.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown before stop: %v", err)
	}

	if status, _ := h.stop("shot-1"); status != http.StatusOK {
		t.Fatalf("stop = %d", status)
	}

	// A fresh provider exporting after stop must be acknowledged, not retried.
	exporter2, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(strings.TrimPrefix(h.ts.URL, "http://")),
		otlptracehttp.WithURLPath("/v1/traces"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithHTTPClient(&http.Client{Transport: counting}),
	)
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}
	tp2 := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter2))
	emitSpans(tp2, 500)

	start := time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := tp2.Shutdown(ctx2); err != nil {
		t.Fatalf("shutdown after stop: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("TracerProvider.Shutdown after stop completed in %s", elapsed)
	if elapsed >= 2*time.Second {
		t.Errorf("Shutdown after stop took %s, want < 2s", elapsed)
	}
	if got := h.droppedOutsideShot(); got < 500 {
		t.Errorf("dropped_outside_shot = %d, want >= 500", got)
	}
	if bad := counting.nonOK(); bad != 0 {
		t.Errorf("%d exporter requests were not 200 (retry storm)", bad)
	}
}

func emitSpans(tp *sdktrace.TracerProvider, n int) {
	tracer := tp.Tracer("burstel-partc")
	for i := 0; i < n; i++ {
		_, span := tracer.Start(context.Background(), "post-stop")
		span.End()
	}
}

type statusCountingTransport struct {
	base http.RoundTripper

	mu     sync.Mutex
	non200 int64
	ok     int64
}

func (s *statusCountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := s.base.RoundTrip(req)
	s.mu.Lock()
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.ok++
	} else {
		s.non200++
	}
	s.mu.Unlock()
	return resp, err
}

func (s *statusCountingTransport) countOK() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ok
}

func (s *statusCountingTransport) nonOK() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.non200
}
