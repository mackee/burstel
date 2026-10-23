package server_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/recorder"
)

// TestStandaloneExporterIsLosslessAt15kSpansPerSecond covers spec 022 Part B:
// the official OTLP/HTTP exporter with *default* BatchSpanProcessor settings
// drives 15k spans/s for 10s against the standalone recorder, and every
// generated span is durably stored (no client-side queue overflow, no retries).
func TestStandaloneExporterIsLosslessAt15kSpansPerSecond(t *testing.T) {
	if testing.Short() {
		t.Skip("10s throughput test skipped in -short mode")
	}
	h := newHarnessWith(t, config.Config{
		MaxInflight:  8,
		MaxQueueWait: config.DefaultMaxQueueWait,
	})
	if status := h.start("shot-throughput", nil); status != http.StatusOK {
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
	// Default BSP settings: queue 2048, max export batch 512, 5s batch timeout.
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
	tracer := tp.Tracer("burstel-partb")

	const (
		targetRate = 15_000 // spans per second
		duration   = 10 * time.Second
		tick       = 10 * time.Millisecond
		perTick    = targetRate / int(time.Second/tick) // 150
	)
	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var generated int64
	for now := time.Now(); !now.After(deadline); now = <-ticker.C {
		for i := 0; i < perTick; i++ {
			_, span := tracer.Start(context.Background(), "bench.operation",
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(),
			)
			span.End()
			generated++
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := tp.Shutdown(ctx); err != nil {
		t.Fatalf("exporter shutdown: %v", err)
	}
	if status, _ := h.stop("shot-throughput"); status != http.StatusOK {
		t.Fatalf("stop = %d", status)
	}

	meta := h.get("/internal/metadata", testToken)
	var m recorder.Metadata
	_ = json.NewDecoder(meta.Body).Decode(&m)
	meta.Body.Close()
	t.Logf("server metadata: spans=%d batches=%d dropped_outside=%d", m.Counters.Spans, m.Counters.Batches["traces"], m.DroppedOutsideShot)

	if bad := counting.nonOK(); bad != 0 {
		t.Errorf("%d exporter requests were not 200", bad)
	}
	if counting.countOK() == 0 {
		t.Error("exporter made no successful requests")
	}

	frozen := filepath.Join(h.dir, "shot-throughput", recorder.ArtifactName)
	db, err := sql.Open("duckdb", frozen+"?access_mode=read_only")
	if err != nil {
		t.Fatalf("open frozen shot: %v", err)
	}
	defer db.Close()
	var stored int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM spans`).Scan(&stored); err != nil {
		t.Fatalf("count stored spans: %v", err)
	}
	if stored != generated {
		t.Fatalf("stored %d spans, generated %d (lossless delivery broken)", stored, generated)
	}
	t.Logf("lossless export: %d spans stored at %d spans/s target, %d OTLP requests, 0 non-200",
		stored, targetRate, counting.countOK())
}
