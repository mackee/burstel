// Command burstel-loadgen drives the official OpenTelemetry Go exporters
// against burstel's ingest endpoint and reports throughput, loss and export
// latency percentiles (spec 023 benchmark harness).
//
// It is deliberately built on the stock SDK: default BatchSpanProcessor /
// BatchProcessor settings and the official OTLP/HTTP exporters, so the numbers
// reflect what a real Go service would experience. The same binary targets both
// the pre-023 baseline (`/ingest` with the ledger) and the DO-free endpoint:
// only `-endpoint` (and optionally `-variant`) changes.
//
// Examples:
//
//	# B1: 3 simulated hosts x 15k spans/s for 60s
//	burstel-loadgen -endpoint https://burstel.example -variant b1 \
//	  -token "$TOKEN" -exporters 3 -spans-per-second 15000 -duration 60s
//
//	# B2: burst then graceful Shutdown
//	burstel-loadgen -endpoint http://127.0.0.1:8787 -variant b2 \
//	  -token "$TOKEN" -exporters 3 -spans-per-second 30000 -duration 10s
//
//	# After compaction, verify stored vs generated for a window shot
//	burstel-loadgen -endpoint https://burstel.example -variant b1 -token "$TOKEN" \
//	  -exporters 3 -spans-per-second 15000 -duration 60s \
//	  -shot-id shot-abc -api-token "$BURSTEL_API_TOKEN"
//
// Span shape is ISUCON-like: one HTTP server root span plus N database child
// spans per trace. Every span, log record and metric point carries
// `burstel.run_id=<run id>` so the verification query can count exactly this
// run.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	apilog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	signals = "traces,metrics,logs"
	// A fixed tick keeps the generator independent of scheduling jitter.
	tickInterval = 10 * time.Millisecond
)

type latencyBuffer struct {
	mu        sync.Mutex
	durations []time.Duration
}

func (b *latencyBuffer) add(d time.Duration) {
	b.mu.Lock()
	b.durations = append(b.durations, d)
	b.mu.Unlock()
}

func (b *latencyBuffer) percentiles() (p50, p95, p99 float64) {
	b.mu.Lock()
	values := append([]time.Duration(nil), b.durations...)
	b.mu.Unlock()
	if len(values) == 0 {
		return 0, 0, 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(p float64) float64 {
		idx := int(float64(len(values)-1) * p)
		return float64(values[idx].Microseconds()) / 1000.0 // ms, 3 decimals
	}
	return at(0.50), at(0.95), at(0.99)
}

type signalStats struct {
	mu          sync.Mutex
	generated   int64
	exported    int64
	exportCalls int64
	errors      int64
	lastError   string
	lat         latencyBuffer
}

func (s *signalStats) generatedAdd(n int64) { atomic.AddInt64(&s.generated, n) }
func (s *signalStats) exportedAdd(n int64)  { atomic.AddInt64(&s.exported, n) }

func (s *signalStats) record(items int64, err error, d time.Duration) {
	atomic.AddInt64(&s.exportCalls, 1)
	if err != nil {
		atomic.AddInt64(&s.errors, 1)
		s.mu.Lock()
		s.lastError = err.Error()
		s.mu.Unlock()
	} else {
		atomic.AddInt64(&s.exported, items)
	}
	s.lat.add(d)
}

type stats struct {
	traces  signalStats
	metrics signalStats
	logs    signalStats
}

type signalReport struct {
	Generated   int64   `json:"generated"`
	Exported    int64   `json:"exported"`
	ExportCalls int64   `json:"export_calls"`
	Errors      int64   `json:"exporter_errors"`
	LastError   string  `json:"last_error,omitempty"`
	P50MS       float64 `json:"p50_ms"`
	P95MS       float64 `json:"p95_ms"`
	P99MS       float64 `json:"p99_ms"`
}

func (s *signalStats) report() signalReport {
	p50, p95, p99 := s.lat.percentiles()
	s.mu.Lock()
	last := s.lastError
	s.mu.Unlock()
	return signalReport{
		Generated:   atomic.LoadInt64(&s.generated),
		Exported:    atomic.LoadInt64(&s.exported),
		ExportCalls: atomic.LoadInt64(&s.exportCalls),
		Errors:      atomic.LoadInt64(&s.errors),
		LastError:   last,
		P50MS:       p50,
		P95MS:       p95,
		P99MS:       p99,
	}
}

// ---------------------------------------------------------------- exporters

type traceExporter struct {
	inner sdktrace.SpanExporter
	stats *signalStats
}

func (e *traceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	start := time.Now()
	err := e.inner.ExportSpans(ctx, spans)
	e.stats.record(int64(len(spans)), err, time.Since(start))
	return err
}

func (e *traceExporter) Shutdown(ctx context.Context) error { return e.inner.Shutdown(ctx) }

type metricExporter struct {
	inner sdkmetric.Exporter
	stats *signalStats
}

func (e *metricExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return e.inner.Temporality(k)
}

func (e *metricExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return e.inner.Aggregation(k)
}

func (e *metricExporter) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	start := time.Now()
	err := e.inner.Export(ctx, data)
	e.stats.record(int64(metricPointCount(data)), err, time.Since(start))
	return err
}

func (e *metricExporter) ForceFlush(ctx context.Context) error { return e.inner.ForceFlush(ctx) }
func (e *metricExporter) Shutdown(ctx context.Context) error   { return e.inner.Shutdown(ctx) }

type logExporter struct {
	inner sdklog.Exporter
	stats *signalStats
}

func (e *logExporter) Export(ctx context.Context, records []sdklog.Record) error {
	start := time.Now()
	err := e.inner.Export(ctx, records)
	e.stats.record(int64(len(records)), err, time.Since(start))
	return err
}

func (e *logExporter) Shutdown(ctx context.Context) error   { return e.inner.Shutdown(ctx) }
func (e *logExporter) ForceFlush(ctx context.Context) error { return e.inner.ForceFlush(ctx) }

func metricPointCount(data *metricdata.ResourceMetrics) int {
	if data == nil {
		return 0
	}
	n := 0
	for _, sm := range data.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				n += len(d.DataPoints)
			case metricdata.Sum[float64]:
				n += len(d.DataPoints)
			case metricdata.Gauge[int64]:
				n += len(d.DataPoints)
			case metricdata.Gauge[float64]:
				n += len(d.DataPoints)
			case metricdata.Histogram[int64]:
				n += len(d.DataPoints)
			case metricdata.Histogram[float64]:
				n += len(d.DataPoints)
			case metricdata.ExponentialHistogram[int64]:
				n += len(d.DataPoints)
			case metricdata.ExponentialHistogram[float64]:
				n += len(d.DataPoints)
			}
		}
	}
	return n
}

// ------------------------------------------------------------------ run

type exporterRun struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	loggerProvider *sdklog.LoggerProvider
	tracer         trace.Tracer
	logger         apilog.Logger
	counter        metric.Int64Counter
}

type config struct {
	endpoint      string
	variant       string
	token         string
	runID         string
	exporters     int
	spansPerSec   int
	duration      time.Duration
	dbSpans       int
	shutdownGrace time.Duration
	shotID        string
	apiURL        string
	apiToken      string
	emitMetrics   bool
	emitLogs      bool
}

func main() {
	cfg := parseFlags()
	if cfg.runID == "" {
		cfg.runID = "loadgen-" + time.Now().UTC().Format("20060102T150405Z") + "-" + randomHex(4)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	headers := map[string]string{
		"x-burstel-token": cfg.token,
		"authorization":   "Bearer " + cfg.token,
	}
	st := &stats{}
	runs, err := buildProviders(ctx, cfg, headers, st)
	if err != nil {
		fatal(err)
	}

	fmt.Fprintf(os.Stderr, "burstel-loadgen run=%s variant=%s target=%s exporters=%d spans/s/exporter=%d db-spans=%d duration=%s\n",
		cfg.runID, cfg.variant, cfg.endpoint, cfg.exporters, cfg.spansPerSec, cfg.dbSpans, cfg.duration)

	start := time.Now()
	var wg sync.WaitGroup
	deadline := start.Add(cfg.duration)
	for i, run := range runs {
		wg.Add(1)
		go func(index int, r *exporterRun) {
			defer wg.Done()
			generate(ctx, r, st, cfg, index, deadline)
		}(i, run)
	}
	wg.Wait()
	generatedElapsed := time.Since(start)

	// B2 (and every run): the stock SDK Shutdown flushes its batch processors
	// and readers. Loss after this point is real loss.
	shutdownStart := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
	defer cancel()
	var shutdownErr error
	for _, run := range runs {
		if err := run.tracerProvider.Shutdown(shutdownCtx); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
		if err := run.meterProvider.Shutdown(shutdownCtx); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
		if err := run.loggerProvider.Shutdown(shutdownCtx); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	shutdownDuration := time.Since(shutdownStart)

	report := map[string]any{
		"run_id":            cfg.runID,
		"variant":           cfg.variant,
		"target":            cfg.endpoint,
		"exporters":         cfg.exporters,
		"spans_per_second":  cfg.spansPerSec,
		"per_exporter":      true,
		"db_spans":          cfg.dbSpans,
		"duration_ms":       cfg.duration.Milliseconds(),
		"generation_ms":     generatedElapsed.Milliseconds(),
		"shutdown_ms":       shutdownDuration.Milliseconds(),
		"shutdown_deadline": cfg.shutdownGrace.String(),
		"shutdown_error":    errString(shutdownErr),
		"signals": map[string]signalReport{
			"traces":  st.traces.report(),
			"metrics": st.metrics.report(),
			"logs":    st.logs.report(),
		},
	}

	if cfg.shotID != "" && cfg.apiToken != "" {
		report["verification"] = verifyWindow(cfg)
	}
	out, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(out))
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.endpoint, "endpoint", "http://127.0.0.1:8787", "burstel base URL (targets both the pre-023 baseline and the new /ingest)")
	flag.StringVar(&cfg.variant, "variant", "b1", "run label: b0 (baseline), b1 (new), b2 (burst)")
	flag.StringVar(&cfg.token, "token", os.Getenv("BURSTEL_PUSH_TOKEN"), "push-only instance token (prefer BURSTEL_PUSH_TOKEN)")
	flag.StringVar(&cfg.runID, "run-id", "", "run id embedded in every signal (default: generated)")
	flag.IntVar(&cfg.exporters, "exporters", 1, "number of simulated hosts/exporters")
	flag.IntVar(&cfg.spansPerSec, "spans-per-second", 15_000, "total spans/s per exporter (root + DB children)")
	flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "load duration before Shutdown")
	flag.IntVar(&cfg.dbSpans, "db-spans", 5, "DB child spans per HTTP root span (ISUCON-like shape)")
	flag.DurationVar(&cfg.shutdownGrace, "shutdown-grace", 60*time.Second, "outer bound for the SDK Shutdown flush")
	flag.StringVar(&cfg.shotID, "shot-id", "", "optional window shot id for stored-vs-generated verification")
	flag.StringVar(&cfg.apiURL, "api-url", "", "control-plane base URL for verification (default: -endpoint)")
	flag.StringVar(&cfg.apiToken, "api-token", os.Getenv("BURSTEL_API_TOKEN"), "admin/member token for verification queries")
	flag.BoolVar(&cfg.emitMetrics, "emit-metrics", true, "emit one OTLP metric point per exporter per second")
	flag.BoolVar(&cfg.emitLogs, "emit-logs", true, "emit one OTLP log record per exporter per second")
	flag.Parse()
	cfg.endpoint = strings.TrimRight(cfg.endpoint, "/")
	if cfg.apiURL == "" {
		cfg.apiURL = cfg.endpoint
	}
	cfg.apiURL = strings.TrimRight(cfg.apiURL, "/")
	if cfg.dbSpans < 0 {
		cfg.dbSpans = 0
	}
	if cfg.exporters < 1 {
		cfg.exporters = 1
	}
	if cfg.spansPerSec < 1 {
		cfg.spansPerSec = 1
	}
	if cfg.token == "" {
		fatal(fmt.Errorf("a push token is required via -token or BURSTEL_PUSH_TOKEN"))
	}
	return cfg
}

func buildProviders(ctx context.Context, cfg config, headers map[string]string, st *stats) ([]*exporterRun, error) {
	runs := make([]*exporterRun, 0, cfg.exporters)
	traceURL, err := url.Parse(cfg.endpoint + "/ingest/v1/traces")
	if err != nil {
		return nil, err
	}
	metricURL, _ := url.Parse(cfg.endpoint + "/ingest/v1/metrics")
	logURL, _ := url.Parse(cfg.endpoint + "/ingest/v1/logs")
	for i := 0; i < cfg.exporters; i++ {
		res := resource.NewWithAttributes(
			"https://opentelemetry.io/schemas/1.24.0",
			attribute.String("service.name", fmt.Sprintf("loadgen-host-%d", i)),
			attribute.String("burstel.run_id", cfg.runID),
			attribute.Int("burstel.exporter.index", i),
		)

		traceExp, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpointURL(traceURL.String()),
			otlptracehttp.WithHeaders(headers),
		)
		if err != nil {
			return nil, fmt.Errorf("trace exporter %d: %w", i, err)
		}
		tp := sdktrace.NewTracerProvider(
			// Default BatchSpanProcessor configuration (spec 023 harness rule).
			sdktrace.WithBatcher(&traceExporter{inner: traceExp, stats: &st.traces}),
			sdktrace.WithResource(res),
		)

		metricExp, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpointURL(metricURL.String()),
			otlpmetrichttp.WithHeaders(headers),
		)
		if err != nil {
			return nil, fmt.Errorf("metric exporter %d: %w", i, err)
		}
		mp := sdkmetric.NewMeterProvider(
			// Default periodic reader (60 s); Shutdown flushes it.
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(&metricExporter{inner: metricExp, stats: &st.metrics})),
			sdkmetric.WithResource(res),
		)
		counter, err := mp.Meter("burstel-loadgen").Int64Counter(
			"burstel.loadgen.spans",
			metric.WithDescription("spans generated by this loadgen run"),
		)
		if err != nil {
			return nil, fmt.Errorf("metric instrument %d: %w", i, err)
		}

		logExp, err := otlploghttp.New(ctx,
			otlploghttp.WithEndpointURL(logURL.String()),
			otlploghttp.WithHeaders(headers),
		)
		if err != nil {
			return nil, fmt.Errorf("log exporter %d: %w", i, err)
		}
		lp := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(&logExporter{inner: logExp, stats: &st.logs})),
			sdklog.WithResource(res),
		)

		runs = append(runs, &exporterRun{
			tracerProvider: tp,
			meterProvider:  mp,
			loggerProvider: lp,
			tracer:         tp.Tracer("burstel-loadgen"),
			logger:         lp.Logger("burstel-loadgen"),
			counter:        counter,
		})
	}
	return runs, nil
}

// generate produces the requested span rate for one exporter until deadline.
func generate(
	ctx context.Context,
	run *exporterRun,
	st *stats,
	cfg config,
	index int,
	deadline time.Time,
) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	spansPerTrace := cfg.dbSpans + 1
	// Fractional accumulation keeps low rates exact over the whole run.
	accumulator := 0.0
	perTick := float64(cfg.spansPerSec) * tickInterval.Seconds()
	lastTelemetry := time.Now()
	metricEmitted := false

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if !now.Before(deadline) {
				return
			}
			accumulator += perTick
			traces := int(accumulator) / spansPerTrace
			accumulator -= float64(traces * spansPerTrace)
			for n := 0; n < traces; n++ {
				emitTrace(ctx, run, st, cfg, index)
			}

			if now.Sub(lastTelemetry) >= time.Second {
				lastTelemetry = now
				if cfg.emitLogs {
					emitLog(ctx, run, st, cfg, index)
				}
				// A cumulative counter is one data point per attribute set, so
				// emit it once per exporter. "generated" therefore counts points,
				// matching what the segment stores.
				if cfg.emitMetrics && !metricEmitted {
					emitMetric(ctx, run, st, cfg, index)
					metricEmitted = true
				}
			}
		}
	}
}

func emitTrace(ctx context.Context, run *exporterRun, st *stats, cfg config, index int) {
	rootCtx, root := run.tracer.Start(ctx, "GET /",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", "GET"),
			attribute.String("url.path", "/"),
			attribute.Int("burstel.exporter.index", index),
			attribute.String("burstel.run_id", cfg.runID),
		),
	)
	for i := 0; i < cfg.dbSpans; i++ {
		_, child := run.tracer.Start(rootCtx, "SELECT",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("db.system", "postgresql"),
				attribute.String("db.statement", "SELECT * FROM users WHERE id = $1"),
				attribute.Int("burstel.db.child", i),
				attribute.String("burstel.run_id", cfg.runID),
			),
		)
		child.End()
	}
	root.End()
	st.traces.generatedAdd(int64(cfg.dbSpans + 1))
}

func emitLog(ctx context.Context, run *exporterRun, st *stats, cfg config, index int) {
	var record apilog.Record
	record.SetTimestamp(time.Now())
	record.SetObservedTimestamp(time.Now())
	record.SetSeverity(apilog.SeverityInfo)
	record.SetSeverityText("INFO")
	record.SetBody(attribute.StringValue("loadgen heartbeat"))
	record.AddAttributes(
		attribute.String("burstel.run_id", cfg.runID),
		attribute.Int("burstel.exporter.index", index),
	)
	run.logger.Emit(ctx, record)
	st.logs.generatedAdd(1)
}

func emitMetric(ctx context.Context, run *exporterRun, st *stats, cfg config, index int) {
	run.counter.Add(ctx, int64(cfg.dbSpans+1), metric.WithAttributes(
		attribute.String("burstel.run_id", cfg.runID),
		attribute.Int("burstel.exporter.index", index),
	))
	st.metrics.generatedAdd(1)
}

// ----------------------------------------------------------- verification

// verifyWindow queries the compacted window shot for this run id and reports
// stored vs generated per signal.
func verifyWindow(cfg config) map[string]any {
	out := map[string]any{"shot_id": cfg.shotID, "run_id": cfg.runID}
	queries := map[string]string{
		"traces":  fmt.Sprintf("SELECT COUNT(*) AS n FROM spans WHERE attributes LIKE '%%%s%%'", cfg.runID),
		"logs":    fmt.Sprintf("SELECT COUNT(*) AS n FROM log_records WHERE attributes LIKE '%%%s%%'", cfg.runID),
		"metrics": fmt.Sprintf("SELECT COUNT(*) AS n FROM metric_points WHERE attributes LIKE '%%%s%%'", cfg.runID),
	}
	stored := map[string]any{}
	for signal, query := range queries {
		n, err := queryCount(cfg.apiURL, cfg.apiToken, cfg.shotID, query)
		if err != nil {
			stored[signal] = map[string]any{"error": err.Error()}
		} else {
			stored[signal] = n
		}
	}
	out["stored"] = stored
	return out
}

func queryCount(baseURL, token, shotID, sql string) (int64, error) {
	body, _ := json.Marshal(map[string]any{"sql": sql})
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/shots/%s/query", baseURL, shotID), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+token)
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("query failed with %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var parsed struct {
		Rows [][]any `json:"rows"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return 0, err
	}
	if len(parsed.Rows) == 0 || len(parsed.Rows[0]) == 0 {
		return 0, nil
	}
	switch v := parsed.Rows[0][0].(type) {
	case float64:
		return int64(v), nil
	case json.Number:
		n, err := v.Int64()
		return n, err
	default:
		return 0, fmt.Errorf("unexpected count type %T", v)
	}
}

// ----------------------------------------------------------------- helpers

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprint(time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "burstel-loadgen: %v\n", err)
	os.Exit(1)
}
