// Command burstel-loadgen drives the official OpenTelemetry Go exporters
// against burstel's ingest endpoint and reports throughput, loss and export
// latency percentiles (spec 023 benchmark harness).
//
// It is built on the stock SDK with the official OTLP/HTTP exporters, so the
// numbers reflect what a real Go service would experience. By default it uses
// the SDK's BatchSpanProcessor / BatchProcessor settings; spec 027 adds
// `-compression`, `-batch-size` and `-queue-size` so the recommended ISUCON
// settings can be measured too. The effective values are always reported.
// The same binary targets both the pre-023 baseline (`/ingest` with the ledger)
// and the DO-free endpoint: only `-endpoint` (and optionally `-variant`) changes.
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
	"strconv"
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

// clientSettings are the OTel Go SDK export settings this run actually uses.
// BatchSize/QueueSize are always effective values (env or SDK defaults are
// filled in), so the JSON report can be compared with the recommended ISUCON
// settings without guessing.
type clientSettings struct {
	Compression string `json:"compression"`
	BatchSize   int    `json:"batch_size"`
	QueueSize   int    `json:"queue_size"`
}

// clientSettingsSources says where each effective value came from:
// "flag", "env" (the standard OTEL_* variable) or "default" (SDK default).
type clientSettingsSources struct {
	Compression string `json:"compression"`
	BatchSize   string `json:"batch_size"`
	QueueSize   string `json:"queue_size"`
}

// clientSettingFlags records both the flag values and whether each flag was
// explicitly supplied. Spec 032 requires an unset flag to defer to the standard
// OTEL_* environment variables, exactly like the stock SDK.
type clientSettingFlags struct {
	Compression    string
	BatchSize      int
	QueueSize      int
	CompressionSet bool
	BatchSizeSet   bool
	QueueSizeSet   bool
}

type envLookup func(string) (string, bool)

// resolveClientSettings implements: explicit flag > OTEL_* env > SDK default.
func resolveClientSettings(flags clientSettingFlags, getenv envLookup) (clientSettings, clientSettingsSources, error) {
	compression := "none"
	compressionSource := "default"
	if flags.CompressionSet && strings.TrimSpace(flags.Compression) != "" {
		compression = strings.TrimSpace(flags.Compression)
		compressionSource = "flag"
	} else if raw, ok := getenv("OTEL_EXPORTER_OTLP_COMPRESSION"); ok && strings.TrimSpace(raw) != "" {
		compression = strings.TrimSpace(raw)
		compressionSource = "env"
	}
	switch compression {
	case "none", "":
		compression = "none"
	case "gzip":
	default:
		return clientSettings{}, clientSettingsSources{}, fmt.Errorf("-compression must be gzip or none, got %q", compression)
	}

	if flags.BatchSize < 0 {
		return clientSettings{}, clientSettingsSources{}, fmt.Errorf("-batch-size must be >= 0, got %d", flags.BatchSize)
	}
	if flags.QueueSize < 0 {
		return clientSettings{}, clientSettingsSources{}, fmt.Errorf("-queue-size must be >= 0, got %d", flags.QueueSize)
	}
	batchSize, batchSource, err := resolveIntSetting(
		flags.BatchSize, flags.BatchSizeSet && flags.BatchSize > 0,
		getenv, "OTEL_BSP_MAX_EXPORT_BATCH_SIZE", sdktrace.DefaultMaxExportBatchSize,
	)
	if err != nil {
		return clientSettings{}, clientSettingsSources{}, err
	}
	queueSize, queueSource, err := resolveIntSetting(
		flags.QueueSize, flags.QueueSizeSet && flags.QueueSize > 0,
		getenv, "OTEL_BSP_MAX_QUEUE_SIZE", sdktrace.DefaultMaxQueueSize,
	)
	if err != nil {
		return clientSettings{}, clientSettingsSources{}, err
	}
	if batchSize > queueSize {
		return clientSettings{}, clientSettingsSources{}, fmt.Errorf("-batch-size (%d) must not exceed -queue-size (%d)", batchSize, queueSize)
	}
	return clientSettings{Compression: compression, BatchSize: batchSize, QueueSize: queueSize}, clientSettingsSources{
		Compression: compressionSource,
		BatchSize:   batchSource,
		QueueSize:   queueSource,
	}, nil
}

func resolveIntSetting(
	flagValue int,
	flagSet bool,
	getenv envLookup,
	envName string,
	fallback int,
) (int, string, error) {
	if flagSet {
		return flagValue, "flag", nil
	}
	if raw, ok := getenv(envName); ok && strings.TrimSpace(raw) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || parsed <= 0 {
			return 0, "", fmt.Errorf("%s must be a positive integer, got %q", envName, raw)
		}
		return parsed, "env", nil
	}
	return fallback, "default", nil
}

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
	compression   string
	batchSize     int
	queueSize     int
	settings      clientSettings
	sources       clientSettingsSources
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err == flag.ErrHelp {
		return
	}
	if err != nil {
		fatal(err)
	}
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

	fmt.Fprintf(os.Stderr, "burstel-loadgen run=%s variant=%s target=%s exporters=%d spans/s/exporter=%d db-spans=%d duration=%s compression=%s(%s) batch-size=%d(%s) queue-size=%d(%s)\n",
		cfg.runID, cfg.variant, cfg.endpoint, cfg.exporters, cfg.spansPerSec, cfg.dbSpans, cfg.duration,
		cfg.settings.Compression, cfg.sources.Compression,
		cfg.settings.BatchSize, cfg.sources.BatchSize,
		cfg.settings.QueueSize, cfg.sources.QueueSize)

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
		"run_id":                  cfg.runID,
		"variant":                 cfg.variant,
		"target":                  cfg.endpoint,
		"exporters":               cfg.exporters,
		"spans_per_second":        cfg.spansPerSec,
		"per_exporter":            true,
		"db_spans":                cfg.dbSpans,
		"duration_ms":             cfg.duration.Milliseconds(),
		"client_settings":         cfg.settings,
		"client_settings_sources": cfg.sources,
		"generation_ms":           generatedElapsed.Milliseconds(),
		"shutdown_ms":             shutdownDuration.Milliseconds(),
		"shutdown_deadline":       cfg.shutdownGrace.String(),
		"shutdown_error":          errString(shutdownErr),
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

func parseFlags(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("burstel-loadgen", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.endpoint, "endpoint", "http://127.0.0.1:8787", "burstel base URL (targets both the pre-023 baseline and the new /ingest)")
	fs.StringVar(&cfg.variant, "variant", "b1", "run label: b0 (baseline), b1 (new), b2 (burst)")
	fs.StringVar(&cfg.token, "token", os.Getenv("BURSTEL_PUSH_TOKEN"), "push-only instance token (prefer BURSTEL_PUSH_TOKEN)")
	fs.StringVar(&cfg.runID, "run-id", "", "run id embedded in every signal (default: generated)")
	fs.IntVar(&cfg.exporters, "exporters", 1, "number of simulated hosts/exporters")
	fs.IntVar(&cfg.spansPerSec, "spans-per-second", 15_000, "total spans/s per exporter (root + DB children)")
	fs.DurationVar(&cfg.duration, "duration", 60*time.Second, "load duration before Shutdown")
	fs.IntVar(&cfg.dbSpans, "db-spans", 5, "DB child spans per HTTP root span (ISUCON-like shape)")
	fs.DurationVar(&cfg.shutdownGrace, "shutdown-grace", 60*time.Second, "outer bound for the SDK Shutdown flush")
	fs.StringVar(&cfg.shotID, "shot-id", "", "optional window shot id for stored-vs-generated verification")
	fs.StringVar(&cfg.apiURL, "api-url", "", "control-plane base URL for verification (default: -endpoint)")
	fs.StringVar(&cfg.apiToken, "api-token", os.Getenv("BURSTEL_API_TOKEN"), "admin/member token for verification queries")
	fs.BoolVar(&cfg.emitMetrics, "emit-metrics", true, "emit one OTLP metric point per exporter per second")
	fs.BoolVar(&cfg.emitLogs, "emit-logs", true, "emit one OTLP log record per exporter per second")
	fs.StringVar(&cfg.compression, "compression", "", "OTLP/HTTP compression: gzip or none (unset = OTEL_EXPORTER_OTLP_COMPRESSION, else none)")
	fs.IntVar(&cfg.batchSize, "batch-size", 0, "max export batch size (unset/0 = OTEL_BSP_MAX_EXPORT_BATCH_SIZE, else SDK 512)")
	fs.IntVar(&cfg.queueSize, "queue-size", 0, "max queued spans/log records (unset/0 = OTEL_BSP_MAX_QUEUE_SIZE, else SDK 2048)")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
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
	settings, sources, err := resolveClientSettings(clientSettingFlags{
		Compression:    cfg.compression,
		BatchSize:      cfg.batchSize,
		QueueSize:      cfg.queueSize,
		CompressionSet: set["compression"],
		BatchSizeSet:   set["batch-size"],
		QueueSizeSet:   set["queue-size"],
	}, os.LookupEnv)
	if err != nil {
		return cfg, err
	}
	cfg.settings = settings
	cfg.sources = sources
	if cfg.token == "" {
		return cfg, fmt.Errorf("a push token is required via -token or BURSTEL_PUSH_TOKEN")
	}
	return cfg, nil
}

func traceHTTPOptions(settings clientSettings) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{otlptracehttp.WithCompression(otlptracehttp.NoCompression)}
	if settings.Compression == "gzip" {
		opts = []otlptracehttp.Option{otlptracehttp.WithCompression(otlptracehttp.GzipCompression)}
	}
	return opts
}

func metricHTTPOptions(settings clientSettings) []otlpmetrichttp.Option {
	opts := []otlpmetrichttp.Option{otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression)}
	if settings.Compression == "gzip" {
		opts = []otlpmetrichttp.Option{otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression)}
	}
	return opts
}

func logHTTPOptions(settings clientSettings) []otlploghttp.Option {
	opts := []otlploghttp.Option{otlploghttp.WithCompression(otlploghttp.NoCompression)}
	if settings.Compression == "gzip" {
		opts = []otlploghttp.Option{otlploghttp.WithCompression(otlploghttp.GzipCompression)}
	}
	return opts
}

func traceBatchOptions(settings clientSettings) []sdktrace.BatchSpanProcessorOption {
	return []sdktrace.BatchSpanProcessorOption{
		sdktrace.WithMaxExportBatchSize(settings.BatchSize),
		sdktrace.WithMaxQueueSize(settings.QueueSize),
	}
}

func logBatchOptions(settings clientSettings) []sdklog.BatchProcessorOption {
	return []sdklog.BatchProcessorOption{
		sdklog.WithExportMaxBatchSize(settings.BatchSize),
		sdklog.WithMaxQueueSize(settings.QueueSize),
	}
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

		traceOpts := []otlptracehttp.Option{
			otlptracehttp.WithEndpointURL(traceURL.String()),
			otlptracehttp.WithHeaders(headers),
		}
		traceOpts = append(traceOpts, traceHTTPOptions(cfg.settings)...)
		traceExp, err := otlptracehttp.New(ctx, traceOpts...)
		if err != nil {
			return nil, fmt.Errorf("trace exporter %d: %w", i, err)
		}
		tp := sdktrace.NewTracerProvider(
			// Spec 023 harness rule left these at SDK defaults; spec 027 makes
			// them explicit flags for the recommended ISUCON settings.
			sdktrace.WithBatcher(
				&traceExporter{inner: traceExp, stats: &st.traces},
				traceBatchOptions(cfg.settings)...,
			),
			sdktrace.WithResource(res),
		)

		metricOpts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpointURL(metricURL.String()),
			otlpmetrichttp.WithHeaders(headers),
		}
		metricOpts = append(metricOpts, metricHTTPOptions(cfg.settings)...)
		metricExp, err := otlpmetrichttp.New(ctx, metricOpts...)
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

		logOpts := []otlploghttp.Option{
			otlploghttp.WithEndpointURL(logURL.String()),
			otlploghttp.WithHeaders(headers),
		}
		logOpts = append(logOpts, logHTTPOptions(cfg.settings)...)
		logExp, err := otlploghttp.New(ctx, logOpts...)
		if err != nil {
			return nil, fmt.Errorf("log exporter %d: %w", i, err)
		}
		lp := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(
				&logExporter{inner: logExp, stats: &st.logs},
				logBatchOptions(cfg.settings)...,
			)),
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
