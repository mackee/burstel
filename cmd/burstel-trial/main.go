// Command burstel-trial generates a bounded, instrumented ISUCON-like HTTP
// workload and exports it to a standalone burstel recorder using the real
// OpenTelemetry Go SDK (traces + metrics + logs over OTLP/HTTP protobuf).
//
// It runs the "application under test" in-process (an HTTP server on
// 127.0.0.1) and drives bounded load against itself. The application exposes
// two normal routes (/api/items/{id} with an N+1 simulated-DB child-span
// pattern, /api/summary) plus a deliberate failure route (/api/error). The
// simulated database is labelled as such in span attributes and is not a real
// database.
//
// The recorder lifecycle (start/stop) is orchestrated by scripts/local-trial.sh;
// this program only runs the workload and force-flushes all three signals
// before exiting.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const buildVersion = "0.4.0-trial"

type trialConfig struct {
	burstelURL      string
	listenAddr      string
	duration        time.Duration
	rate            float64
	workers         int
	mode            string
	dbQueries       int
	dbLatency       time.Duration
	errorRate       float64
	maxErrorRate    float64
	metricsInterval time.Duration
	reportPath      string
	serviceName     string
}

func main() {
	var (
		burstelURL   = flag.String("burstel-url", "http://127.0.0.1:4318", "burstel base URL")
		listenAddr   = flag.String("listen", "127.0.0.1:0", "application listen address (loopback)")
		duration     = flag.Duration("duration", 60*time.Second, "workload duration")
		rate         = flag.Float64("rate", 15, "target application requests per second")
		workers      = flag.Int("workers", 8, "load generator worker goroutines")
		mode         = flag.String("mode", "baseline", "baseline or regressed")
		dbQueries    = flag.Int("db-queries", -1, "simulated DB queries (N+1 fan-out) per items request")
		dbLatency    = flag.Duration("db-latency", 0, "simulated latency per DB query")
		errorRate    = flag.Float64("error-rate", -1, "fraction of requests sent to the deliberate error route")
		maxErrorRate = flag.Float64("max-error-rate", 0.5, "trip the load circuit breaker above this unexpected error rate")
		metricsEvery = flag.Duration("metrics-interval", 5*time.Second, "OTel metric export interval")
		reportPath   = flag.String("report", "", "write the JSON workload report to this path")
		serviceName  = flag.String("service-name", "burstel-trial-app", "OTel service.name for the app")
	)
	flag.Parse()

	cfg := trialConfig{
		burstelURL:      *burstelURL,
		listenAddr:      *listenAddr,
		duration:        *duration,
		rate:            *rate,
		workers:         *workers,
		mode:            *mode,
		errorRate:       0.02,
		maxErrorRate:    *maxErrorRate,
		metricsInterval: *metricsEvery,
		reportPath:      *reportPath,
		serviceName:     *serviceName,
		dbQueries:       4,
		dbLatency:       1 * time.Millisecond,
	}
	switch cfg.mode {
	case "baseline":
		// defaults above
	case "regressed":
		cfg.dbQueries = 16
		cfg.dbLatency = 5 * time.Millisecond
		cfg.errorRate = 0.05
	default:
		fmt.Fprintf(os.Stderr, "burstel-trial: -mode must be baseline or regressed, got %q\n", cfg.mode)
		os.Exit(2)
	}
	if *dbQueries >= 0 {
		cfg.dbQueries = *dbQueries
	}
	if *dbLatency > 0 {
		cfg.dbLatency = *dbLatency
	}
	if *errorRate >= 0 {
		cfg.errorRate = *errorRate
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tele, err := setupTelemetry(ctx, cfg)
	if err != nil {
		logger.Error("setup telemetry", "error", err)
		os.Exit(1)
	}
	meter := tele.meterProvider.Meter("burstel-trial/app")
	tracer := tele.tracerProvider.Tracer("burstel-trial/app")
	otellogger := tele.loggerProvider.Logger("burstel-trial/app")

	// otelhttp.NewHandler resolves its tracer/meter from the global providers,
	// so install the SDK providers we just built. This is what makes the
	// otelhttp-managed SERVER spans and HTTP metrics flow to burstel.
	otel.SetTracerProvider(tele.tracerProvider)
	otel.SetMeterProvider(tele.meterProvider)

	m, err := newAppMetrics(meter)
	if err != nil {
		logger.Error("create metrics", "error", err)
		os.Exit(1)
	}
	app := newTrialApp(cfg, m, tracer, otellogger)

	ln, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		logger.Error("listen", "addr", cfg.listenAddr, "error", err)
		os.Exit(1)
	}
	srv := &http.Server{
		Handler:           app.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("app server", "error", err)
		}
	}()
	baseURL := "http://" + ln.Addr().String()
	logger.Info("workload starting",
		"mode", cfg.mode, "app", baseURL, "burstel", cfg.burstelURL,
		"duration", cfg.duration.String(), "rate", cfg.rate,
		"db_queries", cfg.dbQueries, "db_latency", cfg.dbLatency.String())

	report := runLoad(ctx, baseURL, cfg, app)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = srv.Shutdown(shutdownCtx)
	cancel()

	// Force flush every signal, then close the providers. burstel's stop runs
	// after this process exits, so the recorder sees a fully drained exporter.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 60*time.Second)
	tele.shutdown(flushCtx)
	flushCancel()

	report.Emitted = emittedCounts{
		ServerSpans:        app.serverSpans.Load(),
		DBSpans:            app.dbSpans.Load(),
		LogRecords:         app.logRecords.Load(),
		MetricMeasurements: app.metricRecords.Load(),
	}
	report.Export = tele.stats.snapshot()
	report.Notes = append(report.Notes,
		"emitted.* counts are known by construction; stored counts are read back from the frozen DuckDB by cmd/burstel-inspect",
		"metric_measurements counts SDK measurements recorded, not stored metric_points rows (cumulative re-exports inflate the latter)",
		"the simulated database is in-process; db.system="+simulatedDBSystem,
	)

	raw, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(raw))
	if cfg.reportPath != "" {
		if err := os.WriteFile(cfg.reportPath, raw, 0o644); err != nil {
			logger.Error("write report", "path", cfg.reportPath, "error", err)
			os.Exit(1)
		}
	}
	logger.Info("workload finished",
		"attempted", report.Attempted, "success", report.Success,
		"deliberate_errors", report.DeliberateErrors, "transport_errors", report.TransportErrors,
		"p95_ms", report.Latency.P95, "exports", report.Export.Total)
}

func newAppMetrics(meter metric.Meter) (appMetrics, error) {
	requests, err := meter.Int64Counter("burstel.trial.http.server.requests",
		metric.WithDescription("Application requests handled, by route and status."))
	if err != nil {
		return appMetrics{}, err
	}
	requestDuration, err := meter.Float64Histogram("burstel.trial.http.server.duration",
		metric.WithDescription("Application request duration in milliseconds."),
		metric.WithUnit("ms"))
	if err != nil {
		return appMetrics{}, err
	}
	dbQueries, err := meter.Int64Counter("burstel.trial.db.queries",
		metric.WithDescription("Simulated database queries executed (N+1 fan-out)."))
	if err != nil {
		return appMetrics{}, err
	}
	dbDuration, err := meter.Float64Histogram("burstel.trial.db.duration",
		metric.WithDescription("Simulated database query duration in milliseconds."),
		metric.WithUnit("ms"))
	if err != nil {
		return appMetrics{}, err
	}
	return appMetrics{
		requests:        requests,
		requestDuration: requestDuration,
		dbQueries:       dbQueries,
		dbDuration:      dbDuration,
	}, nil
}
