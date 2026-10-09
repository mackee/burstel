package main

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// telemetry bundles the three OTel SDK providers used by the workload. All
// signals are exported over OTLP/HTTP protobuf to burstel, through a shared
// counting HTTP client so the trial can report real exporter request counts and
// non-200 responses (backpressure/retries) rather than assuming zero loss.
type telemetry struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	loggerProvider *sdklog.LoggerProvider
	stats          *exportStats
}

// exportSnapshot is a point-in-time view of OTLP export traffic.
type exportSnapshot struct {
	RequestsByPath  map[string]int64            `json:"requests_by_path"`
	StatusByPath    map[string]map[string]int64 `json:"status_by_path"`
	TransportErrors int64                       `json:"transport_errors"`
	Total           int64                       `json:"total_requests"`
}

// exportStats counts OTLP HTTP round trips per signal path. Retries issued by
// the OTel exporters are new round trips and are therefore counted.
type exportStats struct {
	mu              sync.Mutex
	requestsByPath  map[string]int64
	statusByPath    map[string]map[string]int64
	transportErrors int64
}

type countingTransport struct {
	base  http.RoundTripper
	stats *exportStats
}

func (c *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.base.RoundTrip(req)
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	path := req.URL.Path
	c.stats.requestsByPath[path]++
	if err != nil {
		c.stats.transportErrors++
		return resp, err
	}
	if c.stats.statusByPath[path] == nil {
		c.stats.statusByPath[path] = map[string]int64{}
	}
	c.stats.statusByPath[path][http.StatusText(resp.StatusCode)]++
	return resp, err
}

func (s *exportStats) snapshot() exportSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := exportSnapshot{
		RequestsByPath:  map[string]int64{},
		StatusByPath:    map[string]map[string]int64{},
		TransportErrors: s.transportErrors,
	}
	for k, v := range s.requestsByPath {
		out.RequestsByPath[k] = v
		out.Total += v
	}
	for path, codes := range s.statusByPath {
		out.StatusByPath[path] = map[string]int64{}
		for code, n := range codes {
			out.StatusByPath[path][code] = n
		}
	}
	return out
}

func setupTelemetry(ctx context.Context, cfg trialConfig) (*telemetry, error) {
	stats := &exportStats{
		requestsByPath: map[string]int64{},
		statusByPath:   map[string]map[string]int64{},
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &countingTransport{base: http.DefaultTransport, stats: stats},
	}

	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(cfg.burstelURL+"/v1/traces"),
		otlptracehttp.WithHTTPClient(client),
	)
	if err != nil {
		return nil, err
	}
	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(cfg.burstelURL+"/v1/metrics"),
		otlpmetrichttp.WithHTTPClient(client),
	)
	if err != nil {
		return nil, err
	}
	logExporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(cfg.burstelURL+"/v1/logs"),
		otlploghttp.WithHTTPClient(client),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		attribute.String("service.name", cfg.serviceName),
		attribute.String("service.version", buildVersion),
		attribute.String("deployment.environment.name", cfg.mode),
		attribute.String("burstel.trial.mode", cfg.mode),
		attribute.String("burstel.trial.simulated_db", "true"),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		// No intentional sampling: every span the SDK creates is exported.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(traceExporter,
			sdktrace.WithMaxQueueSize(32768),
			sdktrace.WithMaxExportBatchSize(512),
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithExportTimeout(25*time.Second),
		),
	)
	reader := sdkmetric.NewPeriodicReader(metricExporter,
		sdkmetric.WithInterval(cfg.metricsInterval),
		sdkmetric.WithTimeout(25*time.Second),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	)
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter,
			sdklog.WithExportInterval(2*time.Second),
			sdklog.WithMaxQueueSize(32768),
			sdklog.WithExportMaxBatchSize(512),
			sdklog.WithExportTimeout(25*time.Second),
		)),
	)
	return &telemetry{
		tracerProvider: tp,
		meterProvider:  mp,
		loggerProvider: lp,
		stats:          stats,
	}, nil
}

// shutdown flushes every pending signal and then closes the providers. It is
// called exactly once, after the workload has stopped, so that burstel's stop
// sees a fully drained exporter.
func (t *telemetry) shutdown(ctx context.Context) {
	_ = t.tracerProvider.ForceFlush(ctx)
	_ = t.meterProvider.ForceFlush(ctx)
	_ = t.loggerProvider.ForceFlush(ctx)
	_ = t.tracerProvider.Shutdown(ctx)
	_ = t.meterProvider.Shutdown(ctx)
	_ = t.loggerProvider.Shutdown(ctx)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
