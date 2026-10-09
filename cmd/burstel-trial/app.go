package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// simulatedDBSystem labels the in-process fake data store. It is deliberately
// NOT a real database: this trial measures burstel's ability to capture and
// analyse nested CLIENT spans and N+1 patterns, not real SQLite/DuckDB engine
// behaviour. The trial report states this explicitly.
const simulatedDBSystem = "burstel.simulated-db"

type appMetrics struct {
	requests        metric.Int64Counter
	requestDuration metric.Float64Histogram
	dbQueries       metric.Int64Counter
	dbDuration      metric.Float64Histogram
}

type trialApp struct {
	cfg    trialConfig
	tracer trace.Tracer
	logger otellog.Logger
	m      appMetrics

	// Emitted counts are known by construction; DuckDB counts are read back by
	// cmd/burstel-inspect so the report can compare the two.
	serverSpans   atomic.Int64
	dbSpans       atomic.Int64
	logRecords    atomic.Int64
	metricRecords atomic.Int64 // number of metric measurements recorded
}

func newTrialApp(cfg trialConfig, m appMetrics, tracer trace.Tracer, logger otellog.Logger) *trialApp {
	return &trialApp{cfg: cfg, tracer: tracer, logger: logger, m: m}
}

func (a *trialApp) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /api/items/{id}", a.handleItems)
	mux.HandleFunc("GET /api/summary", a.handleSummary)
	mux.HandleFunc("GET /api/error", a.handleError)
	return otelhttp.NewHandler(mux, "http.server",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			// Go 1.22+ Request.Pattern already includes the method
			// (e.g. "GET /api/items/{id}").
			if r.Pattern != "" {
				return r.Pattern
			}
			return r.Method + " " + r.URL.Path
		}),
	)
}

func (a *trialApp) handleItems(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	a.serverSpans.Add(1)
	const route = "GET /api/items/{id}"

	id := r.PathValue("id")
	items := make([]map[string]any, 0, a.cfg.dbQueries)
	for i := 0; i < a.cfg.dbQueries; i++ {
		item, err := a.dbQuery(ctx, id, i)
		if err != nil {
			a.observeError(ctx, route, start, err)
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, item)
	}
	a.emitLog(ctx, otellog.SeverityInfo, "items fetched",
		attribute.String("http.route", route),
		attribute.Int("items.count", len(items)),
	)
	writeJSON(w, http.StatusOK, map[string]any{"route": route, "count": len(items), "items": items})
	a.observe(ctx, route, http.StatusOK, start)
}

func (a *trialApp) handleSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	a.serverSpans.Add(1)
	const route = "GET /api/summary"

	// Aggregate route: a couple of simulated DB round trips.
	for i := 0; i < 2; i++ {
		if _, err := a.dbQuery(ctx, "summary", i); err != nil {
			a.observeError(ctx, route, start, err)
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}
	a.emitLog(ctx, otellog.SeverityInfo, "summary computed",
		attribute.String("http.route", route),
	)
	writeJSON(w, http.StatusOK, map[string]any{"route": route, "ok": true})
	a.observe(ctx, route, http.StatusOK, start)
}

// handleError always fails deliberately: it produces an ERROR SERVER span, a
// failing nested CLIENT span and an ERROR-severity correlated log.
func (a *trialApp) handleError(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	a.serverSpans.Add(1)
	const route = "GET /api/error"

	simErr := errors.New("simulated database constraint violation")
	serverSpan := trace.SpanFromContext(ctx)

	dbCtx, span := a.tracer.Start(ctx, "db.query failing",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", simulatedDBSystem),
			attribute.String("db.operation.name", "INSERT"),
			attribute.Bool("db.simulated", true),
		))
	a.dbSpans.Add(1)
	a.m.dbQueries.Add(dbCtx, 1)
	a.metricRecords.Add(2)
	time.Sleep(a.cfg.dbLatency)
	span.RecordError(simErr)
	span.SetStatus(codes.Error, simErr.Error())
	span.End()

	serverSpan.RecordError(simErr)
	serverSpan.SetStatus(codes.Error, simErr.Error())

	a.emitLog(ctx, otellog.SeverityError, "simulated database failure",
		attribute.String("http.route", route),
		attribute.String("error.type", "constraint_violation"),
	)
	a.observe(ctx, route, http.StatusInternalServerError, start)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": simErr.Error()})
}

// dbQuery runs one simulated DB round trip inside a nested CLIENT span. The
// N+1 shape (one span per loop iteration) is what the db_operations analysis
// section is expected to surface.
func (a *trialApp) dbQuery(ctx context.Context, id string, i int) (map[string]any, error) {
	ctx, span := a.tracer.Start(ctx, "db.query items",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", simulatedDBSystem),
			attribute.String("db.operation.name", "SELECT"),
			attribute.Bool("db.simulated", true),
			attribute.String("db.collection.name", "items"),
			attribute.String("db.item.parent_id", id),
			attribute.Int("db.item.index", i),
		))
	defer span.End()
	a.dbSpans.Add(1)
	a.m.dbQueries.Add(ctx, 1)

	begin := time.Now()
	time.Sleep(a.cfg.dbLatency)
	durMs := float64(time.Since(begin).Microseconds()) / 1000.0
	a.m.dbDuration.Record(ctx, durMs)
	a.metricRecords.Add(2) // db query counter + db duration histogram
	span.SetAttributes(attribute.Float64("db.duration_ms", durMs))
	return map[string]any{"id": fmt.Sprintf("%s-%d", id, i), "name": fmt.Sprintf("item-%d", i)}, nil
}

// observe records the request outcome on the current span and on the metrics
// instruments. It is called after the response status is known.
func (a *trialApp) observe(ctx context.Context, route string, status int, start time.Time) {
	durMs := float64(time.Since(start).Microseconds()) / 1000.0
	attrs := metric.WithAttributes(
		attribute.String("http.route", route),
		attribute.Int("http.response.status_code", status),
	)
	a.m.requests.Add(ctx, 1, attrs)
	a.m.requestDuration.Record(ctx, durMs, attrs)
	a.metricRecords.Add(2) // requests counter + request duration histogram
}

func (a *trialApp) observeError(ctx context.Context, route string, start time.Time, err error) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	a.emitLog(ctx, otellog.SeverityError, "request failed",
		attribute.String("http.route", route),
		attribute.String("error.type", "handler_error"),
	)
	a.observe(ctx, route, http.StatusInternalServerError, start)
}

// emitLog sends a real OTLP log record through the OTel logs SDK. The SDK
// attaches the active span's trace/span id from ctx, so logs correlate to
// traces without bespoke plumbing.
func (a *trialApp) emitLog(ctx context.Context, severity otellog.Severity, msg string, attrs ...attribute.KeyValue) {
	var rec otellog.Record
	rec.SetTimestamp(time.Now())
	rec.SetObservedTimestamp(time.Now())
	rec.SetSeverity(severity)
	rec.SetSeverityText(severity.String())
	rec.SetBody(attribute.StringValue(msg))
	rec.AddAttributes(attrs...)
	a.logger.Emit(ctx, rec)
	a.logRecords.Add(1)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
