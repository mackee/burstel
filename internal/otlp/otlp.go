// Package otlp decodes OTLP/HTTP protobuf export requests into a small,
// DuckDB-friendly projection while keeping the original (decompressed)
// protobuf bytes available to the caller for lossless storage.
//
// The projections intentionally cover the fields that are most useful for
// shot analysis (identity, timing, status, attributes, metric aggregation).
// Anything that is expensive or awkward to model relationally (exemplars,
// nested AnyValue, exponential-histogram bucket maps, unknown fields) is not
// flattened here; it stays recoverable from the raw `otlp_batches.payload`
// BLOB, which is the source of truth.
package otlp

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	colllogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// Signal identifies one of the three core OpenTelemetry signals.
type Signal string

const (
	Traces  Signal = "traces"
	Metrics Signal = "metrics"
	Logs    Signal = "logs"
)

// Valid reports whether s is a supported signal.
func (s Signal) Valid() bool {
	switch s {
	case Traces, Metrics, Logs:
		return true
	default:
		return false
	}
}

// Span is the relational projection of an OTLP span.
type Span struct {
	TraceID                string
	SpanID                 string
	ParentSpanID           string
	TraceState             string
	Name                   string
	Kind                   string
	StartTimeUnixNano      uint64
	EndTimeUnixNano        uint64
	StatusCode             string
	StatusMessage          string
	ServiceName            string
	ScopeName              string
	ScopeVersion           string
	ResourceAttributes     string
	ScopeAttributes        string
	Attributes             string
	Flags                  uint32
	DroppedAttributesCount uint32
	DroppedEventsCount     uint32
	DroppedLinksCount      uint32
	EventsCount            int
	LinksCount             int
}

// Duration returns the span duration in nanoseconds, or 0 if the timestamps are
// inverted.
func (s Span) Duration() uint64 {
	if s.EndTimeUnixNano >= s.StartTimeUnixNano {
		return s.EndTimeUnixNano - s.StartTimeUnixNano
	}
	return 0
}

// LogRecord is the relational projection of an OTLP log record.
type LogRecord struct {
	TimeUnixNano           uint64
	ObservedTimeUnixNano   uint64
	SeverityNumber         string
	SeverityText           string
	Body                   string
	ServiceName            string
	ScopeName              string
	ScopeVersion           string
	ResourceAttributes     string
	ScopeAttributes        string
	Attributes             string
	TraceID                string
	SpanID                 string
	EventName              string
	Flags                  uint32
	DroppedAttributesCount uint32
}

// MetricPoint is the relational projection of a single OTLP metric data point
// (number / histogram / exponential-histogram / summary). Type-specific extras
// that are not modelled as columns are folded into Detail as JSON.
type MetricPoint struct {
	MetricName             string
	MetricType             string
	MetricUnit             string
	MetricDescription      string
	ServiceName            string
	ScopeName              string
	ScopeVersion           string
	ResourceAttributes     string
	ScopeAttributes        string
	Attributes             string
	StartTimeUnixNano      uint64
	TimeUnixNano           uint64
	Flags                  uint32
	ValueDouble            *float64
	ValueInt               *int64
	AggregationTemporality int32
	IsMonotonic            bool
	Count                  *uint64
	Sum                    *float64
	Min                    *float64
	Max                    *float64
	BucketCounts           string
	ExplicitBounds         string
	Detail                 string
}

// Decoded is the result of decoding one OTLP export request.
type Decoded struct {
	Signal          Signal
	RecordCount     int64
	ResourceSummary string
	Spans           []Span
	LogRecords      []LogRecord
	MetricPoints    []MetricPoint
}

// Decode parses raw (already decompressed) OTLP protobuf request bytes for the
// given signal. It returns an error only when the payload is not a valid
// message of the expected official Export*ServiceRequest type.
func Decode(signal Signal, raw []byte) (*Decoded, error) {
	switch signal {
	case Traces:
		return decodeTraces(raw)
	case Metrics:
		return decodeMetrics(raw)
	case Logs:
		return decodeLogs(raw)
	default:
		return nil, fmt.Errorf("otlp: unknown signal %q", signal)
	}
}

func decodeTraces(raw []byte) (*Decoded, error) {
	var req colltrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode traces: %w", err)
	}
	d := &Decoded{Signal: Traces}
	serviceNames := map[string]struct{}{}
	for _, rs := range req.GetResourceSpans() {
		resAttrs := attrsJSON(rs.GetResource().GetAttributes())
		service := serviceName(rs.GetResource().GetAttributes())
		if service != "" {
			serviceNames[service] = struct{}{}
		}
		for _, ss := range rs.GetScopeSpans() {
			scope := ss.GetScope()
			scopeAttrs := attrsJSON(scope.GetAttributes())
			for _, sp := range ss.GetSpans() {
				d.Spans = append(d.Spans, Span{
					TraceID:                hexID(sp.GetTraceId()),
					SpanID:                 hexID(sp.GetSpanId()),
					ParentSpanID:           hexID(sp.GetParentSpanId()),
					TraceState:             sp.GetTraceState(),
					Name:                   sp.GetName(),
					Kind:                   sp.GetKind().String(),
					StartTimeUnixNano:      sp.GetStartTimeUnixNano(),
					EndTimeUnixNano:        sp.GetEndTimeUnixNano(),
					StatusCode:             sp.GetStatus().GetCode().String(),
					StatusMessage:          sp.GetStatus().GetMessage(),
					ServiceName:            service,
					ScopeName:              scope.GetName(),
					ScopeVersion:           scope.GetVersion(),
					ResourceAttributes:     resAttrs,
					ScopeAttributes:        scopeAttrs,
					Attributes:             attrsJSON(sp.GetAttributes()),
					Flags:                  sp.GetFlags(),
					DroppedAttributesCount: sp.GetDroppedAttributesCount(),
					DroppedEventsCount:     sp.GetDroppedEventsCount(),
					DroppedLinksCount:      sp.GetDroppedLinksCount(),
					EventsCount:            len(sp.GetEvents()),
					LinksCount:             len(sp.GetLinks()),
				})
			}
		}
	}
	d.RecordCount = int64(len(d.Spans))
	d.ResourceSummary = resourceSummary(serviceNames, len(req.GetResourceSpans()))
	return d, nil
}

func decodeLogs(raw []byte) (*Decoded, error) {
	var req colllogs.ExportLogsServiceRequest
	if err := proto.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode logs: %w", err)
	}
	d := &Decoded{Signal: Logs}
	serviceNames := map[string]struct{}{}
	for _, rl := range req.GetResourceLogs() {
		resAttrs := attrsJSON(rl.GetResource().GetAttributes())
		service := serviceName(rl.GetResource().GetAttributes())
		if service != "" {
			serviceNames[service] = struct{}{}
		}
		for _, sl := range rl.GetScopeLogs() {
			scope := sl.GetScope()
			scopeAttrs := attrsJSON(scope.GetAttributes())
			for _, lr := range sl.GetLogRecords() {
				d.LogRecords = append(d.LogRecords, LogRecord{
					TimeUnixNano:           lr.GetTimeUnixNano(),
					ObservedTimeUnixNano:   lr.GetObservedTimeUnixNano(),
					SeverityNumber:         lr.GetSeverityNumber().String(),
					SeverityText:           lr.GetSeverityText(),
					Body:                   anyValueJSON(lr.GetBody()),
					ServiceName:            service,
					ScopeName:              scope.GetName(),
					ScopeVersion:           scope.GetVersion(),
					ResourceAttributes:     resAttrs,
					ScopeAttributes:        scopeAttrs,
					Attributes:             attrsJSON(lr.GetAttributes()),
					TraceID:                hexID(lr.GetTraceId()),
					SpanID:                 hexID(lr.GetSpanId()),
					EventName:              lr.GetEventName(),
					Flags:                  lr.GetFlags(),
					DroppedAttributesCount: lr.GetDroppedAttributesCount(),
				})
			}
		}
	}
	d.RecordCount = int64(len(d.LogRecords))
	d.ResourceSummary = resourceSummary(serviceNames, len(req.GetResourceLogs()))
	return d, nil
}

func decodeMetrics(raw []byte) (*Decoded, error) {
	var req collmetrics.ExportMetricsServiceRequest
	if err := proto.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode metrics: %w", err)
	}
	d := &Decoded{Signal: Metrics}
	serviceNames := map[string]struct{}{}
	for _, rm := range req.GetResourceMetrics() {
		resAttrs := attrsJSON(rm.GetResource().GetAttributes())
		service := serviceName(rm.GetResource().GetAttributes())
		if service != "" {
			serviceNames[service] = struct{}{}
		}
		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := attrsJSON(scope.GetAttributes())
			for _, m := range sm.GetMetrics() {
				base := metricBase{
					name:        m.GetName(),
					unit:        m.GetUnit(),
					description: m.GetDescription(),
					service:     service,
					scopeName:   scope.GetName(),
					scopeVer:    scope.GetVersion(),
					resAttrs:    resAttrs,
					scopeAttrs:  scopeAttrs,
				}
				d.MetricPoints = append(d.MetricPoints, projectMetric(base, m)...)
			}
		}
	}
	d.RecordCount = int64(len(d.MetricPoints))
	d.ResourceSummary = resourceSummary(serviceNames, len(req.GetResourceMetrics()))
	return d, nil
}

type metricBase struct {
	name        string
	unit        string
	description string
	service     string
	scopeName   string
	scopeVer    string
	resAttrs    string
	scopeAttrs  string
}

func (b metricBase) point(typ string) MetricPoint {
	return MetricPoint{
		MetricName:         b.name,
		MetricType:         typ,
		MetricUnit:         b.unit,
		MetricDescription:  b.description,
		ServiceName:        b.service,
		ScopeName:          b.scopeName,
		ScopeVersion:       b.scopeVer,
		ResourceAttributes: b.resAttrs,
		ScopeAttributes:    b.scopeAttrs,
	}
}

func projectMetric(b metricBase, m *metricspb.Metric) []MetricPoint {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		pts := make([]MetricPoint, 0, len(data.Gauge.GetDataPoints()))
		for _, np := range data.Gauge.GetDataPoints() {
			p := b.point("gauge")
			applyNumber(&p, np)
			pts = append(pts, p)
		}
		return pts
	case *metricspb.Metric_Sum:
		pts := make([]MetricPoint, 0, len(data.Sum.GetDataPoints()))
		for _, np := range data.Sum.GetDataPoints() {
			p := b.point("sum")
			p.AggregationTemporality = int32(data.Sum.GetAggregationTemporality())
			p.IsMonotonic = data.Sum.GetIsMonotonic()
			applyNumber(&p, np)
			pts = append(pts, p)
		}
		return pts
	case *metricspb.Metric_Histogram:
		pts := make([]MetricPoint, 0, len(data.Histogram.GetDataPoints()))
		for _, hp := range data.Histogram.GetDataPoints() {
			p := b.point("histogram")
			p.AggregationTemporality = int32(data.Histogram.GetAggregationTemporality())
			p.StartTimeUnixNano = hp.GetStartTimeUnixNano()
			p.TimeUnixNano = hp.GetTimeUnixNano()
			p.Flags = hp.GetFlags()
			p.Attributes = attrsJSON(hp.GetAttributes())
			count := hp.GetCount()
			p.Count = &count
			p.Sum = hp.Sum
			p.Min = hp.Min
			p.Max = hp.Max
			p.BucketCounts = jsonUint64s(hp.GetBucketCounts())
			p.ExplicitBounds = jsonFloat64s(hp.GetExplicitBounds())
			p.Detail = detailJSON(map[string]any{"exemplar_count": len(hp.GetExemplars())})
			pts = append(pts, p)
		}
		return pts
	case *metricspb.Metric_ExponentialHistogram:
		pts := make([]MetricPoint, 0, len(data.ExponentialHistogram.GetDataPoints()))
		for _, hp := range data.ExponentialHistogram.GetDataPoints() {
			p := b.point("exponential_histogram")
			p.AggregationTemporality = int32(data.ExponentialHistogram.GetAggregationTemporality())
			p.StartTimeUnixNano = hp.GetStartTimeUnixNano()
			p.TimeUnixNano = hp.GetTimeUnixNano()
			p.Flags = hp.GetFlags()
			p.Attributes = attrsJSON(hp.GetAttributes())
			count := hp.GetCount()
			p.Count = &count
			p.Sum = hp.Sum
			p.Min = hp.Min
			p.Max = hp.Max
			p.Detail = detailJSON(map[string]any{
				"scale":           hp.GetScale(),
				"zero_count":      hp.GetZeroCount(),
				"zero_threshold":  hp.GetZeroThreshold(),
				"positive_counts": hp.GetPositive().GetBucketCounts(),
				"positive_offset": hp.GetPositive().GetOffset(),
				"negative_counts": hp.GetNegative().GetBucketCounts(),
				"negative_offset": hp.GetNegative().GetOffset(),
				"exemplar_count":  len(hp.GetExemplars()),
			})
			pts = append(pts, p)
		}
		return pts
	case *metricspb.Metric_Summary:
		pts := make([]MetricPoint, 0, len(data.Summary.GetDataPoints()))
		for _, sp := range data.Summary.GetDataPoints() {
			p := b.point("summary")
			p.StartTimeUnixNano = sp.GetStartTimeUnixNano()
			p.TimeUnixNano = sp.GetTimeUnixNano()
			p.Flags = sp.GetFlags()
			p.Attributes = attrsJSON(sp.GetAttributes())
			count := sp.GetCount()
			p.Count = &count
			sum := sp.GetSum()
			p.Sum = &sum
			quantiles := make([]map[string]any, 0, len(sp.GetQuantileValues()))
			for _, q := range sp.GetQuantileValues() {
				quantiles = append(quantiles, map[string]any{"quantile": q.GetQuantile(), "value": q.GetValue()})
			}
			p.Detail = detailJSON(map[string]any{"quantile_values": quantiles})
			pts = append(pts, p)
		}
		return pts
	default:
		return []MetricPoint{b.point("unknown")}
	}
}

func applyNumber(p *MetricPoint, np *metricspb.NumberDataPoint) {
	p.StartTimeUnixNano = np.GetStartTimeUnixNano()
	p.TimeUnixNano = np.GetTimeUnixNano()
	p.Flags = np.GetFlags()
	p.Attributes = attrsJSON(np.GetAttributes())
	switch v := np.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		d := v.AsDouble
		p.ValueDouble = &d
	case *metricspb.NumberDataPoint_AsInt:
		i := v.AsInt
		p.ValueInt = &i
	}
}

func resourceSummary(serviceNames map[string]struct{}, resourceCount int) string {
	names := make([]string, 0, len(serviceNames))
	for n := range serviceNames {
		names = append(names, n)
	}
	sort.Strings(names)
	return detailJSON(map[string]any{
		"service_names":  names,
		"resource_count": resourceCount,
	})
}

func serviceName(attrs []*commonv1.KeyValue) string {
	for _, kv := range attrs {
		if kv.GetKey() == "service.name" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func hexID(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

func attrsJSON(attrs []*commonv1.KeyValue) string {
	if len(attrs) == 0 {
		return "{}"
	}
	out := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		out[kv.GetKey()] = anyValue(kv.GetValue())
	}
	return detailJSON(out)
}

func anyValueJSON(v *commonv1.AnyValue) string {
	if v == nil {
		return "null"
	}
	return detailJSON(anyValue(v))
}

func anyValue(v *commonv1.AnyValue) any {
	switch x := v.GetValue().(type) {
	case *commonv1.AnyValue_StringValue:
		return x.StringValue
	case *commonv1.AnyValue_BoolValue:
		return x.BoolValue
	case *commonv1.AnyValue_IntValue:
		return x.IntValue
	case *commonv1.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonv1.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(x.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		vals := x.ArrayValue.GetValues()
		out := make([]any, 0, len(vals))
		for _, item := range vals {
			out = append(out, anyValue(item))
		}
		return out
	case *commonv1.AnyValue_KvlistValue:
		kvs := x.KvlistValue.GetValues()
		out := make(map[string]any, len(kvs))
		for _, kv := range kvs {
			out[kv.GetKey()] = anyValue(kv.GetValue())
		}
		return out
	default:
		return nil
	}
}

func detailJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func jsonUint64s(v []uint64) string {
	if len(v) == 0 {
		return "[]"
	}
	return detailJSON(v)
}

func jsonFloat64s(v []float64) string {
	if len(v) == 0 {
		return "[]"
	}
	return detailJSON(v)
}
