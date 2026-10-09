// Package fixtures builds genuine OTLP protobuf export requests for tests and
// local manual verification. It is not used by the server at runtime.
package fixtures

import (
	colllogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Deterministic IDs shared between the trace and log fixtures so tests can
// assert the log->span correlation.
var (
	TraceID       = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	SpanID        = []byte{0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10, 0x11}
	ChildSpanID   = []byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}
	LinkedTraceID = []byte{0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00}
	LinkedSpanID  = []byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38}
)

// StartNanos is the fixed start time of the root span fixture.
const StartNanos = uint64(1_700_000_000_000_000_000)

func checkoutResource() *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
		strAttr("service.name", "checkout"),
		strAttr("host.name", "web-1"),
		{Key: "process.runtime", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{
			KvlistValue: &commonv1.KeyValueList{Values: []*commonv1.KeyValue{
				strAttr("name", "go"),
				strAttr("version", "1.26"),
			}},
		}}},
	}}
}

func scope() *commonv1.InstrumentationScope {
	return &commonv1.InstrumentationScope{
		Name:    "burstel.instrumentation",
		Version: "0.1.0",
		Attributes: []*commonv1.KeyValue{
			strAttr("scope.kind", "manual"),
		},
	}
}

func strAttr(k, v string) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: k, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}}
}

// TraceExport returns a two-span trace: a slow SERVER root span and a CLIENT
// child that ends in error.
func TraceExport() *colltrace.ExportTraceServiceRequest {
	root := &tracepb.Span{
		TraceId:           TraceID,
		SpanId:            SpanID,
		Name:              "GET /checkout",
		Kind:              tracepb.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: StartNanos,
		EndTimeUnixNano:   StartNanos + 5_000_000_000, // 5s
		Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		Attributes: []*commonv1.KeyValue{
			strAttr("http.method", "GET"),
			strAttr("http.route", "/checkout"),
			{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 200}}},
			{Key: "nested", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{
				KvlistValue: &commonv1.KeyValueList{Values: []*commonv1.KeyValue{
					{Key: "a", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 1}}},
					{Key: "b", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{
						ArrayValue: &commonv1.ArrayValue{Values: []*commonv1.AnyValue{
							{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}},
							{Value: &commonv1.AnyValue_StringValue{StringValue: "x"}},
						}},
					}}},
				}},
			}}},
		},
		Events: []*tracepb.Span_Event{{
			TimeUnixNano: StartNanos + 1_000_000,
			Name:         "cache.miss",
			Attributes:   []*commonv1.KeyValue{strAttr("cache.key", "cart:42")},
		}},
		Links: []*tracepb.Span_Link{{
			TraceId:    LinkedTraceID,
			SpanId:     LinkedSpanID,
			TraceState: "vendor=linked",
		}},
	}
	child := &tracepb.Span{
		TraceId:           TraceID,
		SpanId:            ChildSpanID,
		ParentSpanId:      SpanID,
		Name:              "SELECT orders",
		Kind:              tracepb.Span_SPAN_KIND_CLIENT,
		StartTimeUnixNano: StartNanos + 1_500_000_000,
		EndTimeUnixNano:   StartNanos + 3_800_000_000, // 2.3s
		Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "deadlock detected"},
		Attributes:        []*commonv1.KeyValue{strAttr("db.system", "postgresql")},
	}
	return &colltrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: checkoutResource(),
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope:     scope(),
				Spans:     []*tracepb.Span{root, child},
				SchemaUrl: "https://opentelemetry.io/schemas/1.24.0",
			}},
		}},
	}
}

// MetricsExport returns gauge, sum and histogram metrics with an exemplar.
func MetricsExport() *collmetrics.ExportMetricsServiceRequest {
	gauge := &metricspb.Metric{
		Name: "process.runtime.go.goroutines",
		Unit: "1",
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
			DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: StartNanos + 2_000_000_000,
				Attributes:   []*commonv1.KeyValue{strAttr("service.instance.id", "web-1")},
				Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 42},
			}},
		}},
	}
	sum := &metricspb.Metric{
		Name:        "http.server.requests",
		Description: "total server requests",
		Unit:        "1",
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
			DataPoints: []*metricspb.NumberDataPoint{{
				StartTimeUnixNano: StartNanos,
				TimeUnixNano:      StartNanos + 2_000_000_000,
				Value:             &metricspb.NumberDataPoint_AsInt{AsInt: 1000},
			}},
		}},
	}
	hist := &metricspb.Metric{
		Name: "http.server.request.duration",
		Unit: "s",
		Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricspb.HistogramDataPoint{{
				StartTimeUnixNano: StartNanos,
				TimeUnixNano:      StartNanos + 2_000_000_000,
				Count:             100,
				Sum:               ptr(12.5),
				Min:               ptr(0.001),
				Max:               ptr(1.2),
				BucketCounts:      []uint64{10, 80, 10},
				ExplicitBounds:    []float64{0.1, 1.0},
				Attributes:        []*commonv1.KeyValue{strAttr("http.method", "GET")},
				Exemplars: []*metricspb.Exemplar{{
					TimeUnixNano: StartNanos + 1_900_000_000,
					TraceId:      TraceID,
					SpanId:       SpanID,
					Value:        &metricspb.Exemplar_AsDouble{AsDouble: 0.42},
				}},
			}},
		}},
	}
	return &collmetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: checkoutResource(),
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope:   scope(),
				Metrics: []*metricspb.Metric{gauge, sum, hist},
			}},
		}},
	}
}

// LogsExport returns two logs correlated to the trace fixture.
func LogsExport() *colllogs.ExportLogsServiceRequest {
	info := &logspb.LogRecord{
		TimeUnixNano:         StartNanos + 1_100_000_000,
		ObservedTimeUnixNano: StartNanos + 1_100_001_000,
		SeverityNumber:       logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		SeverityText:         "INFO",
		Body:                 &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "order placed"}},
		TraceId:              TraceID,
		SpanId:               SpanID,
		EventName:            "order.created",
		Attributes: []*commonv1.KeyValue{
			strAttr("order.id", "abc-123"),
			{Key: "amount", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 42.5}}},
		},
	}
	warn := &logspb.LogRecord{
		TimeUnixNano:   StartNanos + 3_000_000_000,
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_WARN,
		SeverityText:   "WARN",
		Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{
			KvlistValue: &commonv1.KeyValueList{Values: []*commonv1.KeyValue{
				strAttr("msg", "slow query"),
				{Key: "elapsed_ms", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 2300}}},
			}},
		}},
		TraceId:    TraceID,
		SpanId:     ChildSpanID,
		Attributes: []*commonv1.KeyValue{strAttr("db.system", "postgresql")},
	}
	return &colllogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: checkoutResource(),
			ScopeLogs: []*logspb.ScopeLogs{{
				Scope:      scope(),
				LogRecords: []*logspb.LogRecord{info, warn},
			}},
		}},
	}
}

func ptr[T any](v T) *T { return &v }
