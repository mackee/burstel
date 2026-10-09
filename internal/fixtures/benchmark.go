package fixtures

import (
	colllogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func resourceFor(service string) *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{strAttr("service.name", service)}}
}

// SpanProfile describes one span in a synthetic benchmark trace.
type SpanProfile struct {
	Name          string
	Kind          tracepb.Span_SpanKind
	Service       string
	StartNanos    uint64
	DurationNanos uint64
	Error         bool
	DBSystem      string
	TraceID       []byte
	SpanID        []byte
	ParentSpanID  []byte
}

// TraceExportProfiled builds a genuine OTLP trace request from a span profile
// list. It is used to synthesise benchmark shots with controlled latency and
// error characteristics for analysis tests.
func TraceExportProfiled(spans []SpanProfile) *colltrace.ExportTraceServiceRequest {
	pbSpans := make([]*tracepb.Span, 0, len(spans))
	for _, s := range spans {
		status := tracepb.Status_STATUS_CODE_OK
		if s.Error {
			status = tracepb.Status_STATUS_CODE_ERROR
		}
		attrs := []*commonv1.KeyValue{}
		if s.DBSystem != "" {
			attrs = append(attrs, strAttr("db.system", s.DBSystem))
		}
		pbSpans = append(pbSpans, &tracepb.Span{
			TraceId:           s.TraceID,
			SpanId:            s.SpanID,
			ParentSpanId:      s.ParentSpanID,
			Name:              s.Name,
			Kind:              s.Kind,
			StartTimeUnixNano: s.StartNanos,
			EndTimeUnixNano:   s.StartNanos + s.DurationNanos,
			Status:            &tracepb.Status{Code: status},
			Attributes:        attrs,
		})
	}
	service := "bench"
	if len(spans) > 0 && spans[0].Service != "" {
		service = spans[0].Service
	}
	return &colltrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: resourceFor(service),
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: scope(),
				Spans: pbSpans,
			}},
		}},
	}
}

// LogProfile describes one log record in a synthetic benchmark shot.
type LogProfile struct {
	TimeNanos    uint64
	SeverityText string
	Body         string
	TraceID      []byte
	SpanID       []byte
	Service      string
}

// LogsExportProfiled builds a genuine OTLP logs request from a profile list.
func LogsExportProfiled(logs []LogProfile) *colllogs.ExportLogsServiceRequest {
	pbLogs := make([]*logspb.LogRecord, 0, len(logs))
	for _, l := range logs {
		severity := logspb.SeverityNumber_SEVERITY_NUMBER_INFO
		switch l.SeverityText {
		case "WARN":
			severity = logspb.SeverityNumber_SEVERITY_NUMBER_WARN
		case "ERROR":
			severity = logspb.SeverityNumber_SEVERITY_NUMBER_ERROR
		case "DEBUG":
			severity = logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG
		}
		pbLogs = append(pbLogs, &logspb.LogRecord{
			TimeUnixNano:   l.TimeNanos,
			SeverityNumber: severity,
			SeverityText:   l.SeverityText,
			Body:           &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: l.Body}},
			TraceId:        l.TraceID,
			SpanId:         l.SpanID,
		})
	}
	service := "bench"
	if len(logs) > 0 && logs[0].Service != "" {
		service = logs[0].Service
	}
	return &colllogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: resourceFor(service),
			ScopeLogs: []*logspb.ScopeLogs{{
				Scope:      scope(),
				LogRecords: pbLogs,
			}},
		}},
	}
}
