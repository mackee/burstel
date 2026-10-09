package otlp_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
)

func TestDecodeTraces(t *testing.T) {
	raw, _ := proto.Marshal(fixtures.TraceExport())
	d, err := otlp.Decode(otlp.Traces, raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.Signal != otlp.Traces {
		t.Fatalf("signal = %q", d.Signal)
	}
	if d.RecordCount != 2 || len(d.Spans) != 2 {
		t.Fatalf("record count = %d spans = %d", d.RecordCount, len(d.Spans))
	}
	root := d.Spans[0]
	if root.Name != "GET /checkout" || root.Kind != "SPAN_KIND_SERVER" {
		t.Fatalf("root span = %+v", root)
	}
	if root.StatusCode != "STATUS_CODE_OK" {
		t.Fatalf("root status = %q", root.StatusCode)
	}
	if root.ServiceName != "checkout" {
		t.Fatalf("service name = %q", root.ServiceName)
	}
	if root.TraceID != "0102030405060708090a0b0c0d0e0f10" {
		t.Fatalf("trace id = %q", root.TraceID)
	}
	if root.Duration() != 5_000_000_000 {
		t.Fatalf("root duration = %d", root.Duration())
	}
	if len(root.Attributes) == 0 || !strings.Contains(root.Attributes, "http.method") {
		t.Fatalf("root attributes = %q", root.Attributes)
	}
	if root.EventsCount != 1 || root.LinksCount != 1 {
		t.Fatalf("events/links = %d/%d", root.EventsCount, root.LinksCount)
	}
	child := d.Spans[1]
	if child.ParentSpanID != root.SpanID {
		t.Fatalf("child parent = %q, want %q", child.ParentSpanID, root.SpanID)
	}
	if child.StatusCode != "STATUS_CODE_ERROR" || child.StatusMessage != "deadlock detected" {
		t.Fatalf("child status = %+v", child)
	}
	if !strings.Contains(d.ResourceSummary, "checkout") {
		t.Fatalf("resource summary = %q", d.ResourceSummary)
	}
}

func TestDecodeMetrics(t *testing.T) {
	raw, _ := proto.Marshal(fixtures.MetricsExport())
	d, err := otlp.Decode(otlp.Metrics, raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.RecordCount != 3 || len(d.MetricPoints) != 3 {
		t.Fatalf("record count = %d points = %d", d.RecordCount, len(d.MetricPoints))
	}
	byName := map[string]otlp.MetricPoint{}
	for _, mp := range d.MetricPoints {
		byName[mp.MetricName] = mp
	}
	if got := byName["process.runtime.go.goroutines"]; got.MetricType != "gauge" || got.ValueInt == nil || *got.ValueInt != 42 {
		t.Fatalf("gauge = %+v", got)
	}
	sum := byName["http.server.requests"]
	if sum.MetricType != "sum" || !sum.IsMonotonic || sum.AggregationTemporality != 2 {
		t.Fatalf("sum = %+v", sum)
	}
	hist := byName["http.server.request.duration"]
	if hist.MetricType != "histogram" || hist.Count == nil || *hist.Count != 100 {
		t.Fatalf("histogram = %+v", hist)
	}
	if hist.Sum == nil || *hist.Sum != 12.5 {
		t.Fatalf("histogram sum = %v", hist.Sum)
	}
	if hist.BucketCounts != "[10,80,10]" {
		t.Fatalf("bucket counts = %q", hist.BucketCounts)
	}
	if !strings.Contains(hist.Detail, "exemplar_count") {
		t.Fatalf("histogram detail = %q", hist.Detail)
	}
}

func TestDecodeLogs(t *testing.T) {
	raw, _ := proto.Marshal(fixtures.LogsExport())
	d, err := otlp.Decode(otlp.Logs, raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.RecordCount != 2 || len(d.LogRecords) != 2 {
		t.Fatalf("record count = %d records = %d", d.RecordCount, len(d.LogRecords))
	}
	first := d.LogRecords[0]
	if first.SeverityNumber != "SEVERITY_NUMBER_INFO" || first.SeverityText != "INFO" {
		t.Fatalf("first log severity = %+v", first)
	}
	if first.TraceID != "0102030405060708090a0b0c0d0e0f10" || first.SpanID == "" {
		t.Fatalf("first log correlation = %+v", first)
	}
	if first.Body != `"order placed"` {
		t.Fatalf("first log body = %q", first.Body)
	}
	second := d.LogRecords[1]
	if second.SeverityNumber != "SEVERITY_NUMBER_WARN" {
		t.Fatalf("second log severity = %+v", second)
	}
	if !strings.Contains(second.Body, "slow query") {
		t.Fatalf("second log body = %q", second.Body)
	}
}

func TestDecodeMalformed(t *testing.T) {
	if _, err := otlp.Decode(otlp.Traces, []byte{0x08}); err == nil {
		t.Fatal("expected error for truncated protobuf")
	}
	if _, err := otlp.Decode(otlp.Metrics, []byte("this is not protobuf")); err == nil {
		t.Fatal("expected error for non-protobuf bytes")
	}
	if _, err := otlp.Decode(otlp.Signal("bogus"), nil); err == nil {
		t.Fatal("expected error for unknown signal")
	}
}
