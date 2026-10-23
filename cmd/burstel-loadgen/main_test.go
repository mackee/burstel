package main

import (
	"testing"
	"time"
)

func TestLatencyPercentiles(t *testing.T) {
	var buf latencyBuffer
	for i := 1; i <= 100; i++ {
		buf.add(time.Duration(i) * time.Millisecond)
	}
	p50, p95, p99 := buf.percentiles()
	if p50 != 50 {
		t.Fatalf("p50 = %v, want 50ms", p50)
	}
	if p95 != 95 {
		t.Fatalf("p95 = %v, want 95ms", p95)
	}
	if p99 != 99 {
		t.Fatalf("p99 = %v, want 99ms", p99)
	}

	var empty latencyBuffer
	if a, b, c := empty.percentiles(); a != 0 || b != 0 || c != 0 {
		t.Fatalf("empty percentiles = %v/%v/%v, want zeroes", a, b, c)
	}
}

func TestMetricPointCountNil(t *testing.T) {
	if got := metricPointCount(nil); got != 0 {
		t.Fatalf("metricPointCount(nil) = %d, want 0", got)
	}
}

func TestRandomHex(t *testing.T) {
	got := randomHex(4)
	if len(got) != 8 {
		t.Fatalf("randomHex(4) = %q, want 8 hex chars", got)
	}
	if randomHex(4) == "" {
		t.Fatal("randomHex returned an empty string")
	}
}
