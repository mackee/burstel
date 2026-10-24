package main

import (
	"strings"
	"testing"
	"time"
)

// TestEffectiveClientSettingsDefaults proves the flags default to the stock SDK
// settings (BSP/log batch 512, queue 2048, no compression).
func TestEffectiveClientSettingsDefaults(t *testing.T) {
	got, err := effectiveClientSettings("none", 0, 0)
	if err != nil {
		t.Fatalf("effectiveClientSettings: %v", err)
	}
	want := clientSettings{Compression: "none", BatchSize: 512, QueueSize: 2048}
	if got != want {
		t.Fatalf("settings = %+v, want %+v", got, want)
	}

	// The empty string is the SDK default as well.
	got, err = effectiveClientSettings("", 0, 0)
	if err != nil {
		t.Fatalf("effectiveClientSettings(empty): %v", err)
	}
	if got != want {
		t.Fatalf("settings = %+v, want %+v", got, want)
	}
}

func TestEffectiveClientSettingsRecommended(t *testing.T) {
	got, err := effectiveClientSettings("gzip", 8192, 131072)
	if err != nil {
		t.Fatalf("effectiveClientSettings: %v", err)
	}
	want := clientSettings{Compression: "gzip", BatchSize: 8192, QueueSize: 131072}
	if got != want {
		t.Fatalf("settings = %+v, want %+v", got, want)
	}
}

func TestEffectiveClientSettingsRejectsBadInput(t *testing.T) {
	cases := []struct {
		name        string
		compression string
		batch       int
		queue       int
		want        string
	}{
		{name: "compression", compression: "zstd", want: "-compression must be gzip or none"},
		{name: "negative batch", compression: "none", batch: -1, want: "-batch-size must be >= 0"},
		{name: "negative queue", compression: "none", queue: -1, want: "-queue-size must be >= 0"},
		{name: "batch above queue", compression: "none", batch: 4096, queue: 2048, want: "must not exceed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := effectiveClientSettings(tc.compression, tc.batch, tc.queue)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseFlagsWiresClientSettings(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-token", "test-token",
		"-compression", "gzip",
		"-batch-size", "8192",
		"-queue-size", "131072",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.compression != "gzip" || cfg.batchSize != 8192 || cfg.queueSize != 131072 {
		t.Fatalf("raw flags = %q/%d/%d", cfg.compression, cfg.batchSize, cfg.queueSize)
	}
	want := clientSettings{Compression: "gzip", BatchSize: 8192, QueueSize: 131072}
	if cfg.settings != want {
		t.Fatalf("settings = %+v, want %+v", cfg.settings, want)
	}
}

func TestParseFlagsDefaultsToSDK(t *testing.T) {
	cfg, err := parseFlags([]string{"-token", "test-token"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	want := clientSettings{Compression: "none", BatchSize: 512, QueueSize: 2048}
	if cfg.settings != want {
		t.Fatalf("settings = %+v, want %+v", cfg.settings, want)
	}
}

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
