package main

import (
	"strings"
	"testing"
	"time"
)

func env(values map[string]string) envLookup {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// TestResolveClientSettingsDefaults proves an entirely unset run uses the stock
// SDK settings (BSP/log batch 512, queue 2048, no compression).
func TestResolveClientSettingsDefaults(t *testing.T) {
	got, sources, err := resolveClientSettings(clientSettingFlags{}, env(nil))
	if err != nil {
		t.Fatalf("resolveClientSettings: %v", err)
	}
	want := clientSettings{Compression: "none", BatchSize: 512, QueueSize: 2048}
	if got != want {
		t.Fatalf("settings = %+v, want %+v", got, want)
	}
	wantSources := clientSettingsSources{Compression: "default", BatchSize: "default", QueueSize: "default"}
	if sources != wantSources {
		t.Fatalf("sources = %+v, want %+v", sources, wantSources)
	}
}

// TestResolveClientSettingsEnvWhenFlagsUnset proves the standard OTEL_* env vars
// are honoured when the flags are absent (spec 032).
func TestResolveClientSettingsEnvWhenFlagsUnset(t *testing.T) {
	got, sources, err := resolveClientSettings(clientSettingFlags{}, env(map[string]string{
		"OTEL_EXPORTER_OTLP_COMPRESSION": "gzip",
		"OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "8192",
		"OTEL_BSP_MAX_QUEUE_SIZE":        "131072",
	}))
	if err != nil {
		t.Fatalf("resolveClientSettings: %v", err)
	}
	want := clientSettings{Compression: "gzip", BatchSize: 8192, QueueSize: 131072}
	if got != want {
		t.Fatalf("settings = %+v, want %+v", got, want)
	}
	wantSources := clientSettingsSources{Compression: "env", BatchSize: "env", QueueSize: "env"}
	if sources != wantSources {
		t.Fatalf("sources = %+v, want %+v", sources, wantSources)
	}
}

// TestResolveClientSettingsFlagWinsOverEnv proves an explicit flag beats the
// corresponding env var.
func TestResolveClientSettingsFlagWinsOverEnv(t *testing.T) {
	got, sources, err := resolveClientSettings(clientSettingFlags{
		Compression:    "none",
		BatchSize:      4096,
		QueueSize:      65536,
		CompressionSet: true,
		BatchSizeSet:   true,
		QueueSizeSet:   true,
	}, env(map[string]string{
		"OTEL_EXPORTER_OTLP_COMPRESSION": "gzip",
		"OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "8192",
		"OTEL_BSP_MAX_QUEUE_SIZE":        "131072",
	}))
	if err != nil {
		t.Fatalf("resolveClientSettings: %v", err)
	}
	want := clientSettings{Compression: "none", BatchSize: 4096, QueueSize: 65536}
	if got != want {
		t.Fatalf("settings = %+v, want %+v", got, want)
	}
	wantSources := clientSettingsSources{Compression: "flag", BatchSize: "flag", QueueSize: "flag"}
	if sources != wantSources {
		t.Fatalf("sources = %+v, want %+v", sources, wantSources)
	}
}

func TestResolveClientSettingsRejectsBadInput(t *testing.T) {
	cases := []struct {
		name  string
		flags clientSettingFlags
		env   map[string]string
		want  string
	}{
		{name: "compression", flags: clientSettingFlags{Compression: "zstd", CompressionSet: true}, want: "-compression must be gzip or none"},
		{name: "negative batch", flags: clientSettingFlags{BatchSize: -1, BatchSizeSet: true}, want: "-batch-size must be >= 0"},
		{name: "negative queue", flags: clientSettingFlags{QueueSize: -1, QueueSizeSet: true}, want: "-queue-size must be >= 0"},
		{name: "batch above queue", flags: clientSettingFlags{BatchSize: 4096, BatchSizeSet: true, QueueSize: 2048, QueueSizeSet: true}, want: "must not exceed"},
		{name: "bad env int", env: map[string]string{"OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "nope"}, want: "OTEL_BSP_MAX_EXPORT_BATCH_SIZE must be a positive integer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := resolveClientSettings(tc.flags, env(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseFlagsWiresClientSettings(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")
	t.Setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "1")
	t.Setenv("OTEL_BSP_MAX_QUEUE_SIZE", "2")
	cfg, err := parseFlags([]string{
		"-token", "test-token",
		"-compression", "none",
		"-batch-size", "8192",
		"-queue-size", "131072",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.compression != "none" || cfg.batchSize != 8192 || cfg.queueSize != 131072 {
		t.Fatalf("raw flags = %q/%d/%d", cfg.compression, cfg.batchSize, cfg.queueSize)
	}
	want := clientSettings{Compression: "none", BatchSize: 8192, QueueSize: 131072}
	if cfg.settings != want {
		t.Fatalf("settings = %+v, want %+v", cfg.settings, want)
	}
	wantSources := clientSettingsSources{Compression: "flag", BatchSize: "flag", QueueSize: "flag"}
	if cfg.sources != wantSources {
		t.Fatalf("sources = %+v, want %+v", cfg.sources, wantSources)
	}
}

func TestParseFlagsDefaultsToSDK(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "")
	t.Setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "")
	t.Setenv("OTEL_BSP_MAX_QUEUE_SIZE", "")
	cfg, err := parseFlags([]string{"-token", "test-token"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	want := clientSettings{Compression: "none", BatchSize: 512, QueueSize: 2048}
	if cfg.settings != want {
		t.Fatalf("settings = %+v, want %+v", cfg.settings, want)
	}
	wantSources := clientSettingsSources{Compression: "default", BatchSize: "default", QueueSize: "default"}
	if cfg.sources != wantSources {
		t.Fatalf("sources = %+v, want %+v", cfg.sources, wantSources)
	}
}

func TestParseFlagsAppliesOTELEnvWhenUnset(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")
	t.Setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "8192")
	t.Setenv("OTEL_BSP_MAX_QUEUE_SIZE", "131072")
	cfg, err := parseFlags([]string{"-token", "test-token"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	want := clientSettings{Compression: "gzip", BatchSize: 8192, QueueSize: 131072}
	if cfg.settings != want {
		t.Fatalf("settings = %+v, want %+v", cfg.settings, want)
	}
	wantSources := clientSettingsSources{Compression: "env", BatchSize: "env", QueueSize: "env"}
	if cfg.sources != wantSources {
		t.Fatalf("sources = %+v, want %+v", cfg.sources, wantSources)
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
