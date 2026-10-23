// Package config holds burstel process configuration. Values come from command
// line flags (see cmd/burstel) with environment-variable defaults.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Default values used when neither flag nor environment overrides them.
const (
	DefaultAddr            = ":4318"
	DefaultDataDir         = "./data"
	DefaultMaxRequestBytes = 64 << 20 // 64 MiB decompressed
	DefaultMaxInflight     = 8
	DefaultWriteTimeout    = 30 * time.Second
	DefaultAnalysisTimeout = 15 * time.Second
	// DefaultMaxQueueWait is how long an OTLP request may wait for an in-flight
	// write slot before the server answers 503 + Retry-After instead of queueing
	// unboundedly (spec 022, Part C).
	DefaultMaxQueueWait = 500 * time.Millisecond
	// DefaultWALCheckpointBytes bounds the standalone DuckDB WAL (spec 024,
	// R4). A background guard checkpoints once the WAL reaches this size, off
	// the ingest hot path.
	DefaultWALCheckpointBytes int64 = 256 << 20 // 256 MiB
)

// Config describes a running burstel recorder.
type Config struct {
	// Addr is the listen address, e.g. ":4318".
	Addr string
	// DataDir is the directory under which per-shot directories are created.
	DataDir string
	// InternalToken, when non-empty, is required as a bearer token on every
	// /internal/* endpoint. OTLP ingest paths are intentionally not gated by
	// it so a real OTLP exporter can reach them; deployments rely on the
	// Worker/DO routing boundary plus network policy for that.
	//
	// Production must configure a distinct, non-empty token: the /internal/*
	// routes fail closed when it is empty unless DevInsecureAuth is explicitly
	// enabled. The Worker passes this token into the container; it is never the
	// admin/control token or the push-only ingest token.
	InternalToken string
	// DevInsecureAuth is the explicit local-development opt-in that allows the
	// /internal/* routes to run without a token. It must never be enabled in
	// production (and is disabled by default).
	DevInsecureAuth bool
	// MaxRequestBytes bounds the decompressed size of an OTLP request.
	MaxRequestBytes int64
	// MaxInflight bounds concurrent durable writes.
	MaxInflight int
	// MaxQueueWait bounds how long an OTLP request waits for an in-flight slot.
	// Zero means fail fast with 503 + Retry-After.
	MaxQueueWait time.Duration
	// WriteTimeout bounds a single durable write (and the DB lock wait).
	WriteTimeout time.Duration
	// AnalysisTimeout bounds a single read-only analysis query/restore.
	AnalysisTimeout time.Duration
	// WALCheckpointBytes bounds the standalone DuckDB WAL. The background guard
	// checkpoints once it reaches this size (spec 024, R4).
	WALCheckpointBytes int64
}

// Load returns configuration from the environment, falling back to defaults.
func Load() Config {
	cfg := Config{
		Addr:            envString("BURSTEL_ADDR", DefaultAddr),
		DataDir:         envString("BURSTEL_DATA_DIR", DefaultDataDir),
		InternalToken:   os.Getenv("BURSTEL_INTERNAL_TOKEN"),
		DevInsecureAuth: envBool("BURSTEL_DEV_INSECURE_AUTH"),
		MaxRequestBytes: envInt64("BURSTEL_MAX_REQUEST_BYTES", DefaultMaxRequestBytes),
		MaxInflight:     envInt("BURSTEL_MAX_INFLIGHT", DefaultMaxInflight),
		MaxQueueWait:    envDuration("BURSTEL_MAX_QUEUE_WAIT", DefaultMaxQueueWait),
		WriteTimeout:    envDuration("BURSTEL_WRITE_TIMEOUT", DefaultWriteTimeout),
		AnalysisTimeout: envDuration("BURSTEL_ANALYSIS_TIMEOUT", DefaultAnalysisTimeout),
		WALCheckpointBytes: envInt64(
			"BURSTEL_WAL_CHECKPOINT_BYTES",
			DefaultWALCheckpointBytes,
		),
	}
	return cfg
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
