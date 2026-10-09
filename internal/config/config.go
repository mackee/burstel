// Package config holds burstel process configuration. Values come from command
// line flags (see cmd/burstel) with environment-variable defaults.
package config

import (
	"os"
	"strconv"
	"time"
)

// Default values used when neither flag nor environment overrides them.
const (
	DefaultAddr            = ":4318"
	DefaultDataDir         = "./data"
	DefaultMaxRequestBytes = 64 << 20 // 64 MiB decompressed
	DefaultMaxInflight     = 8
	DefaultWriteTimeout    = 30 * time.Second
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
	InternalToken string
	// MaxRequestBytes bounds the decompressed size of an OTLP request.
	MaxRequestBytes int64
	// MaxInflight bounds concurrent durable writes.
	MaxInflight int
	// WriteTimeout bounds a single durable write (and the DB lock wait).
	WriteTimeout time.Duration
}

// Load returns configuration from the environment, falling back to defaults.
func Load() Config {
	cfg := Config{
		Addr:            envString("BURSTEL_ADDR", DefaultAddr),
		DataDir:         envString("BURSTEL_DATA_DIR", DefaultDataDir),
		InternalToken:   os.Getenv("BURSTEL_INTERNAL_TOKEN"),
		MaxRequestBytes: envInt64("BURSTEL_MAX_REQUEST_BYTES", DefaultMaxRequestBytes),
		MaxInflight:     envInt("BURSTEL_MAX_INFLIGHT", DefaultMaxInflight),
		WriteTimeout:    envDuration("BURSTEL_WRITE_TIMEOUT", DefaultWriteTimeout),
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

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
