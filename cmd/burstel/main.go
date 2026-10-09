// Command burstel runs the OTLP/HTTP -> DuckDB shot recorder.
//
// Usage:
//
//	burstel --addr :4318 --data-dir ./data
//
// The recorder exposes OTLP/HTTP protobuf ingest on /v1/{traces,metrics,logs}
// and an internal lifecycle API under /internal/*.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/server"
)

func main() {
	cfg := config.Load()

	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP listen address")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "directory for per-shot DuckDB files")
	flag.StringVar(&cfg.InternalToken, "internal-token", cfg.InternalToken, "bearer token required on /internal/* (required unless -dev-insecure-auth)")
	flag.BoolVar(&cfg.DevInsecureAuth, "dev-insecure-auth", cfg.DevInsecureAuth, "trusted local development: allow /internal/* without a token (never in production)")
	flag.Int64Var(&cfg.MaxRequestBytes, "max-request-bytes", cfg.MaxRequestBytes, "max decompressed OTLP request size in bytes")
	flag.IntVar(&cfg.MaxInflight, "max-inflight", cfg.MaxInflight, "max concurrent durable writes")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", cfg.WriteTimeout, "timeout for a single durable write")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	rec := recorder.New(cfg)
	handler := server.New(cfg, rec)
	defer handler.Close()

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("burstel listening", "addr", cfg.Addr, "data_dir", cfg.DataDir, "schema_version", "1")
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		logger.Error("server failed", "error", err)
		os.Exit(1)
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	logger.Info("burstel stopped")
}
