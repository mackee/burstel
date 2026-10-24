// Package server wires the HTTP surface: OTLP/HTTP ingest endpoints plus the
// internal shot lifecycle/artifact endpoints. It contains no storage logic;
// all lifecycle rules live in the recorder package.
package server

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/mackee/burstel/internal/analysis"
	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/r2s3"
	"github.com/mackee/burstel/internal/recorder"
)

// Server is the burstel HTTP handler set.
type Server struct {
	cfg      config.Config
	rec      *recorder.Recorder
	sem      chan struct{}
	mux      *http.ServeMux
	analysis *analysis.Store
	window   *analysis.WindowStore
}

// New builds the router. MaxInflight bounds concurrent durable writes.
func New(cfg config.Config, rec *recorder.Recorder) *Server {
	s := &Server{
		cfg: cfg,
		rec: rec,
		sem: make(chan struct{}, cfg.MaxInflight),
	}
	s.analysis = analysis.NewStore(filepath.Join(cfg.DataDir, "analysis"), analysis.StoreOptions{})
	var fetcher analysis.ObjectFetcher
	if cfg.R2AccessKeyID != "" && cfg.R2SecretAccessKey != "" && cfg.R2Bucket != "" && (cfg.R2AccountID != "" || cfg.R2Endpoint != "") {
		client, err := r2s3.New(r2s3.Config{
			AccountID:       cfg.R2AccountID,
			AccessKeyID:     cfg.R2AccessKeyID,
			SecretAccessKey: cfg.R2SecretAccessKey,
			Bucket:          cfg.R2Bucket,
			Endpoint:        cfg.R2Endpoint,
		})
		if err == nil {
			fetcher = client
		}
	}
	s.window = analysis.NewWindowStore(filepath.Join(cfg.DataDir, "windows"), analysis.WindowStoreOptions{
		Fetcher:          fetcher,
		FetchParallelism: cfg.AnalysisFetchParallelism,
	})
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)

	mux.HandleFunc("POST /internal/start", s.internal(s.handleStart))
	mux.HandleFunc("POST /internal/stop", s.internal(s.handleStop))
	mux.HandleFunc("GET /internal/artifact", s.internal(s.handleArtifact))
	mux.HandleFunc("GET /internal/metadata", s.internal(s.handleMetadata))

	// Read-only analysis of frozen R2 snapshots. These are only ever reached
	// through the Durable Object; the container is not publicly addressable.
	mux.HandleFunc("POST /internal/restore", s.internal(s.handleRestore))
	mux.HandleFunc("POST /internal/query", s.internal(s.handleQuery))
	mux.HandleFunc("POST /internal/analyze", s.internal(s.handleAnalyze))
	mux.HandleFunc("POST /internal/compare", s.internal(s.handleCompare))
	mux.HandleFunc("POST /internal/release", s.internal(s.handleRelease))
	mux.HandleFunc("GET /internal/schema", s.internal(s.handleAnalysisSchema))

	// Logical-window analysis (spec 016). A window session is built from
	// verified segment bytes streamed by the Durable Object; user SQL only runs
	// against the resulting shot-scoped snapshot.
	mux.HandleFunc("POST /internal/window/open", s.internal(s.handleWindowOpen))
	mux.HandleFunc("POST /internal/window/preload", s.internal(s.handleWindowPreload))
	mux.HandleFunc("POST /internal/window/segment", s.internal(s.handleWindowSegment))
	mux.HandleFunc("POST /internal/window/commit", s.internal(s.handleWindowCommit))
	mux.HandleFunc("POST /internal/window/query", s.internal(s.handleWindowQuery))
	mux.HandleFunc("POST /internal/window/analyze", s.internal(s.handleWindowAnalyze))
	mux.HandleFunc("POST /internal/window/compare", s.internal(s.handleWindowCompare))
	mux.HandleFunc("POST /internal/window/release", s.internal(s.handleWindowRelease))

	mux.HandleFunc("POST /v1/traces", s.otlpHandler(otlp.Traces))
	mux.HandleFunc("POST /v1/metrics", s.otlpHandler(otlp.Metrics))
	mux.HandleFunc("POST /v1/logs", s.otlpHandler(otlp.Logs))

	s.mux = mux
	return s
}

// Handler returns the root http.Handler.
func (s *Server) Handler() http.Handler { return s.mux }

// Close releases the analysis restore store.
func (s *Server) Close() error {
	if s.analysis != nil {
		_ = s.analysis.Close()
	}
	if s.window != nil {
		_ = s.window.Close()
	}
	return nil
}

// internal applies defense-in-depth bearer auth to internal endpoints.
//
// Production requires a non-empty BURSTEL_INTERNAL_TOKEN: when it is absent the
// routes fail closed so an unguarded recorder can never be reached through the
// Worker/DO routing boundary or a misconfigured deployment. The only way to run
// the internal routes without a token is the explicit local-development opt-in
// BURSTEL_DEV_INSECURE_AUTH (see config.DevInsecureAuth).
func (s *Server) internal(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.InternalToken == "" {
			if !s.cfg.DevInsecureAuth {
				writeError(
					w,
					http.StatusServiceUnavailable,
					errors.New("internal API disabled: BURSTEL_INTERNAL_TOKEN is not configured"),
				)
				return
			}
			next(w, r)
			return
		}
		got := bearerToken(r)
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.InternalToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) >= len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return strings.TrimSpace(h[len(prefix):])
	}
	return ""
}
