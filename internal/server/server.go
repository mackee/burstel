// Package server wires the HTTP surface: OTLP/HTTP ingest endpoints plus the
// internal shot lifecycle/artifact endpoints. It contains no storage logic;
// all lifecycle rules live in the recorder package.
package server

import (
	"crypto/subtle"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/mackee/burstel/internal/analysis"
	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/recorder"
)

// Server is the burstel HTTP handler set.
type Server struct {
	cfg      config.Config
	rec      *recorder.Recorder
	sem      chan struct{}
	mux      *http.ServeMux
	analysis *analysis.Store
}

// New builds the router. MaxInflight bounds concurrent durable writes.
func New(cfg config.Config, rec *recorder.Recorder) *Server {
	s := &Server{
		cfg: cfg,
		rec: rec,
		sem: make(chan struct{}, cfg.MaxInflight),
	}
	s.analysis = analysis.NewStore(filepath.Join(cfg.DataDir, "analysis"), analysis.StoreOptions{})
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
	if s.analysis == nil {
		return nil
	}
	return s.analysis.Close()
}

// internal applies defense-in-depth bearer auth to internal endpoints. When
// no token is configured the routes are open, which is only appropriate behind
// a trusted routing boundary (e.g. a Cloudflare Worker/DO or a private net).
func (s *Server) internal(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.InternalToken != "" {
			got := bearerToken(r)
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.InternalToken)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
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
