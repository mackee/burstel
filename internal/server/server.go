// Package server wires the HTTP surface: OTLP/HTTP ingest endpoints plus the
// internal shot lifecycle/artifact endpoints. It contains no storage logic;
// all lifecycle rules live in the recorder package.
package server

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/recorder"
)

// Server is the burstel HTTP handler set.
type Server struct {
	cfg config.Config
	rec *recorder.Recorder
	sem chan struct{}
	mux *http.ServeMux
}

// New builds the router. MaxInflight bounds concurrent durable writes.
func New(cfg config.Config, rec *recorder.Recorder) *Server {
	s := &Server{
		cfg: cfg,
		rec: rec,
		sem: make(chan struct{}, cfg.MaxInflight),
	}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)

	mux.HandleFunc("POST /internal/start", s.internal(s.handleStart))
	mux.HandleFunc("POST /internal/stop", s.internal(s.handleStop))
	mux.HandleFunc("GET /internal/artifact", s.internal(s.handleArtifact))
	mux.HandleFunc("GET /internal/metadata", s.internal(s.handleMetadata))

	mux.HandleFunc("POST /v1/traces", s.otlpHandler(otlp.Traces))
	mux.HandleFunc("POST /v1/metrics", s.otlpHandler(otlp.Metrics))
	mux.HandleFunc("POST /v1/logs", s.otlpHandler(otlp.Logs))

	s.mux = mux
	return s
}

// Handler returns the root http.Handler.
func (s *Server) Handler() http.Handler { return s.mux }

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
