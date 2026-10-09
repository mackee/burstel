package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/mackee/burstel/internal/analysis"
)

// analysisContext applies the configured analysis timeout.
func (s *Server) analysisContext(r *http.Request) (context.Context, context.CancelFunc) {
	timeout := s.cfg.AnalysisTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return context.WithTimeout(r.Context(), timeout)
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	restoreID := r.Header.Get("X-Burstel-Restore-Id")
	size := int64(0)
	if raw := r.Header.Get("X-Burstel-Size"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, errors.New("invalid X-Burstel-Size header"))
			return
		}
		size = n
	}
	sha := r.Header.Get("X-Burstel-Sha256")

	ctx, cancel := s.analysisContext(r)
	defer cancel()

	info, err := s.analysis.Restore(ctx, restoreID, r.Body, size, sha)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		analysis.QueryRequest
		RestoreID string `json:"restore_id"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	engine, err := s.analysis.Engine(req.RestoreID)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	ctx, cancel := s.analysisContext(r)
	defer cancel()
	result, err := engine.Query(ctx, s.analysis.Policy(), req.QueryRequest)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	var req struct {
		analysis.AnalyzeRequest
		RestoreID string `json:"restore_id"`
		ShotID    string `json:"shot_id"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	engine, err := s.analysis.Engine(req.RestoreID)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	ctx, cancel := s.analysisContext(r)
	defer cancel()
	result, err := engine.Analyze(ctx, s.analysis.Policy(), req.ShotID, req.AnalyzeRequest)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	var req analysis.CompareRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	left, err := s.analysis.Engine(req.LeftRestoreID)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	right, err := s.analysis.Engine(req.RightRestoreID)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	ctx, cancel := s.analysisContext(r)
	defer cancel()
	result, err := analysis.CompareEngines(ctx, s.analysis.Policy(), left, right, req.LeftRestoreID, req.RightRestoreID, req.TopN)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RestoreID string `json:"restore_id"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	released := s.analysis.Release(req.RestoreID)
	writeJSON(w, http.StatusOK, map[string]any{"restore_id": req.RestoreID, "released": released})
}

func (s *Server) handleAnalysisSchema(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, analysis.SchemaDocuments(s.analysis.Policy()))
}

func writeAnalysisError(w http.ResponseWriter, err error) {
	var policyErr *analysis.PolicyError
	switch {
	case errors.As(err, &policyErr):
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": policyErr.Message, "code": policyErr.Code})
	case errors.Is(err, analysis.ErrRestoreNotFound):
		writeError(w, http.StatusNotFound, errors.New("frozen shot has not been restored"))
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, errors.New("analysis query timed out"))
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusRequestTimeout, errors.New("analysis query canceled"))
	default:
		writeError(w, http.StatusServiceUnavailable, err)
	}
}
