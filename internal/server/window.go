package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/mackee/burstel/internal/analysis"
)

var (
	errInvalidSize           = errors.New("invalid X-Burstel-Size header")
	errSessionAndKeyRequired = errors.New("X-Burstel-Session-Id and X-Burstel-Segment-Key are required")
)

// Logical-window analysis handlers (spec 016). These are only reached through
// the Durable Object: the Worker streams verified segment bytes and then runs
// bounded, shot-scoped queries.

func (s *Server) handleWindowOpen(w http.ResponseWriter, r *http.Request) {
	var req analysis.WindowOpenRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	started := time.Now()
	selection, cached, err := s.window.OpenWindowWithManifest(r.Context(), req.SessionID, req.Window, req.Catalog, req.Download, req.Manifest())
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, analysis.WindowOpenResponse{
		SessionID:  req.SessionID,
		Window:     req.Window,
		Selection:  selection,
		CachedKeys: cached,
		Timings:    analysis.ScopedTimings{OpenMS: time.Since(started).Milliseconds()},
	})
}

// handleWindowPreload is spec 035's materialization-complete hook: it opens the
// shot session, downloads every part directly from R2 with bounded parallelism
// and commits the zero-copy views, so a later analyze only runs queries.
func (s *Server) handleWindowPreload(w http.ResponseWriter, r *http.Request) {
	var req analysis.WindowOpenRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	selection, cached, commit, err := s.window.Prefetch(r.Context(), req.SessionID, req.Window, req.Catalog, req.Manifest())
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, analysis.WindowPreloadResponse{
		SessionID:           req.SessionID,
		Selection:           selection,
		CachedKeys:          cached,
		Coverage:            commit.ScopedCoverage,
		Timings:             commit.Timings,
		AttachedParts:       commit.AttachedParts,
		PartRows:            commit.PartRows,
		AttachedSpans:       commit.AttachedSpans,
		ViewSpans:           commit.ViewSpans,
		ExpectedParts:       commit.ExpectedParts,
		ExpectedSpans:       commit.ExpectedSpans,
		ViewMatchesManifest: commit.ViewMatchesManifest,
		Verified:            commit.Verified,
	})
}

func (s *Server) handleWindowSegment(w http.ResponseWriter, r *http.Request) {
	sessionID := r.Header.Get("X-Burstel-Session-Id")
	key := r.Header.Get("X-Burstel-Segment-Key")
	instance := r.Header.Get("X-Burstel-Segment-Instance")
	sha := r.Header.Get("X-Burstel-Sha256")
	size := int64(0)
	if raw := r.Header.Get("X-Burstel-Size"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, errInvalidSize)
			return
		}
		size = n
	}
	if sessionID == "" || key == "" {
		writeError(w, http.StatusBadRequest, errSessionAndKeyRequired)
		return
	}
	if err := s.window.AddSegment(r.Context(), sessionID, analysis.SegmentRef{
		Key:      key,
		Instance: instance,
	}, r.Body, size, sha); err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"accepted": true, "segment_key": key})
}

func (s *Server) handleWindowCommit(w http.ResponseWriter, r *http.Request) {
	var req analysis.WindowCommitRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	commit, err := s.window.CommitWithManifest(r.Context(), req.SessionID, req.Manifest())
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, commit)
}

func (s *Server) handleWindowQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		analysis.QueryRequest
		SessionID string `json:"session_id"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.window.Query(r.Context(), req.SessionID, req.QueryRequest)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleWindowAnalyze(w http.ResponseWriter, r *http.Request) {
	var req struct {
		analysis.AnalyzeRequest
		SessionID string `json:"session_id"`
		ShotID    string `json:"shot_id"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.window.Analyze(r.Context(), req.SessionID, req.ShotID, req.AnalyzeRequest)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleWindowCompare(w http.ResponseWriter, r *http.Request) {
	var req struct {
		LeftSessionID  string `json:"left_session_id"`
		RightSessionID string `json:"right_session_id"`
		TopN           int    `json:"top_n"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.window.Compare(r.Context(), req.LeftSessionID, req.RightSessionID, req.TopN)
	if err != nil {
		writeAnalysisError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleWindowRelease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	released := s.window.Release(req.SessionID)
	writeJSON(w, http.StatusOK, map[string]any{"session_id": req.SessionID, "released": released})
}
