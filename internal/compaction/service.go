package compaction

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mackee/burstel/internal/otlp"
)

// ServiceConfig bounds the short-lived, DO-mediated compactor HTTP service.
//
// The service deliberately never talks to R2: the coordinator Durable Object
// streams bounded raw journal bodies in and streams the resulting immutable
// segment bytes back out, so no account-wide R2/S3 credential ever reaches the
// Container instance (spec 018).
type ServiceConfig struct {
	// InternalToken gates /v1/*. When empty the service fails closed unless
	// DevInsecureAuth is explicitly set (local development only).
	InternalToken   string
	DevInsecureAuth bool
	// WorkDir is the scratch directory for segment builds (default: temp).
	WorkDir string
	// MaxBatchObjects / MaxBatchBytes bound one build request.
	MaxBatchObjects int
	MaxBatchBytes   int64
	// MaxSegmentBytes bounds the built segment held for download.
	MaxSegmentBytes int64
	// SegmentTTL bounds how long a built segment is retained for download.
	SegmentTTL time.Duration
}

// DefaultServiceConfig returns conservative operational bounds.
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		MaxBatchObjects: 4096,
		MaxBatchBytes:   64 << 20,
		MaxSegmentBytes: 256 << 20,
		SegmentTTL:      5 * time.Minute,
	}
}

// BuildObject is one bounded raw journal object streamed in by the coordinator.
type BuildObject struct {
	Key             string `json:"key"`
	Signal          string `json:"signal"`
	ReceivedAt      int64  `json:"received_at"`
	ContentEncoding string `json:"content_encoding"`
	ContentType     string `json:"content_type"`
	Size            int64  `json:"size"`
	SHA256          string `json:"sha256"`
	// InstanceID/Instance are the object's own trusted attribution. A segment
	// aggregates a cross-instance time bucket (spec 023 D3.3), so they may differ
	// from the request scope. Empty means "use the request scope".
	InstanceID string `json:"instance_id"`
	Instance   string `json:"instance"`
	// BodyBase64 is the exact stored wire bytes (possibly gzip), base64-encoded.
	BodyBase64 string `json:"body_base64"`
}

// BuildRequest asks the service to build exactly one immutable segment.
type BuildRequest struct {
	InstanceID string        `json:"instance_id"`
	Instance   string        `json:"instance"`
	Objects    []BuildObject `json:"objects"`
}

// CoverageAtom attributes one journal object to the built segment.
type CoverageAtom struct {
	JournalKey string `json:"journal_key"`
	Signal     string `json:"signal"`
	ReceivedAt int64  `json:"received_at"`
}

// BuildResponse describes the built, downloadable segment.
type BuildResponse struct {
	SegmentKey      string         `json:"segment_key"`
	SizeBytes       int64          `json:"size_bytes"`
	SHA256          string         `json:"sha256"`
	FirstReceivedAt int64          `json:"first_received_at"`
	LastReceivedAt  int64          `json:"last_received_at"`
	RequestCount    int64          `json:"request_count"`
	RecordCount     int64          `json:"record_count"`
	SpanCount       int64          `json:"span_count"`
	LogCount        int64          `json:"log_count"`
	MetricCount     int64          `json:"metric_count"`
	EventMinNS      *int64         `json:"event_min_ns"`
	EventMaxNS      *int64         `json:"event_max_ns"`
	Coverage        []CoverageAtom `json:"coverage"`
	DownloadToken   string         `json:"download_token"`
}

type storedSegment struct {
	bytes    []byte
	sha256   string
	expires  time.Time
	filename string
}

// Service is a bounded, in-memory HTTP surface over the compactor engine. It is
// deliberately process-local and short-lived: built segments are held only long
// enough for the coordinator to stream them into R2 and verify them.
type Service struct {
	cfg  ServiceConfig
	now  func() time.Time
	mu   sync.Mutex
	segs map[string]*storedSegment
}

// NewService builds a compactor service.
func NewService(cfg ServiceConfig) *Service {
	if cfg.MaxBatchObjects <= 0 {
		cfg.MaxBatchObjects = 4096
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 64 << 20
	}
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = 256 << 20
	}
	if cfg.SegmentTTL <= 0 {
		cfg.SegmentTTL = 5 * time.Minute
	}
	return &Service{cfg: cfg, now: func() time.Time { return time.Now().UTC() }, segs: map[string]*storedSegment{}}
}

// Handler returns the HTTP handler for the service.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("POST /v1/segments", s.auth(s.handleBuild))
	mux.HandleFunc("GET /v1/segments/{token}", s.auth(s.handleDownload))
	mux.HandleFunc("DELETE /v1/segments/{token}", s.auth(s.handleDelete))
	return mux
}

func (s *Service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "burstel-compact"})
}

func (s *Service) handleReady(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "service": "burstel-compact"})
}

// auth mirrors the recorder's internal-token gate: production fails closed when
// no token is configured; the explicit local-development opt-in is required to
// run without one.
func (s *Service) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.InternalToken == "" {
			if !s.cfg.DevInsecureAuth {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{
					"error": "compactor API disabled: BURSTEL_INTERNAL_TOKEN is not configured",
					"code":  "internal_auth_unconfigured",
				})
				return
			}
			next(w, r)
			return
		}
		got := bearer(r)
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.InternalToken)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized", "code": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func (s *Service) handleBuild(w http.ResponseWriter, r *http.Request) {
	body, err := readLimited(r, s.cfg.MaxBatchBytes*4/3+1<<20)
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": err.Error(), "code": "payload_too_large"})
		return
	}
	var req BuildRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON request", "code": "bad_request"})
		return
	}
	resp, err := s.Build(r.Context(), req)
	if err != nil {
		var be *badRequestError
		if errors.As(err, &be) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": be.Error(), "code": "bad_request"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "code": "build_failed"})
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

// Build validates a bounded batch, builds one immutable segment and registers
// its bytes for a single bounded download.
func (s *Service) Build(ctx context.Context, req BuildRequest) (BuildResponse, error) {
	if strings.TrimSpace(req.InstanceID) == "" {
		return BuildResponse{}, badReq("instance_id is required")
	}
	if len(req.Objects) == 0 {
		return BuildResponse{}, badReq("at least one journal object is required")
	}
	if len(req.Objects) > s.cfg.MaxBatchObjects {
		return BuildResponse{}, badReq(fmt.Sprintf("batch exceeds %d objects", s.cfg.MaxBatchObjects))
	}

	records := make([]JournalRecord, 0, len(req.Objects))
	coverage := make([]CoverageAtom, 0, len(req.Objects))
	var totalBytes int64
	srcKeys := make([]string, 0, len(req.Objects))
	for _, o := range req.Objects {
		if strings.TrimSpace(o.Key) == "" {
			return BuildResponse{}, badReq("object key is required")
		}
		signal := otlp.Signal(o.Signal)
		if !signal.Valid() {
			return BuildResponse{}, badReq(fmt.Sprintf("invalid signal %q", o.Signal))
		}
		if o.ReceivedAt <= 0 {
			return BuildResponse{}, badReq("received_at must be a positive epoch-ms value")
		}
		wire, err := base64.StdEncoding.DecodeString(o.BodyBase64)
		if err != nil {
			return BuildResponse{}, badReq(fmt.Sprintf("object %s: invalid base64 body", o.Key))
		}
		if o.Size > 0 && int64(len(wire)) != o.Size {
			return BuildResponse{}, badReq(fmt.Sprintf("object %s: size mismatch", o.Key))
		}
		if o.SHA256 != "" && !strings.EqualFold(sha256Hex(wire), o.SHA256) {
			return BuildResponse{}, badReq(fmt.Sprintf("object %s: sha256 mismatch", o.Key))
		}
		totalBytes += int64(len(wire))
		if totalBytes > s.cfg.MaxBatchBytes {
			return BuildResponse{}, badReq(fmt.Sprintf("batch exceeds %d bytes", s.cfg.MaxBatchBytes))
		}
		payload := wire
		if strings.EqualFold(o.ContentEncoding, "gzip") {
			payload, err = gunzipBytes(wire)
			if err != nil {
				return BuildResponse{}, badReq(fmt.Sprintf("object %s: %v", o.Key, err))
			}
		}
		decoded, err := otlp.Decode(signal, payload)
		if err != nil {
			return BuildResponse{}, badReq(fmt.Sprintf("object %s: decode: %v", o.Key, err))
		}
		objectInstanceID := strings.TrimSpace(o.InstanceID)
		if objectInstanceID == "" {
			objectInstanceID = req.InstanceID
		}
		objectInstance := o.Instance
		if objectInstance == "" {
			objectInstance = req.Instance
		}
		rec := JournalRecord{
			Object: JournalObject{
				Key:             o.Key,
				InstanceID:      objectInstanceID,
				Instance:        objectInstance,
				Signal:          signal,
				ReceivedAt:      time.UnixMilli(o.ReceivedAt).UTC(),
				ReceivedAtMS:    o.ReceivedAt,
				ContentEncoding: strings.ToLower(o.ContentEncoding),
				ContentType:     o.ContentType,
				SHA256:          o.SHA256,
				Size:            int64(len(wire)),
				RequestID:       requestIDFromKey(o.Key),
			},
			Wire:    wire,
			Payload: payload,
			Decoded: decoded,
		}
		records = append(records, rec)
		srcKeys = append(srcKeys, o.Key)
		coverage = append(coverage, CoverageAtom{JournalKey: o.Key, Signal: o.Signal, ReceivedAt: o.ReceivedAt})
	}

	first, last := records[0].Object.ReceivedAtMS, records[0].Object.ReceivedAtMS
	for _, rec := range records {
		if rec.Object.ReceivedAtMS < first {
			first = rec.Object.ReceivedAtMS
		}
		if rec.Object.ReceivedAtMS > last {
			last = rec.Object.ReceivedAtMS
		}
	}
	sort.Strings(srcKeys)
	key := SegmentKey(req.InstanceID, first, last, srcKeys)

	workDir := s.cfg.WorkDir
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return BuildResponse{}, err
	}
	tmp := filepath.Join(workDir, "seg-"+shortHash(key)+".duckdb")
	defer os.Remove(tmp) //nolint:errcheck
	info, err := BuildSegment(ctx, tmp, key, req.InstanceID, req.Instance, records)
	if err != nil {
		return BuildResponse{}, err
	}
	if info.Size > s.cfg.MaxSegmentBytes {
		return BuildResponse{}, fmt.Errorf("segment %s exceeds the %d-byte limit", key, s.cfg.MaxSegmentBytes)
	}
	data, err := os.ReadFile(tmp)
	if err != nil {
		return BuildResponse{}, err
	}

	token, err := randomToken()
	if err != nil {
		return BuildResponse{}, err
	}
	s.mu.Lock()
	s.pruneLocked()
	s.segs[token] = &storedSegment{bytes: data, sha256: info.SHA256, expires: s.now().Add(s.cfg.SegmentTTL), filename: key}
	s.mu.Unlock()

	return BuildResponse{
		SegmentKey:      key,
		SizeBytes:       info.Size,
		SHA256:          info.SHA256,
		FirstReceivedAt: info.FirstReceivedAt,
		LastReceivedAt:  info.LastReceivedAt,
		RequestCount:    info.RequestCount,
		RecordCount:     info.RecordCount,
		SpanCount:       info.SpanCount,
		LogCount:        info.LogCount,
		MetricCount:     info.MetricCount,
		EventMinNS:      info.EventMinNS,
		EventMaxNS:      info.EventMaxNS,
		Coverage:        coverage,
		DownloadToken:   token,
	}, nil
}

func (s *Service) handleDownload(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	seg, ok := s.take(token)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown or expired segment", "code": "not_found"})
		return
	}
	w.Header().Set("content-type", "application/vnd.duckdb")
	w.Header().Set("cache-control", "no-store")
	w.Header().Set("x-burstel-size", strconv.FormatInt(int64(len(seg.bytes)), 10))
	w.Header().Set("x-burstel-sha256", seg.sha256)
	w.Header().Set("content-length", strconv.FormatInt(int64(len(seg.bytes)), 10))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(seg.bytes)
}

func (s *Service) handleDelete(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	s.mu.Lock()
	_, ok := s.segs[token]
	delete(s.segs, token)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": ok})
}

func (s *Service) take(token string) (*storedSegment, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked()
	seg, ok := s.segs[token]
	if !ok {
		return nil, false
	}
	// Single-use: a segment is streamed out exactly once.
	delete(s.segs, token)
	return seg, true
}

func (s *Service) pruneLocked() {
	now := s.now()
	for k, v := range s.segs {
		if now.After(v.expires) {
			delete(s.segs, k)
		}
	}
}

// ------------------------------------------------------------- helpers

type badRequestError struct{ msg string }

func (e *badRequestError) Error() string { return e.msg }

func badReq(msg string) error { return &badRequestError{msg: msg} }

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) >= len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return strings.TrimSpace(h[len(prefix):])
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readLimited(r *http.Request, limit int64) ([]byte, error) {
	if limit <= 0 {
		limit = 1 << 30
	}
	defer r.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("request exceeds %d bytes", limit)
	}
	return data, nil
}

func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// requestIDFromKey extracts the trailing id from
// `journal/{instance_id}/{signal}/{received_at}-{id}.bin`.
func requestIDFromKey(key string) string {
	base := key
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	base = strings.TrimSuffix(base, ".bin")
	if i := strings.IndexByte(base, '-'); i > 0 {
		return base[i+1:]
	}
	return ""
}
