package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	colllogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/storage"
)

const (
	maxJSONBodyBytes = 1 << 20 // 1 MiB for internal JSON endpoints

	// retryAfterSeconds is the advisory backoff on a transient 503. One second
	// matches the OTLP exporter's minimum retry interval and keeps a client
	// responsive once the server recovers.
	retryAfterSeconds = 1

	// noActiveShotMessage is the stable OTLP partial_success message for data
	// that arrived outside any shot window (spec 022, Part C).
	noActiveShotMessage = "no active shot; data discarded"
)

var (
	errTooLarge             = errors.New("payload exceeds configured limit")
	errUnsupportedEncoding  = errors.New("unsupported content-encoding")
	errUnsupportedMediaType = errors.New("unsupported content-type, expected application/x-protobuf")
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.rec.Ready() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ready\n")
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req recorder.StartRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	info, err := s.rec.Start(r.Context(), req)
	if err != nil {
		writeRecorderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ShotID string `json:"shot_id"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	res, err := s.rec.Stop(r.Context(), req.ShotID)
	if err != nil {
		writeRecorderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleMetadata(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.rec.Metadata())
}

func (s *Server) handleArtifact(w http.ResponseWriter, _ *http.Request) {
	info, err := s.rec.Artifact()
	if err != nil {
		writeRecorderError(w, err)
		return
	}
	f, err := os.Open(info.Path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", info.Name))
	w.Header().Set("ETag", `"`+info.SHA256+`"`)
	w.Header().Set("X-Burstel-SHA256", info.SHA256)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
}

func (s *Server) otlpHandler(signal otlp.Signal) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.acquire(r.Context()) {
			// Transient overload (in-flight limit / shutting down): retryable with
			// explicit backoff guidance so a client never waits unboundedly.
			writeRetryableError(w, http.StatusServiceUnavailable, errors.New("server is busy or shutting down"), retryAfterSeconds)
			return
		}
		defer s.release()

		payload, encoding, err := s.readPayload(w, r)
		if err != nil {
			writePayloadError(w, err)
			return
		}

		decoded, err := otlp.Decode(signal, payload)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}

		// Spec 022, Part C: outside an active shot the standalone recorder
		// acknowledges the export with an OTLP partial_success and discards it.
		// Returning 503 here makes BSP retry with backoff until its queue
		// overflows and TracerProvider.Shutdown hangs.
		switch s.rec.State() {
		case recorder.StateCreated, recorder.StateFrozen:
			s.discardOutsideShot(w, signal, decoded.RecordCount)
			return
		case recorder.StateFailed:
			writeRetryableError(w, http.StatusServiceUnavailable, recorder.ErrStorageUnavailable, retryAfterSeconds)
			return
		case recorder.StateStopping:
			writeRetryableError(w, http.StatusServiceUnavailable, recorder.ErrNotRecording, retryAfterSeconds)
			return
		}

		shotID, ok := s.rec.ShotID()
		if !ok {
			s.discardOutsideShot(w, signal, decoded.RecordCount)
			return
		}

		batch := &storage.Batch{
			ShotID:          shotID,
			Signal:          signal,
			ReceivedAt:      time.Now().UTC(),
			ContentEncoding: encoding,
			ResourceSummary: decoded.ResourceSummary,
			Payload:         payload,
			Spans:           decoded.Spans,
			LogRecords:      decoded.LogRecords,
			MetricPoints:    decoded.MetricPoints,
		}

		ctx, cancel := context.WithTimeout(r.Context(), s.cfg.WriteTimeout)
		defer cancel()
		if err := s.rec.Ingest(ctx, batch); err != nil {
			if errors.Is(err, recorder.ErrNotRecording) {
				// The shot stopped between the state check and the write. Nothing
				// was committed, so acknowledge-and-discard like any other
				// out-of-shot export (never a retry storm).
				s.discardOutsideShot(w, signal, decoded.RecordCount)
				return
			}
			writeRecorderError(w, err)
			return
		}

		writeOTLPResponse(w, signal)
	}
}

// discardOutsideShot accounts for and acknowledges records that arrived with no
// active shot. The OTLP partial_success reports the rejected count and a stable
// message so an operator can tell deliberate discards from real loss.
func (s *Server) discardOutsideShot(w http.ResponseWriter, signal otlp.Signal, records int64) {
	s.rec.RecordDroppedOutsideShot(records)
	writeOTLPPartialSuccess(w, signal, records, noActiveShotMessage)
}

// readPayload reads and bounds the request body, decompressing gzip when the
// exporter requests it. The returned encoding is the wire Content-Encoding.
func (s *Server) readPayload(w http.ResponseWriter, r *http.Request) ([]byte, string, error) {
	ct := r.Header.Get("Content-Type")
	if ct != "" && !isProtobufContentType(ct) {
		return nil, "", errUnsupportedMediaType
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, "", errTooLarge
		}
		return nil, "", fmt.Errorf("read request body: %w", err)
	}

	switch enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc {
	case "", "identity":
		return raw, "", nil
	case "gzip":
		decoded, err := gunzipBounded(raw, s.cfg.MaxRequestBytes)
		if err != nil {
			return nil, "", err
		}
		return decoded, "gzip", nil
	default:
		return nil, "", errUnsupportedEncoding
	}
}

func gunzipBounded(raw []byte, limit int64) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	defer zr.Close()
	decoded, err := io.ReadAll(io.LimitReader(zr, limit+1))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	if int64(len(decoded)) > limit {
		return nil, errTooLarge
	}
	return decoded, nil
}

func isProtobufContentType(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.ToLower(strings.TrimSpace(ct))
	switch ct {
	case "application/x-protobuf", "application/protobuf", "application/octet-stream":
		return true
	default:
		return false
	}
}

func writeOTLPResponse(w http.ResponseWriter, signal otlp.Signal) {
	var (
		body []byte
		err  error
	)
	switch signal {
	case otlp.Traces:
		body, err = proto.Marshal(&colltrace.ExportTraceServiceResponse{})
	case otlp.Metrics:
		body, err = proto.Marshal(&collmetrics.ExportMetricsServiceResponse{})
	case otlp.Logs:
		body, err = proto.Marshal(&colllogs.ExportLogsServiceResponse{})
	default:
		err = fmt.Errorf("unknown signal %q", signal)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeProtobuf(w, body)
}

// writeOTLPPartialSuccess returns an OTLP 200 whose signal-specific
// partial_success reports `rejected` records and the given message. This is how
// the standalone recorder acknowledges data it deliberately discarded outside a
// shot (spec 022, Part C), mirroring the Cloudflare legacy behavior.
func writeOTLPPartialSuccess(w http.ResponseWriter, signal otlp.Signal, rejected int64, message string) {
	var (
		body []byte
		err  error
	)
	switch signal {
	case otlp.Traces:
		body, err = proto.Marshal(&colltrace.ExportTraceServiceResponse{
			PartialSuccess: &colltrace.ExportTracePartialSuccess{
				RejectedSpans: rejected,
				ErrorMessage:  message,
			},
		})
	case otlp.Metrics:
		body, err = proto.Marshal(&collmetrics.ExportMetricsServiceResponse{
			PartialSuccess: &collmetrics.ExportMetricsPartialSuccess{
				RejectedDataPoints: rejected,
				ErrorMessage:       message,
			},
		})
	case otlp.Logs:
		body, err = proto.Marshal(&colllogs.ExportLogsServiceResponse{
			PartialSuccess: &colllogs.ExportLogsPartialSuccess{
				RejectedLogRecords: rejected,
				ErrorMessage:       message,
			},
		})
	default:
		err = fmt.Errorf("unknown signal %q", signal)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeProtobuf(w, body)
}

func writeProtobuf(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// acquire reserves an in-flight write slot. It waits at most cfg.MaxQueueWait
// (zero means fail fast) so overload is reported as a retryable 503 instead of
// an unbounded queue behind a slow/failed write.
func (s *Server) acquire(ctx context.Context) bool {
	select {
	case s.sem <- struct{}{}:
		return true
	default:
	}
	if s.cfg.MaxQueueWait <= 0 {
		return false
	}
	timer := time.NewTimer(s.cfg.MaxQueueWait)
	defer timer.Stop()
	select {
	case s.sem <- struct{}{}:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

func (s *Server) release() { <-s.sem }

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func writePayloadError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err)
	case errors.Is(err, errUnsupportedEncoding), errors.Is(err, errUnsupportedMediaType):
		writeError(w, http.StatusUnsupportedMediaType, err)
	default:
		writeError(w, http.StatusBadRequest, err)
	}
}

func writeRecorderError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, recorder.ErrInvalidShotID):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, recorder.ErrAlreadyRecording),
		errors.Is(err, recorder.ErrWrongShot),
		errors.Is(err, recorder.ErrNotStarted),
		errors.Is(err, recorder.ErrNotFrozen):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, recorder.ErrNotRecording),
		errors.Is(err, recorder.ErrStopping),
		errors.Is(err, recorder.ErrStorageUnavailable),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		writeRetryableError(w, http.StatusServiceUnavailable, err, retryAfterSeconds)
	default:
		writeRetryableError(w, http.StatusServiceUnavailable, err, retryAfterSeconds)
	}
}

// writeRetryableError writes a transient failure with an explicit Retry-After
// so clients back off instead of hammering a busy or stopping server.
func writeRetryableError(w http.ResponseWriter, status int, err error, retryAfterSeconds int) {
	if retryAfterSeconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	writeError(w, status, err)
}
