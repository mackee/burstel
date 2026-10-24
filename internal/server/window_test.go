package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mackee/burstel/internal/analysis"
	"github.com/mackee/burstel/internal/compaction"
	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/otlp"
)

func buildWindowSegment(t *testing.T, dir, key, instanceID, instance string, received time.Time, spans []otlp.Span) (analysis.SegmentMeta, []byte) {
	t.Helper()
	path := dir + "/" + strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(key) + ".duckdb"
	d := &otlp.Decoded{Signal: otlp.Traces, RecordCount: int64(len(spans)), Spans: spans}
	info, err := compaction.BuildSegment(context.Background(), path, key, instanceID, instance, []compaction.JournalRecord{{
		Object: compaction.JournalObject{
			Key:          key,
			InstanceID:   instanceID,
			Instance:     instance,
			Signal:       otlp.Traces,
			ReceivedAt:   received.UTC(),
			ReceivedAtMS: received.UnixMilli(),
			RequestID:    key,
		},
		Wire:    []byte("wire-" + key),
		Payload: []byte("payload-" + key),
		Decoded: d,
	}})
	if err != nil {
		t.Fatalf("BuildSegment: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	return analysis.SegmentMeta{
		Key:             key,
		InstanceID:      instanceID,
		Instance:        instance,
		Status:          "verified",
		Size:            info.Size,
		SHA256:          info.SHA256,
		FirstReceivedAt: info.FirstReceivedAt,
		LastReceivedAt:  info.LastReceivedAt,
		EventMinNS:      info.EventMinNS,
		EventMaxNS:      info.EventMaxNS,
	}, raw
}

func postRaw(t *testing.T, h *harness, path string, headers map[string]string, body []byte) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.ts.URL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+testToken)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return h.do(req)
}

func TestWindowSessionEndToEnd(t *testing.T) {
	h := newHarness(t)
	start := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	window := analysis.NewServerWindow(start, start.Add(2*time.Minute), []string{"web-1", "db-1"}, "bench")

	webKey := "journal/inst-web/traces/a.bin"
	dbKey := "journal/inst-db/traces/b.bin"
	webMeta, webBytes := buildWindowSegment(t, h.dir, webKey, "inst-web", "web-1", start.Add(time.Minute), []otlp.Span{
		{Name: "GET /", ServiceName: "web-1", StartTimeUnixNano: uint64(window.StartNS() + 10), EndTimeUnixNano: uint64(window.StartNS() + 20), StatusCode: "STATUS_CODE_OK"},
	})
	dbMeta, dbBytes := buildWindowSegment(t, h.dir, dbKey, "inst-db", "db-1", start.Add(time.Minute), []otlp.Span{
		{Name: "SELECT", ServiceName: "db-1", StartTimeUnixNano: uint64(window.StartNS() + 30), EndTimeUnixNano: uint64(window.StartNS() + 50), StatusCode: "STATUS_CODE_ERROR"},
	})

	openBody := analysis.WindowOpenRequest{SessionID: "sess-1", Window: window, Catalog: []analysis.SegmentMeta{webMeta, dbMeta}}
	resp := h.postJSON("/internal/window/open", testToken, openBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("open status %d", resp.StatusCode)
	}
	var opened analysis.WindowOpenResponse
	if err := json.NewDecoder(resp.Body).Decode(&opened); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if opened.Selection.Expected != 2 {
		t.Fatalf("expected 2 candidate segments, got %d", opened.Selection.Expected)
	}

	for i, seg := range []struct {
		key  string
		inst string
		raw  []byte
		size int64
		sha  string
	}{{webKey, "web-1", webBytes, int64(len(webBytes)), webMeta.SHA256}, {dbKey, "db-1", dbBytes, int64(len(dbBytes)), dbMeta.SHA256}} {
		r := postRaw(t, h, "/internal/window/segment", map[string]string{
			"X-Burstel-Session-Id":       "sess-1",
			"X-Burstel-Segment-Key":      seg.key,
			"X-Burstel-Segment-Instance": seg.inst,
			"X-Burstel-Size":             strconv.FormatInt(seg.size, 10),
			"X-Burstel-Sha256":           seg.sha,
		}, seg.raw)
		if r.StatusCode != http.StatusOK {
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(r.Body)
			t.Fatalf("segment %d status %d: %s", i, r.StatusCode, buf.String())
		}
		r.Body.Close()
	}

	resp = h.postJSON("/internal/window/commit", testToken, map[string]any{"session_id": "sess-1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("commit status %d", resp.StatusCode)
	}
	var cov analysis.ScopedCoverage
	if err := json.NewDecoder(resp.Body).Decode(&cov); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !cov.Complete {
		t.Fatalf("expected complete coverage, got %+v", cov)
	}

	resp = h.postJSON("/internal/window/query", testToken, map[string]any{"session_id": "sess-1", "sql": "SELECT service_name FROM spans ORDER BY service_name"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status %d", resp.StatusCode)
	}
	var qr analysis.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if qr.RowCount != 2 {
		t.Fatalf("expected 2 scoped spans, got %d", qr.RowCount)
	}

	// A hostile query is rejected.
	resp = h.postJSON("/internal/window/query", testToken, map[string]any{"session_id": "sess-1", "sql": "SELECT * FROM _burstel_baseline_metrics"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected hostile query rejection, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = h.postJSON("/internal/window/analyze", testToken, map[string]any{"session_id": "sess-1", "shot_id": "shot-1", "sections": []string{"overview"}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("analyze status %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = h.postJSON("/internal/window/release", testToken, map[string]any{"session_id": "sess-1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("release status %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestWindowSessionMissingSegmentIncomplete(t *testing.T) {
	h := newHarness(t)
	start := time.Now().UTC().Add(-3 * time.Minute).Truncate(time.Second)
	window := analysis.NewServerWindow(start, start.Add(time.Minute), []string{"web-1"}, "")

	key := "journal/inst-web/traces/c.bin"
	meta, raw := buildWindowSegment(t, h.dir, key, "inst-web", "web-1", start, []otlp.Span{
		{Name: "GET /", ServiceName: "web-1", StartTimeUnixNano: uint64(window.StartNS() + 1), EndTimeUnixNano: uint64(window.StartNS() + 2)},
	})
	missing := analysis.SegmentMeta{Key: "journal/inst-web/traces/missing.bin", Instance: "web-1", Status: "missing", FirstReceivedAt: window.Start.UnixMilli(), LastReceivedAt: window.End.UnixMilli()}

	resp := h.postJSON("/internal/window/open", testToken, analysis.WindowOpenRequest{SessionID: "sess-2", Window: window, Catalog: []analysis.SegmentMeta{meta, missing}})
	resp.Body.Close()
	resp = postRaw(t, h, "/internal/window/segment", map[string]string{
		"X-Burstel-Session-Id":       "sess-2",
		"X-Burstel-Segment-Key":      key,
		"X-Burstel-Segment-Instance": "web-1",
		"X-Burstel-Size":             strconv.FormatInt(int64(len(raw)), 10),
		"X-Burstel-Sha256":           meta.SHA256,
	}, raw)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("segment status %d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = h.postJSON("/internal/window/commit", testToken, map[string]any{"session_id": "sess-2"})
	var cov analysis.ScopedCoverage
	_ = json.NewDecoder(resp.Body).Decode(&cov)
	resp.Body.Close()
	if cov.Complete {
		t.Fatal("expected incomplete coverage with a missing segment")
	}
	if len(cov.MissingSegments) != 1 {
		t.Fatalf("expected 1 missing segment, got %v", cov.MissingSegments)
	}

	// The window is still queryable; incompleteness is disclosed, not hidden.
	resp = h.postJSON("/internal/window/query", testToken, map[string]any{"session_id": "sess-2", "sql": "SELECT count(*) FROM spans"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// Spec 035: the container downloads the shot's parts directly from R2 and the
// preload endpoint opens, downloads and commits the session in one call.
func TestWindowPreloadDownloadsPartsDirectly(t *testing.T) {
	start := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	window := analysis.NewServerWindow(start, start.Add(2*time.Minute), []string{"web-1"}, "preload")

	key := "journal/inst-web/traces/preload.bin"
	// Build the part first so its verified bytes are known.
	tmp := newHarness(t)
	meta, raw := buildWindowSegment(t, tmp.dir, key, "inst-web", "web-1", start.Add(time.Minute), []otlp.Span{
		{Name: "GET /", ServiceName: "web-1", StartTimeUnixNano: uint64(window.StartNS() + 10), EndTimeUnixNano: uint64(window.StartNS() + 20), StatusCode: "STATUS_CODE_OK"},
	})

	var hits int32
	objects := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, key) {
			atomic.AddInt32(&hits, 1)
			_, _ = w.Write(raw)
			return
		}
		http.NotFound(w, r)
	}))
	defer objects.Close()

	h := newHarnessWith(t, config.Config{
		DataDir:                  t.TempDir(),
		R2AccessKeyID:            "test-key",
		R2SecretAccessKey:        "test-secret",
		R2Bucket:                 "burstel-test",
		R2Endpoint:               objects.URL,
		AnalysisFetchParallelism: 2,
	})

	resp := h.postJSON("/internal/window/preload", testToken, analysis.WindowOpenRequest{
		SessionID: "preload-1",
		Window:    window,
		Catalog:   []analysis.SegmentMeta{meta},
		Download:  true,
	})
	if resp.StatusCode != http.StatusOK {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		t.Fatalf("preload status %d: %s", resp.StatusCode, buf.String())
	}
	var preloaded analysis.WindowPreloadResponse
	if err := json.NewDecoder(resp.Body).Decode(&preloaded); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !preloaded.Coverage.Complete {
		t.Fatalf("preload coverage = %+v", preloaded.Coverage)
	}
	if len(preloaded.CachedKeys) != 1 || preloaded.CachedKeys[0] != key {
		t.Fatalf("cached keys = %v", preloaded.CachedKeys)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("direct R2 GETs = %d, want 1", got)
	}

	// The preloaded session is already committed: analysis runs queries only.
	resp = h.postJSON("/internal/window/query", testToken, map[string]any{"session_id": "preload-1", "sql": "SELECT count(*) FROM spans"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status %d", resp.StatusCode)
	}
	var qr analysis.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if qr.RowCount != 1 {
		t.Fatalf("query rows = %d", qr.RowCount)
	}

	// Without a bucket-scoped credential the endpoint fails closed and the
	// Worker keeps the legacy streaming path.
	plain := newHarness(t)
	resp = plain.postJSON("/internal/window/preload", testToken, analysis.WindowOpenRequest{
		SessionID: "preload-2",
		Window:    window,
		Catalog:   []analysis.SegmentMeta{meta},
		Download:  true,
	})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("preload without R2 credential = %d, want 503", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestWindowEndpointsRequireInternalAuth(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodPost, h.ts.URL+"/internal/window/query", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	resp := h.do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without internal token, got %d", resp.StatusCode)
	}
}
