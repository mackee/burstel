package server_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	colllogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/server"
)

const testToken = "s3cret-token"

type harness struct {
	t    *testing.T
	ts   *httptest.Server
	dir  string
	rec  *recorder.Recorder
	file string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Config{
		DataDir:         dir,
		InternalToken:   testToken,
		MaxRequestBytes: 8 << 20,
		MaxInflight:     4,
		WriteTimeout:    30 * time.Second,
	}
	rec := recorder.New(cfg)
	ts := httptest.NewServer(server.New(cfg, rec).Handler())
	t.Cleanup(ts.Close)
	return &harness{t: t, ts: ts, dir: dir, rec: rec, file: filepath.Join(dir, "shot-1", recorder.ArtifactName)}
}

func (h *harness) do(req *http.Request) *http.Response {
	h.t.Helper()
	resp, err := h.ts.Client().Do(req)
	if err != nil {
		h.t.Fatalf("request %s %s: %v", req.Method, req.URL, err)
	}
	return resp
}

func (h *harness) postJSON(path, token string, body any) *http.Response {
	h.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		h.t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, h.ts.URL+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return h.do(req)
}

func (h *harness) get(path, token string) *http.Response {
	h.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, h.ts.URL+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return h.do(req)
}

func (h *harness) postOTLP(path, contentType string, body []byte, encoding string) *http.Response {
	h.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.ts.URL+path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	return h.do(req)
}

func (h *harness) start(shotID string, meta map[string]any) int {
	h.t.Helper()
	resp := h.postJSON("/internal/start", testToken, map[string]any{"shot_id": shotID, "metadata": meta})
	defer resp.Body.Close()
	return resp.StatusCode
}

func (h *harness) stop(shotID string) (int, recorder.Result) {
	h.t.Helper()
	resp := h.postJSON("/internal/stop", testToken, map[string]any{"shot_id": shotID})
	defer resp.Body.Close()
	var res recorder.Result
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			h.t.Fatalf("decode stop result: %v", err)
		}
	}
	return resp.StatusCode, res
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func gunzip(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

func TestShotEndToEnd(t *testing.T) {
	h := newHarness(t)

	if got := h.get("/healthz", "").StatusCode; got != http.StatusOK {
		t.Fatalf("healthz status = %d", got)
	}
	if got := h.get("/readyz", "").StatusCode; got != http.StatusOK {
		t.Fatalf("readyz status = %d", got)
	}

	// Internal endpoints require the bearer token when configured.
	if got := h.postJSON("/internal/start", "", map[string]any{"shot_id": "shot-1"}).StatusCode; got != http.StatusUnauthorized {
		t.Fatalf("start without token status = %d, want 401", got)
	}

	// Artifact is disabled before stop.
	if got := h.get("/internal/artifact", testToken).StatusCode; got != http.StatusConflict {
		t.Fatalf("artifact before stop status = %d, want 409", got)
	}

	// Start is idempotent for identical inputs.
	if got := h.start("shot-1", map[string]any{"scenario": "bench"}); got != http.StatusOK {
		t.Fatalf("start status = %d", got)
	}
	if got := h.start("shot-1", map[string]any{"scenario": "bench"}); got != http.StatusOK {
		t.Fatalf("idempotent start status = %d", got)
	}
	if got := h.start("shot-2", nil); got != http.StatusConflict {
		t.Fatalf("conflicting start status = %d, want 409", got)
	}

	// Malformed protobuf is a client error.
	if got := h.postOTLP("/v1/traces", "application/x-protobuf", []byte("this is not protobuf"), "").StatusCode; got != http.StatusBadRequest {
		t.Fatalf("malformed protobuf status = %d, want 400", got)
	}
	// Unsupported media type is a client error.
	if got := h.postOTLP("/v1/logs", "application/json", []byte("{}"), "").StatusCode; got != http.StatusUnsupportedMediaType {
		t.Fatalf("json content-type status = %d, want 415", got)
	}

	// Genuine protobuf fixtures for all three signals (metrics gzipped).
	traceBytes := mustMarshal(t, fixtures.TraceExport())
	metricsBytes := mustMarshal(t, fixtures.MetricsExport())
	logsBytes := mustMarshal(t, fixtures.LogsExport())
	metricsGz := gunzip(t, metricsBytes)

	accepted := 0
	post := func(path, ct string, body []byte, enc string) {
		resp := h.postOTLP(path, ct, body, enc)
		raw := readAll(t, resp) // response body is the OTLP Export*ServiceResponse
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("OTLP %s status = %d, body = %s", path, resp.StatusCode, raw)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Fatalf("OTLP response content-type = %q", got)
		}
		accepted++
	}
	post("/v1/traces", "application/x-protobuf", traceBytes, "")
	post("/v1/metrics", "application/x-protobuf", metricsGz, "gzip")
	post("/v1/logs", "application/x-protobuf", logsBytes, "")
	post("/v1/traces", "application/x-protobuf", traceBytes, "")

	// Metadata reflects the accepted batches before stop.
	metaResp := h.get("/internal/metadata", testToken)
	var meta recorder.Metadata
	if err := json.NewDecoder(metaResp.Body).Decode(&meta); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	metaResp.Body.Close()
	if meta.State != recorder.StateRecording {
		t.Fatalf("metadata state = %q", meta.State)
	}
	if meta.Counters.Batches["traces"] != 2 || meta.Counters.Batches["metrics"] != 1 || meta.Counters.Batches["logs"] != 1 {
		t.Fatalf("metadata counters = %+v", meta.Counters)
	}
	if meta.SchemaVersion != "1" {
		t.Fatalf("schema version = %q", meta.SchemaVersion)
	}

	status, stopRes := h.stop("shot-1")
	if status != http.StatusOK {
		t.Fatalf("stop status = %d", status)
	}
	if stopRes.State != recorder.StateFrozen || stopRes.SHA256 == "" || stopRes.SizeBytes <= 0 {
		t.Fatalf("stop result = %+v", stopRes)
	}
	// Idempotent stop returns the same artifact identity.
	status2, stopRes2 := h.stop("shot-1")
	if status2 != http.StatusOK || stopRes2.SHA256 != stopRes.SHA256 || stopRes2.SizeBytes != stopRes.SizeBytes {
		t.Fatalf("second stop = %d %+v", status2, stopRes2)
	}
	// New OTLP after stop is refused, not silently dropped.
	if got := h.postOTLP("/v1/traces", "application/x-protobuf", traceBytes, "").StatusCode; got == http.StatusOK {
		t.Fatalf("post after stop status = %d, want non-200", got)
	}

	// Artifact bytes match size and checksum.
	artResp := h.get("/internal/artifact", testToken)
	artBytes := readAll(t, artResp)
	if int64(len(artBytes)) != stopRes.SizeBytes {
		t.Fatalf("artifact length = %d, want %d", len(artBytes), stopRes.SizeBytes)
	}
	if cl := artResp.Header.Get("Content-Length"); cl != fmt.Sprint(stopRes.SizeBytes) {
		t.Fatalf("artifact Content-Length = %q, want %d", cl, stopRes.SizeBytes)
	}
	if got := artResp.Header.Get("X-Burstel-SHA256"); got != stopRes.SHA256 {
		t.Fatalf("artifact sha = %q, want %q", got, stopRes.SHA256)
	}

	// Reopen the frozen file read-only, in this process, and verify contents.
	frozen := h.readFrozen(t)
	defer frozen.Close()

	if n := frozen.count(t, "SELECT count(*) FROM otlp_batches"); n != int64(accepted) {
		t.Fatalf("otlp_batches count = %d, want %d", n, accepted)
	}
	if n := frozen.count(t, "SELECT count(*) FROM spans"); n != 4 {
		t.Fatalf("spans count = %d, want 4", n)
	}
	if n := frozen.count(t, "SELECT count(*) FROM log_records"); n != 2 {
		t.Fatalf("log_records count = %d, want 2", n)
	}
	if n := frozen.count(t, "SELECT count(*) FROM metric_points"); n != 3 {
		t.Fatalf("metric_points count = %d, want 3", n)
	}
	if v := frozen.scalarString(t, "SELECT value FROM schema_meta WHERE key = 'schema_version'"); v != "1" {
		t.Fatalf("schema_version = %q", v)
	}

	// Every saved raw batch decodes back to the original Export*ServiceRequest.
	expected := []proto.Message{
		fixtures.TraceExport(),
		fixtures.MetricsExport(),
		fixtures.LogsExport(),
		fixtures.TraceExport(),
	}
	frozen.checkRawRoundTrip(t, expected)

	// A useful read-only analysis query works on the frozen file.
	if name := frozen.scalarString(t, `SELECT name FROM spans
		ORDER BY (end_time_unix_nano - start_time_unix_nano) DESC, name LIMIT 1`); name != "GET /checkout" {
		t.Fatalf("slowest span = %q, want GET /checkout", name)
	}
}

// TestConcurrentIngestAndStop verifies that no accepted batch is lost across a
// concurrent stop: the number of batches in the frozen DB must equal the number
// of HTTP 200 responses.
func TestConcurrentIngestAndStop(t *testing.T) {
	h := newHarness(t)
	if got := h.start("shot-1", nil); got != http.StatusOK {
		t.Fatalf("start status = %d", got)
	}

	traceBytes := mustMarshal(t, fixtures.TraceExport())
	const writers = 24
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
	)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp := h.postOTLP("/v1/traces", "application/x-protobuf", traceBytes, "")
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	close(start)
	// Stop races the writers. Stop must drain everything accepted.
	time.Sleep(2 * time.Millisecond)
	status, res := h.stop("shot-1")
	wg.Wait()
	if status != http.StatusOK {
		t.Fatalf("stop status = %d (%+v)", status, res)
	}

	frozen := h.readFrozen(t)
	defer frozen.Close()
	if got := frozen.count(t, "SELECT count(*) FROM otlp_batches"); got != int64(accepted) {
		t.Fatalf("otlp_batches = %d, want %d accepted", got, accepted)
	}
	if got := frozen.count(t, "SELECT count(*) FROM spans"); got != int64(accepted)*2 {
		t.Fatalf("spans = %d, want %d", got, accepted*2)
	}
}

type frozenDB struct {
	db *sql.DB
	t  *testing.T
}

func (h *harness) readFrozen(t *testing.T) *frozenDB {
	t.Helper()
	db, err := sql.Open("duckdb", h.file+"?access_mode=read_only&threads=2")
	if err != nil {
		t.Fatalf("open frozen db: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		t.Fatalf("ping frozen db: %v", err)
	}
	return &frozenDB{db: db, t: t}
}

func (f *frozenDB) Close() { _ = f.db.Close() }

func (f *frozenDB) count(t *testing.T, query string) int64 {
	t.Helper()
	var n int64
	if err := f.db.QueryRowContext(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func (f *frozenDB) scalarString(t *testing.T, query string) string {
	t.Helper()
	var v string
	if err := f.db.QueryRowContext(context.Background(), query).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

func (f *frozenDB) checkRawRoundTrip(t *testing.T, expected []proto.Message) {
	t.Helper()
	rows, err := f.db.QueryContext(context.Background(),
		"SELECT signal, payload FROM otlp_batches ORDER BY ordinal")
	if err != nil {
		t.Fatalf("query batches: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var (
			signal  string
			payload []byte
		)
		if err := rows.Scan(&signal, &payload); err != nil {
			t.Fatalf("scan batch: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("more batches than expected")
		}
		decoded, err := otlp.Decode(otlp.Signal(signal), payload)
		if err != nil {
			t.Fatalf("decode batch %d (%s): %v", i, signal, err)
		}
		if decoded.RecordCount == 0 {
			t.Fatalf("batch %d decoded to zero records", i)
		}
		// Re-decode with the exact official request type and compare to original.
		if !proto.Equal(redecode(t, otlp.Signal(signal), payload), expected[i]) {
			t.Fatalf("batch %d payload does not equal the original request", i)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(expected) {
		t.Fatalf("batch count = %d, want %d", i, len(expected))
	}
}

func redecode(t *testing.T, signal otlp.Signal, payload []byte) proto.Message {
	t.Helper()
	switch signal {
	case otlp.Traces:
		m := &colltrace.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(payload, m); err != nil {
			t.Fatal(err)
		}
		return m
	case otlp.Metrics:
		m := &collmetrics.ExportMetricsServiceRequest{}
		if err := proto.Unmarshal(payload, m); err != nil {
			t.Fatal(err)
		}
		return m
	case otlp.Logs:
		m := &colllogs.ExportLogsServiceRequest{}
		if err := proto.Unmarshal(payload, m); err != nil {
			t.Fatal(err)
		}
		return m
	default:
		t.Fatalf("unknown signal %q", signal)
		return nil
	}
}
