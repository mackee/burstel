package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/server"
	"github.com/mackee/burstel/internal/storage"
)

func analysisTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Config{
		DataDir:         dir,
		InternalToken:   testToken,
		MaxRequestBytes: 8 << 20,
		MaxInflight:     4,
		WriteTimeout:    30 * time.Second,
		AnalysisTimeout: 30 * time.Second,
	}
	rec := recorder.New(cfg)
	srv := server.New(cfg, rec)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		_ = srv.Close()
	})
	return ts
}

func writeProfiledShot(t *testing.T, dir, shotID string, baseMs int, errorEvery int) (string, string) {
	t.Helper()
	traceID := []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	var spans []fixtures.SpanProfile
	for i := 0; i < 10; i++ {
		spans = append(spans, fixtures.SpanProfile{
			Name: "GET /checkout", Kind: tracepb.Span_SPAN_KIND_SERVER, Service: "checkout",
			StartNanos:    uint64(1_700_000_000_000_000_000 + i*1_000_000),
			DurationNanos: uint64(baseMs+i) * 1_000_000,
			Error:         errorEvery > 0 && i%errorEvery == 0,
			TraceID:       traceID, SpanID: []byte{0, 0, 0, 0, 0, 0, 0, byte(i + 1)},
		})
	}
	req := fixtures.TraceExportProfiled(spans)
	payload, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := otlp.Decode(otlp.Traces, payload)
	if err != nil {
		t.Fatal(err)
	}
	shotDir := filepath.Join(dir, shotID)
	if err := os.MkdirAll(shotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(shotDir, "telemetry.duckdb")
	st, err := storage.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatch(context.Background(), &storage.Batch{
		ShotID: shotID, Signal: otlp.Traces, ReceivedAt: time.Now().UTC(),
		ResourceSummary: dec.ResourceSummary, Payload: payload, Spans: dec.Spans,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return path, sha256File(t, path)
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func restoreArtifact(t *testing.T, ts *httptest.Server, restoreID, path, sha string) *http.Response {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/internal/restore", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Burstel-Restore-Id", restoreID)
	req.Header.Set("X-Burstel-Size", fmt.Sprint(len(raw)))
	req.Header.Set("X-Burstel-Sha256", sha)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func postAnalysis(t *testing.T, ts *httptest.Server, path string, body any, token string) *http.Response {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return out
}

func TestAnalysisHTTPEndToEnd(t *testing.T) {
	ts := analysisTestServer(t)
	dir := t.TempDir()

	basePath, baseSHA := writeProfiledShot(t, dir, "baseline", 5, 0)
	regPath, regSHA := writeProfiledShot(t, dir, "regressed", 50, 3)

	// Restore requires auth.
	unauth := restoreArtifact(t, ts, "baseline", basePath, baseSHA)
	// restoreArtifact always sets the token; issue a separate unauth request.
	raw, _ := os.ReadFile(basePath)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/internal/restore", bytes.NewReader(raw))
	req.Header.Set("X-Burstel-Restore-Id", "nope")
	noTok, _ := ts.Client().Do(req)
	noTok.Body.Close()
	if noTok.StatusCode != http.StatusUnauthorized {
		t.Fatalf("restore without token = %d, want 401", noTok.StatusCode)
	}
	if unauth.StatusCode != http.StatusOK {
		t.Fatalf("restore = %d", unauth.StatusCode)
	}
	restoreInfo := decodeBody(t, unauth)
	if restoreInfo["schema_version"] != "1" {
		t.Fatalf("restore info = %+v", restoreInfo)
	}

	regResp := restoreArtifact(t, ts, "regressed", regPath, regSHA)
	if regResp.StatusCode != http.StatusOK {
		t.Fatalf("restore regressed = %d", regResp.StatusCode)
	}
	regResp.Body.Close()

	// Constrained query.
	qResp := postAnalysis(t, ts, "/internal/query", map[string]any{
		"restore_id": "baseline",
		"sql":        "SELECT name, count(*) AS n FROM spans GROUP BY name ORDER BY n DESC",
	}, testToken)
	if qResp.StatusCode != http.StatusOK {
		t.Fatalf("query = %d: %s", qResp.StatusCode, readAll(t, qResp))
	}
	qr := decodeBody(t, qResp)
	if qr["row_count"].(float64) == 0 {
		t.Fatalf("expected rows: %+v", qr)
	}

	// Hostile SQL is rejected at the analysis boundary.
	for _, hostile := range []string{
		"SELECT * FROM read_csv_auto('/etc/passwd')",
		"SELECT 1; DROP TABLE spans",
		"COPY (SELECT 1) TO '/tmp/x'",
	} {
		resp := postAnalysis(t, ts, "/internal/query", map[string]any{"restore_id": "baseline", "sql": hostile}, testToken)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("hostile SQL %q status = %d, want 400", hostile, resp.StatusCode)
		}
		body := decodeBody(t, resp)
		if body["code"] == nil {
			t.Fatalf("hostile SQL rejection missing code: %+v", body)
		}
	}

	// Missing restore -> 404.
	missing := postAnalysis(t, ts, "/internal/query", map[string]any{"restore_id": "ghost", "sql": "SELECT 1"}, testToken)
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing restore = %d, want 404", missing.StatusCode)
	}

	// Analyze.
	aResp := postAnalysis(t, ts, "/internal/analyze", map[string]any{"restore_id": "baseline", "shot_id": "baseline", "top_n": 5}, testToken)
	if aResp.StatusCode != http.StatusOK {
		t.Fatalf("analyze = %d: %s", aResp.StatusCode, readAll(t, aResp))
	}
	analysis := decodeBody(t, aResp)
	if analysis["sections"] == nil {
		t.Fatalf("analyze missing sections: %+v", analysis)
	}

	// Compare two frozen shots.
	cResp := postAnalysis(t, ts, "/internal/compare", map[string]any{
		"left_restore_id": "baseline", "right_restore_id": "regressed", "top_n": 5,
	}, testToken)
	if cResp.StatusCode != http.StatusOK {
		t.Fatalf("compare = %d: %s", cResp.StatusCode, readAll(t, cResp))
	}
	cmp := decodeBody(t, cResp)
	if cmp["sample_sizes"] == nil || cmp["disclosure"] == nil {
		t.Fatalf("compare missing fields: %+v", cmp)
	}

	// Schema documentation.
	sReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/internal/schema", nil)
	sReq.Header.Set("Authorization", "Bearer "+testToken)
	sResp, err := ts.Client().Do(sReq)
	if err != nil {
		t.Fatal(err)
	}
	schema := decodeBody(t, sResp)
	if schema["tables"] == nil || schema["safety"] == nil {
		t.Fatalf("schema missing fields: %+v", schema)
	}

	// Release is idempotent.
	rel := postAnalysis(t, ts, "/internal/release", map[string]any{"restore_id": "baseline"}, testToken)
	if rel.StatusCode != http.StatusOK {
		t.Fatalf("release = %d", rel.StatusCode)
	}
	rel.Body.Close()
}
