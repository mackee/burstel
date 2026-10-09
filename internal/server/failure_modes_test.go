package server_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/recorder"
)

// TestFailureModesInvalidExportPayloadRejected verifies that malformed or
// unsupported OTLP requests are refused with an explicit client error rather
// than being accepted or silently dropped.
func TestFailureModesInvalidExportPayloadRejected(t *testing.T) {
	h := newHarness(t)
	if got := h.start("shot-bad", nil); got != http.StatusOK {
		t.Fatalf("start = %d", got)
	}
	invalid := []byte{0xff, 0x00, 0xff, 0x00, 0xff, 0x00}
	cases := []struct {
		path       string
		ct         string
		enc        string
		body       []byte
		wantStatus int
	}{
		{"/v1/traces", "application/x-protobuf", "", invalid, http.StatusBadRequest},
		{"/v1/metrics", "application/x-protobuf", "", invalid, http.StatusBadRequest},
		{"/v1/logs", "application/x-protobuf", "", invalid, http.StatusBadRequest},
		{"/v1/traces", "application/json", "", []byte("{}"), http.StatusUnsupportedMediaType},
		{"/v1/traces", "application/x-protobuf", "br", []byte("xx"), http.StatusUnsupportedMediaType},
	}
	for _, tc := range cases {
		resp := h.postOTLP(tc.path, tc.ct, tc.body, tc.enc)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.wantStatus {
			t.Errorf("%s ct=%q enc=%q status = %d, want %d", tc.path, tc.ct, tc.enc, resp.StatusCode, tc.wantStatus)
		}
	}
}

// TestFailureModesIngestAfterStopRejected verifies that once a shot is frozen,
// new OTLP exports are refused (503) so an exporter can detect the teardown.
func TestFailureModesIngestAfterStopRejected(t *testing.T) {
	h := newHarness(t)
	if got := h.start("shot-1", nil); got != http.StatusOK {
		t.Fatalf("start = %d", got)
	}
	resp := h.postOTLP("/v1/traces", "application/x-protobuf", mustMarshal(t, fixtures.TraceExport()), "")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pre-stop ingest = %d", resp.StatusCode)
	}
	if status, _ := h.stop("shot-1"); status != http.StatusOK {
		t.Fatalf("stop = %d", status)
	}
	after := h.postOTLP("/v1/traces", "application/x-protobuf", mustMarshal(t, fixtures.TraceExport()), "")
	_, _ = io.Copy(io.Discard, after.Body)
	after.Body.Close()
	if after.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("post-stop ingest = %d, want 503", after.StatusCode)
	}
}

// TestFailureModesDoubleStopIdempotent verifies that stopping twice returns the
// same frozen identity and does not rewrite the artifact.
func TestFailureModesDoubleStopIdempotent(t *testing.T) {
	h := newHarness(t)
	if got := h.start("shot-1", nil); got != http.StatusOK {
		t.Fatalf("start = %d", got)
	}
	resp := h.postOTLP("/v1/traces", "application/x-protobuf", mustMarshal(t, fixtures.TraceExport()), "")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	status1, res1 := h.stop("shot-1")
	status2, res2 := h.stop("shot-1")
	if status1 != http.StatusOK || status2 != http.StatusOK {
		t.Fatalf("stop statuses = %d, %d", status1, status2)
	}
	if res1.SHA256 == "" || res1.SHA256 != res2.SHA256 || res1.SizeBytes != res2.SizeBytes {
		t.Fatalf("non-idempotent stop: %+v vs %+v", res1, res2)
	}
}

// TestFailureModesCorruptArtifactRestoreRejected verifies the restore boundary
// checks size and SHA-256 and refuses corrupt or mis-declared artifacts.
func TestFailureModesCorruptArtifactRestoreRejected(t *testing.T) {
	ts := analysisTestServer(t)
	dir := t.TempDir()
	path, sha := writeProfiledShot(t, dir, "base", 5, 0)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)/2] ^= 0xff

	cases := []struct {
		name     string
		body     []byte
		size     int64
		sha      string
		wantCode string
	}{
		{"wrong sha", raw, int64(len(raw)), strings.Repeat("0", 64), "sha256_mismatch"},
		{"wrong size", raw, int64(len(raw)) + 17, sha, "size_mismatch"},
		{"oversized declared", raw, int64(9) << 30, sha, "artifact_too_large"},
		{"corrupt bytes", corrupt, int64(len(corrupt)), sha, "sha256_mismatch"},
	}

	for _, tc := range cases {
		resp := restoreRaw(t, ts, "corrupt-"+strings.ReplaceAll(tc.name, " ", "-"), tc.body, tc.size, tc.sha)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", tc.name, resp.StatusCode)
		}
		body := decodeBody(t, resp)
		if got := body["code"]; got != tc.wantCode {
			t.Errorf("%s: code = %v, want %s (body=%v)", tc.name, got, tc.wantCode, body)
		}
	}
}

// TestFailureModesDangerousSQLRejected verifies the read-only query policy is
// enforced at the HTTP boundary for file/network/mutation queries.
func TestFailureModesDangerousSQLRejected(t *testing.T) {
	ts := analysisTestServer(t)
	dir := t.TempDir()
	path, sha := writeProfiledShot(t, dir, "base", 5, 0)
	resp := restoreArtifact(t, ts, "base", path, sha)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("restore = %d", resp.StatusCode)
	}

	hostile := []struct {
		name string
		sql  string
	}{
		{"read file", "SELECT * FROM read_csv_auto('/etc/passwd')"},
		{"read parquet", "SELECT * FROM read_parquet('/etc/passwd')"},
		{"glob", "SELECT * FROM glob('/etc/*')"},
		{"multi statement", "SELECT 1; DROP TABLE spans"},
		{"copy", "COPY (SELECT 1) TO '/tmp/burstel-escape'"},
		{"attach", "ATTACH '/tmp/other.duckdb' AS other"},
		{"pragma", "PRAGMA database_list"},
		{"unknown table", "SELECT * FROM secrets"},
		{"unknown function", "SELECT getenv('HOME')"},
	}
	for _, tc := range hostile {
		out := postAnalysis(t, ts, "/internal/query", map[string]any{"restore_id": "base", "sql": tc.sql}, testToken)
		body := decodeBody(t, out)
		if out.StatusCode != http.StatusBadRequest {
			t.Errorf("%s %q: status = %d, want 400 (body=%v)", tc.name, tc.sql, out.StatusCode, body)
			continue
		}
		if body["code"] == nil {
			t.Errorf("%s %q: rejection missing stable code", tc.name, tc.sql)
		}
	}
}

// TestFailureModesTwoRecordersIsolated verifies two independent recorder
// instances (distinct data dirs and servers) can each record and freeze a
// different shot without interfering.
func TestFailureModesTwoRecordersIsolated(t *testing.T) {
	h1 := newHarness(t)
	h2 := newHarness(t)
	if h1.start("alpha", nil) != http.StatusOK || h2.start("beta", nil) != http.StatusOK {
		t.Fatal("starts failed")
	}
	trace := mustMarshal(t, fixtures.TraceExport())
	for _, h := range []*harness{h1, h2} {
		r := h.postOTLP("/v1/traces", "application/x-protobuf", trace, "")
		_, _ = io.Copy(io.Discard, r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("ingest = %d", r.StatusCode)
		}
	}
	s1, r1 := h1.stop("alpha")
	s2, r2 := h2.stop("beta")
	if s1 != http.StatusOK || s2 != http.StatusOK {
		t.Fatalf("stop statuses = %d, %d", s1, s2)
	}
	if r1.SHA256 == "" || r2.SHA256 == "" || r1.SHA256 == r2.SHA256 {
		t.Fatalf("artifacts not independently frozen: %+v %+v", r1, r2)
	}
	if _, err := os.Stat(filepath.Join(h1.dir, "alpha", recorder.ArtifactName)); err != nil {
		t.Fatalf("recorder 1 artifact: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h2.dir, "beta", recorder.ArtifactName)); err != nil {
		t.Fatalf("recorder 2 artifact: %v", err)
	}
}

// TestFailureModesProtobufEquivalence is a guard that the invalid payload used
// above is genuinely rejected by proto.Unmarshal (otherwise the test would be
// vacuous).
func TestFailureModesInvalidBytesAreActuallyInvalid(t *testing.T) {
	var m proto.Message = fixtures.TraceExport()
	if err := proto.Unmarshal([]byte{0xff, 0x00, 0xff, 0x00, 0xff, 0x00}, m); err == nil {
		t.Fatal("expected invalid protobuf bytes to fail proto.Unmarshal")
	}
}

// restoreRaw posts an arbitrary body with explicit size/sha headers.
func restoreRaw(t *testing.T, ts *httptest.Server, id string, body []byte, size int64, sha string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/internal/restore", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Burstel-Restore-Id", id)
	req.Header.Set("X-Burstel-Size", strconv.FormatInt(size, 10))
	req.Header.Set("X-Burstel-Sha256", sha)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
