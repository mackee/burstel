package compaction

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func serviceObject(t *testing.T, key, signal string, receivedAt int64, wire []byte) BuildObject {
	t.Helper()
	return BuildObject{
		Key:         key,
		Signal:      signal,
		ReceivedAt:  receivedAt,
		Size:        int64(len(wire)),
		SHA256:      sha256Hex(wire),
		BodyBase64:  base64.StdEncoding.EncodeToString(wire),
		ContentType: "application/x-protobuf",
	}
}

func TestServiceBuildDownloadAndVerify(t *testing.T) {
	dir := t.TempDir()
	svc := NewService(ServiceConfig{InternalToken: "secret", WorkDir: dir})
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	traces := spanBatch("svc", 3, 1_700_000_000_000_000_000)
	logs := logBatch("svc", 2, 1_700_000_000_500_000_000)
	req := BuildRequest{
		InstanceID: "inst-1",
		Instance:   "web-1",
		Objects: []BuildObject{
			serviceObject(t, "journal/inst-1/traces/1700000000000-a.bin", "traces", 1_700_000_000_000, traces),
			serviceObject(t, "journal/inst-1/logs/1700000000500-b.bin", "logs", 1_700_000_000_500, logs),
		},
	}
	body, _ := json.Marshal(req)

	httpReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/segments", bytes.NewReader(body))
	httpReq.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("build status = %d", resp.StatusCode)
	}
	var out BuildResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.SegmentKey == "" || out.DownloadToken == "" {
		t.Fatalf("missing key/token: %+v", out)
	}
	if out.SpanCount != 3 || out.LogCount != 2 {
		t.Fatalf("counts = spans %d logs %d", out.SpanCount, out.LogCount)
	}
	if len(out.Coverage) != 2 {
		t.Fatalf("coverage = %d", len(out.Coverage))
	}
	if out.FirstReceivedAt != 1_700_000_000_000 || out.LastReceivedAt != 1_700_000_000_500 {
		t.Fatalf("range = %d..%d", out.FirstReceivedAt, out.LastReceivedAt)
	}

	dl, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/segments/"+out.DownloadToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	dl.Header.Set("Authorization", "Bearer secret")
	dresp, err := http.DefaultClient.Do(dl)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Fatalf("download status = %d", dresp.StatusCode)
	}
	data := make([]byte, out.SizeBytes)
	if _, err := readFull(dresp.Body, data); err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if sha256Hex(data) != out.SHA256 {
		t.Fatalf("segment sha mismatch")
	}
	if dresp.Header.Get("x-burstel-sha256") != out.SHA256 {
		t.Fatalf("header sha mismatch")
	}

	// The downloaded bytes are a real, readable DuckDB segment.
	path := filepath.Join(dir, "verify.duckdb")
	if err := os.WriteFile(path, data, 0o444); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenSegmentReadOnly(context.Background(), path, out.SHA256)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer reader.Close()
	spans, logsN, metrics, batches, err := reader.Counts(context.Background())
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if spans != 3 || logsN != 2 || metrics != 0 || batches != 2 {
		t.Fatalf("stored counts = spans %d logs %d metrics %d batches %d", spans, logsN, metrics, batches)
	}

	// Download is single-use.
	again, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/segments/"+out.DownloadToken, nil)
	again.Header.Set("Authorization", "Bearer secret")
	aresp, err := http.DefaultClient.Do(again)
	if err != nil {
		t.Fatal(err)
	}
	defer aresp.Body.Close()
	if aresp.StatusCode != http.StatusNotFound {
		t.Fatalf("second download status = %d, want 404", aresp.StatusCode)
	}
}

func TestServiceAuthFailsClosed(t *testing.T) {
	svc := NewService(ServiceConfig{WorkDir: t.TempDir()})
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/segments", bytes.NewReader([]byte("{}")))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when no token configured", resp.StatusCode)
	}
}

func TestServiceRejectsBadBatches(t *testing.T) {
	svc := NewService(ServiceConfig{InternalToken: "secret", WorkDir: t.TempDir(), MaxBatchObjects: 1})
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	traces := spanBatch("svc", 1, 1_700_000_000_000_000_000)
	good := serviceObject(t, "journal/i/traces/1700000000000-a.bin", "traces", 1_700_000_000_000, traces)

	cases := map[string]BuildRequest{
		"bad_signal":   {InstanceID: "i", Objects: []BuildObject{{Key: "k", Signal: "bogus", ReceivedAt: 1, BodyBase64: base64.StdEncoding.EncodeToString(traces)}}},
		"sha_mismatch": {InstanceID: "i", Objects: []BuildObject{{Key: "k", Signal: "traces", ReceivedAt: 1, SHA256: "deadbeef", BodyBase64: base64.StdEncoding.EncodeToString(traces)}}},
		"too_many":     {InstanceID: "i", Objects: []BuildObject{good, good}},
	}
	for name, req := range cases {
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/segments", bytes.NewReader(body))
		httpReq.Header.Set("Authorization", "Bearer secret")
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", name, resp.StatusCode)
		}
	}
}

func TestServiceUnauthorized(t *testing.T) {
	svc := NewService(ServiceConfig{InternalToken: "secret", WorkDir: t.TempDir()})
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/healthz", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d", resp.StatusCode)
	}
	bad, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/segments", bytes.NewReader([]byte("{}")))
	bad.Header.Set("Authorization", "Bearer wrong")
	bresp, err := http.DefaultClient.Do(bad)
	if err != nil {
		t.Fatal(err)
	}
	defer bresp.Body.Close()
	if bresp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", bresp.StatusCode)
	}
}

func readFull(r interface{ Read([]byte) (int, error) }, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			if total == len(buf) {
				return total, nil
			}
			return total, err
		}
	}
	return total, nil
}
