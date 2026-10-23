package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// recordedRequest is one request the fake Worker received.
type recordedRequest struct {
	method string
	path   string
	body   map[string]any
}

// fakeWorker answers every request with a JSON object and records the body so
// the CLI's wire contract can be asserted without a live deployment.
func fakeWorker(t *testing.T, response map[string]any) (*httptest.Server, *[]recordedRequest) {
	t.Helper()
	var seen []recordedRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		entry := recordedRequest{method: r.Method, path: r.URL.Path}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &entry.body)
		}
		seen = append(seen, entry)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(ts.Close)
	return ts, &seen
}

func runCLI(t *testing.T, base string, args ...string) error {
	t.Helper()
	return run(append([]string{"--base-url", base, "--token", "test-token"}, args...))
}

// TestShotStartDefaultsToWindow covers spec 022 Part A1/A3 for the CLI: the
// default request omits `mode` so the Worker creates a window shot, and
// `--mode recording` still asks for the deprecated legacy shot.
func TestShotStartDefaultsToWindow(t *testing.T) {
	ts, seen := fakeWorker(t, map[string]any{"shot_id": "AAAAAAAAAAAAAAAA", "kind": "window", "state": "open"})
	if err := runCLI(t, ts.URL, "shot", "start", "--name", "bench"); err != nil {
		t.Fatalf("shot start: %v", err)
	}
	if len(*seen) != 1 {
		t.Fatalf("requests = %d, want 1", len(*seen))
	}
	req := (*seen)[0]
	if req.method != http.MethodPost || req.path != "/api/shots" {
		t.Fatalf("request = %s %s", req.method, req.path)
	}
	if _, ok := req.body["mode"]; ok {
		t.Errorf("default shot start must omit mode (Worker defaults to window), got %v", req.body["mode"])
	}
	if req.body["name"] != "bench" {
		t.Errorf("name = %v", req.body["name"])
	}
}

func TestShotStartExplicitRecording(t *testing.T) {
	ts, seen := fakeWorker(t, map[string]any{"shot_id": "AAAAAAAAAAAAAAAA", "state": "recording", "deprecated": true})
	if err := runCLI(t, ts.URL, "shot", "start", "--mode", "recording"); err != nil {
		t.Fatalf("shot start --mode recording: %v", err)
	}
	if got := (*seen)[0].body["mode"]; got != "recording" {
		t.Errorf("mode = %v, want recording", got)
	}
}

func TestShotStartRejectsUnknownMode(t *testing.T) {
	ts, _ := fakeWorker(t, map[string]any{})
	err := runCLI(t, ts.URL, "shot", "start", "--mode", "video")
	if err == nil || !strings.Contains(err.Error(), "--mode must be window or recording") {
		t.Fatalf("err = %v, want a --mode validation error", err)
	}
}
