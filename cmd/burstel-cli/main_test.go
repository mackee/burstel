package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestAnalyzeDefaultTimeoutAndFlag(t *testing.T) {
	c, err := newClient("https://example.invalid", "test-token", "")
	if err != nil {
		t.Fatal(err)
	}
	if c.http.Timeout != 180*time.Second {
		t.Fatalf("default analyze HTTP timeout = %s, want 180s", c.http.Timeout)
	}

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"shot_id": "AAAAAAAAAAAAAAAA"})
	}))
	defer slow.Close()
	err = runCLI(t, slow.URL, "shot", "analyze", "AAAAAAAAAAAAAAAA", "--timeout", "20ms")
	if err == nil || !strings.Contains(err.Error(), "Client.Timeout") {
		t.Fatalf("err = %v, want a client timeout from --timeout 20ms", err)
	}
	if err := runCLI(t, slow.URL, "shot", "analyze", "AAAAAAAAAAAAAAAA", "--timeout", "soon"); err == nil ||
		!strings.Contains(err.Error(), "--timeout must be a positive duration") {
		t.Fatalf("err = %v, want --timeout validation", err)
	}
}

func TestShotMaterializeDefaultsAndFlags(t *testing.T) {
	ts, seen := fakeWorker(t, map[string]any{"shot_id": "AAAAAAAAAAAAAAAA", "kind": "window"})
	if err := runCLI(t, ts.URL, "shot", "materialize", "AAAAAAAAAAAAAAAA"); err != nil {
		t.Fatalf("shot materialize: %v", err)
	}
	req := (*seen)[0]
	if req.method != http.MethodPost || req.path != "/api/shots/AAAAAAAAAAAAAAAA/materialize" {
		t.Fatalf("request = %s %s", req.method, req.path)
	}
	if req.body["force"] != true {
		t.Errorf("default force = %v, want true", req.body["force"])
	}
	if _, ok := req.body["wait"]; ok {
		t.Errorf("default request must not wait, got %v", req.body["wait"])
	}

	ts2, seen2 := fakeWorker(t, map[string]any{"shot_id": "AAAAAAAAAAAAAAAA"})
	if err := runCLI(t, ts2.URL, "shot", "materialize", "AAAAAAAAAAAAAAAA", "--no-force", "--wait"); err != nil {
		t.Fatalf("shot materialize --no-force --wait: %v", err)
	}
	req2 := (*seen2)[0]
	if req2.body["force"] != false {
		t.Errorf("--no-force body = %v", req2.body["force"])
	}
	if req2.body["wait"] != true {
		t.Errorf("--wait body = %v", req2.body["wait"])
	}
}

func TestSuggestBatchSize(t *testing.T) {
	cases := []struct {
		name   string
		target int
		p50    time.Duration
		want   int
	}{
		// 15000 * 0.330 = 4950 -> next power of two.
		{name: "measured staging p50", target: 15000, p50: 330 * time.Millisecond, want: 8192},
		{name: "fast link", target: 15000, p50: 100 * time.Millisecond, want: 2048},
		{name: "already power of two", target: 8192, p50: 125 * time.Millisecond, want: 1024},
		{name: "small clamps to 512", target: 1, p50: time.Millisecond, want: 512},
		{name: "large clamps to 16384", target: 15000, p50: 2 * time.Second, want: 16384},
		{name: "no target", target: 0, p50: time.Second, want: 512},
		{name: "no latency", target: 15000, p50: 0, want: 512},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := suggestBatchSize(tc.target, tc.p50); got != tc.want {
				t.Fatalf("suggestBatchSize(%d, %s) = %d, want %d", tc.target, tc.p50, got, tc.want)
			}
		})
	}
}

func TestParseColo(t *testing.T) {
	trace := "fl=123\nh=burstel.example\ncolo=SIN\nloc=JP\n"
	if got := parseColo(trace); got != "SIN" {
		t.Fatalf("parseColo = %q, want SIN", got)
	}
	if got := parseColo("loc=JP\n"); got != "" {
		t.Fatalf("parseColo without colo = %q, want empty", got)
	}
}

func TestCollectDoctor(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case "/cdn-cgi/trace":
			_, _ = w.Write([]byte("colo=NRT\nloc=JP\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	report, err := collectDoctor(ts.URL, 15000, 3, 330*time.Millisecond)
	if err != nil {
		t.Fatalf("collectDoctor: %v", err)
	}
	if report.HealthzSamples != 3 {
		t.Errorf("samples = %d, want 3", report.HealthzSamples)
	}
	if report.HealthzRTTMS <= 0 {
		t.Errorf("healthz p50 = %f, want > 0", report.HealthzRTTMS)
	}
	if report.ServingColo != "NRT" {
		t.Errorf("colo = %q, want NRT", report.ServingColo)
	}
	if report.P50ExportMS != 330 {
		t.Errorf("p50 export = %f, want 330", report.P50ExportMS)
	}
	if report.SuggestedBatchSize != 8192 {
		t.Errorf("suggested batch = %d, want 8192", report.SuggestedBatchSize)
	}
}

// TestResolveBaseURLOrder covers spec 030: an explicit flag wins over the
// environment, the environment wins over the config file, and no built-in
// production default exists.
func TestResolveBaseURLOrder(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("BURSTEL_BASE_URL", "")
	cfgDir := filepath.Join(home, ".config", "burstel")
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	cfgPath := filepath.Join(cfgDir, "config")
	if err := os.WriteFile(cfgPath, []byte("# burstel-cli settings\nbase_url=https://config.example.workers.dev/\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	got, err := resolveBaseURL("https://flag.example.workers.dev/")
	if err != nil {
		t.Fatalf("flag: %v", err)
	}
	if got != "https://flag.example.workers.dev" {
		t.Errorf("flag base = %q", got)
	}

	t.Setenv("BURSTEL_BASE_URL", "https://env.example.workers.dev/")
	got, err = resolveBaseURL("")
	if err != nil {
		t.Fatalf("env: %v", err)
	}
	if got != "https://env.example.workers.dev" {
		t.Errorf("env base = %q", got)
	}

	t.Setenv("BURSTEL_BASE_URL", "")
	got, err = resolveBaseURL("")
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if got != "https://config.example.workers.dev" {
		t.Errorf("config base = %q", got)
	}

	if err := os.Remove(cfgPath); err != nil {
		t.Fatalf("remove config: %v", err)
	}
	_, err = resolveBaseURL("")
	if err == nil {
		t.Fatal("expected an error when no base URL is configured")
	}
	for _, want := range []string{"--base-url", "BURSTEL_BASE_URL", "base_url="} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestNewClientRequiresBaseURL asserts the CLI fails closed instead of falling
// back to an operator's production URL.
func TestNewClientRequiresBaseURL(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("BURSTEL_BASE_URL", "")
	if _, err := newClient("", "test-token", ""); err == nil {
		t.Fatal("newClient with no base URL must fail")
	}
}
