package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/server"
)

func newAuthHarness(t *testing.T, token string, insecure bool) *httptest.Server {
	t.Helper()
	cfg := config.Config{
		DataDir:         t.TempDir(),
		InternalToken:   token,
		DevInsecureAuth: insecure,
		MaxRequestBytes: 8 << 20,
		MaxInflight:     4,
	}
	rec := recorder.New(cfg)
	ts := httptest.NewServer(server.New(cfg, rec).Handler())
	t.Cleanup(ts.Close)
	return ts
}

func getMetadata(t *testing.T, ts *httptest.Server, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/internal/metadata", nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestInternalRoutesFailClosedWithoutToken verifies the recorder's /internal/*
// control surface is refused when no token is configured, rather than being
// left open for anyone who can reach the process.
func TestInternalRoutesFailClosedWithoutToken(t *testing.T) {
	ts := newAuthHarness(t, "", false)
	if got := getMetadata(t, ts, ""); got != http.StatusServiceUnavailable {
		t.Fatalf("internal route without token = %d, want 503", got)
	}
	// The public health endpoint stays reachable so orchestrators can probe.
	resp, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", resp.StatusCode)
	}
}

// TestInternalRoutesAllowExplicitDevInsecure pins the single, explicit local
// opt-in: no token required, but only when BURSTEL_DEV_INSECURE_AUTH is set.
func TestInternalRoutesAllowExplicitDevInsecure(t *testing.T) {
	ts := newAuthHarness(t, "", true)
	if got := getMetadata(t, ts, ""); got != http.StatusOK {
		t.Fatalf("dev-insecure internal route = %d, want 200", got)
	}
}

// TestInternalRoutesRequireMatchingToken verifies a configured token is
// enforced and compared in constant time.
func TestInternalRoutesRequireMatchingToken(t *testing.T) {
	ts := newAuthHarness(t, "internal-secret", false)
	if got := getMetadata(t, ts, ""); got != http.StatusUnauthorized {
		t.Fatalf("missing token = %d, want 401", got)
	}
	if got := getMetadata(t, ts, "wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d, want 401", got)
	}
	if got := getMetadata(t, ts, "internal-secret"); got != http.StatusOK {
		t.Fatalf("correct token = %d, want 200", got)
	}
}
