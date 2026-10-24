package r2s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientSignsAndTransfersObjects(t *testing.T) {
	var gotAuth, gotPayloadHash, gotBody string
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotAuth = r.Header.Get("Authorization")
		gotPayloadHash = r.Header.Get("x-amz-content-sha256")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("hello"))
			return
		}
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint:        server.URL,
		Bucket:          "burstel-test",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
		Now:             func() time.Time { return time.Date(2026, 10, 24, 3, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Put(ctx, "shots/shot-1/part-0.duckdb", []byte("data"), "application/vnd.duckdb"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if gotPath != "/burstel-test/shots/shot-1/part-0.duckdb" {
		t.Fatalf("unexpected path %q", gotPath)
	}
	if !strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 Credential=access/20261024/auto/s3/aws4_request") {
		t.Fatalf("unexpected authorization %q", gotAuth)
	}
	if gotPayloadHash != sha256Hex([]byte("data")) {
		t.Fatalf("unexpected payload hash %q", gotPayloadHash)
	}
	if gotBody != "data" {
		t.Fatalf("unexpected body %q", gotBody)
	}

	data, err := client.Get(ctx, "journal/inst-1/traces/1-a.bin")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected get body %q", data)
	}
	if gotPath != "/burstel-test/journal/inst-1/traces/1-a.bin" {
		t.Fatalf("unexpected path %q", gotPath)
	}
}

func TestNewRequiresBucketAndCredentials(t *testing.T) {
	if _, err := New(Config{AccessKeyID: "a", SecretAccessKey: "b"}); err == nil {
		t.Fatal("expected a missing bucket to fail")
	}
	if _, err := New(Config{Bucket: "b", AccessKeyID: "a"}); err == nil {
		t.Fatal("expected a missing secret to fail")
	}
}
