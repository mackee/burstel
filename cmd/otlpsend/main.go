// Command otlpsend is a tiny local demo/smoke client. By default it starts a
// shot, sends the three built-in OTLP protobuf fixtures (metrics gzipped) and
// stops the shot:
//
//	otlpsend -url http://localhost:4318 -shot demo-1 -token "$BURSTEL_INTERNAL_TOKEN"
//
// With -ingest it instead targets the Worker's stable ingest endpoint, sending
// the same three real fixtures to <ingest>/ingest/v1/{traces,metrics,logs} with
// a push-only token and no start/stop (the admin controls lifecycle over REST):
//
//	BURSTEL_PUSH_TOKEN=... otlpsend -ingest https://<host>
//
// The token is deliberately taken from the BURSTEL_PUSH_TOKEN environment
// variable when the flag is omitted so a secret never has to appear in argv.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/mackee/burstel/internal/fixtures"
)

func main() {
	baseURL := flag.String("url", "http://localhost:4318", "burstel base URL")
	shotID := flag.String("shot", "demo-1", "shot id")
	token := flag.String("token", os.Getenv("BURSTEL_INTERNAL_TOKEN"), "internal bearer token")
	ingestURL := flag.String("ingest", "", "stable ingest base URL (Worker); when set, only send the three fixtures to <ingest>/ingest/v1/* and exit")
	pushToken := flag.String("push-token", os.Getenv("BURSTEL_PUSH_TOKEN"), "push-only token for -ingest (prefer the BURSTEL_PUSH_TOKEN env var)")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second}

	if *ingestURL != "" {
		if *pushToken == "" {
			fail(fmt.Errorf("-ingest requires a push token via -push-token or BURSTEL_PUSH_TOKEN"))
		}
		base := strings.TrimRight(*ingestURL, "/")
		for _, sig := range []string{"traces", "metrics", "logs"} {
			body, encoding, err := fixture(sig)
			if err != nil {
				fail(err)
			}
			if err := send(client, base+"/ingest/v1/"+sig, body, encoding, *pushToken); err != nil {
				fail(err)
			}
			fmt.Printf("sent %s (%d bytes, encoding=%q)\n", sig, len(body), encoding)
		}
		return
	}

	if err := start(client, *baseURL, *token, *shotID); err != nil {
		fail(err)
	}
	for _, sig := range []string{"traces", "metrics", "logs"} {
		body, encoding, err := fixture(sig)
		if err != nil {
			fail(err)
		}
		if err := send(client, *baseURL+"/v1/"+sig, body, encoding, ""); err != nil {
			fail(err)
		}
		fmt.Printf("sent %s (%d bytes, encoding=%q)\n", sig, len(body), encoding)
	}
	result, err := stop(client, *baseURL, *token, *shotID)
	if err != nil {
		fail(err)
	}
	fmt.Printf("frozen artifact=%v size=%v sha256=%v\n", result["artifact"], result["size_bytes"], result["sha256"])
}

func fixture(signal string) ([]byte, string, error) {
	var (
		msg proto.Message
	)
	switch signal {
	case "traces":
		msg = fixtures.TraceExport()
	case "metrics":
		msg = fixtures.MetricsExport()
	case "logs":
		msg = fixtures.LogsExport()
	default:
		return nil, "", fmt.Errorf("unknown signal %q", signal)
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil, "", err
	}
	if signal != "metrics" {
		return raw, "", nil
	}
	// Demonstrate optional gzip on one signal.
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		return nil, "", err
	}
	if err := zw.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), "gzip", nil
}

func start(client *http.Client, base, token, shotID string) error {
	body, _ := json.Marshal(map[string]any{
		"shot_id":  shotID,
		"metadata": map[string]any{"source": "otlpsend"},
	})
	return doJSON(client, http.MethodPost, base+"/internal/start", token, body, nil)
}

func stop(client *http.Client, base, token, shotID string) (map[string]any, error) {
	body, _ := json.Marshal(map[string]any{"shot_id": shotID})
	var out map[string]any
	if err := doJSON(client, http.MethodPost, base+"/internal/stop", token, body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func send(client *http.Client, url string, body []byte, encoding, pushToken string) error {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	if pushToken != "" {
		req.Header.Set("X-Burstel-Token", pushToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d: %s", url, resp.StatusCode, payload)
	}
	return nil
}

func doJSON(client *http.Client, method, url, token string, body []byte, out any) error {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d: %s", url, resp.StatusCode, payload)
	}
	if out != nil {
		return json.Unmarshal(payload, out)
	}
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "otlpsend:", err)
	os.Exit(1)
}
