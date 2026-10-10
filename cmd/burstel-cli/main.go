// Command burstel-cli is a small admin client for a deployed burstel Worker.
//
// It creates/reads/stops shots, mints and revokes the 24h push-only ingest
// token, and runs REST query/analyze/schema/compare against a frozen shot's
// DuckDB snapshot. The admin token is sent as `Authorization: Bearer`; the
// push-only token is never accepted here.
//
// Usage:
//
//	burstel-cli [--base-url URL] [--token TOKEN | --token-file PATH] <command> [args]
//
// Commands:
//
//	health
//	token issue [--out FILE]     mint the 24h push-only ingest token
//	token revoke                 revoke the current ingest token
//	shot start [--metadata k=v ...]
//	shot get <shot-id>
//	shot stop <shot-id>
//	shot query <shot-id> --sql "SELECT ..." [--max-rows N]
//	shot analyze <shot-id> [--sections a,b] [--top-n N]
//	shot schema <shot-id>
//	shot compare <shot-id> --other <shot-id> [--top-n N]
//
// Flags may appear before or after the positional shot id.
//
// Environment:
//
//	BURSTEL_BASE_URL   base URL of the deployed Worker
//	BURSTEL_API_TOKEN  admin token (prefer --token-file or the env over argv)
//
// The admin token is resolved in order: --token, $BURSTEL_API_TOKEN,
// --token-file, ./.secrets/BURSTEL_API_TOKEN, then ~/.config/burstel/token.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const defaultBaseURL = "https://burstel.example.workers.dev"

const defaultTokenFile = ".secrets/BURSTEL_API_TOKEN"

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, errHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "burstel-cli: "+err.Error())
		os.Exit(1)
	}
}

var errHelp = errors.New("help")

func run(args []string) error {
	base, token, tokenFile, rest := extractGlobal(args)
	if len(rest) == 0 {
		usage()
		return nil
	}

	client, err := newClient(base, token, tokenFile)
	if err != nil {
		return err
	}

	cmd, sub := rest[0], rest[1:]
	switch cmd {
	case "help", "-h", "--help":
		usage()
		return nil
	case "health":
		return client.getJSON("/healthz")
	case "token":
		return client.tokenCommand(sub)
	case "shot":
		return client.shotCommand(sub)
	default:
		return fmt.Errorf("unknown command %q (try `burstel-cli help`)", cmd)
	}
}

// extractGlobal pulls the global flags out of args (they may appear anywhere)
// and returns the remaining command arguments.
func extractGlobal(args []string) (base, token, tokenFile string, rest []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--base-url" && i+1 < len(args):
			base = args[i+1]
			i++
		case strings.HasPrefix(a, "--base-url="):
			base = strings.TrimPrefix(a, "--base-url=")
		case a == "--token" && i+1 < len(args):
			token = args[i+1]
			i++
		case strings.HasPrefix(a, "--token="):
			token = strings.TrimPrefix(a, "--token=")
		case a == "--token-file" && i+1 < len(args):
			tokenFile = args[i+1]
			i++
		case strings.HasPrefix(a, "--token-file="):
			tokenFile = strings.TrimPrefix(a, "--token-file=")
		default:
			rest = append(rest, a)
		}
	}
	return base, token, tokenFile, rest
}

// parseOpts splits args into positionals and flags. Flags may appear anywhere.
// A flag whose name is in valued consumes the next argument as its value
// unless written as --name=value. Repeatable flags accumulate.
func parseOpts(args []string, valued map[string]bool) (pos []string, opts map[string][]string, err error) {
	opts = map[string][]string{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			pos = append(pos, a)
			continue
		}
		name := strings.TrimPrefix(a, "--")
		val := ""
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			val = name[eq+1:]
			name = name[:eq]
		} else if valued[name] {
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("--%s needs a value", name)
			}
			val = args[i+1]
			i++
		}
		opts[name] = append(opts[name], val)
	}
	return pos, opts, nil
}

func last(opts map[string][]string, name string) string {
	v := opts[name]
	if len(v) == 0 {
		return ""
	}
	return v[len(v)-1]
}

func optInt(opts map[string][]string, name string) (int, bool, error) {
	v := last(opts, name)
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false, fmt.Errorf("--%s must be an integer, got %q", name, v)
	}
	return n, true, nil
}

type client struct {
	base  string
	token string
	http  *http.Client
}

func newClient(base, token, tokenFile string) (*client, error) {
	if base == "" {
		base = os.Getenv("BURSTEL_BASE_URL")
	}
	if base == "" {
		base = defaultBaseURL
	}
	base = strings.TrimRight(base, "/")

	if token == "" {
		token = os.Getenv("BURSTEL_API_TOKEN")
	}
	if token == "" {
		path := tokenFile
		if path == "" {
			for _, cand := range defaultTokenPaths() {
				if _, err := os.Stat(cand); err == nil {
					path = cand
					break
				}
			}
		}
		if path != "" {
			b, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read token file %s: %w", path, err)
			}
			token = strings.TrimSpace(string(b))
		}
	}
	if token == "" {
		return nil, fmt.Errorf("no admin token: set --token, $BURSTEL_API_TOKEN or --token-file")
	}
	return &client{base: base, token: token, http: &http.Client{Timeout: 120 * time.Second}}, nil
}

// defaultTokenPaths returns the token files tried when no explicit token is
// given: the repository-local .secrets/ file first, then a per-user
// ~/.config/burstel/token so an installed binary works from any directory.
func defaultTokenPaths() []string {
	paths := []string{defaultTokenFile}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".config", "burstel", "token"))
	}
	return paths
}

// do performs an admin request. If out is non-nil the JSON body is decoded into
// it; on a non-2xx status it returns an error carrying the response body.
func (c *client) do(method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func (c *client) getJSON(path string) error {
	var v any
	if err := c.do(http.MethodGet, path, nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

func (c *client) postJSON(method, path string, body any) error {
	var v any
	if err := c.do(method, path, body, &v); err != nil {
		return err
	}
	return printJSON(v)
}

func printJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func (c *client) tokenCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("token: expected `issue` or `revoke`")
	}
	switch args[0] {
	case "issue":
		_, opts, err := parseOpts(args[1:], map[string]bool{"out": true})
		if err != nil {
			return err
		}
		var resp struct {
			Token     string `json:"token"`
			IssuedAt  string `json:"issued_at"`
			ExpiresAt string `json:"expires_at"`
			TTL       int    `json:"ttl_seconds"`
		}
		if err := c.do(http.MethodPost, "/api/ingest-token", nil, &resp); err != nil {
			return err
		}
		if out := last(opts, "out"); out != "" {
			if err := writeSecretFile(out, resp.Token); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "wrote token to %s (mode 600)\n", out)
		}
		fmt.Fprintf(os.Stderr, "issued_at=%s expires_at=%s ttl_seconds=%d\n", resp.IssuedAt, resp.ExpiresAt, resp.TTL)
		fmt.Fprintln(os.Stdout, resp.Token)
		return nil
	case "revoke":
		return c.postJSON(http.MethodDelete, "/api/ingest-token", nil)
	default:
		return fmt.Errorf("token: unknown action %q", args[0])
	}
}

func (c *client) shotCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("shot: expected start|get|stop|query|analyze|schema|compare")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "start":
		pos, opts, err := parseOpts(rest, map[string]bool{"metadata": true})
		if err != nil {
			return err
		}
		if len(pos) != 0 {
			return fmt.Errorf("shot start: unexpected argument %q", pos[0])
		}
		meta := map[string]string{}
		for _, kv := range opts["metadata"] {
			key, val, ok := strings.Cut(kv, "=")
			if !ok || key == "" {
				return fmt.Errorf("--metadata must be key=value, got %q", kv)
			}
			meta[key] = val
		}
		return c.postJSON(http.MethodPost, "/api/shots", map[string]any{"metadata": meta})
	case "get", "stop", "schema":
		pos, _, err := parseOpts(rest, nil)
		if err != nil {
			return err
		}
		id, err := oneShotID(pos)
		if err != nil {
			return err
		}
		switch action {
		case "stop":
			return c.postJSON(http.MethodPost, "/api/shots/"+id+"/stop", nil)
		case "schema":
			return c.getJSON("/api/shots/" + id + "/schema")
		default: // get
			return c.getJSON("/api/shots/" + id)
		}
	case "query":
		pos, opts, err := parseOpts(rest, map[string]bool{"sql": true, "max-rows": true})
		if err != nil {
			return err
		}
		id, err := oneShotID(pos)
		if err != nil {
			return err
		}
		sql := last(opts, "sql")
		if sql == "" {
			return errors.New("shot query: --sql is required")
		}
		body := map[string]any{"sql": sql}
		if n, ok, err := optInt(opts, "max-rows"); err != nil {
			return err
		} else if ok {
			body["max_rows"] = n
		}
		return c.postJSON(http.MethodPost, "/api/shots/"+id+"/query", body)
	case "analyze":
		pos, opts, err := parseOpts(rest, map[string]bool{"sections": true, "top-n": true})
		if err != nil {
			return err
		}
		id, err := oneShotID(pos)
		if err != nil {
			return err
		}
		body := map[string]any{}
		if s := last(opts, "sections"); s != "" {
			body["sections"] = strings.Split(s, ",")
		}
		if n, ok, err := optInt(opts, "top-n"); err != nil {
			return err
		} else if ok {
			body["top_n"] = n
		}
		return c.postJSON(http.MethodPost, "/api/shots/"+id+"/analyze", body)
	case "compare":
		pos, opts, err := parseOpts(rest, map[string]bool{"other": true, "top-n": true})
		if err != nil {
			return err
		}
		id, err := oneShotID(pos)
		if err != nil {
			return err
		}
		other := last(opts, "other")
		if other == "" {
			return errors.New("shot compare: --other is required")
		}
		body := map[string]any{"other": other}
		if n, ok, err := optInt(opts, "top-n"); err != nil {
			return err
		} else if ok {
			body["top_n"] = n
		}
		return c.postJSON(http.MethodPost, "/api/shots/"+id+"/compare", body)
	default:
		return fmt.Errorf("shot: unknown action %q", action)
	}
}

func oneShotID(pos []string) (string, error) {
	if len(pos) != 1 {
		return "", fmt.Errorf("expected exactly one shot id, got %d", len(pos))
	}
	return pos[0], nil
}

func writeSecretFile(path, value string) error {
	if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

func usage() {
	fmt.Fprint(os.Stderr, `burstel-cli - admin client for a deployed burstel Worker

Usage:
  burstel-cli [--base-url URL] [--token TOKEN | --token-file PATH] <command> [args]

Global flags (may appear anywhere):
  --base-url URL     base URL (default $BURSTEL_BASE_URL, else `+defaultBaseURL+`)
  --token TOKEN      admin token (default $BURSTEL_API_TOKEN)
  --token-file PATH  read the admin token from PATH (default .secrets/BURSTEL_API_TOKEN, else ~/.config/burstel/token)

Commands:
  health
  token issue [--out FILE]        mint the 24h push-only ingest token (prints the token)
  token revoke                    revoke the current ingest token
  shot start [--metadata k=v ...]
  shot get <shot-id>
  shot stop <shot-id>
  shot query <shot-id> --sql "SELECT ..." [--max-rows N]
  shot analyze <shot-id> [--sections a,b] [--top-n N]
  shot schema <shot-id>
  shot compare <shot-id> --other <shot-id> [--top-n N]

Examples:
  export BURSTEL_BASE_URL=https://burstel.example.workers.dev
  TOKEN=$(burstel-cli token issue)
  burstel-cli shot start --metadata source=isucON
  burstel-cli shot stop <shot-id>
  burstel-cli shot query <shot-id> --sql "SELECT span_name, count(*) c FROM spans GROUP BY 1 ORDER BY c DESC" --max-rows 10
`)
}
