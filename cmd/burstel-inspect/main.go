// Command burstel-inspect restores and analyses frozen burstel shots in an
// independent process. It starts a fresh in-process burstel analysis server
// (the same HTTP surface used behind Cloudflare), streams frozen DuckDB bytes
// into /internal/restore with size + SHA-256 headers, then calls the read-only
// /internal/query, /internal/analyze and /internal/compare endpoints. It does
// not duplicate analytics: all aggregates come from internal/analysis.
//
// It reports a human-readable summary plus optional JSON. No Cloudflare, R2 or
// remote MCP is involved.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mackee/burstel/internal/analysis"
	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/server"
)

type inspectOutput struct {
	GeneratedAt time.Time               `json:"generated_at"`
	Left        *shotInspection         `json:"left,omitempty"`
	Right       *shotInspection         `json:"right,omitempty"`
	Compare     *analysis.CompareResult `json:"compare,omitempty"`
	Artifacts   map[string]artifactInfo `json:"artifacts"`
}

type artifactInfo struct {
	Path   string `json:"path"`
	Size   int64  `json:"size_bytes"`
	SHA256 string `json:"sha256"`
}

type shotInspection struct {
	Restore       analysis.RestoreInfo   `json:"restore"`
	Analyze       analysis.AnalyzeResult `json:"analyze"`
	NPlusOne      *analysis.QueryResult  `json:"n_plus_one_queries,omitempty"`
	MaxDBPerTrace *analysis.QueryResult  `json:"max_db_spans_per_trace,omitempty"`
	CustomQuery   *analysis.QueryResult  `json:"custom_query,omitempty"`
}

func main() {
	var (
		leftPath  = flag.String("left", "", "path to a frozen baseline telemetry.duckdb")
		rightPath = flag.String("right", "", "path to a frozen regressed telemetry.duckdb")
		leftID    = flag.String("left-id", "baseline", "restore id for the left shot")
		rightID   = flag.String("right-id", "regressed", "restore id for the right shot")
		top       = flag.Int("top", 10, "top-N groups for analysis")
		customSQL = flag.String("query", "", "optional extra read-only SQL to run against the left shot")
		outPath   = flag.String("out", "", "write the full JSON result to this path")
	)
	flag.Parse()

	if *leftPath == "" && *rightPath == "" {
		fmt.Fprintln(os.Stderr, "burstel-inspect: provide -left and/or -right")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	baseDir, err := os.MkdirTemp("", "burstel-inspect-")
	if err != nil {
		fatal(logger, "temp dir", err)
	}
	defer os.RemoveAll(baseDir)

	token := randomToken()
	cfg := config.Config{
		DataDir:         filepath.Join(baseDir, "data"),
		InternalToken:   token,
		MaxRequestBytes: 64 << 20,
		MaxInflight:     8,
		WriteTimeout:    30 * time.Second,
		AnalysisTimeout: 120 * time.Second,
	}
	rec := recorder.New(cfg)
	srv := server.New(cfg, rec)
	defer srv.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatal(logger, "listen", err)
	}
	httpSrv := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = httpSrv.Serve(ln) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	}()

	baseURL := "http://" + ln.Addr().String()
	logger.Info("analysis server started", "url", baseURL)
	client := &inspectClient{base: baseURL, token: token, http: &http.Client{Timeout: 120 * time.Second}}

	out := &inspectOutput{GeneratedAt: time.Now().UTC(), Artifacts: map[string]artifactInfo{}}

	if *leftPath != "" {
		insp, art, err := inspectShot(client, *leftID, *leftPath, *top, *customSQL)
		if err != nil {
			fatal(logger, "inspect left", err)
		}
		out.Left = insp
		out.Artifacts[*leftID] = art
	}
	if *rightPath != "" {
		insp, art, err := inspectShot(client, *rightID, *rightPath, *top, "")
		if err != nil {
			fatal(logger, "inspect right", err)
		}
		out.Right = insp
		out.Artifacts[*rightID] = art
	}
	if *leftPath != "" && *rightPath != "" {
		cmp, err := client.compare(*leftID, *rightID, *top)
		if err != nil {
			fatal(logger, "compare", err)
		}
		out.Compare = cmp
	}

	raw, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fatal(logger, "marshal output", err)
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, raw, 0o644); err != nil {
			fatal(logger, "write output", err)
		}
	}
	printHuman(out)
	if *outPath != "" {
		fmt.Printf("\nJSON written to %s (%d bytes)\n", *outPath, len(raw))
	}
}

func inspectShot(c *inspectClient, id, path string, top int, customSQL string) (*shotInspection, artifactInfo, error) {
	size, sha, err := fileSizeSHA(path)
	if err != nil {
		return nil, artifactInfo{}, err
	}
	restore, err := c.restore(id, path, size, sha)
	if err != nil {
		return nil, artifactInfo{}, fmt.Errorf("restore %s: %w", id, err)
	}
	az, err := c.analyze(id, id, top)
	if err != nil {
		return nil, artifactInfo{}, fmt.Errorf("analyze %s: %w", id, err)
	}
	nPlusOne, err := c.query(id, `SELECT name, count(*) AS n, round(quantile_cont((end_time_unix_nano - start_time_unix_nano)/1e6, 0.95), 3) AS p95_ms
		FROM spans WHERE kind = 'SPAN_KIND_CLIENT' GROUP BY name ORDER BY n DESC LIMIT 20`)
	if err != nil {
		return nil, artifactInfo{}, fmt.Errorf("n+1 query %s: %w", id, err)
	}
	maxDB, err := c.query(id, `SELECT trace_id, count(*) AS db_spans FROM spans
		WHERE kind = 'SPAN_KIND_CLIENT' GROUP BY trace_id ORDER BY db_spans DESC LIMIT 5`)
	if err != nil {
		return nil, artifactInfo{}, fmt.Errorf("max db query %s: %w", id, err)
	}
	insp := &shotInspection{Restore: *restore, Analyze: *az, NPlusOne: nPlusOne, MaxDBPerTrace: maxDB}
	if customSQL != "" {
		qr, err := c.query(id, customSQL)
		if err != nil {
			return nil, artifactInfo{}, fmt.Errorf("custom query %s: %w", id, err)
		}
		insp.CustomQuery = qr
	}
	art := artifactInfo{Path: path, Size: size, SHA256: sha}
	return insp, art, nil
}

type inspectClient struct {
	base  string
	token string
	http  *http.Client
}

func (c *inspectClient) restore(id, path string, size int64, sha string) (*analysis.RestoreInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.base+"/internal/restore", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Burstel-Restore-Id", id)
	req.Header.Set("X-Burstel-Size", fmt.Sprint(size))
	req.Header.Set("X-Burstel-Sha256", sha)
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, raw)
	}
	var info analysis.RestoreInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (c *inspectClient) query(id, sql string) (*analysis.QueryResult, error) {
	raw, err := c.postJSON("/internal/query", map[string]any{"restore_id": id, "sql": sql})
	if err != nil {
		return nil, err
	}
	var qr analysis.QueryResult
	if err := json.Unmarshal(raw, &qr); err != nil {
		return nil, err
	}
	return &qr, nil
}

func (c *inspectClient) analyze(id, shotID string, top int) (*analysis.AnalyzeResult, error) {
	raw, err := c.postJSON("/internal/analyze", map[string]any{"restore_id": id, "shot_id": shotID, "top_n": top})
	if err != nil {
		return nil, err
	}
	var az analysis.AnalyzeResult
	if err := json.Unmarshal(raw, &az); err != nil {
		return nil, err
	}
	return &az, nil
}

func (c *inspectClient) compare(left, right string, top int) (*analysis.CompareResult, error) {
	raw, err := c.postJSON("/internal/compare", map[string]any{"left_restore_id": left, "right_restore_id": right, "top_n": top})
	if err != nil {
		return nil, err
	}
	var cmp analysis.CompareResult
	if err := json.Unmarshal(raw, &cmp); err != nil {
		return nil, err
	}
	return &cmp, nil
}

func (c *inspectClient) postJSON(path string, body any) ([]byte, error) {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s status %d: %s", path, resp.StatusCode, out)
	}
	return out, nil
}

func (c *inspectClient) auth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

func fileSizeSHA(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func randomToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "inspect-token"
	}
	return hex.EncodeToString(b)
}

func fatal(logger *slog.Logger, msg string, err error) {
	logger.Error(msg, "error", err)
	fmt.Fprintf(os.Stderr, "burstel-inspect: %s: %v\n", msg, err)
	os.Exit(1)
}

// ---- human-readable rendering ----

func printHuman(out *inspectOutput) {
	for _, side := range []struct {
		name string
		insp *shotInspection
	}{
		{"LEFT (baseline)", out.Left},
		{"RIGHT (regressed)", out.Right},
	} {
		if side.insp == nil {
			continue
		}
		fmt.Printf("\n=== %s ===\n", side.name)
		i := side.insp
		fmt.Printf("artifact: size=%d sha256=%s schema=%s reused=%v\n",
			i.Restore.SizeBytes, shortSHA(i.Restore.SHA256), i.Restore.SchemaVersion, i.Restore.Reused)
		fmt.Printf("raw counts: %s\n", countsString(i.Analyze.RawCounts))
		printSection(i.Analyze, "latency")
		printSection(i.Analyze, "db_operations")
		printSection(i.Analyze, "errors")
		printSection(i.Analyze, "log_correlation")
		printSection(i.Analyze, "log_severity")
		printSection(i.Analyze, "metrics")
		if i.NPlusOne != nil {
			printQuery("N+1 CLIENT spans by name", i.NPlusOne)
		}
		if i.MaxDBPerTrace != nil {
			printQuery("max CLIENT spans per trace", i.MaxDBPerTrace)
		}
		if i.CustomQuery != nil {
			printQuery("custom query", i.CustomQuery)
		}
	}
	if out.Compare != nil {
		fmt.Printf("\n=== COMPARE (right vs left) ===\n")
		fmt.Printf("sample sizes: left=%s right=%s\n", countsString(out.Compare.SampleSizes.Left), countsString(out.Compare.SampleSizes.Right))
		fmt.Printf("span-seconds: left=%.3f right=%.3f\n", out.Compare.Left.SpanSeconds, out.Compare.Right.SpanSeconds)
		fmt.Println("\nlatency deltas (sorted by |delta p95|):")
		fmt.Printf("  %-28s %-34s %8s %8s %8s %10s %10s\n", "service", "operation", "n_l", "n_r", "p95_l", "p95_r", "pct_p95")
		for _, d := range out.Compare.Latency {
			fmt.Printf("  %-28s %-34s %8d %8d %8.3f %10.3f %9.1f%%\n",
				trunc(d.Service, 28), trunc(d.Operation, 34), d.LeftN, d.RightN, d.LeftP95, d.RightP95, d.PctP95)
		}
		fmt.Println("\nerror deltas:")
		for _, d := range out.Compare.Errors {
			fmt.Printf("  %-28s %-34s left=%d right=%d delta=%d\n", trunc(d.Service, 28), trunc(d.Operation, 34), d.LeftErrors, d.RightErrors, d.DeltaErrors)
		}
		fmt.Println("\ndisclosure:")
		for _, s := range out.Compare.Disclosure {
			fmt.Printf("  - %s\n", s)
		}
		for _, s := range out.Compare.Hypotheses {
			fmt.Printf("  hypothesis: %s\n", s)
		}
	}
}

func printSection(az analysis.AnalyzeResult, name string) {
	for _, s := range az.Sections {
		if s.Name != name {
			continue
		}
		fmt.Printf("\n%s (unit=%s sample_size=%d groups=%d observed=%v)\n", s.Name, s.Unit, s.SampleSize, s.GroupCount, s.Observed)
		cols := make([]string, len(s.Columns))
		for i, c := range s.Columns {
			cols[i] = c.Name
		}
		fmt.Printf("  %s\n", strings.Join(cols, " | "))
		for _, row := range s.Rows {
			vals := make([]string, len(row))
			for i, v := range row {
				vals[i] = fmt.Sprint(v)
			}
			fmt.Printf("  %s\n", strings.Join(vals, " | "))
		}
	}
}

func printQuery(label string, qr *analysis.QueryResult) {
	fmt.Printf("\nquery: %s (rows=%d)\n", label, qr.RowCount)
	cols := make([]string, len(qr.Columns))
	for i, c := range qr.Columns {
		cols[i] = c.Name
	}
	fmt.Printf("  %s\n", strings.Join(cols, " | "))
	for _, row := range qr.Rows {
		vals := make([]string, len(row))
		for i, v := range row {
			vals[i] = fmt.Sprint(v)
		}
		fmt.Printf("  %s\n", strings.Join(vals, " | "))
	}
}

func countsString(m map[string]int64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}

func shortSHA(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
