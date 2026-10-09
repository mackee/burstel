package analysis

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/storage"
)

func traceKindServer() tracepb.Span_SpanKind { return tracepb.Span_SPAN_KIND_SERVER }
func traceKindClient() tracepb.Span_SpanKind { return tracepb.Span_SPAN_KIND_CLIENT }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func benchTraceBatch(t *testing.T, shotID string, spans []fixtures.SpanProfile) *storage.Batch {
	t.Helper()
	req := fixtures.TraceExportProfiled(spans)
	payload := mustMarshal(t, req)
	dec, err := otlp.Decode(otlp.Traces, payload)
	if err != nil {
		t.Fatalf("decode traces: %v", err)
	}
	return &storage.Batch{
		ShotID: shotID, Signal: otlp.Traces, ReceivedAt: time.Now().UTC(),
		ResourceSummary: dec.ResourceSummary, Payload: payload, Spans: dec.Spans,
	}
}

func benchLogBatch(t *testing.T, shotID string, logs []fixtures.LogProfile) *storage.Batch {
	t.Helper()
	req := fixtures.LogsExportProfiled(logs)
	payload := mustMarshal(t, req)
	dec, err := otlp.Decode(otlp.Logs, payload)
	if err != nil {
		t.Fatalf("decode logs: %v", err)
	}
	return &storage.Batch{
		ShotID: shotID, Signal: otlp.Logs, ReceivedAt: time.Now().UTC(),
		ResourceSummary: dec.ResourceSummary, Payload: payload, LogRecords: dec.LogRecords,
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func writeShotDB(t *testing.T, dir, shotID string, batches []*storage.Batch) string {
	t.Helper()
	shotDir := filepath.Join(dir, shotID)
	if err := os.MkdirAll(shotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(shotDir, "telemetry.duckdb")
	st, err := storage.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	for _, b := range batches {
		if err := st.InsertBatch(context.Background(), b); err != nil {
			t.Fatalf("insert batch: %v", err)
		}
	}
	if err := st.Checkpoint(context.Background()); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dbPath
}

func fileSHA(t *testing.T, path string) string {
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

// syntheticProfiles returns a deterministic latency/error profile for a shot.
func syntheticProfiles(baseMs int, errorEvery int) []fixtures.SpanProfile {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var spans []fixtures.SpanProfile
	for i := 0; i < 20; i++ {
		spanID := []byte{0, 0, 0, 0, 0, 0, 0, byte(i + 1)}
		dur := uint64(baseMs+(i%5)) * 1_000_000
		errSpan := errorEvery > 0 && i%errorEvery == 0
		spans = append(spans, fixtures.SpanProfile{
			Name: "GET /checkout", Kind: traceKindServer(), Service: "checkout",
			StartNanos: uint64(1_700_000_000_000_000_000 + i*1_000_000), DurationNanos: dur,
			Error: errSpan, TraceID: traceID, SpanID: spanID,
		})
	}
	// One DB operation span per shot.
	spans = append(spans, fixtures.SpanProfile{
		Name: "SELECT orders", Kind: traceKindClient(), Service: "checkout",
		StartNanos: 1_700_000_000_500_000_000, DurationNanos: uint64(baseMs) * 1_000_000,
		Error: errorEvery > 0, DBSystem: "postgresql", TraceID: traceID, SpanID: []byte{0, 0, 0, 0, 0, 0, 0, 0xff},
	})
	return spans
}

// ---------------------------------------------------------------------------
// end-to-end: restore from "R2", query, analyze, compare
// ---------------------------------------------------------------------------

func TestFrozenShotRestoreQueryAnalyzeCompare(t *testing.T) {
	ctx := context.Background()
	minio := t.TempDir() // stands in for the local R2-compatible bucket

	baselineSpans := syntheticProfiles(5, 0)
	regressedSpans := syntheticProfiles(50, 4)
	baselinePath := writeShotDB(t, minio, "baseline", []*storage.Batch{
		benchTraceBatch(t, "baseline", baselineSpans),
		benchLogBatch(t, "baseline", []fixtures.LogProfile{
			{TimeNanos: 1_700_000_000_100_000_000, SeverityText: "INFO", Body: "ok", TraceID: baselineSpans[0].TraceID, Service: "checkout"},
		}),
	})
	regressedPath := writeShotDB(t, minio, "regressed", []*storage.Batch{
		benchTraceBatch(t, "regressed", regressedSpans),
		benchLogBatch(t, "regressed", []fixtures.LogProfile{
			{TimeNanos: 1_700_000_000_100_000_000, SeverityText: "ERROR", Body: "boom", TraceID: regressedSpans[0].TraceID, Service: "checkout"},
		}),
	})

	baseSHA := fileSHA(t, baselinePath)
	regSHA := fileSHA(t, regressedPath)

	store := NewStore(filepath.Join(t.TempDir(), "restore"), StoreOptions{Open: OpenOptions{MemoryLimit: "256MB", Threads: 2}})
	t.Cleanup(func() { _ = store.Close() })

	restore := func(id, path, sha string) *RestoreInfo {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		info, err := store.Restore(ctx, id, f, 0, sha)
		if err != nil {
			t.Fatalf("restore %s: %v", id, err)
		}
		return info
	}

	baseInfo := restore("baseline", baselinePath, baseSHA)
	if baseInfo.Reused || baseInfo.SHA256 != baseSHA || baseInfo.SchemaVersion != "1" {
		t.Fatalf("unexpected restore info: %+v", baseInfo)
	}
	// Idempotent when the checksum matches: the same engine is reused.
	again := restore("baseline", baselinePath, baseSHA)
	if !again.Reused {
		t.Fatalf("expected reuse for identical restore, got %+v", again)
	}
	regInfo := restore("regressed", regressedPath, regSHA)

	baseEng, err := store.Engine("baseline")
	if err != nil {
		t.Fatal(err)
	}
	regEng, err := store.Engine("regressed")
	if err != nil {
		t.Fatal(err)
	}
	if len(baseEng.ConfigNotes()) != 0 {
		t.Fatalf("unexpected config notes: %v", baseEng.ConfigNotes())
	}

	// Constrained query over a frozen shot with no recording container involved.
	qr, err := baseEng.Query(ctx, store.Policy(), QueryRequest{
		SQL: `SELECT service_name, count(*) AS n FROM spans GROUP BY service_name ORDER BY n DESC`,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if qr.RowCount == 0 {
		t.Fatalf("expected rows, got %+v", qr)
	}

	// WITH ... SELECT is wrapped and accepted; a CTE name is a valid table ref.
	if _, err := baseEng.Query(ctx, store.Policy(), QueryRequest{
		SQL: `WITH per_op AS (SELECT name, count(*) AS n FROM spans GROUP BY name) SELECT name, n FROM per_op ORDER BY n DESC`,
	}); err != nil {
		t.Fatalf("CTE query: %v", err)
	}

	// Analyze both.
	baseAnalysis, err := baseEng.Analyze(ctx, store.Policy(), "baseline", AnalyzeRequest{TopN: 10})
	if err != nil {
		t.Fatalf("analyze baseline: %v", err)
	}
	regAnalysis, err := regEng.Analyze(ctx, store.Policy(), "regressed", AnalyzeRequest{TopN: 10})
	if err != nil {
		t.Fatalf("analyze regressed: %v", err)
	}
	if baseAnalysis.RawCounts["spans"] == 0 || regAnalysis.RawCounts["spans"] == 0 {
		t.Fatalf("raw counts missing: %+v %+v", baseAnalysis.RawCounts, regAnalysis.RawCounts)
	}

	// SQL-derived compare.
	cmp, err := CompareEngines(ctx, store.Policy(), baseEng, regEng, "baseline", "regressed", 10)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if cmp.SampleSizes.Left["spans"] != cmp.Left.Counts["spans"] {
		t.Fatalf("sample size mismatch")
	}
	var foundLatency bool
	for _, d := range cmp.Latency {
		if strings.Contains(d.Operation, "GET /checkout") {
			foundLatency = true
			if d.DeltaP95 <= 0 {
				t.Fatalf("expected regressed p95 to exceed baseline, got %+v", d)
			}
			if d.RightN == 0 || d.LeftN == 0 {
				t.Fatalf("expected sample sizes on both sides: %+v", d)
			}
			if d.Unit != "ms" {
				t.Fatalf("expected ms unit, got %q", d.Unit)
			}
		}
	}
	if !foundLatency {
		t.Fatalf("no latency delta for GET /checkout: %+v", cmp.Latency)
	}
	// Error concentration increased.
	var errDelta int64
	for _, d := range cmp.Errors {
		errDelta += d.DeltaErrors
	}
	if errDelta <= 0 {
		t.Fatalf("expected more errors in regressed shot, deltas=%+v", cmp.Errors)
	}
	if len(cmp.Disclosure) == 0 {
		t.Fatalf("compare must disclose caveats")
	}

	// Raw OTLP bytes and the frozen file itself are unchanged by analysis.
	if got := fileSHA(t, baselinePath); got != baseSHA {
		t.Fatalf("baseline file mutated: %s != %s", got, baseSHA)
	}
	if got := fileSHA(t, regressedPath); got != regSHA {
		t.Fatalf("regressed file mutated: %s != %s", got, regSHA)
	}
	assertPayloadRoundTrip(t, baselinePath, benchTraceBatch(t, "baseline", baselineSpans).Payload)

	_ = regInfo
}

// assertPayloadRoundTrip opens the raw file read-only and proves the stored raw
// OTLP payload bytes are byte-for-byte unchanged.
func assertPayloadRoundTrip(t *testing.T, path string, want []byte) {
	t.Helper()
	db, err := sql.Open("duckdb", path+"?access_mode=read_only&threads=2")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got []byte
	if err := db.QueryRowContext(context.Background(),
		"SELECT payload FROM otlp_batches WHERE signal = 'traces' ORDER BY ordinal LIMIT 1").Scan(&got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if hex.EncodeToString(got) != hex.EncodeToString(want) {
		t.Fatalf("raw OTLP payload changed")
	}
}

// ---------------------------------------------------------------------------
// adversarial SQL
// ---------------------------------------------------------------------------

func TestPolicyRejectsHostileSQL(t *testing.T) {
	p := DefaultPolicy()
	cases := []struct {
		name string
		sql  string
		code string
	}{
		{"multiple statements", "SELECT 1; DROP TABLE spans", "sql_multi_statement"},
		{"drop", "DROP TABLE spans", "sql_not_select"},
		{"insert", "INSERT INTO spans VALUES (1)", "sql_not_select"},
		{"update", "SELECT 1; UPDATE spans SET name='x'", "sql_multi_statement"},
		{"attach", "SELECT * FROM spans ATTACH DATABASE '/tmp/x.db'", "sql_forbidden_keyword"},
		{"copy", "COPY (SELECT 1) TO '/tmp/out.csv'", "sql_not_select"},
		{"install", "SELECT 1; INSTALL httpfs", "sql_multi_statement"},
		{"pragma", "PRAGMA database_list", "sql_not_select"},
		{"set", "SET memory_limit='1TB'", "sql_not_select"},
		{"read_csv", "SELECT * FROM read_csv_auto('/etc/passwd')", "sql_table_not_allowed"},
		{"read_parquet", "SELECT * FROM read_parquet('/etc/shadow')", "sql_table_not_allowed"},
		{"glob", "SELECT * FROM glob('/etc/*')", "sql_table_not_allowed"},
		{"read_blob_fn", "SELECT read_blob('/etc/passwd')", "sql_function_not_allowed"},
		{"getenv", "SELECT getenv('AWS_SECRET_ACCESS_KEY')", "sql_function_not_allowed"},
		{"duckdb_settings", "SELECT * FROM duckdb_settings()", "sql_table_not_allowed"},
		{"httpfs", "SELECT * FROM read_csv('http://evil/x.csv')", "sql_table_not_allowed"},
		{"unknown_table", "SELECT * FROM secrets", "sql_table_not_allowed"},
		{"information_schema", "SELECT * FROM information_schema.tables", "sql_table_not_allowed"},
		{"derived_table", "SELECT * FROM (SELECT * FROM spans) t", "sql_derived_table"},
		{"recursive", "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t", "sql_recursive"},
		{"unknown_function", "SELECT evil_func(1)", "sql_function_not_allowed"},
		{"empty", "   ", "sql_empty"},
		{"comment only", "-- nothing here", "sql_empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.Validate(tc.sql)
			if err == nil {
				t.Fatalf("expected rejection for %q", tc.sql)
			}
			pe, ok := err.(*PolicyError)
			if !ok {
				t.Fatalf("expected PolicyError, got %T: %v", err, err)
			}
			if pe.Code != tc.code {
				t.Fatalf("code = %q, want %q (msg: %s)", pe.Code, tc.code, pe.Message)
			}
		})
	}
}

func TestPolicyAcceptsUsefulQueriesAndAppliesLimit(t *testing.T) {
	p := DefaultPolicy()
	cases := []string{
		"SELECT name, (end_time_unix_nano - start_time_unix_nano) AS dur_ns FROM spans ORDER BY dur_ns DESC",
		"SELECT count(*) FROM spans WHERE status_code = 'STATUS_CODE_ERROR'",
		"SELECT l.trace_id, l.body FROM log_records l JOIN spans s ON l.trace_id = s.trace_id LIMIT 5",
		"SELECT metric_name, avg(value_double) FROM metric_points GROUP BY metric_name",
		"WITH a AS (SELECT 1 AS x) SELECT x FROM a",
		"SELECT replace(name, 'GET ', '') AS op FROM spans",
	}
	for _, sqlText := range cases {
		safe, err := p.Validate(sqlText)
		if err != nil {
			t.Fatalf("unexpected rejection of %q: %v", sqlText, err)
		}
		if !strings.Contains(safe, "_burstel_sub") || !strings.HasSuffix(safe, fmt.Sprintf("LIMIT %d", p.MaxRows)) {
			t.Fatalf("expected enforced outer limit in %q", safe)
		}
	}
	// Semicolon inside a string literal must not split statements.
	if _, err := p.Validate("SELECT name FROM spans WHERE name = 'a;b'"); err != nil {
		t.Fatalf("string with semicolon rejected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// tamper detection
// ---------------------------------------------------------------------------

func TestRestoreVerifiesSizeAndChecksum(t *testing.T) {
	ctx := context.Background()
	path := writeShotDB(t, t.TempDir(), "tamper", []*storage.Batch{benchTraceBatch(t, "tamper", syntheticProfiles(5, 0))})
	want := fileSHA(t, path)

	store := NewStore(t.TempDir(), StoreOptions{})
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.Restore(ctx, "tamper", strings.NewReader("not a duckdb file"), 0, want); err == nil {
		t.Fatalf("expected checksum failure")
	}
	if _, err := store.Restore(ctx, "tamper", strings.NewReader("not a duckdb file"), 3, strings.Repeat("0", 64)); err == nil {
		t.Fatalf("expected invalid duckdb failure or size mismatch")
	}
	if _, err := store.Restore(ctx, "bad id", strings.NewReader("x"), 1, ""); err == nil {
		t.Fatalf("expected invalid restore id rejection")
	}
	// A missing artifact from the bucket is a not-found at the caller; here we
	// verify an empty stream is rejected.
	if _, err := store.Restore(ctx, "empty", strings.NewReader(""), 0, want); err == nil {
		t.Fatalf("expected empty artifact rejection")
	}
}

func TestUnknownRestoreIsNotFound(t *testing.T) {
	store := NewStore(t.TempDir(), StoreOptions{})
	defer store.Close()
	if _, err := store.Engine("nope"); err != ErrRestoreNotFound {
		t.Fatalf("err = %v, want ErrRestoreNotFound", err)
	}
}
