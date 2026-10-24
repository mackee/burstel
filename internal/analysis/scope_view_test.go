package analysis

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/mackee/burstel/internal/compaction"
	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
)

// partFixture builds a projection-only part (spec 034) with the fixture trace,
// log and metric payloads. Several of these make a multi-part shot.
type partFixture struct {
	path string
	info compaction.SegmentInfo
	meta SegmentMeta
}

func buildFixturePart(t *testing.T, dir, name string, receivedAt time.Time, spans []fixtures.SpanProfile, traces, logs, metrics bool) partFixture {
	t.Helper()
	var recs []compaction.JournalRecord
	add := func(signal otlp.Signal, wire []byte) {
		decoded, err := otlp.Decode(signal, wire)
		if err != nil {
			t.Fatalf("decode %s: %v", signal, err)
		}
		recs = append(recs, compaction.JournalRecord{
			Object: compaction.JournalObject{
				Key:          "journal/inst-1/" + string(signal) + "/" + name + ".bin",
				InstanceID:   "inst-1",
				Instance:     "node-1",
				Signal:       signal,
				ReceivedAt:   receivedAt,
				ReceivedAtMS: receivedAt.UnixMilli(),
			},
			WireSHA256:    compactionHash(wire),
			PayloadSHA256: compactionHash(wire),
			PayloadLength: int64(len(wire)),
			Decoded:       decoded,
		})
	}
	if traces {
		wire, err := proto.Marshal(fixtures.TraceExportProfiled(spans))
		if err != nil {
			t.Fatal(err)
		}
		add(otlp.Traces, wire)
	}
	if logs {
		wire, err := proto.Marshal(fixtures.LogsExport())
		if err != nil {
			t.Fatal(err)
		}
		add(otlp.Logs, wire)
	}
	if metrics {
		wire, err := proto.Marshal(fixtures.MetricsExport())
		if err != nil {
			t.Fatal(err)
		}
		add(otlp.Metrics, wire)
	}
	key := "shots/shot-1/" + name + ".duckdb"
	path := filepath.Join(dir, name+".duckdb")
	info, err := compaction.BuildPart(context.Background(), path, key, "", "", recs)
	if err != nil {
		t.Fatalf("build part %s: %v", name, err)
	}
	return partFixture{
		path: path,
		info: info,
		meta: SegmentMeta{
			Key:             key,
			InstanceID:      "all",
			Status:          "verified",
			Size:            info.Size,
			SHA256:          info.SHA256,
			FirstReceivedAt: info.FirstReceivedAt,
			LastReceivedAt:  info.LastReceivedAt,
		},
	}
}

func spanProfiles(baseNS uint64, n int, offset uint64) []fixtures.SpanProfile {
	out := make([]fixtures.SpanProfile, 0, n)
	for i := 0; i < n; i++ {
		traceID := make([]byte, 16)
		spanID := make([]byte, 8)
		traceID[0] = byte(i + 1)
		spanID[0] = byte(i + 1)
		out = append(out, fixtures.SpanProfile{
			Name:          "op",
			Service:       "svc",
			StartNanos:    baseNS + offset + uint64(i)*1_000_000,
			DurationNanos: 1_000_000,
			TraceID:       traceID,
			SpanID:        spanID,
		})
	}
	return out
}

// Spec 035 acceptance: the zero-copy view path returns identical results to the
// copy path for a multi-part fixture (spans, logs, metrics and part_meta
// batches).
func TestScopedViewMatchesCopyOnMultiPartFixture(t *testing.T) {
	ctx := context.Background()
	baseMS := int64(1_700_000_000_000)
	baseNS := uint64(baseMS) * 1_000_000
	dir := t.TempDir()
	received := time.UnixMilli(baseMS + 1_000).UTC()

	parts := []partFixture{
		buildFixturePart(t, dir, "part-0", received, spanProfiles(baseNS, 3, 0), true, true, true),
		buildFixturePart(t, dir, "part-1", received, spanProfiles(baseNS, 4, 10_000_000), true, false, false),
		buildFixturePart(t, dir, "part-2", received, spanProfiles(baseNS, 2, 20_000_000), true, false, false),
	}
	var refs []SegmentRef
	for _, part := range parts {
		refs = append(refs, SegmentRef{Key: part.meta.Key, Instance: "node-1", Path: part.path})
	}
	window := NewServerWindow(
		time.UnixMilli(baseMS).UTC(),
		time.UnixMilli(baseMS+60_000).UTC(),
		[]string{"node-1"},
		"zero-copy",
	)

	build := func(mode ScopedMode) *ScopedEngine {
		scoped, err := BuildScopedEngine(ctx, window, refs, ScopedOptions{WorkDir: t.TempDir(), Mode: mode})
		if err != nil {
			t.Fatalf("build %s: %v", mode, err)
		}
		t.Cleanup(func() { _ = scoped.Close() })
		return scoped
	}
	view := build(ScopedModeView)
	copied := build(ScopedModeCopy)

	if view.Timings.MaterializeMS != 0 {
		t.Fatalf("view path must not materialize, got %+v", view.Timings)
	}
	if copied.Timings.MaterializeMS <= 0 {
		// A tiny fixture can complete in <1 ms; only assert the copy path ran.
		t.Logf("copy timings %+v", copied.Timings)
	}

	queries := []string{
		"SELECT count(*) FROM spans",
		"SELECT count(*) FROM otlp_batches",
		"SELECT coalesce(sum(record_count), 0) FROM otlp_batches",
		"SELECT count(*) FROM log_records",
		"SELECT count(*) FROM metric_points",
		"SELECT value FROM schema_meta WHERE key = 'window_start_ns'",
		"SELECT service_name, count(*) c FROM spans GROUP BY 1 ORDER BY 1",
	}
	for _, sql := range queries {
		v, err := view.Engine.Query(ctx, ScopedPolicy(DefaultPolicy()), QueryRequest{SQL: sql})
		if err != nil {
			t.Fatalf("view query %q: %v", sql, err)
		}
		c, err := copied.Engine.Query(ctx, ScopedPolicy(DefaultPolicy()), QueryRequest{SQL: sql})
		if err != nil {
			t.Fatalf("copy query %q: %v", sql, err)
		}
		vj, _ := json.Marshal(v.Rows)
		cj, _ := json.Marshal(c.Rows)
		if string(vj) != string(cj) {
			t.Fatalf("query %q differs:\n view=%s\n copy=%s", sql, vj, cj)
		}
	}

	vOut, err := view.Engine.Analyze(ctx, ScopedPolicy(DefaultPolicy()), "shot-1", AnalyzeRequest{})
	if err != nil {
		t.Fatalf("view analyze: %v", err)
	}
	cOut, err := copied.Engine.Analyze(ctx, ScopedPolicy(DefaultPolicy()), "shot-1", AnalyzeRequest{})
	if err != nil {
		t.Fatalf("copy analyze: %v", err)
	}
	vOut.Timings = nil
	cOut.Timings = nil
	canonicalizeSectionRows(t, vOut)
	canonicalizeSectionRows(t, cOut)
	vj, _ := json.Marshal(vOut)
	cj, _ := json.Marshal(cOut)
	if string(vj) != string(cj) {
		t.Fatalf("analyze sections differ:\n view=%s\n copy=%s", vj, cj)
	}
}

// canonicalizeSectionRows removes tie-order nondeterminism (for example equal
// duration slow spans) so the two builds can be compared byte-for-byte.
func canonicalizeSectionRows(t *testing.T, result *AnalyzeResult) {
	t.Helper()
	for i := range result.Sections {
		rows := result.Sections[i].Rows
		encoded := make([]string, 0, len(rows))
		byKey := make(map[string][]any, len(rows))
		for _, row := range rows {
			b, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			key := string(b)
			encoded = append(encoded, key)
			byKey[key] = row
		}
		sort.Strings(encoded)
		sorted := make([][]any, 0, len(rows))
		for _, key := range encoded {
			sorted = append(sorted, byKey[key])
		}
		result.Sections[i].Rows = sorted
	}
}

// fakeFetcher serves part bytes from memory and records concurrency.
type fakeFetcher struct {
	mu        sync.Mutex
	data      map[string][]byte
	delay     time.Duration
	calls     int
	active    int
	maxActive int
}

func (f *fakeFetcher) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	f.mu.Lock()
	f.calls++
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	data := f.data[key]
	delay := f.delay
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if data == nil {
		return 0, os.ErrNotExist
	}
	n, err := w.Write(data)
	return int64(n), err
}

func (f *fakeFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestWindowStoreDirectDownloadVerifiesChecksum(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	window := NewServerWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "download")
	dir := t.TempDir()
	part := buildFixturePart(t, dir, "part-0", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 2, 0), true, false, false)
	raw, err := os.ReadFile(part.path)
	if err != nil {
		t.Fatal(err)
	}

	// Wrong bytes / wrong checksum must be rejected before the part is cached.
	bad := &fakeFetcher{data: map[string][]byte{part.meta.Key: []byte("not a duckdb file")}}
	store := NewWindowStore(filepath.Join(dir, "windows-bad"), WindowStoreOptions{Fetcher: bad})
	if _, err := store.OpenWindow("s1", window, []SegmentMeta{part.meta}); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, _, _, _, err := store.Prefetch(ctx, "s1", window, []SegmentMeta{part.meta}); err == nil {
		t.Fatal("expected a checksum/size failure for corrupt part bytes")
	}
	if _, _, ok := store.cachedSegment(part.meta.Key, part.meta.Size, part.meta.SHA256); ok {
		t.Fatal("corrupt part must not be cached")
	}
	_ = store.Close()

	good := &fakeFetcher{data: map[string][]byte{part.meta.Key: raw}}
	store = NewWindowStore(filepath.Join(dir, "windows"), WindowStoreOptions{Fetcher: good})
	defer store.Close()
	_, keys, cov, timings, err := store.Prefetch(ctx, "s1", window, []SegmentMeta{part.meta})
	if err != nil {
		t.Fatalf("prefetch: %v", err)
	}
	if len(keys) != 1 || keys[0] != part.meta.Key {
		t.Fatalf("cached keys = %v", keys)
	}
	if !cov.Complete || timings.AttachMS < 0 {
		t.Fatalf("coverage/timings = %+v / %+v", cov, timings)
	}
	if _, _, ok := store.cachedSegment(part.meta.Key, part.meta.Size, part.meta.SHA256); !ok {
		t.Fatal("verified part must be cached")
	}
}

func TestWindowStoreReusesAttachedSession(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	window := NewServerWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "reuse")
	dir := t.TempDir()
	parts := []partFixture{
		buildFixturePart(t, dir, "part-0", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 2, 0), true, false, false),
		buildFixturePart(t, dir, "part-1", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 3, 10_000_000), true, false, false),
	}
	raw := map[string][]byte{}
	catalog := make([]SegmentMeta, 0, len(parts))
	for _, part := range parts {
		b, err := os.ReadFile(part.path)
		if err != nil {
			t.Fatal(err)
		}
		raw[part.meta.Key] = b
		catalog = append(catalog, part.meta)
	}
	fetcher := &fakeFetcher{data: raw, delay: 5 * time.Millisecond}
	store := NewWindowStore(filepath.Join(dir, "windows"), WindowStoreOptions{Fetcher: fetcher, FetchParallelism: 2})
	defer store.Close()

	if _, _, err := store.OpenWindowWithCache(ctx, "s1", window, catalog, true); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, _, err := store.CommitWithTimings(ctx, "s1"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := fetcher.callCount(); got != len(parts) {
		t.Fatalf("downloads = %d, want %d", got, len(parts))
	}

	// The same shot with the same parts must reuse the attached session: no
	// download and no re-ATTACH at commit.
	_, cached, err := store.OpenWindowWithCache(ctx, "s1", window, catalog, true)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(cached) != len(parts) {
		t.Fatalf("reused cached keys = %v", cached)
	}
	if got := fetcher.callCount(); got != len(parts) {
		t.Fatalf("reopen re-downloaded parts: calls = %d", got)
	}
	_, timings, err := store.CommitWithTimings(ctx, "s1")
	if err != nil {
		t.Fatalf("recommit: %v", err)
	}
	if !timings.Reused {
		t.Fatalf("expected an unchanged session to reuse attached views, got %+v", timings)
	}
	res, err := store.Query(ctx, "s1", QueryRequest{SQL: "SELECT count(*) FROM spans"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := asInt64(res.Rows[0][0]); got != 5 {
		t.Fatalf("spans = %d, want 5", got)
	}
}

func TestWindowStoreDirectDownloadBoundedParallelism(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	window := NewServerWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "bounded")
	dir := t.TempDir()
	raw := map[string][]byte{}
	var catalog []SegmentMeta
	for i := 0; i < 4; i++ {
		part := buildFixturePart(t, dir, fmt.Sprintf("part-%d", i), now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 1, uint64(i)*1_000_000), true, false, false)
		b, err := os.ReadFile(part.path)
		if err != nil {
			t.Fatal(err)
		}
		raw[part.meta.Key] = b
		catalog = append(catalog, part.meta)
	}
	fetcher := &fakeFetcher{data: raw, delay: 10 * time.Millisecond}
	store := NewWindowStore(filepath.Join(dir, "windows"), WindowStoreOptions{Fetcher: fetcher, FetchParallelism: 2})
	defer store.Close()
	if _, _, err := store.OpenWindowWithCache(ctx, "s1", window, catalog, true); err != nil {
		t.Fatalf("open: %v", err)
	}
	if fetcher.maxActive > 2 {
		t.Fatalf("concurrent downloads = %d, want <= 2", fetcher.maxActive)
	}
}
