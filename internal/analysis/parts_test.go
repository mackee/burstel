package analysis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/mackee/burstel/internal/compaction"
	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
)

func TestScopedEngineAttachesMultipleMaterializationParts(t *testing.T) {
	ctx := context.Background()
	baseMS := int64(1_700_000_000_000)
	baseNS := uint64(baseMS) * 1_000_000

	build := func(name string, n int, offset uint64) SegmentRef {
		spans := make([]fixtures.SpanProfile, 0, n)
		for i := 0; i < n; i++ {
			traceID := make([]byte, 16)
			spanID := make([]byte, 8)
			traceID[0] = byte(i + 1)
			spanID[0] = byte(i + 1)
			spans = append(spans, fixtures.SpanProfile{
				Name:          "op",
				Service:       "svc",
				StartNanos:    baseNS + offset + uint64(i)*1_000_000,
				DurationNanos: 1_000_000,
				TraceID:       traceID,
				SpanID:        spanID,
			})
		}
		wire, err := proto.Marshal(fixtures.TraceExportProfiled(spans))
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := otlp.Decode(otlp.Traces, wire)
		if err != nil {
			t.Fatal(err)
		}
		rec := compaction.JournalRecord{
			Object: compaction.JournalObject{
				Key:          "journal/inst-1/traces/" + name,
				InstanceID:   "inst-1",
				Instance:     "node-1",
				Signal:       otlp.Traces,
				ReceivedAt:   time.UnixMilli(baseMS + int64(offset)/1_000_000).UTC(),
				ReceivedAtMS: baseMS + int64(offset)/1_000_000,
			},
			Wire: wire, Payload: wire, Decoded: decoded,
		}
		path := filepath.Join(t.TempDir(), name+".duckdb")
		if _, err := compaction.BuildPart(ctx, path, "shots/shot-1/"+name+".duckdb", "", "", []compaction.JournalRecord{rec}); err != nil {
			t.Fatalf("build part: %v", err)
		}
		return SegmentRef{Key: "shots/shot-1/" + name + ".duckdb", Instance: "node-1", Path: path}
	}

	refs := []SegmentRef{
		build("part-0", 3, 0),
		build("part-1", 4, 10_000_000),
	}
	window := NewServerWindow(
		time.UnixMilli(baseMS).UTC(),
		time.UnixMilli(baseMS+60_000).UTC(),
		[]string{"node-1"},
		"parts",
	)
	scoped, err := BuildScopedEngine(ctx, window, refs, ScopedOptions{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("build scoped: %v", err)
	}
	defer scoped.Close()

	res, err := scoped.Engine.Query(ctx, ScopedPolicy(DefaultPolicy()), QueryRequest{SQL: "SELECT COUNT(*) AS n FROM spans"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	if got := res.Rows[0][0]; got != int64(7) && got != float64(7) && got != "7" {
		t.Fatalf("expected 7 spans across both parts, got %v (%T)", got, got)
	}
}

// Spec 034: a projection-only part has no otlp_batches table. Analysis must
// still report the batch/record volume (from part_meta) and run every section
// unchanged.
func TestProjectionOnlyPartAnalysis(t *testing.T) {
	ctx := context.Background()
	baseMS := int64(1_700_000_000_000)
	baseNS := uint64(baseMS) * 1_000_000

	spans := make([]fixtures.SpanProfile, 0, 8)
	for i := 0; i < 8; i++ {
		traceID := make([]byte, 16)
		spanID := make([]byte, 8)
		traceID[0] = byte(i + 1)
		spanID[0] = byte(i + 1)
		spans = append(spans, fixtures.SpanProfile{
			Name:          "GET /api",
			Service:       "svc",
			StartNanos:    baseNS + uint64(i)*1_000_000,
			DurationNanos: 2_000_000,
			TraceID:       traceID,
			SpanID:        spanID,
		})
	}
	wire, err := proto.Marshal(fixtures.TraceExportProfiled(spans))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := otlp.Decode(otlp.Traces, wire)
	if err != nil {
		t.Fatal(err)
	}
	rec := compaction.JournalRecord{
		Object: compaction.JournalObject{
			Key:          "journal/inst-1/traces/part-only.bin",
			InstanceID:   "inst-1",
			Instance:     "node-1",
			Signal:       otlp.Traces,
			ReceivedAt:   time.UnixMilli(baseMS).UTC(),
			ReceivedAtMS: baseMS,
		},
		WireSHA256:    compactionHash(wire),
		PayloadSHA256: compactionHash(wire),
		PayloadLength: int64(len(wire)),
		Decoded:       decoded,
	}
	path := filepath.Join(t.TempDir(), "part.duckdb")
	if _, err := compaction.BuildPart(ctx, path, "shots/shot-only/part-0.duckdb", "", "", []compaction.JournalRecord{rec}); err != nil {
		t.Fatalf("build part: %v", err)
	}
	window := NewServerWindow(
		time.UnixMilli(baseMS).UTC(),
		time.UnixMilli(baseMS+60_000).UTC(),
		[]string{"node-1"},
		"projection-only",
	)
	scoped, err := BuildScopedEngine(ctx, window, []SegmentRef{{Key: "shots/shot-only/part-0.duckdb", Instance: "node-1", Path: path}}, ScopedOptions{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("build scoped: %v", err)
	}
	defer scoped.Close()

	res, err := scoped.Engine.Query(ctx, ScopedPolicy(DefaultPolicy()), QueryRequest{
		SQL: "SELECT count(*) AS batches, coalesce(sum(record_count), 0) AS records FROM otlp_batches",
	})
	if err != nil {
		t.Fatalf("query part_meta projection: %v", err)
	}
	if got := asInt64(res.Rows[0][0]); got != 1 {
		t.Fatalf("batches = %d, want 1 (from part_meta)", got)
	}
	if got := asInt64(res.Rows[0][1]); got != 8 {
		t.Fatalf("records = %d, want 8 (from part_meta)", got)
	}
	if _, err := scoped.Engine.Analyze(ctx, ScopedPolicy(DefaultPolicy()), "shot-only", AnalyzeRequest{}); err != nil {
		t.Fatalf("analyze projection-only part: %v", err)
	}
}

func compactionHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
