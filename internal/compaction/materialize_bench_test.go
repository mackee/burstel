package compaction

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/mackee/burstel/internal/fixtures"
	"github.com/mackee/burstel/internal/otlp"
)

// isuconSpanBatch builds the ISUCON span shape used by burstel-loadgen: one
// HTTP server root span plus `dbSpans` database child spans per trace, each
// child carrying `db.system`.
func isuconSpanBatch(service string, traces, dbSpans int, base uint64) []byte {
	spans := make([]fixtures.SpanProfile, 0, traces*(dbSpans+1))
	for i := 0; i < traces; i++ {
		traceID := make([]byte, 16)
		rootID := make([]byte, 8)
		traceID[0] = byte(i)
		traceID[1] = byte(i >> 8)
		traceID[2] = byte(traces)
		rootID[0] = byte(i + 1)
		rootStart := base + uint64(i)*100_000
		spans = append(spans, fixtures.SpanProfile{
			Name:          "GET /api/condition",
			Kind:          tracepb.Span_SPAN_KIND_SERVER,
			Service:       service,
			StartNanos:    rootStart,
			DurationNanos: 20_000_000,
			TraceID:       traceID,
			SpanID:        rootID,
		})
		for d := 0; d < dbSpans; d++ {
			childID := make([]byte, 8)
			childID[0] = byte(d + 1)
			childID[1] = byte(i + 1)
			spans = append(spans, fixtures.SpanProfile{
				Name:          fmt.Sprintf("SELECT isucon_%d", d),
				Kind:          tracepb.Span_SPAN_KIND_CLIENT,
				Service:       service,
				StartNanos:    rootStart + uint64(d)*1_000_000,
				DurationNanos: 3_000_000,
				DBSystem:      "mysql",
				TraceID:       traceID,
				SpanID:        childID,
				ParentSpanID:  rootID,
			})
		}
	}
	raw, err := proto.Marshal(fixtures.TraceExportProfiled(spans))
	if err != nil {
		panic(err)
	}
	return raw
}

func gzipWire(b *testing.B, raw []byte) []byte {
	b.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		b.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// BenchmarkMaterializePartThroughput measures one projection-only part build
// (GET + gunzip + decode + parallel append + CHECKPOINT + hash + PUT) with the
// ISUCON span shape (spec 034, R6). The store is in-memory, so the number is
// the build ceiling per container, not an R2-bound number.
//
// Target: >= 300,000 spans/s on a 4-vCPU host. Record GOMAXPROCS alongside the
// result (`-cpu 4` on a larger host).
func BenchmarkMaterializePartThroughput(b *testing.B) {
	const (
		objects        = 32
		tracesPerBatch = 500
		dbSpans        = 5
	)
	const spansPerBatch = tracesPerBatch * (dbSpans + 1)

	remote := newMemoryRemote()
	objList := make([]MaterializeObject, 0, objects)
	var gzipBytes int64
	for i := 0; i < objects; i++ {
		raw := isuconSpanBatch("bench", tracesPerBatch, dbSpans, 1_700_000_000_000_000_000+uint64(i)*1_000_000_000)
		wire := gzipWire(b, raw)
		sum := sha256.Sum256(wire)
		key := fmt.Sprintf("journal/inst-1/traces/%d-%02d.bin", 1_700_000_000_000+i, i)
		remote.objects[key] = wire
		gzipBytes += int64(len(wire))
		objList = append(objList, MaterializeObject{
			Key:             key,
			Signal:          "traces",
			ReceivedAt:      1_700_000_000_000 + int64(i),
			ContentEncoding: "gzip",
			ContentType:     "application/x-protobuf",
			Size:            int64(len(wire)),
			SHA256:          hex.EncodeToString(sum[:]),
			InstanceID:      "inst-1",
			Instance:        "node-1",
		})
	}

	svc := NewServiceWithRemote(DefaultServiceConfig(), remote, 32)
	svc.cfg.WorkDir = b.TempDir()

	b.SetBytes(gzipBytes)
	b.ReportAllocs()
	b.ResetTimer()
	var last MaterializeResponse
	for i := 0; i < b.N; i++ {
		resp, err := svc.Materialize(context.Background(), MaterializeRequest{
			ShotID:    "bench-shot",
			PartIndex: 0,
			PartTotal: 1,
			Objects:   objList,
		})
		if err != nil {
			b.Fatalf("materialize: %v", err)
		}
		last = resp
	}
	elapsed := b.Elapsed().Seconds()
	spans := float64(spansPerBatch*objects) * float64(b.N)
	b.ReportMetric(spans/elapsed, "spans/s")
	b.ReportMetric(float64(b.N)/elapsed, "parts/s")
	b.ReportMetric(float64(last.SizeBytes)/1024, "KiB/part")
	b.Logf("GOMAXPROCS=%d part_spans=%d part_bytes=%d gzip_bytes=%d get_ms=%d decode_ms=%d append_ms=%d checkpoint_ms=%d put_ms=%d",
		runtime.GOMAXPROCS(0), spansPerBatch*objects, last.SizeBytes, gzipBytes,
		last.Timings.GetMS, last.Timings.DecodeMS, last.Timings.AppendMS, last.Timings.CheckpointMS, last.Timings.PutMS)
}

// BenchmarkPartBuildLayout compares the projection-only part layout (spec 034)
// against the pre-034 raw layout (wire+payload BLOBs and three secondary
// indexes) for the exact same decoded records. It isolates the build phases from
// R2 I/O so the effect of the layout change is visible.
func BenchmarkPartBuildLayout(b *testing.B) {
	const (
		objects        = 16
		tracesPerBatch = 500
		dbSpans        = 5
	)
	records := make([]JournalRecord, 0, objects)
	for i := 0; i < objects; i++ {
		raw := isuconSpanBatch("bench", tracesPerBatch, dbSpans, 1_700_000_000_000_000_000+uint64(i)*1_000_000_000)
		wire := gzipWire(b, raw)
		decoded, err := otlp.Decode(otlp.Traces, raw)
		if err != nil {
			b.Fatal(err)
		}
		records = append(records, JournalRecord{
			Object: JournalObject{
				Key:          fmt.Sprintf("journal/inst-1/traces/%d-%02d.bin", 1_700_000_000_000+i, i),
				InstanceID:   "inst-1",
				Signal:       otlp.Traces,
				ReceivedAt:   time.UnixMilli(1_700_000_000_000 + int64(i)).UTC(),
				ReceivedAtMS: 1_700_000_000_000 + int64(i),
			},
			Wire:          wire,
			Payload:       raw,
			Decoded:       decoded,
			WireSHA256:    sha256Hex(wire),
			PayloadSHA256: sha256Hex(raw),
			PayloadLength: int64(len(raw)),
		})
	}
	const totalSpans = objects * tracesPerBatch * (dbSpans + 1)

	for _, layout := range []string{"projection_only", "raw_with_indexes"} {
		b.Run(layout, func(b *testing.B) {
			dir := b.TempDir()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				path := fmt.Sprintf("%s/%s-%d.duckdb", dir, layout, i)
				var err error
				if layout == "projection_only" {
					_, err = buildPart(context.Background(), path, "shots/bench/part-0.duckdb", "", "", records, nil)
				} else {
					_, err = buildSegment(context.Background(), path, "segments/bench.duckdb", "", "", records, true, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(totalSpans*b.N)/b.Elapsed().Seconds(), "spans/s")
		})
	}
}
