package compaction

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// ingestWorkload writes a bounded load of traces/logs/metrics batches.
func ingestWorkload(tb testing.TB, env *testEnv, instances []string, batches int) {
	tb.Helper()
	base := env.clock.Now()
	for i := 0; i < batches; i++ {
		at := base.Add(time.Duration(i) * time.Second)
		env.clock.Set(at)
		inst := instances[i%len(instances)]
		ingestSignalsTB(tb, env, inst, "svc-"+inst, at, i, 4, 2)
	}
}

func ingestSignalsTB(tb testing.TB, env *testEnv, instanceID, service string, at time.Time, tick int, spansPer, logsPer int) map[string]string {
	tb.Helper()
	ctx := context.Background()
	// Reuse the t-based helper shape but tolerate *testing.B.
	tracesWire := spanBatch(service, spansPer, uint64(at.UnixNano()))
	if _, err := WriteJournal(ctx, env.store, instanceID, instanceID, "traces", at, fmt.Sprintf("t%d-traces", tick), tracesWire, ""); err != nil {
		tb.Fatalf("write traces: %v", err)
	}
	logsWire := logBatch(service, logsPer, uint64(at.UnixNano()))
	if _, err := WriteJournal(ctx, env.store, instanceID, instanceID, "logs", at, fmt.Sprintf("t%d-logs", tick), logsWire, ""); err != nil {
		tb.Fatalf("write logs: %v", err)
	}
	metricsWire := metricBatch()
	if _, err := WriteJournal(ctx, env.store, instanceID, instanceID, "metrics", at, fmt.Sprintf("t%d-metrics", tick), metricsWire, ""); err != nil {
		tb.Fatalf("write metrics: %v", err)
	}
	return map[string]string{}
}

// BenchmarkCompactionThroughput measures end-to-end ingest+compact throughput
// and surfaces the approximate R2 PUT cost.
func BenchmarkCompactionThroughput(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Window = 30 * time.Second
	cfg.Grace = 5 * time.Second
	cfg.WorkDir = b.TempDir()

	b.ResetTimer()
	var records, putBytes int64
	for i := 0; i < b.N; i++ {
		dir := filepath.Join(b.TempDir(), fmt.Sprintf("run-%d", i))
		store, err := NewFakeR2(filepath.Join(dir, "r2"))
		if err != nil {
			b.Fatal(err)
		}
		manifest, err := OpenManifest(context.Background(), filepath.Join(dir, "manifest.duckdb"))
		if err != nil {
			b.Fatal(err)
		}
		clock := newTestClock(time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC))
		comp := New(store, manifest, cfg)
		comp.SetClock(clock.Now)
		env := &testEnv{t: b, dir: dir, store: store, manifest: manifest, comp: comp, clock: clock}

		ingestWorkload(b, env, []string{"node-1", "node-2", "node-3"}, 300)
		clock.Advance(10 * time.Minute)
		st, err := comp.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		records += st.RecordsCovered
		putBytes += store.Calls().PutBytes
		_ = manifest.Close()
	}
	secs := b.Elapsed().Seconds()
	calls := StoreCalls{Put: int64(b.N) * 3, PutBytes: putBytes}
	cost := DefaultCostModel().EstimateCost(calls, putBytes)
	b.ReportMetric(float64(records)/secs, "records/s")
	b.ReportMetric(float64(putBytes)/secs, "bytes/s")
	b.ReportMetric(cost.TotalPerRun*1e6, "usd-per-Mput")
}

// TestCostSurface documents the approximate R2 cost surface on a bounded load.
func TestCostSurface(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Window = 30 * time.Second
	cfg.Grace = 5 * time.Second
	env := newTestEnv(t, cfg)
	ingestWorkload(t, env, []string{"node-1", "node-2", "node-3"}, 200)
	env.clock.Advance(10 * time.Minute)
	st, err := env.comp.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	calls := env.store.Calls()
	cost := DefaultCostModel().EstimateCost(calls, st.BytesCovered)
	t.Logf("bounded load: %d records, %d bytes covered in %s; %d PUTs, %d GETs, %d HEADs, %d LISTs",
		st.RecordsCovered, st.BytesCovered, st.Duration, calls.Put, calls.Get, calls.Head, calls.List)
	t.Logf("%s", cost.String())
	if cost.ClassACost <= 0 {
		t.Fatal("expected a non-zero Class A (PUT) cost")
	}
}
