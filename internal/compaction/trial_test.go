package compaction

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// runTrial drives a full ingest span with multiple instances and no active
// shots, compacting periodically, then makes the strong end-to-end assertions:
// every ACKed raw object is covered exactly once, byte-for-byte, counts
// reconcile, GC frees the journal without losing bytes, and a read-only restore
// works after the writer processes have exited.
func runTrial(t *testing.T, cfg Config, instances []string, span, tick, compactEvery time.Duration, realTime bool) {
	t.Helper()
	ctx := context.Background()
	env := newTestEnv(t, cfg)
	base := env.clock.Now()
	exp := newExpected()

	steps := int(span / tick)
	for i := 0; i < steps; i++ {
		elapsed := time.Duration(i) * tick
		if realTime {
			env.clock.Set(time.Now())
		} else {
			env.clock.Set(base.Add(elapsed))
		}
		at := env.clock.Now()
		for _, inst := range instances {
			hashes := ingestSignals(t, env, inst, "svc-"+inst, at, i, 4, 2)
			exp.add(inst, 4, 2, 3, hashes)
		}
		if elapsed%compactEvery == 0 {
			if _, err := env.comp.Run(ctx); err != nil {
				t.Fatalf("periodic run: %v", err)
			}
		}
		if realTime {
			time.Sleep(tick)
		}
	}

	// Finalize: advance past the grace window and drain.
	env.clock.Advance(cfg.Grace + cfg.Window + time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("final run: %v", err)
	}
	rep, err := env.comp.Reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rep.PendingObjects != 0 {
		t.Fatalf("%d raw objects left pending after finalize", rep.PendingObjects)
	}
	if rep.SegmentsMissing != 0 || rep.ChecksumFailures != 0 {
		t.Fatalf("reconcile found damage: %+v", rep)
	}
	if pins, _ := env.manifest.Pins(ctx); len(pins) != 0 {
		t.Fatalf("trial must have no pins (no active shots), got %d", len(pins))
	}

	segs := allSegments(t, env)
	if len(segs) < len(instances) {
		t.Fatalf("got %d segments for %d instances", len(segs), len(instances))
	}
	assertCountsReconcile(t, env, exp)
	assertByteForByte(t, env, exp)

	// Simulate every writer process exiting, then restore read-only from the
	// durable bucket + manifest.
	if err := env.manifest.Close(); err != nil {
		t.Fatalf("close manifest: %v", err)
	}
	store2, err := NewFakeR2(filepath.Join(env.dir, "r2"))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	manifest2, err := OpenManifest(ctx, filepath.Join(env.dir, "manifest.duckdb"))
	if err != nil {
		t.Fatalf("reopen manifest: %v", err)
	}
	defer manifest2.Close()

	var gotSpans, gotLogs, gotMetrics, gotBatches int64
	seg2, err := manifest2.ListSegments(ctx, "", 1000, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for i, seg := range seg2 {
		reader, _, err := RestoreSegment(ctx, store2, manifest2, seg.Key, filepath.Join(env.dir, "restore2", time.Duration(i).String()))
		if err != nil {
			t.Fatalf("read-only restore %s: %v", seg.Key, err)
		}
		spans, logs, metrics, batches, err := reader.Counts(ctx)
		if err != nil {
			t.Fatalf("counts: %v", err)
		}
		gotSpans += spans
		gotLogs += logs
		gotMetrics += metrics
		gotBatches += batches
		_ = reader.Close()
	}
	var wantSpans, wantLogs, wantMetrics, wantBatches int64
	for _, c := range exp.PerInstance {
		wantSpans += c.Spans
		wantLogs += c.Logs
		wantMetrics += c.Metrics
		wantBatches += c.Batches
	}
	if gotSpans != wantSpans || gotLogs != wantLogs || gotMetrics != wantMetrics || gotBatches != wantBatches {
		t.Fatalf("restored counts mismatch: spans %d/%d logs %d/%d metrics %d/%d batches %d/%d",
			gotSpans, wantSpans, gotLogs, wantLogs, gotMetrics, wantMetrics, gotBatches, wantBatches)
	}
}

// TestTrial60sMultiInstanceNoActiveShots runs a full 60 s receive-time span on
// a deterministic clock: 2 instances, no shots, real OTLP bytes, real DuckDB.
func TestTrial60sMultiInstanceNoActiveShots(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 20 * time.Second
	cfg.Grace = 10 * time.Second
	runTrial(t, cfg, []string{"node-1", "node-2"}, 60*time.Second, 5*time.Second, 15*time.Second, false)
}

// TestTrial180sMultiInstanceNoActiveShots runs a full 180 s receive-time span on
// a deterministic clock: 3 instances, no shots.
func TestTrial180sMultiInstanceNoActiveShots(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 60 * time.Second
	cfg.Grace = 30 * time.Second
	runTrial(t, cfg, []string{"node-1", "node-2", "node-3"}, 180*time.Second, 5*time.Second, 30*time.Second, false)
}

// TestRealTrial60s is the wall-clock variant of the 60 s trial. It is opt-in via
// BURSTEL_REAL_TRIALS=1 so `go test ./...` stays fast; the virtual trial above
// asserts the exact same invariants over a 60 s receive-time span.
func TestRealTrial60s(t *testing.T) {
	if os.Getenv("BURSTEL_REAL_TRIALS") == "" {
		t.Skip("set BURSTEL_REAL_TRIALS=1 to run the wall-clock 60 s trial")
	}
	cfg := DefaultConfig()
	cfg.Window = 20 * time.Second
	cfg.Grace = 10 * time.Second
	runTrial(t, cfg, []string{"node-1", "node-2"}, 60*time.Second, 5*time.Second, 15*time.Second, true)
}

// TestRealTrial180s is the wall-clock variant of the 180 s trial.
func TestRealTrial180s(t *testing.T) {
	if os.Getenv("BURSTEL_REAL_TRIALS") == "" {
		t.Skip("set BURSTEL_REAL_TRIALS=1 to run the wall-clock 180 s trial")
	}
	cfg := DefaultConfig()
	cfg.Window = 60 * time.Second
	cfg.Grace = 30 * time.Second
	runTrial(t, cfg, []string{"node-1", "node-2", "node-3"}, 180*time.Second, 5*time.Second, 30*time.Second, true)
}
