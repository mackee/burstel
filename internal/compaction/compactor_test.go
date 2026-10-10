package compaction

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/mackee/burstel/internal/otlp"
)

func baseConfig() Config {
	return Config{Window: 30 * time.Second, Grace: 10 * time.Second, Retention: 24 * time.Hour}
}

// ingestTicks writes tick batches for each instance at `step` intervals.
func ingestTicks(t *testing.T, env *testEnv, instances []string, ticks int, step time.Duration, spansPer, logsPer int) *expectedCounts {
	t.Helper()
	exp := newExpected()
	base := env.clock.Now()
	for tick := 0; tick < ticks; tick++ {
		at := base.Add(time.Duration(tick) * step)
		env.clock.Set(at)
		for _, inst := range instances {
			hashes := ingestSignals(t, env, inst, "svc-"+inst, at, tick, spansPer, logsPer)
			exp.add(inst, spansPer, logsPer, 3, hashes)
		}
	}
	return exp
}

func assertCountsReconcile(t *testing.T, env *testEnv, exp *expectedCounts) {
	t.Helper()
	segs := allSegments(t, env)
	sumSpans, sumLogs, sumMetrics := int64(0), int64(0), int64(0)
	for _, s := range segs {
		if s.Status != StatusVerified {
			t.Fatalf("segment %s status %q, want verified", s.Key, s.Status)
		}
		sumSpans += s.SpanCount
		sumLogs += s.LogCount
		sumMetrics += s.MetricCount
	}
	var wantSpans, wantLogs, wantMetrics int64
	for _, c := range exp.PerInstance {
		wantSpans += c.Spans
		wantLogs += c.Logs
		wantMetrics += c.Metrics
	}
	if sumSpans != wantSpans || sumLogs != wantLogs || sumMetrics != wantMetrics {
		t.Fatalf("count mismatch: spans %d/%d logs %d/%d metrics %d/%d",
			sumSpans, wantSpans, sumLogs, wantLogs, sumMetrics, wantMetrics)
	}
}

func assertByteForByte(t *testing.T, env *testEnv, exp *expectedCounts) {
	t.Helper()
	got := coverageKeys(t, env, allSegments(t, env))
	if len(got) != len(exp.Keys) {
		t.Fatalf("coverage keys %d, want %d", len(got), len(exp.Keys))
	}
	for k, want := range exp.Keys {
		if got[k] != want {
			t.Fatalf("key %s raw sha %s, want %s", k, got[k], want)
		}
	}
}

func TestCompactBasic(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a", "inst-b"}, 3, 10*time.Second, 4, 2)
	base := env.clock.Now()

	env.clock.Advance(5 * time.Minute)
	st, err := env.comp.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if st.Eligible != int64(len(exp.Keys)) {
		t.Fatalf("eligible %d, want %d", st.Eligible, len(exp.Keys))
	}
	if st.SegmentsCreated == 0 {
		t.Fatal("expected at least one segment")
	}
	assertCountsReconcile(t, env, exp)
	assertByteForByte(t, env, exp)

	complete, err := env.comp.CheckCompleteness(ctx, "", base.UnixMilli(), base.Add(60*time.Second).UnixMilli())
	if err != nil {
		t.Fatalf("completeness: %v", err)
	}
	if !complete.Complete {
		t.Fatalf("expected complete, got %+v", complete)
	}

	// A second pass is a no-op: coverage is idempotent.
	st2, err := env.comp.Run(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if st2.SegmentsCreated != 0 || st2.Eligible != 0 {
		t.Fatalf("second pass created %d segments from %d eligible", st2.SegmentsCreated, st2.Eligible)
	}
}

func TestGCDeletesOnlyCoveredAndPreservesBytes(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a", "inst-b"}, 3, 10*time.Second, 4, 2)
	env.clock.Advance(5 * time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	segsBefore := allSegments(t, env)

	gc, err := env.comp.GC(ctx, true)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if gc.Deleted != int64(len(exp.Keys)) {
		t.Fatalf("gc deleted %d, want %d", gc.Deleted, len(exp.Keys))
	}
	for key := range exp.Keys {
		if _, ok, _ := env.store.Head(ctx, key); ok {
			t.Fatalf("journal object %s survived force GC", key)
		}
	}
	// The immutable segments still yield the exact bytes.
	assertByteForByte(t, env, exp)
	if len(allSegments(t, env)) != len(segsBefore) {
		t.Fatal("GC must not touch segments")
	}
}

func TestFailureInjectedBeforeUpload(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a"}, 2, 10*time.Second, 3, 2)
	env.clock.Advance(5 * time.Minute)

	env.comp.SetHooks(Hooks{BeforeUpload: func(SegmentInfo) error { return ErrFaultInjected }})
	if _, err := env.comp.Run(ctx); err == nil {
		t.Fatal("expected injected failure")
	}
	if got := len(allSegments(t, env)); got != 0 {
		t.Fatalf("manifest advertised %d segments after a pre-upload crash", got)
	}
	for key := range exp.Keys {
		if _, ok, _ := env.store.Head(ctx, key); !ok {
			t.Fatalf("journal object %s lost after pre-upload crash", key)
		}
	}

	env.comp.SetHooks(Hooks{})
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("recovery run: %v", err)
	}
	assertCountsReconcile(t, env, exp)
	assertByteForByte(t, env, exp)
}

func TestFailureInjectedAfterUploadBeforeManifest(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a"}, 2, 10*time.Second, 3, 2)
	env.clock.Advance(5 * time.Minute)

	env.comp.SetHooks(Hooks{AfterUpload: func(SegmentInfo) error { return ErrFaultInjected }})
	if _, err := env.comp.Run(ctx); err == nil {
		t.Fatal("expected injected failure")
	}
	if got := len(allSegments(t, env)); got != 0 {
		t.Fatalf("manifest advertised %d segments before the commit", got)
	}
	infos, _, err := env.store.List(ctx, SegmentsPrefix+"/", "", 100)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("uploaded segment objects = %d, want 1", len(infos))
	}
	putsBefore := env.store.Calls().Put

	env.comp.SetHooks(Hooks{})
	st, err := env.comp.Run(ctx)
	if err != nil {
		t.Fatalf("recovery run: %v", err)
	}
	if st.SegmentsReused == 0 {
		t.Fatal("expected the abandoned upload to be reused")
	}
	if env.store.Calls().Put != putsBefore {
		t.Fatalf("recovery re-uploaded: puts %d -> %d", putsBefore, env.store.Calls().Put)
	}
	assertCountsReconcile(t, env, exp)
	assertByteForByte(t, env, exp)
}

func TestFailureInjectedMidGC(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a", "inst-b"}, 3, 10*time.Second, 3, 2)
	env.clock.Advance(5 * time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	count := 0
	env.comp.SetHooks(Hooks{DuringGC: func(string) error {
		count++
		if count == 2 {
			return ErrFaultInjected
		}
		return nil
	}})
	gc, err := env.comp.GC(ctx, true)
	if err == nil {
		t.Fatal("expected mid-GC failure")
	}
	if gc.Deleted != 1 {
		t.Fatalf("deleted %d before crash, want 1", gc.Deleted)
	}

	env.comp.SetHooks(Hooks{})
	if _, err := env.comp.GC(ctx, true); err != nil {
		t.Fatalf("gc resume: %v", err)
	}
	for key := range exp.Keys {
		if _, ok, _ := env.store.Head(ctx, key); ok {
			t.Fatalf("journal object %s survived resumed GC", key)
		}
	}
	assertByteForByte(t, env, exp)
}

func TestMissingSegmentIsExplicitGapThenRepaired(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a", "inst-b"}, 3, 10*time.Second, 3, 2)
	env.clock.Advance(5 * time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	segs := allSegments(t, env)
	victim := segs[0]
	if err := env.store.Delete(ctx, victim.Key); err != nil {
		t.Fatalf("delete segment: %v", err)
	}

	rep, err := env.comp.Reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rep.SegmentsMissing != 1 {
		t.Fatalf("reconcile missing = %d, want 1", rep.SegmentsMissing)
	}
	complete, err := env.comp.CheckCompleteness(ctx, "", 0, 0)
	if err != nil {
		t.Fatalf("completeness: %v", err)
	}
	if complete.Complete {
		t.Fatal("a missing segment must be visible as incomplete")
	}
	if len(complete.Missing) == 0 {
		t.Fatalf("expected a missing-segment note, got %+v", complete)
	}

	rr, err := env.comp.Repair(ctx)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if rr.Repaired != 1 {
		t.Fatalf("repaired %d, want 1", rr.Repaired)
	}
	if rep2, err := env.comp.Reconcile(ctx); err != nil || rep2.SegmentsMissing != 0 {
		t.Fatalf("post-repair reconcile: %+v err=%v", rep2, err)
	}
	complete2, err := env.comp.CheckCompleteness(ctx, "", 0, 0)
	if err != nil {
		t.Fatalf("completeness: %v", err)
	}
	if !complete2.Complete {
		t.Fatalf("expected complete after repair, got %+v", complete2)
	}
	assertByteForByte(t, env, exp)
}

func TestLateEventVisibleAsIncomplete(t *testing.T) {
	ctx := context.Background()
	cfg := baseConfig()
	cfg.Window = 15 * time.Second
	cfg.Grace = 5 * time.Second
	env := newTestEnv(t, cfg)

	base := env.clock.Now()
	exp := newExpected()
	for _, at := range []time.Time{base, base.Add(10 * time.Second)} {
		env.clock.Set(at)
		hashes := ingestSignals(t, env, "inst-a", "svc-a", at, int(at.Sub(base)/time.Second), 2, 1)
		exp.add("inst-a", 2, 1, 3, hashes)
	}
	env.clock.Advance(time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	// A late event lands at a receive time already finalized.
	lateAt := base.Add(5 * time.Second)
	hashes := ingestSignals(t, env, "inst-a", "svc-a", lateAt, 999, 2, 1)
	exp.add("inst-a", 2, 1, 3, hashes)
	env.clock.Advance(2 * time.Minute)
	st, err := env.comp.Run(ctx)
	if err != nil {
		t.Fatalf("late run: %v", err)
	}
	if st.LateObjects == 0 {
		t.Fatal("expected late objects to be counted")
	}
	late := 0
	for _, s := range allSegments(t, env) {
		if s.Late {
			late++
		}
	}
	if late == 0 {
		t.Fatal("expected a late-tagged segment")
	}
	complete, err := env.comp.CheckCompleteness(ctx, "inst-a", base.UnixMilli(), base.Add(30*time.Second).UnixMilli())
	if err != nil {
		t.Fatalf("completeness: %v", err)
	}
	if complete.Complete || len(complete.Late) == 0 {
		t.Fatalf("late events must be visible as incomplete, got %+v", complete)
	}
	// The late bytes are still covered exactly once.
	assertByteForByte(t, env, exp)
	counters, _ := env.manifest.Counters(ctx)
	if counters["late_events"] == 0 {
		t.Fatal("late_events counter not incremented")
	}
}

func TestPinProtectsGCUntilReleased(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a"}, 2, 10*time.Second, 2, 2)
	base := env.clock.Now()
	env.clock.Advance(5 * time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := env.manifest.AddPin(ctx, "shot-1", "inst-a", base.UnixMilli(), base.Add(time.Minute).UnixMilli(), env.clock.Now().UnixMilli()); err != nil {
		t.Fatalf("pin: %v", err)
	}
	gc, err := env.comp.GC(ctx, true)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if gc.Deleted != 0 || gc.Protected == 0 {
		t.Fatalf("pinned GC deleted %d protected %d", gc.Deleted, gc.Protected)
	}
	if n, err := env.manifest.RemovePins(ctx, "shot-1"); err != nil || n != 1 {
		t.Fatalf("remove pins n=%d err=%v", n, err)
	}
	gc2, err := env.comp.GC(ctx, true)
	if err != nil {
		t.Fatalf("gc2: %v", err)
	}
	if gc2.Deleted != int64(len(exp.Keys)) {
		t.Fatalf("post-unpin GC deleted %d, want %d", gc2.Deleted, len(exp.Keys))
	}
}

func TestUnsupportedMetricVariantRetainsRawBytes(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())

	// A Metric with no data (an unsupported/unknown variant).
	weird := &collmetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "mystery.metric",
					Unit: "widgets",
				}},
			}},
		}},
	}
	wire, err := proto.Marshal(weird)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	at := env.clock.Now()
	if _, err := WriteJournal(ctx, env.store, "inst-x", "inst-x", otlp.Metrics, at, "weird", wire, ""); err != nil {
		t.Fatalf("write: %v", err)
	}
	env.clock.Advance(5 * time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	segs := allSegments(t, env)
	if len(segs) != 1 || segs[0].MetricCount != 1 {
		t.Fatalf("expected one unknown metric point, got %+v", segs)
	}
	reader, _, err := RestoreSegment(ctx, env.store, env.manifest, segs[0].Key, filepath.Join(env.dir, "restore"))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	defer reader.Close()
	hashes, err := reader.RawPayloadHashes(ctx)
	if err != nil {
		t.Fatalf("hashes: %v", err)
	}
	if hashes[JournalKeyFor("inst-x", otlp.Metrics, at, "weird")] != shaHex(wire) {
		t.Fatal("unsupported metric variant lost its raw bytes")
	}
}
