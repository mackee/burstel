package compaction

// Independent acceptance audit for spec 017 (compaction side).
//
// The existing suite proves a *missing* segment is an explicit, repairable gap.
// This closes the adjacent contract the spec calls out separately: a *corrupt*
// (byte-flipped) segment object must be detected by its size/SHA-256, must never
// be silently trusted, and must be repairable from the still-present raw journal.

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestAudit017CorruptSegmentDetectedThenRepaired(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, baseConfig())
	exp := ingestTicks(t, env, []string{"inst-a", "inst-b"}, 3, 10*time.Second, 3, 2)
	env.clock.Advance(5 * time.Minute)
	if _, err := env.comp.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	segs := allSegments(t, env)
	if len(segs) == 0 {
		t.Fatal("expected at least one segment")
	}
	victim := segs[0]

	// Flip a byte in the stored segment object without touching its metadata, so
	// the advertised checksum no longer matches the bytes.
	raw, err := os.ReadFile(env.store.objectPath(victim.Key))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("empty segment object")
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(env.store.objectPath(victim.Key), raw, 0o644); err != nil {
		t.Fatalf("corrupt segment: %v", err)
	}

	rep, err := env.comp.Reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rep.ChecksumFailures != 1 {
		t.Fatalf("expected exactly 1 checksum failure, got %+v", rep)
	}
	if rep.SegmentsMissing != 0 {
		t.Fatalf("a corrupt object is present, not missing: %+v", rep)
	}
	complete, err := env.comp.CheckCompleteness(ctx, "", 0, 0)
	if err != nil {
		t.Fatalf("completeness: %v", err)
	}
	if complete.Complete {
		t.Fatalf("a corrupt segment must be disclosed as incomplete: %+v", complete)
	}

	// Repair rebuilds the segment from the raw journal and clears the gap.
	rr, err := env.comp.Repair(ctx)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if rr.Repaired != 1 || rr.Unrepairable != 0 {
		t.Fatalf("repair report = %+v, want one repaired", rr)
	}
	rep2, err := env.comp.Reconcile(ctx)
	if err != nil {
		t.Fatalf("post-repair reconcile: %v", err)
	}
	if rep2.ChecksumFailures != 0 || rep2.SegmentsMissing != 0 {
		t.Fatalf("post-repair reconcile still damaged: %+v", rep2)
	}
	complete2, err := env.comp.CheckCompleteness(ctx, "", 0, 0)
	if err != nil {
		t.Fatalf("completeness: %v", err)
	}
	if !complete2.Complete {
		t.Fatalf("expected complete after repair, got %+v", complete2)
	}

	// The raw bytes are still covered exactly once and byte-for-byte.
	assertCountsReconcile(t, env, exp)
	assertByteForByte(t, env, exp)
}
