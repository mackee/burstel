// Bounded local integration tests for the spec-018 compactor coordinator.
//
// These run the real production orchestration (`CompactorCore`) against a real
// SQLite-backed `IngestCore`, a fake R2 bucket that enforces the PUT SHA-256
// precondition and exposes HEAD/DELETE, and an in-process fake of the Go
// compactor Container's bounded `/v1/segments` contract. The real Go DuckDB
// service is exercised separately in compactor-go.test.ts and in
// `go test ./internal/compaction`.

import { describe, expect, test } from "bun:test";

import type { CompactorConfig } from "../src/compactor-core";
import { CompactorCore } from "../src/compactor-core";
import { normalizeCoverage } from "../src/ingest-core";
import type { IngestCore } from "../src/ingest-core";
import { createTools } from "../src/tools";
import {
  FakeBucket,
  FakeCompactorContainer,
  FakeCompactorNamespace,
  FakeIngestNamespace,
  makeIngest,
  sha256Hex,
} from "./fakes";
import type { FakeCompactorOptions } from "./fakes";

interface Harness {
  bucket: FakeBucket;
  ingest: IngestCore;
  container: FakeCompactorContainer;
  compactor: CompactorCore;
  armed: number[];
  clock: { now: number };
  logs: Array<{ message: string; fields?: Record<string, unknown> }>;
}

function makeHarness(options: {
  compactor?: FakeCompactorOptions;
  config?: Partial<CompactorConfig>;
  start?: number;
} = {}): Harness {
  const bucket = new FakeBucket();
  const clock = { now: options.start ?? 1_700_000_000_000 };
  const now = () => clock.now;
  const { core: ingest } = makeIngest({ now, bucket });
  const ns = new FakeIngestNamespace(ingest);
  const container = new FakeCompactorContainer(options.compactor ?? {});
  const armed: number[] = [];
  const logs: Array<{ message: string; fields?: Record<string, unknown> }> = [];
  const compactor = new CompactorCore({
    ingest: ns,
    bucket,
    container,
    holder: "holder-A",
    now,
    config: { graceMs: 90_000, windowMs: 60_000, ...(options.config ?? {}) },
    scheduler: {
      set: (at) => {
        armed.push(at);
      },
    },
    log: (message, fields) => {
      logs.push({ message, fields });
    },
  });
  return { bucket, ingest, container, compactor, armed, clock, logs };
}

let seq = 0;

/** Durably writes one raw journal object exactly as the ingest path does. */
async function seed(
  h: Harness,
  opts: { instanceId: string; instance: string; signal: string; receivedAt: number; bytes?: Uint8Array; tokenId?: string },
): Promise<{ key: string; sha: string; size: number }> {
  seq += 1;
  const bytes = opts.bytes ?? new TextEncoder().encode(`payload-${opts.signal}-${seq}`);
  const sha = await sha256Hex(bytes);
  const key = `journal/${opts.instanceId}/${opts.signal}/${opts.receivedAt}-${seq}.bin`;
  await h.bucket.put(key, bytes, {
    sha256: sha,
    httpMetadata: { contentType: "application/x-protobuf" },
    customMetadata: {
      sha256: sha,
      signal: opts.signal,
      instance_id: opts.instanceId,
      instance: opts.instance,
      received_at: String(opts.receivedAt),
      token_id: opts.tokenId ?? "tok-seed",
      content_encoding: "",
      content_type: "application/x-protobuf",
    },
  });
  return { key, sha, size: bytes.byteLength };
}

describe("compactor coordinator (spec 018)", () => {
  test("compacts a two-instance stream into verified segments and clears the backlog", async () => {
    const h = makeHarness();
    const old = h.clock.now - 600_000;
    for (const instanceId of ["inst-a", "inst-b"]) {
      for (const signal of ["traces", "metrics", "logs"]) {
        await seed(h, { instanceId, instance: instanceId, signal, receivedAt: old + 1000 });
      }
    }

    const before = await h.ingest.compactionStatus();
    expect(before.unprocessed_journal).toBe(6);

    const report = await h.compactor.tick();
    expect(report.ok).toBe(true);
    // Spec 023 D3.3: one cross-instance time-bucket segment, not one per instance.
    expect(report.segments_published).toBe(1);
    expect(report.journal_objects).toBe(6);

    const segments = await h.ingest.listSegments({ limit: 50, offset: 0 });
    expect(segments.total).toBe(1);
    expect(segments.segments[0]!.instance_id).toBe("all");
    for (const seg of segments.segments) {
      expect(seg.status).toBe("verified");
      const stored = await h.bucket.get(seg.object_key);
      expect(stored).not.toBeNull();
      expect(stored!.size).toBe(seg.size_bytes);
      expect(stored!.sha256).toBe(seg.sha256);
    }

    const after = await h.ingest.compactionStatus();
    expect(after.unprocessed_journal).toBe(0);
    expect(after.consecutive_failures).toBe(0);
    expect(h.armed.length).toBe(1);
  });

  test("is idempotent across repeated ticks (lookback re-list de-duplicates)", async () => {
    const h = makeHarness();
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 130_000 });
    const first = await h.compactor.tick();
    expect(first.segments_published).toBe(1);
    const second = await h.compactor.tick();
    expect(second.segments_published).toBe(0);
    expect(second.discovered_objects).toBe(0);
    const segments = await h.ingest.listSegments({ limit: 50, offset: 0 });
    expect(segments.total).toBe(1);
  });

  test("skips when a live lease is held by another holder and takes over after expiry", async () => {
    const h = makeHarness();
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const held = await h.ingest.acquireLease("primary", "holder-B", 60_000);
    expect(held.ok).toBe(true);

    const skipped = await h.compactor.tick();
    expect(skipped.skipped).toBe("lease_held");
    expect(skipped.segments_published).toBe(0);
    expect(h.container.buildCalls).toBe(0);

    // After the lease expires a different holder may take over with a new fence.
    h.clock.now += 120_000;
    const taken = await h.compactor.tick();
    expect(taken.ok).toBe(true);
    expect(taken.segments_published).toBe(1);
  });

  test("a manual run while a tick is already building is skipped quickly, not overlapped", async () => {
    const h = makeHarness({ compactor: { buildDelaysMs: [500] } });
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });

    // Start the first tick without awaiting it; the second trigger arrives
    // while the same holder still owns the lease/build.
    const firstPromise = h.compactor.tick();
    const second = await h.compactor.tick();
    expect(second.skipped).toBe("lease_held");
    expect(second.segments_published).toBe(0);
    expect(second.duration_ms).toBeLessThan(200);

    const first = await firstPromise;
    expect(first.ok).toBe(true);
    expect(first.segments_published).toBe(1);
    // Exactly one build: the manual run never reached the container.
    expect(h.container.buildCalls).toBe(1);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(1);

    const status = await h.ingest.compactionStatus();
    expect(status.last_skipped_reason).toBe("lease_held");
  });

  test("a build past the container timeout releases the lease and retries without double-publishing", async () => {
    const h = makeHarness({
      compactor: { buildDelaysMs: [300] },
      config: { containerTimeoutMs: 50 },
    });
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });

    const first = await h.compactor.tick();
    expect(first.ok).toBe(false);
    expect(first.segments_published).toBe(0);
    expect(first.error).toContain("timed out");
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(0);
    // The timeout path still releases the lease so the next tick can retry.
    expect((await h.ingest.compactionStatus()).lease).toBeNull();

    const second = await h.compactor.tick();
    expect(second.ok).toBe(true);
    expect(second.segments_published).toBe(1);

    // Let the abandoned first container call settle, then drain again: the
    // deterministic key + unique coverage must keep exactly one segment.
    await Bun.sleep(350);
    const third = await h.compactor.tick();
    expect(third.ok).toBe(true);
    expect(third.segments_published).toBe(0);

    const segments = await h.ingest.listSegments({ limit: 10, offset: 0 });
    expect(segments.total).toBe(1);
    const status = await h.ingest.compactionStatus();
    expect(status.coverage_rows).toBe(1);
    expect(status.unprocessed_journal).toBe(0);

    // Spec 029 observability: one failure log with the timeout/error and one
    // success log with key, object count, bytes and duration.
    const failure = h.logs.find((entry) => entry.message === "burstel: compaction build failed");
    expect(failure).toBeDefined();
    expect(String(failure!.fields?.error)).toContain("timed out");
    expect(failure!.fields?.timeout_ms).toBe(50);
    const success = h.logs.find((entry) => entry.message === "burstel: compaction build");
    expect(success).toBeDefined();
    expect(String(success!.fields?.segment_key)).toStartWith("segments/");
    expect(success!.fields?.objects).toBe(1);
    expect(typeof success!.fields?.bytes).toBe("number");
    expect(typeof success!.fields?.duration_ms).toBe("number");
    expect(success!.fields?.timeout_ms).toBe(50);
  });

  test("a lease-skipped tick is recorded in status instead of staying stale (R4)", async () => {
    const h = makeHarness();
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    await h.ingest.acquireLease("primary", "holder-B", 60_000);

    const skipped = await h.compactor.tick();
    expect(skipped.skipped).toBe("lease_held");

    const status = await h.ingest.compactionStatus();
    expect(status.last_skipped_reason).toBe("lease_held");
    expect(typeof status.last_skipped_at).toBe("number");
    // A skip is not a success or a failure.
    expect(status.last_success_at).toBeNull();
    expect(status.last_failure_at).toBeNull();
    expect(status.consecutive_failures).toBe(0);

    // The coordinator's in-memory report also exposes the skip.
    const coordinator = await h.compactor.status();
    expect((coordinator.last_report as Record<string, unknown>).skipped).toBe("lease_held");
  });

  test("a truncated lookback is visible in status and withholds window completeness (R3)", async () => {
    // 4 objects per scan: enough to make the lookback truncated while a
    // backlog of 3 exists, and to clear once only one covered object remains.
    // GC is enabled so the drained backlog leaves R2; the initial object is
    // pinned so one covered object remains in the lookback.
    const h = makeHarness({
      config: { batchObjects: 1, maxBatchesPerTick: 4, gcEnabled: true, retentionMs: 1000 },
    });
    const t = h.clock.now;
    // Establish a discovery cursor first (floor starts unset, so the first tick
    // has no lookback range at all).
    const keeper = await seed(h, {
      instanceId: "inst-a",
      instance: "inst-a",
      signal: "traces",
      receivedAt: t - 130_000,
    });
    await h.ingest.addPin("keep-1", "inst-a", t - 130_000, t - 130_000);
    expect((await h.compactor.tick()).segments_published).toBe(1);

    // Three finalized objects inside the 2 x grace lookback backlog.
    for (let i = 0; i < 3; i += 1) {
      await seed(h, {
        instanceId: "inst-a",
        instance: "inst-a",
        signal: "traces",
        receivedAt: t - 175_000 + i,
      });
    }

    const truncated = await h.compactor.tick();
    expect(truncated.ok).toBe(true);
    expect(truncated.lookback_truncated).toBe(true);
    expect(truncated.lookback_floor_ms).toBe(t - 180_000);
    expect(truncated.lookback_until_ms).toBe(t - 90_000);

    const status = await h.ingest.compactionStatus();
    expect(status.lookback_truncated).toBe(true);
    expect(status.lookback_floor_ms).toBe(t - 180_000);
    expect(status.lookback_until_ms).toBe(t - 90_000);

    // An empty slice inside the truncated lookback has nothing pending, yet it
    // must not be called complete while the lookback warning stands.
    const risk = await h.ingest.pendingJournalInRange(["inst-a"], t - 170_000, t - 169_000);
    expect(risk.pending_journal).toBe(0);
    expect(risk.lookback_truncated).toBe(true);
    expect(risk.lookback_in_range).toBe(true);
    expect(
      normalizeCoverage(
        {
          expected_segments: 0,
          available_segments: 0,
          missing_segments: [],
          instances: [],
          pending_journal: 0,
          lookback_truncated: true,
          complete: true,
        },
        [],
      ).complete,
    ).toBe(false);

    // The truncated tick already published and GC'd the backlog; the next scan
    // fits inside the budget and clears the warning.
    const drained = await h.compactor.tick();
    expect(drained.lookback_truncated).toBe(false);
    const after = await h.ingest.compactionStatus();
    expect(after.lookback_truncated).toBe(false);
    expect(await h.bucket.get(keeper.key)).not.toBeNull();
    expect(await h.ingest.journalStats().then((s) => s.pending)).toBe(0);
    const coverage = normalizeCoverage(
      {
        expected_segments: 0,
        available_segments: 0,
        missing_segments: [],
        instances: [],
        pending_journal: 0,
        lookback_truncated: false,
        complete: true,
      },
      [],
    );
    expect(coverage.complete).toBe(true);
  });

  test("does not publish when the compactor build fails", async () => {
    const h = makeHarness({ compactor: { failBuild: true } });
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const report = await h.compactor.tick();
    expect(report.ok).toBe(false);
    expect(report.segments_published).toBe(0);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(0);
    // The lease is released so the next tick can retry.
    const status = await h.ingest.compactionStatus();
    expect(status.lease).toBeNull();
  });

  test("rejects a segment whose checksum does not match the bytes (R2 sha precondition)", async () => {
    const h = makeHarness({ compactor: { lieAboutSha: true } });
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const report = await h.compactor.tick();
    expect(report.ok).toBe(false);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(0);
  });

  test("rejects a forged segment key", async () => {
    const h = makeHarness({ compactor: { wrongKey: true } });
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const report = await h.compactor.tick();
    expect(report.ok).toBe(false);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(0);
  });

  test("rejects coverage outside the requested batch", async () => {
    const h = makeHarness({ compactor: { extraCoverage: true } });
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const report = await h.compactor.tick();
    expect(report.ok).toBe(false);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(0);
  });

  test("fails safely when a listed journal object disappears before the build", async () => {
    const h = makeHarness();
    const seeded = await seed(h, {
      instanceId: "inst-a",
      instance: "inst-a",
      signal: "traces",
      receivedAt: h.clock.now - 600_000,
    });
    h.bucket.failGetForKey = seeded.key;
    const report = await h.compactor.tick();
    expect(report.ok).toBe(false);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(0);
  });

  test("publishSegment re-verifies the stored object and rejects a wrong checksum", async () => {
    const bucket = new FakeBucket();
    const { core } = makeIngest({ bucket });
    const bytes = new TextEncoder().encode("segment-bytes");
    const realSha = await sha256Hex(bytes);
    const key = "segments/inst-a/1-2-abcdef.duckdb";
    await bucket.put(key, bytes, { sha256: realSha, customMetadata: { sha256: realSha } });

    const base = {
      segment_key: key,
      instance_id: "inst-a",
      object_key: key,
      size_bytes: bytes.byteLength,
      first_received_at: 1,
      last_received_at: 2,
      request_count: 1,
      record_count: 1,
      coverage: [{ journal_key: "journal/inst-a/traces/1-a.bin", signal: "traces", received_at: 1 }],
    };
    await expect(core.publishSegment({ ...base, sha256: "0".repeat(64) })).rejects.toThrow();
    const ok = await core.publishSegment({ ...base, sha256: realSha });
    expect(ok.committed).toBe(true);
    // Idempotent on a repeated publish.
    const dup = await core.publishSegment({ ...base, sha256: realSha });
    expect(dup.duplicate).toBe(true);
  });

  test("GC deletes verified unpinned journal but protects pinned ranges", async () => {
    const h = makeHarness({ config: { gcEnabled: true, retentionMs: 1000 } });
    const old = h.clock.now - 600_000;
    const a = await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: old });
    const b = await seed(h, { instanceId: "inst-b", instance: "inst-b", signal: "traces", receivedAt: old });
    // Pin instance-b's range so its journal must survive GC.
    await h.ingest.addPin("shot-1", "inst-b", old, old);

    const report = await h.compactor.tick();
    expect(report.ok).toBe(true);
    expect(report.segments_published).toBe(1);
    expect(report.gc_deleted).toBe(1);
    expect(report.gc_protected).toBe(1);

    // The pinned object survives; the unpinned one is gone from R2.
    expect(await h.bucket.get(b.key)).not.toBeNull();
    expect(await h.bucket.get(a.key)).toBeNull();
    expect([...h.bucket.objects.keys()].filter((key) => key.startsWith("journal/"))).toEqual([b.key]);
  });

  test("a window shot auto-pins its receive-time range from GC", async () => {
    const h = makeHarness({ config: { gcEnabled: true, retentionMs: 1000 } });
    const old = h.clock.now - 600_000;
    const a = await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: old });
    await h.ingest.createWindowShot({
      shot_id: "shot-pin-1",
      started_at_ms: old - 1000,
      ended_at_ms: old + 1000,
      instances: [],
    });
    const pins = await h.ingest.listPins();
    expect(pins.pins.length).toBe(1);
    expect(pins.pins[0]!.expires_at).not.toBeNull();

    const report = await h.compactor.tick();
    expect(report.ok).toBe(true);
    expect(report.gc_protected).toBe(1);
    expect(report.gc_deleted).toBe(0);
    expect(await h.bucket.get(a.key)).not.toBeNull();
  });

  test("late/backdated data inside the lookback becomes its own segment without duplicate coverage", async () => {
    const h = makeHarness();
    // Both receive times are already past the grace watermark but inside the
    // 2 x grace lookback that every run re-lists (spec 023 D3.1).
    const old = h.clock.now - 130_000;
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: old });
    const first = await h.compactor.tick();
    expect(first.segments_published).toBe(1);

    // A late arrival inside the already-scanned receive-time range.
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: old + 1000 });
    const second = await h.compactor.tick();
    expect(second.segments_published).toBe(1);

    const segments = await h.ingest.listSegments({ limit: 50, offset: 0 });
    expect(segments.total).toBe(2);
    const status = await h.ingest.compactionStatus();
    expect(status.coverage_rows).toBe(2);
    expect(status.unprocessed_journal).toBe(0);
  });

  test("compaction_status/compaction_run tools proxy to the coordinator", async () => {
    const h = makeHarness();
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const tools = createTools({
      namespace: { getByName: () => ({ fetch: async () => new Response("{}", { status: 200 }) }) },
      ingest: new FakeIngestNamespace(h.ingest),
      compactor: new FakeCompactorNamespace(h.compactor),
      origin: "https://burstel.example",
    });
    const status = await tools.compactionStatus();
    expect(status.holder).toBe("holder-A");
    expect(status.unprocessed_journal).toBe(1);
    const run = await tools.compactionRun();
    expect(run.ok).toBe(true);
    expect(run.segments_published).toBe(1);
  });

  test("compaction status reports backlog, lease and scheduler", async () => {
    const h = makeHarness();
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const status = await h.compactor.status();
    expect(status.unprocessed_journal).toBe(1);
    expect(status.scheduler_active).toBe(false);
    const tick = await h.compactor.tick();
    expect(tick.ok).toBe(true);
    const after = await h.compactor.status();
    expect(after.unprocessed_journal).toBe(0);
    expect(after.scheduler_active).toBe(true);
    expect(after.last_tick_at).not.toBeNull();
  });

  test("compaction yields while a shot materialization is active (spec 031)", async () => {
    const h = makeHarness();
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    await h.ingest.beginMaterializationRun("shot-yield", 60_000);
    const yielded = await h.compactor.tick();
    expect(yielded.skipped).toBe("materialization_in_progress");
    expect(yielded.segments_published).toBe(0);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(0);

    await h.ingest.endMaterializationRun("shot-yield");
    const resumed = await h.compactor.tick();
    expect(resumed.ok).toBe(true);
    expect(resumed.segments_published).toBe(1);
  });
});
