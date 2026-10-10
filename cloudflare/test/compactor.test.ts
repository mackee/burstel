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
  });
  return { bucket, ingest, container, compactor, armed, clock };
}

let seq = 0;

/** Durably writes one raw journal object and its ledger row. */
async function seed(
  h: Harness,
  opts: { instanceId: string; instance: string; signal: string; receivedAt: number; bytes?: Uint8Array },
): Promise<{ key: string; sha: string; size: number }> {
  seq += 1;
  const bytes = opts.bytes ?? new TextEncoder().encode(`payload-${opts.signal}-${seq}`);
  const sha = await sha256Hex(bytes);
  const key = `journal/${opts.instanceId}/${opts.signal}/${opts.receivedAt}-${seq}.bin`;
  await h.bucket.put(key, bytes, {
    sha256: sha,
    httpMetadata: { contentType: "application/x-protobuf" },
    customMetadata: { sha256: sha, signal: opts.signal, instance_id: opts.instanceId, received_at: String(opts.receivedAt) },
  });
  await h.ingest.commitJournal({
    key,
    instance_id: opts.instanceId,
    instance: opts.instance,
    signal: opts.signal,
    size: bytes.byteLength,
    sha256: sha,
    content_type: "application/x-protobuf",
    content_encoding: "",
    received_at: opts.receivedAt,
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
    expect(report.segments_published).toBe(2); // one window per instance
    expect(report.journal_objects).toBe(6);

    const segments = await h.ingest.listSegments({ limit: 50, offset: 0 });
    expect(segments.total).toBe(2);
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

  test("is idempotent across repeated ticks", async () => {
    const h = makeHarness();
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: h.clock.now - 600_000 });
    const first = await h.compactor.tick();
    expect(first.segments_published).toBe(1);
    const second = await h.compactor.tick();
    expect(second.segments_published).toBe(0);
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

    // After the lease expires a different holder may take over with a new fence.
    h.clock.now += 120_000;
    const taken = await h.compactor.tick();
    expect(taken.ok).toBe(true);
    expect(taken.segments_published).toBe(1);
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

  test("fails safely when a journal object is missing from R2", async () => {
    const h = makeHarness();
    const seeded = await seed(h, {
      instanceId: "inst-a",
      instance: "inst-a",
      signal: "traces",
      receivedAt: h.clock.now - 600_000,
    });
    await h.bucket.delete(seeded.key);
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
    expect(report.segments_published).toBe(2);
    expect(report.gc_deleted).toBe(1);
    expect(report.gc_protected).toBe(1);

    // The pinned object survives; the unpinned one is gone from R2 + ledger.
    expect(await h.bucket.get(b.key)).not.toBeNull();
    expect(await h.bucket.get(a.key)).toBeNull();
    const journal = await h.ingest.listJournal({ limit: 50, offset: 0 });
    expect(journal.entries.map((e) => e.key)).toEqual([b.key]);
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

  test("late/backdated data becomes its own segment without duplicate coverage", async () => {
    const h = makeHarness();
    const old = h.clock.now - 600_000;
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: old });
    const first = await h.compactor.tick();
    expect(first.segments_published).toBe(1);

    // A late arrival inside the already-finalized receive-time window.
    await seed(h, { instanceId: "inst-a", instance: "inst-a", signal: "traces", receivedAt: old + 1000 });
    const second = await h.compactor.tick();
    expect(second.segments_published).toBe(1);

    const segments = await h.ingest.listSegments({ limit: 50, offset: 0 });
    expect(segments.total).toBe(2);
    const status = await h.ingest.compactionStatus();
    expect(status.coverage_rows).toBe(2);
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
});
