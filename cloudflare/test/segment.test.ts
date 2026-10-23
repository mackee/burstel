// Spec 015: the durable segment manifest published by the bounded compactor.
//
// These tests exercise the real IngestCore over in-process SQLite: atomic
// publication, exactly-once coverage, idempotent retries, hard conflict on a
// changed checksum, validation and the storage/health status surface.

import { describe, expect, test } from "bun:test";

import type { SegmentPublishInput } from "../src/ingest-core";
import { makeIngest } from "./fakes";

const SHA_A = "a".repeat(64);
const SHA_B = "b".repeat(64);

function publishInput(overrides: Partial<SegmentPublishInput> = {}): SegmentPublishInput {
  return {
    segment_key: "segments/inst-1/1000-2000-deadbeef.duckdb",
    instance_id: "inst-1",
    object_key: "segments/inst-1/1000-2000-deadbeef.duckdb",
    size_bytes: 4096,
    sha256: SHA_A,
    first_received_at: 1000,
    last_received_at: 2000,
    request_count: 3,
    record_count: 7,
    span_count: 4,
    log_count: 2,
    metric_count: 1,
    coverage: [
      { journal_key: "journal/inst-1/traces/1000-a.bin", signal: "traces", received_at: 1000 },
      { journal_key: "journal/inst-1/logs/1500-b.bin", signal: "logs", received_at: 1500 },
      { journal_key: "journal/inst-1/metrics/2000-c.bin", signal: "metrics", received_at: 2000 },
    ],
    ...overrides,
  };
}

describe("segment manifest publication", () => {
  test("publishes a verified segment and its coverage atomically", async () => {
    const { core } = makeIngest({ now: () => 5000 });
    const result = await core.publishSegment(publishInput());
    expect(result).toEqual({ committed: true, duplicate: false, covered_new: 3, covered_skipped: 0 });

    const listed = await core.listSegments({ limit: 10, offset: 0 });
    expect(listed.total).toBe(1);
    expect(listed.segments[0]!.sha256).toBe(SHA_A);
    expect(listed.segments[0]!.status).toBe("verified");
    expect(listed.segments[0]!.record_count).toBe(7);
    expect(listed.segments[0]!.verified_at).toBe(5000);

    const status = await core.segmentStatus();
    expect(status.segments).toBe(1);
    expect(status.coverage).toBe(3);
    expect(status.unverified).toBe(0);
  });

  test("a retried publish is an idempotent duplicate with no new coverage", async () => {
    const { core } = makeIngest({ now: () => 5000 });
    await core.publishSegment(publishInput());
    const again = await core.publishSegment(publishInput());
    expect(again).toEqual({ committed: false, duplicate: true, covered_new: 0, covered_skipped: 3 });
    const status = await core.segmentStatus();
    expect(status.segments).toBe(1);
    expect(status.coverage).toBe(3);
    expect((status.counters as Record<string, number>).segment_duplicate).toBe(1);
  });

  test("a changed checksum for the same segment key is a hard conflict", async () => {
    const { core } = makeIngest({ now: () => 5000 });
    await core.publishSegment(publishInput());
    await expect(core.publishSegment(publishInput({ sha256: SHA_B }))).rejects.toThrow(/different checksum/);
  });

  test("a journal object can be covered by at most one segment", async () => {
    const { core } = makeIngest({ now: () => 5000 });
    await core.publishSegment(publishInput());
    // A second segment tries to claim an already-covered journal key.
    const result = await core.publishSegment(
      publishInput({
        segment_key: "segments/inst-1/2500-3000-feedface.duckdb",
        first_received_at: 2500,
        last_received_at: 3000,
        sha256: SHA_B,
        coverage: [
          { journal_key: "journal/inst-1/traces/1000-a.bin", signal: "traces", received_at: 1000 },
          { journal_key: "journal/inst-1/traces/2500-d.bin", signal: "traces", received_at: 2500 },
        ],
      }),
    );
    expect(result.committed).toBe(true);
    expect(result.covered_new).toBe(1);
    expect(result.covered_skipped).toBe(1);
    const status = await core.segmentStatus();
    expect(status.coverage).toBe(4);
  });

  test("payload is validated and a partial/manifest-less publish is rejected", async () => {
    const { core } = makeIngest({ now: () => 5000 });
    await expect(
      core.publishSegment(publishInput({ sha256: "not-hex", segment_key: "segments/x/y.duckdb" })),
    ).rejects.toThrow();
    await expect(core.publishSegment(publishInput({ size_bytes: 0 }))).rejects.toThrow();
    await expect(
      core.publishSegment(publishInput({ coverage: undefined as unknown as SegmentPublishInput["coverage"] })),
    ).rejects.toThrow();
    const status = await core.segmentStatus();
    expect(status.segments).toBe(0);
  });

  test("a late segment is recorded and counted", async () => {
    const { core } = makeIngest({ now: () => 5000 });
    await core.publishSegment(
      publishInput({
        segment_key: "segments/inst-1/500-900-late.duckdb",
        first_received_at: 500,
        last_received_at: 900,
        sha256: SHA_B,
        late: true,
        coverage: [{ journal_key: "journal/inst-1/traces/700-e.bin", signal: "traces", received_at: 700 }],
      }),
    );
    const status = await core.segmentStatus();
    expect(status.late).toBe(1);
    expect((status.counters as Record<string, number>).segment_late).toBe(1);
  });

  test("restart preserves the manifest and coverage", async () => {
    const first = makeIngest({ now: () => 5000 });
    await first.core.publishSegment(publishInput());
    const restarted = makeIngest({ now: () => 6000, sql: first.sql });
    await restarted.core.load();
    const status = await restarted.core.segmentStatus();
    expect(status.segments).toBe(1);
    expect(status.coverage).toBe(3);
  });
});
