// Spec 031: shot-time parallel materialization.
import { describe, expect, test } from "bun:test";

import type { CompactorNamespaceLike, DoStubLike } from "../src/env";
import type { MaterializationObject } from "../src/ingest-core";
import {
  chunkMaterializationPlan,
  ensureMaterialized,
  materializeShot,
  waitForMaterialization,
} from "../src/materialize";
import { FakeBucket, FakeIngestNamespace, makeIngest, sha256Hex } from "./fakes";

function obj(index: number, size: number): MaterializationObject {
  return {
    key: `journal/inst-1/traces/${String(1_700_000_000_000 + index)}-${index}.bin`,
    signal: "traces",
    received_at: 1_700_000_000_000 + index,
    content_encoding: "",
    content_type: "application/x-protobuf",
    size,
    sha256: "a".repeat(64),
    instance_id: "inst-1",
    instance: "node-1",
  };
}

class FakeMaterializerNamespace implements CompactorNamespaceLike {
  readonly names: string[] = [];
  readonly requests: Array<Record<string, unknown>> = [];
  fail = false;

  getByName(name: string): DoStubLike {
    return {
      fetch: (request: Request) => {
        this.names.push(name);
        return this.handle(request);
      },
    };
  }

  private async handle(request: Request): Promise<Response> {
    if (new URL(request.url).pathname !== "/internal/materialize") {
      return new Response(JSON.stringify({ error: "not found" }), { status: 404 });
    }
    if (this.fail) return new Response(JSON.stringify({ error: "boom" }), { status: 500 });
    const body = (await request.json()) as Record<string, unknown>;
    this.requests.push(body);
    const index = Number(body.part_index ?? 0);
    const objects = (body.objects ?? []) as MaterializationObject[];
    return new Response(
      JSON.stringify({
        shot_id: body.shot_id,
        part_index: index,
        part_key: `shots/${String(body.shot_id)}/part-${index}.duckdb`,
        size_bytes: 4096,
        sha256: "b".repeat(64),
        object_count: objects.length,
        request_count: objects.length,
        record_count: objects.length * 2,
        span_count: objects.length * 2,
        log_count: 0,
        metric_count: 0,
        first_received_at: objects[0]?.received_at ?? 0,
        last_received_at: objects[objects.length - 1]?.received_at ?? 0,
      }),
      { status: 201, headers: { "content-type": "application/json" } },
    );
  }
}

async function seedJournal(bucket: FakeBucket, receivedAt: number, id: string): Promise<string> {
  const bytes = new TextEncoder().encode(`payload-${id}`);
  const sha = await sha256Hex(bytes);
  const key = `journal/inst-1/traces/${receivedAt}-${id}.bin`;
  await bucket.put(key, bytes, {
    sha256: sha,
    customMetadata: {
      instance_id: "inst-1",
      instance: "node-1",
      signal: "traces",
      received_at: String(receivedAt),
      sha256: sha,
      content_encoding: "",
      content_type: "application/x-protobuf",
      token_id: "",
    },
  });
  return key;
}

function setup(bucket: FakeBucket, now: number) {
  const { core } = makeIngest({ now: () => now, bucket, lateWriteLimitMs: 10_000 });
  return { core, ingest: new FakeIngestNamespace(core) };
}

describe("materialization plan", () => {
  test("chunks are contiguous, byte-balanced and never empty", () => {
    const objects = Array.from({ length: 10 }, (_, i) => obj(i, 100));
    const chunks = chunkMaterializationPlan(objects, 4);
    expect(chunks.length).toBe(4);
    expect(chunks.every((c) => c.objects.length > 0)).toBe(true);
    expect(chunks.reduce((n, c) => n + c.objects.length, 0)).toBe(10);
    // Contiguous in receive-time order.
    const flattened = chunks.flatMap((c) => c.objects.map((o) => o.key));
    expect(flattened).toEqual(objects.map((o) => o.key));
    expect(chunkMaterializationPlan([], 4)).toEqual([]);
    expect(chunkMaterializationPlan([obj(0, 1)], 4).length).toBe(1);
  });

  test("enumerates [start-5s, end+late-write] and honours exclusions", async () => {
    const now = 1_700_000_100_000;
    const bucket = new FakeBucket();
    const { core } = setup(bucket, now);
    await core.startWindowShot({ shot_id: "shot-plan", owner: "tester", instances: ["node-1"] });
    const record = core.getShotRecord("shot-plan")!;
    const start = record.window!.start_ms;
    const end = start + 10_000;
    await core.stopWindowShot("shot-plan", end);

    const inWindow = await seedJournal(bucket, end - 1_000, "in");
    const excluded = await seedJournal(bucket, end - 500, "excluded");
    const tooOld = await seedJournal(bucket, start - 6_000, "too-old");
    // Materialization range is [start-5000, end+10000).
    const late = await seedJournal(bucket, end + 9_999, "late");
    const tooLate = await seedJournal(bucket, end + 10_000, "too-late");

    await core.recordExcludedRevoked([
      {
        key: excluded,
        token_id: "tok",
        instance_id: "inst-1",
        received_at: end - 500,
        size: 1,
        reason: "revoked",
      },
    ]);

    const plan = await core.materializationPlan("shot-plan");
    expect(plan.from_ms).toBe(start - 5_000);
    expect(plan.to_ms).toBe(end + 10_000);
    const keys = plan.objects.map((o) => o.key);
    expect(keys).toContain(inWindow);
    expect(keys).toContain(late);
    expect(keys).not.toContain(excluded);
    expect(keys).not.toContain(tooOld);
    expect(keys).not.toContain(tooLate);
    expect(plan.excluded).toContain(excluded);
  });
});

describe("materialization state machine", () => {
  test("runs chunks on distinct DO names and stores a part manifest", async () => {
    const now = 1_700_000_100_000;
    const bucket = new FakeBucket();
    const { core, ingest } = setup(bucket, now);
    await core.startWindowShot({ shot_id: "shot-run", owner: "tester", instances: ["node-1"] });
    const record = core.getShotRecord("shot-run")!;
    const start = record.window!.start_ms;
    for (let i = 0; i < 4; i++) await seedJournal(bucket, start + i * 1_000, `o${i}`);
    const stopped = await core.stopWindowShot("shot-run", start + 10_000);

    const compactor = new FakeMaterializerNamespace();
    const completed = await materializeShot({ ingest, compactor }, stopped, {
      parallelism: 2,
      maxInstances: 2,
      now: () => now,
    });
    expect(completed.materialization?.state).toBe("complete");
    expect(completed.materialization?.parts_total).toBe(2);
    expect(completed.materialization?.parts[0]?.object_key).toBe("shots/shot-run/part-0.duckdb");
    expect(completed.materialization?.parts[1]?.object_key).toBe("shots/shot-run/part-1.duckdb");
    expect(new Set(compactor.names).size).toBe(2);
    expect(compactor.names).toContain("materializer-shot-run-0");
    expect(compactor.names).toContain("materializer-shot-run-1");

    // Re-running with force replaces the same deterministic part keys.
    const again = await materializeShot({ ingest, compactor }, completed, {
      parallelism: 2,
      maxInstances: 2,
      now: () => now,
      force: true,
    });
    expect(again.materialization?.parts.map((p) => p.object_key)).toEqual(
      completed.materialization?.parts.map((p) => p.object_key),
    );
  });

  test("lazy ensure waits for an already running materialization", async () => {
    const now = 1_700_000_100_000;
    const bucket = new FakeBucket();
    const { core, ingest } = setup(bucket, now);
    await core.startWindowShot({ shot_id: "shot-wait", owner: "tester", instances: ["node-1"] });
    const start = core.getShotRecord("shot-wait")!.window!.start_ms;
    await core.stopWindowShot("shot-wait", start + 1_000);
    await core.setMaterialization("shot-wait", {
      state: "running",
      parts_done: 0,
      parts_total: 1,
      spans: 0,
      started_at: now,
      finished_at: null,
      parts: [],
    });
    setTimeout(() => {
      void core.setMaterialization("shot-wait", {
        state: "complete",
        parts_done: 1,
        parts_total: 1,
        spans: 1,
        started_at: now,
        finished_at: now + 1,
        parts: [],
      });
    }, 50);
    const waited = await ensureMaterialized({ ingest }, core.getShotRecord("shot-wait")!, { waitMs: 2_000 });
    expect(waited.materialization?.state).toBe("complete");
    expect((await waitForMaterialization({ ingest }, "shot-wait", 1_000)).materialization?.state).toBe("complete");
  });

  test("compaction yields while a materialization run is active", async () => {
    const now = 1_700_000_100_000;
    const { core } = setup(new FakeBucket(), now);
    expect((await core.materializationYield()).yield).toBe(false);
    await core.beginMaterializationRun("shot-yield", 60_000);
    expect((await core.materializationYield()).yield).toBe(true);
    await core.endMaterializationRun("shot-yield");
    expect((await core.materializationYield()).yield).toBe(false);
  });
});
