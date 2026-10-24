// Spec 032: alarm-driven durable materialization.
import { describe, expect, test } from "bun:test";

import type { CompactorNamespaceLike, DoStubLike, MaterializerNamespaceLike } from "../src/env";
import type { MaterializationObject, WindowShotRecord } from "../src/ingest-core";
import type { MaterializeRunState } from "../src/materialize-driver";
import { MaterializeDriver } from "../src/materialize-driver";
import { ensureMaterialized, isMaterializationLeaseStale } from "../src/materialize";
import { createTools } from "../src/tools";
import { FakeBucket, FakeIngestNamespace, makeIngest, sha256Hex } from "./fakes";

class MemoryStore {
  run: MaterializeRunState | null = null;

  async load(): Promise<MaterializeRunState | null> {
    return this.run ? (JSON.parse(JSON.stringify(this.run)) as MaterializeRunState) : null;
  }

  async save(run: MaterializeRunState): Promise<void> {
    this.run = JSON.parse(JSON.stringify(run)) as MaterializeRunState;
  }
}

class FakeScheduler {
  readonly armed: number[] = [];

  set(atMs: number): void {
    this.armed.push(atMs);
  }
}

class FakeMaterializers implements CompactorNamespaceLike {
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
    if (this.fail) return new Response(JSON.stringify({ error: "container exploded" }), { status: 500 });
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

interface Harness {
  clock: { now: number };
  core: ReturnType<typeof makeIngest>["core"];
  ingest: FakeIngestNamespace;
  store: MemoryStore;
  scheduler: FakeScheduler;
  compactor: FakeMaterializers;
  driver: MaterializeDriver;
  stopped: WindowShotRecord;
}

async function harness(options: { maxAttempts?: number; parallelism?: number } = {}): Promise<Harness> {
  const clock = { now: 1_700_000_100_000 };
  const bucket = new FakeBucket();
  const { core } = makeIngest({ now: () => clock.now, bucket, lateWriteLimitMs: 10_000 });
  const ingest = new FakeIngestNamespace(core);
  await core.startWindowShot({ shot_id: "shot_alarm_materialize", owner: "tester", instances: ["node-1"] });
  const start = core.getShotRecord("shot_alarm_materialize")!.window!.start_ms;
  for (let i = 0; i < 4; i++) await seedJournal(bucket, start + i * 1_000, `o${i}`);
  const stopped = await core.stopWindowShot("shot_alarm_materialize", start + 10_000);
  const store = new MemoryStore();
  const scheduler = new FakeScheduler();
  const compactor = new FakeMaterializers();
  const driver = new MaterializeDriver({
    shotId: "shot_alarm_materialize",
    ingest,
    compactor,
    store,
    scheduler,
    now: () => clock.now,
    config: {
      parallelism: options.parallelism ?? 2,
      maxInstances: 2,
      maxAttempts: options.maxAttempts ?? 3,
      leaseTtlMs: 10_000,
      retryDelayMs: 10,
      heartbeatMs: 0,
      lateWriteLimitMs: 10_000,
    },
    log: () => undefined,
  });
  return { clock, core, ingest, store, scheduler, compactor, driver, stopped };
}

describe("alarm-driven materialization", () => {
  test("completes from the alarm alone after shot_stop returns (no Worker waitUntil)", async () => {
    const h = await harness();
    expect(h.stopped.coverage?.complete).toBe(false);
    expect(h.stopped.materialization?.state).toBe("pending");

    // shot_stop's only durable action: record the run and arm the DO alarm.
    await h.driver.schedule(h.stopped);
    expect(h.scheduler.armed.at(-1)).toBe(h.stopped.window!.end_ms + 10_000);
    expect((await h.store.load())?.state).toBe("scheduled");

    // The original request is gone; only the alarm runs now.
    h.clock.now = h.scheduler.armed.at(-1)!;
    await h.driver.alarm();

    const run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.results.length).toBe(2);
    expect(run?.spans).toBe(8);
    expect(h.compactor.names).toContain("materializer-shot_alarm_materialize-0");
    expect(h.compactor.names).toContain("materializer-shot_alarm_materialize-1");
    const record = h.core.getShotRecord("shot_alarm_materialize")!;
    expect(record.materialization?.state).toBe("complete");
    expect(record.coverage?.complete).toBe(true);
  });

  test("shot_stop only schedules the DO; the alarm completes after the request is gone", async () => {
    const clock = { now: 1_700_000_100_000 };
    const bucket = new FakeBucket();
    const { core } = makeIngest({ now: () => clock.now, bucket, lateWriteLimitMs: 10_000 });
    const ingest = new FakeIngestNamespace(core);
    await core.startWindowShot({ shot_id: "shot_alarm_materialize", owner: "tester", instances: ["node-1"] });
    const start = core.getShotRecord("shot_alarm_materialize")!.window!.start_ms;
    for (let i = 0; i < 4; i++) await seedJournal(bucket, start + i * 1_000, `o${i}`);
    const store = new MemoryStore();
    const scheduler = new FakeScheduler();
    const compactor = new FakeMaterializers();
    const driver = new MaterializeDriver({
      shotId: "shot_alarm_materialize",
      ingest,
      compactor,
      store,
      scheduler,
      now: () => clock.now,
      config: {
        parallelism: 2,
        maxInstances: 2,
        maxAttempts: 3,
        leaseTtlMs: 10_000,
        retryDelayMs: 10,
        heartbeatMs: 0,
        lateWriteLimitMs: 10_000,
      },
      log: () => undefined,
    });
    const materializer: MaterializerNamespaceLike = {
      getByName: () => ({
        fetch: async (request: Request) => {
          const body = (await request.json()) as { shot_id: string; force?: boolean; restart?: boolean };
          const record = core.getShotRecord(body.shot_id)!;
          const run = await driver.schedule(record, { force: body.force, restart: body.restart });
          return new Response(JSON.stringify({ shot_id: body.shot_id, run_id: run.run_id, state: run.state }), {
            status: 202,
            headers: { "content-type": "application/json" },
          });
        },
      }),
    };
    const tools = createTools({
      namespace: { getByName: () => ({ fetch: async () => new Response("{}", { status: 200 }) }) },
      ingest,
      compactor,
      materializer,
      origin: "https://burstel.test",
    });

    // No waitUntil is provided. The stop response records the run and the
    // request ends here; only the DO alarm may continue the work.
    const stopped = await tools.stop("shot_alarm_materialize", start + 10_000);
    expect((stopped.coverage as { complete?: boolean }).complete).toBe(false);
    expect((await store.load())?.state).toBe("scheduled");

    clock.now = scheduler.armed.at(-1)!;
    await driver.alarm();
    expect((await store.load())?.state).toBe("complete");
    expect(compactor.names.length).toBe(2);
  });

  test("retries a stale part lease on the next alarm", async () => {
    const h = await harness();
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;

    // Simulate a coordinator crash after claiming part 0: the lease expired,
    // part 1 was still pending, and no result was recorded.
    const run = (await h.store.load())!;
    run.state = "running";
    run.started_at = h.clock.now;
    run.lease_expires_at = h.clock.now - 1;
    run.parts_total = 2;
    run.parts = [
      {
        index: 0,
        state: "running",
        attempts: 1,
        lease_owner: "dead-coordinator",
        lease_expires_at: h.clock.now - 1,
        started_at: h.clock.now - 5_000,
        finished_at: null,
        error: null,
      },
      {
        index: 1,
        state: "pending",
        attempts: 0,
        lease_owner: null,
        lease_expires_at: null,
        started_at: null,
        finished_at: null,
        error: null,
      },
    ];
    await h.store.save(run);

    await h.driver.alarm();

    const retried = await h.store.load();
    expect(retried?.state).toBe("complete");
    expect(retried?.parts[0]?.attempts).toBe(2);
    expect(h.compactor.requests.length).toBe(2);
  });

  test("ends terminal failed with a reason after the bounded attempts", async () => {
    const h = await harness({ maxAttempts: 2 });
    h.compactor.fail = true;
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;

    await h.driver.alarm();
    let run = await h.store.load();
    expect(run?.state).toBe("scheduled");
    expect(run?.parts.every((part) => part.state === "pending")).toBe(true);

    h.clock.now = (run?.scheduled_at ?? h.clock.now) + 1;
    await h.driver.alarm();

    run = await h.store.load();
    expect(run?.state).toBe("failed");
    expect(run?.error).toMatch(/part \d failed after 2 attempts/);
    expect(run?.error).toContain("container exploded");
    const record = h.core.getShotRecord("shot_alarm_materialize")!;
    expect(record.materialization?.state).toBe("failed");
    expect(record.materialization?.error).toContain("container exploded");
    expect(record.coverage?.complete).toBe(false);

    // A terminal run never calls the container again.
    const calls = h.compactor.names.length;
    h.clock.now += 60_000;
    await h.driver.alarm();
    expect(h.compactor.names.length).toBe(calls);
  });

  test("coverage is false while running and exposes the materialization state", async () => {
    const h = await harness();
    expect(h.stopped.coverage?.materialization_state).toBe("pending");
    expect(h.stopped.coverage?.complete).toBe(false);

    await h.core.setMaterialization("shot_alarm_materialize", {
      state: "running",
      parts_done: 0,
      parts_total: 2,
      spans: 0,
      started_at: h.clock.now,
      finished_at: null,
      parts: [],
      error: null,
      lease_expires_at: h.clock.now + 10_000,
      part_states: [],
      attempts: 1,
    });
    const whileRunning = await h.core.setWindowCoverage("shot_alarm_materialize", {
      expected_segments: 0,
      available_segments: 0,
      missing_segments: [],
      instances: ["node-1"],
      complete: true,
    });
    expect(whileRunning.coverage?.complete).toBe(false);
    expect(whileRunning.coverage?.materialization_state).toBe("running");
    expect(whileRunning.state).toBe("incomplete");

    const completed = await h.core.setMaterialization("shot_alarm_materialize", {
      state: "complete",
      parts_done: 2,
      parts_total: 2,
      spans: 8,
      started_at: h.clock.now,
      finished_at: h.clock.now,
      parts: [],
      error: null,
      lease_expires_at: null,
      part_states: [],
      attempts: 1,
    });
    expect(completed.coverage?.complete).toBe(true);
    expect(completed.coverage?.materialization_state).toBe("complete");
  });
});

describe("lazy path restart", () => {
  test("classifies a running run with a missing or expired lease as stale", () => {
    const now = 1_700_000_100_000;
    expect(
      isMaterializationLeaseStale(
        {
          state: "running",
          parts_done: 0,
          parts_total: 1,
          spans: 0,
          started_at: now,
          finished_at: null,
          parts: [],
          lease_expires_at: null,
        },
        now,
      ),
    ).toBe(true);
    expect(
      isMaterializationLeaseStale(
        {
          state: "running",
          parts_done: 0,
          parts_total: 1,
          spans: 0,
          started_at: now,
          finished_at: null,
          parts: [],
          lease_expires_at: now + 1,
        },
        now,
      ),
    ).toBe(false);
    expect(
      isMaterializationLeaseStale(
        {
          state: "complete",
          parts_done: 1,
          parts_total: 1,
          spans: 1,
          started_at: now,
          finished_at: now,
          parts: [],
          lease_expires_at: null,
        },
        now,
      ),
    ).toBe(false);
  });

  test("ensureMaterialized asks the coordinator to restart a stale run", async () => {
    const h = await harness();
    // A legacy/stale running record from the 031 path (no lease).
    await h.core.setMaterialization("shot_alarm_materialize", {
      state: "running",
      parts_done: 0,
      parts_total: 2,
      spans: 0,
      started_at: h.clock.now,
      finished_at: null,
      parts: [],
      error: null,
    });
    const calls: Array<Record<string, unknown>> = [];
    const materializer: MaterializerNamespaceLike = {
      getByName: (name: string) => ({
        fetch: async (request: Request) => {
          calls.push({ name, body: await request.json() });
          // Simulate the alarm finishing immediately so ensureMaterialized can
          // return through its normal wait loop.
          await h.core.setMaterialization("shot_alarm_materialize", {
            state: "complete",
            parts_done: 2,
            parts_total: 2,
            spans: 8,
            started_at: h.clock.now,
            finished_at: h.clock.now,
            parts: [],
            error: null,
          });
          return new Response(JSON.stringify({ shot_id: "shot_alarm_materialize", state: "scheduled" }), {
            status: 202,
            headers: { "content-type": "application/json" },
          });
        },
      }),
    };

    const result = await ensureMaterialized(
      { ingest: h.ingest, materializer },
      h.core.getShotRecord("shot_alarm_materialize")!,
      { waitMs: 2_000 },
    );
    expect(result.materialization?.state).toBe("complete");
    expect(calls.length).toBe(1);
    expect(calls[0]?.name).toBe("shot_alarm_materialize");
    expect((calls[0]?.body as { restart?: boolean }).restart).toBe(true);
  });
});
