// Spec 032: alarm-driven durable materialization.
import { describe, expect, test } from "bun:test";

import type { CompactorNamespaceLike, DoStubLike, MaterializerNamespaceLike } from "../src/env";
import type { MaterializationObject, WindowShotRecord } from "../src/ingest-core";
import type { MaterializePrewarmState, MaterializeRunState } from "../src/materialize-driver";
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

class MemoryPrewarmStore {
  state: MaterializePrewarmState | null = null;

  async load(): Promise<MaterializePrewarmState | null> {
    return this.state ? { ...this.state } : null;
  }

  async save(state: MaterializePrewarmState | null): Promise<void> {
    this.state = state ? { ...state } : null;
  }
}

class FakeMaterializers implements CompactorNamespaceLike {
  readonly names: string[] = [];
  readonly requests: Array<Record<string, unknown>> = [];
  /** Spec 037: which member attempted which part, in call order. */
  readonly assignments: Array<{ member: string; part: number }> = [];
  readonly prewarms: string[] = [];
  readonly keepAlives: string[] = [];
  fail = false;
  capacity = false;
  /** Spec 037: capacity errors only on these pool members. */
  readonly capacityMembers = new Set<string>();
  /** Optional per-part hold; lets a test observe pool queueing. */
  gate: ((body: Record<string, unknown>) => Promise<void>) | undefined;

  getByName(name: string): DoStubLike {
    return {
      fetch: (request: Request) => {
        this.names.push(name);
        return this.handle(request);
      },
    };
  }

  private async handle(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    const name = this.names.at(-1) ?? "";
    if (path === "/internal/prewarm") {
      this.prewarms.push(name);
      return new Response(JSON.stringify({ prewarmed: true }), { status: 200 });
    }
    if (path === "/internal/keepalive") {
      this.keepAlives.push(name);
      return new Response(JSON.stringify({ keepalive_ms: 600000 }), { status: 200 });
    }
    if (path !== "/internal/materialize") {
      return new Response(JSON.stringify({ error: "not found" }), { status: 404 });
    }
    const body = (await request.json()) as Record<string, unknown>;
    const index = Number(body.part_index ?? 0);
    this.assignments.push({ member: name, part: index });
    if (this.fail) return new Response(JSON.stringify({ error: "container exploded" }), { status: 500 });
    if (this.capacity || this.capacityMembers.has(name)) {
      return new Response(
        JSON.stringify({ error: "There is no container instance that can be provided to this Durable Object" }),
        { status: 503 },
      );
    }
    if (this.gate) await this.gate(body);
    this.requests.push(body);
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
  prewarmStore: MemoryPrewarmStore;
  scheduler: FakeScheduler;
  compactor: FakeMaterializers;
  driver: MaterializeDriver;
  stopped: WindowShotRecord;
  logs: Array<{ message: string; fields?: Record<string, unknown> }>;
}

async function harness(
  options: {
    maxAttempts?: number;
    parallelism?: number;
    capacityBackoffMs?: number;
    memberCooldownMs?: number;
    preloadAnalysis?: (shotId: string) => Promise<void>;
  } = {},
): Promise<Harness> {
  const clock = { now: 1_700_000_100_000 };
  const bucket = new FakeBucket();
  const { core } = makeIngest({ now: () => clock.now, bucket, lateWriteLimitMs: 10_000 });
  const ingest = new FakeIngestNamespace(core);
  await core.startWindowShot({ shot_id: "shot_alarm_materialize", owner: "tester", instances: ["node-1"] });
  const start = core.getShotRecord("shot_alarm_materialize")!.window!.start_ms;
  for (let i = 0; i < 4; i++) await seedJournal(bucket, start + i * 1_000, `o${i}`);
  const stopped = await core.stopWindowShot("shot_alarm_materialize", start + 10_000);
  const store = new MemoryStore();
  const prewarmStore = new MemoryPrewarmStore();
  const scheduler = new FakeScheduler();
  const compactor = new FakeMaterializers();
  const logs: Array<{ message: string; fields?: Record<string, unknown> }> = [];
  const driver = new MaterializeDriver({
    shotId: "shot_alarm_materialize",
    ingest,
    compactor,
    store,
    prewarmStore,
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
      keepAliveMs: 10 * 60 * 1000,
      capacityBackoffMs: options.capacityBackoffMs ?? 2_000,
      memberCooldownMs: options.memberCooldownMs ?? 1_000,
    },
    log: (message, fields) => {
      logs.push({ message, fields });
    },
    preloadAnalysis: options.preloadAnalysis,
  });
  return { clock, core, ingest, store, prewarmStore, scheduler, compactor, driver, stopped, logs };
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
    expect(run?.results.length).toBe(4);
    expect(run?.parts_total).toBe(4);
    expect(run?.spans).toBe(8);
    expect(h.compactor.names).toContain("materializer-0");
    expect(h.compactor.names).toContain("materializer-1");
    const record = h.core.getShotRecord("shot_alarm_materialize")!;
    expect(record.materialization?.state).toBe("complete");
    expect(record.coverage?.complete).toBe(true);
  });

  test("a complete run notifies the ShotDO preload alarm exactly once", async () => {
    const calls: string[] = [];
    const h = await harness({
      preloadAnalysis: async (shotId) => {
        calls.push(shotId);
      },
    });
    await h.driver.schedule(h.stopped);
    h.clock.now = h.scheduler.armed.at(-1)!;
    await h.driver.alarm();
    expect(calls).toEqual(["shot_alarm_materialize"]);
    const run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.analysis_preloaded).toBe(true);

    // A watchdog alarm must not notify again.
    h.clock.now += 10_000;
    await h.driver.alarm();
    expect(calls).toEqual(["shot_alarm_materialize"]);
  });

  test("a failed preload notification retries on the next alarm", async () => {
    let attempts = 0;
    const h = await harness({
      preloadAnalysis: async () => {
        attempts += 1;
        if (attempts < 2) throw new Error("shot do unavailable");
      },
    });
    await h.driver.schedule(h.stopped);
    h.clock.now = h.scheduler.armed.at(-1)!;
    await h.driver.alarm();
    let run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.analysis_preloaded).not.toBe(true);
    expect(run?.analysis_preload_attempts).toBe(1);
    expect(h.scheduler.armed.at(-1)).toBeGreaterThan(h.clock.now);

    h.clock.now = h.scheduler.armed.at(-1)!;
    await h.driver.alarm();
    run = await h.store.load();
    expect(run?.analysis_preloaded).toBe(true);
    expect(attempts).toBe(2);
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
    expect(compactor.requests.length).toBe(4);
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
    run.parts_total = 4;
    run.parts = [0, 1, 2, 3].map((index) => ({
      index,
      state: index === 0 ? ("running" as const) : ("pending" as const),
      attempts: index === 0 ? 1 : 0,
      lease_owner: index === 0 ? "dead-coordinator" : null,
      lease_expires_at: index === 0 ? h.clock.now - 1 : null,
      started_at: index === 0 ? h.clock.now - 5_000 : null,
      finished_at: null,
      error: null,
    }));
    await h.store.save(run);

    await h.driver.alarm();

    const retried = await h.store.load();
    expect(retried?.state).toBe("complete");
    expect(retried?.parts[0]?.attempts).toBe(2);
    expect(h.compactor.requests.length).toBe(4);
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

describe("spec 033 fixed materializer pool", () => {
  test("assigns parts to fixed pool members and never exceeds parallelism", async () => {
    const h = await harness({ parallelism: 2 });
    let active = 0;
    let maxActive = 0;
    h.compactor.gate = async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 1));
      active -= 1;
    };
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;
    await h.driver.alarm();

    const run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.results.length).toBe(4);
    expect(maxActive).toBeLessThanOrEqual(2);
    expect(new Set(h.compactor.names)).toEqual(new Set(["materializer-0", "materializer-1"]));
    expect(run?.parts.every((part) => part.pool_member == null)).toBe(true);
  });

  test("no-capacity backoff queues the part without consuming an attempt", async () => {
    const h = await harness({ parallelism: 2, capacityBackoffMs: 2_000 });
    h.compactor.capacity = true;
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;
    await h.driver.alarm();

    let run = await h.store.load();
    expect(run?.state).toBe("scheduled");
    expect(run?.scheduled_at).toBe(h.clock.now + 2_000);
    expect(run?.parts.every((part) => part.state === "pending")).toBe(true);
    expect(run?.parts.every((part) => part.attempts === 0)).toBe(true);

    // Once capacity returns, the same run completes with its first real attempt.
    h.compactor.capacity = false;
    h.clock.now = run!.scheduled_at;
    await h.driver.alarm();
    run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.parts.every((part) => part.attempts === 1)).toBe(true);
  });

  test("keeps a finished pool member warm for the next shot", async () => {
    const h = await harness({ parallelism: 2 });
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;
    await h.driver.alarm();
    expect((await h.store.load())?.state).toBe("complete");
    expect(h.compactor.keepAlives).toContain("materializer-0");
    expect(h.compactor.keepAlives).toContain("materializer-1");
  });

  test("prewarm is armed by the request and executed by the alarm", async () => {
    const h = await harness({ parallelism: 2 });
    await h.driver.schedulePrewarm();
    expect(h.scheduler.armed.at(-1)).toBe(h.clock.now);
    expect(h.prewarmStore.state?.done).toBe(false);
    expect(h.compactor.prewarms).toEqual([]);

    await h.driver.alarm();
    expect(h.compactor.prewarms).toEqual(["materializer-0", "materializer-1"]);
    expect(h.prewarmStore.state?.done).toBe(true);
  });
});

describe("spec 037 materializer work stealing", () => {
  test("a capacity failure cools only that member down and an idle member steals the part", async () => {
    const h = await harness({ parallelism: 2, capacityBackoffMs: 2_000, memberCooldownMs: 60_000 });
    h.compactor.capacityMembers.add("materializer-0");
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;
    await h.driver.alarm();

    const run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.results.length).toBe(4);
    expect(run?.parts.every((part) => part.state === "complete")).toBe(true);
    // The stolen part was retried once on the healthy member; the capacity
    // error itself never consumed an attempt.
    expect(run?.parts[0]?.attempts).toBe(1);
    expect(h.compactor.assignments.filter((a) => a.member === "materializer-1").length).toBe(4);
    expect(h.compactor.assignments.filter((a) => a.member === "materializer-0").length).toBe(1);

    const unhealthy = h.logs.find((entry) => entry.message === "materialize.member.unhealthy");
    expect(unhealthy?.fields?.["pool_member"]).toBe(0);
    expect(unhealthy?.fields?.["cooldown_ms"]).toBe(60_000);
    const stolen = h.logs.find((entry) => entry.message === "materialize.part.stolen");
    expect(stolen?.fields?.["part"]).toBe(0);
    expect(stolen?.fields?.["from_member"]).toBe(0);
    expect(stolen?.fields?.["to_member"]).toBe(1);

    // No capacity backoff alarm: the run finished inside this tick.
    expect(h.scheduler.armed.at(-1)).toBeLessThanOrEqual(h.clock.now);
  });

  test("a finished member pulls the next part while another member is still running", async () => {
    const h = await harness({ parallelism: 2 });
    const started: number[] = [];
    const finished: number[] = [];
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    h.compactor.gate = async (body) => {
      const index = Number(body.part_index);
      started.push(index);
      // Hold part 0 until part 1 is in flight, so the concurrency assertion
      // does not depend on request ordering.
      if (index === 0) {
        while (!started.includes(1)) await new Promise((resolve) => setTimeout(resolve, 1));
      }
      // Part 1 holds member 1 until member 0 has pulled part 2.
      if (index === 1) await blocked;
      finished.push(index);
    };
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;
    const alarm = h.driver.alarm();
    const deadline = Date.now() + 5_000;
    while (!started.includes(2) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    // Part 2 started while part 1 is still inside the gate, i.e. the finished
    // member pulled the next pending part without an alarm round-trip.
    expect(started).toContain(2);
    expect(finished).not.toContain(1);
    release();
    await alarm;

    const run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.results.length).toBe(4);
    expect(h.compactor.requests.length).toBe(4);
  });

  test("all-unhealthy members keep the capacity backoff until the cooldown expires", async () => {
    const h = await harness({ parallelism: 1, capacityBackoffMs: 2_000, memberCooldownMs: 60_000 });
    h.compactor.capacity = true;
    await h.driver.schedule(h.stopped);
    h.clock.now = h.stopped.window!.end_ms + 10_000;
    await h.driver.alarm();

    let run = await h.store.load();
    expect(run?.state).toBe("scheduled");
    expect(run?.scheduled_at).toBe(h.clock.now + 2_000);
    expect(run?.parts.every((part) => part.state === "pending" && part.attempts === 0)).toBe(true);
    const cooldownUntil = run?.member_cooldowns?.["0"];
    expect(cooldownUntil).toBe(h.clock.now + 60_000);

    // Inside the cooldown the only member is unhealthy: same backoff, no work.
    h.clock.now = run!.scheduled_at;
    await h.driver.alarm();
    run = await h.store.load();
    expect(run?.state).toBe("scheduled");
    expect(run?.scheduled_at).toBe(h.clock.now + 2_000);
    expect(h.compactor.assignments.length).toBe(1);

    // Once the cooldown expires the parts run normally.
    h.compactor.capacity = false;
    h.clock.now = cooldownUntil!;
    await h.driver.alarm();
    run = await h.store.load();
    expect(run?.state).toBe("complete");
    expect(run?.parts.every((part) => part.attempts === 1)).toBe(true);
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
