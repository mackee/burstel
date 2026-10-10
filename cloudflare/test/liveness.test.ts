// Spec 012: keep the recording container alive and make container loss
// terminal. Incident: a shot's container stopped ~2 minutes after shot_start
// (the ShotDO went idle) and every later OTLP push was answered with a
// retryable 503 that could never succeed.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import { ANALYSIS_KEEPALIVE_MS, RECORDING_KEEPALIVE_MS } from "../src/core";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { FakeContainer, FakeIngestNamespace, makeCore, makeIngest } from "./fakes";
import type { TestHarness } from "./fakes";

const HOUR = 60 * 60 * 1000;

function otlpRequest(): Request {
  return new Request("https://worker.test/v1/traces", {
    method: "POST",
    headers: { "content-type": "application/x-protobuf" },
    body: new Uint8Array([0x0a, 0x02, 0x01, 0x02]),
  });
}

/** A controllable clock for heartbeat-cap tests. */
function clock(start = 1_700_000_000_000) {
  const state = { now: start };
  return { state, now: () => state.now };
}

describe("recording keepalive", () => {
  test("the 6h inactivity timeout is applied after readiness, before recording", async () => {
    const h = makeCore("shot-keep");
    const info = await h.core.create({});
    expect(info.state).toBe("recording");
    expect(h.container.keepAlives).toEqual([RECORDING_KEEPALIVE_MS]);
  });

  test("a keepalive failure fails the start instead of recording unprotected", async () => {
    const container = new FakeContainer({ keepAliveError: new Error("platform refused") });
    const h = makeCore("shot-keepfail", { container });
    await expect(h.core.create({})).rejects.toMatchObject({ status: 503, code: "container_keepalive" });
    expect((await h.core.status()).state).toBe("failed");
    expect(h.scheduler.armed).toHaveLength(0);

    container.keepAliveError = undefined;
    expect((await h.core.create({})).state).toBe("recording");
  });

  test("analysis containers get a short timeout", async () => {
    const h = makeCore("shot-analysis");
    await h.core.create({});
    await h.core.stop();
    h.container.keepAlives.length = 0;
    await h.core.query("SELECT 1");
    expect(h.container.keepAlives).toEqual([ANALYSIS_KEEPALIVE_MS]);
  });
});

describe("recording heartbeat", () => {
  test("is armed on start and re-armed while the container runs", async () => {
    const c = clock();
    const h = makeCore("shot-hb", { now: c.now });
    await h.core.create({});
    expect(h.scheduler.armed).toEqual([c.state.now + 60_000]);

    c.state.now += 60_000;
    await h.core.heartbeat();
    expect(h.scheduler.armed).toHaveLength(2);
    expect(h.scheduler.armed[1]).toBe(c.state.now + 60_000);
    expect(h.container.keepAlives).toEqual([RECORDING_KEEPALIVE_MS, RECORDING_KEEPALIVE_MS]);
  });

  test("detects a stopped container, records the loss and stops re-arming", async () => {
    const h = makeCore("shot-hb-lost");
    await h.core.create({});
    h.container.runningFlag = false;
    await h.core.heartbeat();

    const info = await h.core.status();
    expect(info.state).toBe("failed");
    expect(info.last_error).toMatch(/lost/);
    expect(info.freeze).toMatchObject({ phase: "recording", retryable: false, lost: true });
    expect(h.scheduler.armed).toHaveLength(1);
  });

  test("does nothing once the shot is frozen", async () => {
    const h = makeCore("shot-hb-frozen");
    await h.core.create({});
    await h.core.stop();
    await h.core.heartbeat();
    expect(h.scheduler.armed).toHaveLength(1);
    expect((await h.core.status()).state).toBe("frozen");
  });

  test("stops re-arming after the max recording time", async () => {
    const c = clock();
    const h = makeCore("shot-hb-cap", { now: c.now, maxRecordingMs: 24 * HOUR });
    await h.core.create({});
    c.state.now += 24 * HOUR;
    await h.core.heartbeat();
    expect(h.scheduler.armed).toHaveLength(1);
    expect((await h.core.status()).state).toBe("recording");
  });

  test("a restarted DO re-applies the timeout and re-arms for a running container", async () => {
    const h = makeCore("shot-restart");
    await h.core.create({});

    const container = new FakeContainer();
    container.runningFlag = true;
    const restarted = makeCore("shot-restart", { sql: h.sql, container });
    await restarted.core.load();
    expect(container.keepAlives).toEqual([RECORDING_KEEPALIVE_MS]);
    expect(restarted.scheduler.armed).toHaveLength(1);
    expect((await restarted.core.status()).state).toBe("recording");
  });

  test("a restarted DO whose container is gone records the loss", async () => {
    const h = makeCore("shot-restart-lost");
    await h.core.create({});
    const restarted = makeCore("shot-restart-lost", { sql: h.sql }); // container not running
    await restarted.core.load();
    expect((await restarted.core.status()).state).toBe("failed");
    expect(restarted.scheduler.armed).toHaveLength(0);
  });

  test("a scheduler failure does not break recording", async () => {
    const h = makeCore("shot-hb-schedfail");
    h.scheduler.failWith = new Error("alarm unavailable");
    expect((await h.core.create({})).state).toBe("recording");
  });
});

describe("container loss is terminal", () => {
  function lostShot(id: string): Promise<TestHarness> {
    const h = makeCore(id);
    return h.core.create({}).then(() => {
      h.container.runningFlag = false;
      return h;
    });
  }

  test("ingest rejects definitively with shot_lost and keeps doing so", async () => {
    const h = await lostShot("shot-lost-ingest");
    for (let i = 0; i < 2; i++) {
      const resp = await h.core.handleInternal(
        new Request("https://do/internal/ingest/traces", { method: "POST", body: new Uint8Array([1]) }),
      );
      expect(resp.status).toBe(409);
      expect(resp.headers.get("x-burstel-disposition")).toBe("ignored-shot-lost");
      expect(((await resp.json()) as { code: string }).code).toBe("shot_lost");
    }
    expect(h.container.ingestBodies).toHaveLength(0);
  });

  test("stop does not touch the container and returns shot_lost", async () => {
    const h = await lostShot("shot-lost-stop");
    const before = h.container.requests.length;
    await expect(h.core.stop()).rejects.toMatchObject({ status: 409, code: "shot_lost", retryable: false });
    expect(h.container.requests.length).toBe(before);
    expect(h.artifacts.putCalls).toBe(0);
    // Idempotent terminal answer.
    await expect(h.core.stop()).rejects.toMatchObject({ code: "shot_lost" });
    await expect(h.core.create({})).rejects.toMatchObject({ status: 409 });
  });

  test("a retryable stop failure whose container is gone becomes terminal", async () => {
    const h = makeCore("shot-stuck");
    await h.core.create({});
    h.artifacts.failPut = new Error("R2 down");
    await expect(h.core.stop()).rejects.toMatchObject({ code: "freeze_failed" });
    expect((await h.core.status()).freeze).toMatchObject({ phase: "stop", retryable: true });

    h.container.runningFlag = false;
    await expect(h.core.stop()).rejects.toMatchObject({ code: "shot_lost" });
    expect((await h.core.status()).freeze).toMatchObject({ lost: true, retryable: false });
  });

  test("a running recorder's genuine 503 is still propagated, not treated as loss", async () => {
    const h = makeCore("shot-503", { container: new FakeContainer({ otlpStatus: 503 }) });
    await h.core.create({});
    const resp = await h.core.ingest("traces", otlpRequest());
    expect(resp.status).toBe(503);
    expect((await h.core.status()).state).toBe("recording");
  });
});

describe("stable /ingest and stop with a lost shot", () => {
  class CoreNamespace implements ShotNamespaceLike {
    constructor(readonly h: TestHarness) {}
    getByName(): DoStubLike {
      return { fetch: (request: Request) => this.h.core.handleInternal(request) };
    }
  }

  async function setup() {
    const h = makeCore("shotLostE2eAbcdef01");
    await h.core.create({});
    const ingest = makeIngest({ now: () => 1_700_000_000_000, generateToken: () => "blt_lost" });
    const issued = await ingest.core.issueShotToken("shotLostE2eAbcdef01");
    const env: AppEnv = {
      SHOT_DO: new CoreNamespace(h),
      INGEST_DO: new FakeIngestNamespace(ingest.core),
      PUBLIC_BASE_URL: "https://burstel.example",
      BURSTEL_API_TOKEN: "admin-token",
    };
    return { h, ingest: ingest.core, env, token: issued.token };
  }

  test("push to a lost shot is acknowledged as a discard", async () => {
    const { h, env, token } = await setup();
    h.container.runningFlag = false;
    const resp = await handleRequest(
      new Request("https://burstel.example/ingest/v1/traces", {
        method: "POST",
        headers: { "content-type": "application/x-protobuf", "x-burstel-token": token },
        body: new Uint8Array([0x0a, 0x00]),
      }),
      env,
    );
    expect(resp.status).toBe(200);
    expect(resp.headers.get("x-burstel-disposition")).toBe("ignored-shot-lost");
  });

  test("admin stop of a lost shot returns shot_lost and frees the recording slot", async () => {
    const { h, ingest, env } = await setup();
    h.container.runningFlag = false;
    const resp = await handleRequest(
      new Request("https://burstel.example/api/shots/shotLostE2eAbcdef01/stop", {
        method: "POST",
        headers: { authorization: "Bearer admin-token" },
      }),
      env,
    );
    expect(resp.status).toBe(409);
    expect(((await resp.json()) as { code: string }).code).toBe("shot_lost");
    const status = (await ingest.status()) as { recording: number };
    expect(status.recording).toBe(0);
  });
});
