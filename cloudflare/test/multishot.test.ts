// Spec 013, Part A: concurrent multi-shot ingest.
//
// Deterministic Worker-level tests over the real request handler, the real
// IngestCore/ShotCore over in-process SQLite, and fake containers. They pin
// token -> shot routing isolation and the MAX_CONCURRENT_RECORDING cap.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { DEFAULT_MAX_CONCURRENT_RECORDING } from "../src/env";
import type { IngestCore } from "../src/ingest-core";
import { FakeArtifacts, FakeIngestNamespace, makeCore, makeIngest } from "./fakes";
import type { TestHarness } from "./fakes";

const TOKEN = "multishot-admin-token";

class FakeShotNamespace implements ShotNamespaceLike {
  readonly shots = new Map<string, TestHarness>();
  readonly artifacts = new FakeArtifacts();

  getByName(name: string): DoStubLike {
    let harness = this.shots.get(name);
    if (!harness) {
      harness = makeCore(name, { artifacts: this.artifacts });
      this.shots.set(name, harness);
    }
    const core = harness.core;
    return { fetch: (request: Request) => core.handleInternal(request) };
  }
}

interface Setup {
  env: AppEnv;
  shots: FakeShotNamespace;
  ingest: IngestCore;
}

let tokenCounter = 0;
function nextToken(): string {
  tokenCounter += 1;
  return `blt_multi_${tokenCounter}`;
}

function setup(options: { max?: number } = {}): Setup {
  const shots = new FakeShotNamespace();
  const ingest = makeIngest({ generateToken: nextToken });
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    BURSTEL_API_TOKEN: TOKEN,
    PUBLIC_BASE_URL: "https://burstel.example",
  };
  if (options.max !== undefined) env.MAX_CONCURRENT_RECORDING = String(options.max);
  return { env, shots, ingest: ingest.core };
}

function post(path: string, body: unknown, token: string | null = TOKEN): Request {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (token) headers["authorization"] = `Bearer ${token}`;
  return new Request(`https://burstel.example${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
}

function push(token: string | null, signal = "traces", bytes = new Uint8Array([0x0a, 0x00])): Request {
  const headers: Record<string, string> = { "content-type": "application/x-protobuf" };
  if (token) headers["x-burstel-token"] = token;
  return new Request(`https://burstel.example/ingest/v1/${signal}`, {
    method: "POST",
    headers,
    body: bytes,
  });
}

async function createShot(env: AppEnv): Promise<{ id: string; token: string; body: Record<string, unknown> }> {
  const resp = await handleRequest(post("/api/shots", { metadata: {} }), env);
  expect(resp.status).toBe(201);
  const body = (await resp.json()) as Record<string, unknown>;
  return { id: String(body.shot_id), token: String(body.ingest_token), body };
}

describe("concurrent shots route by token", () => {
  test("two recording shots with distinct tokens land only in their own DuckDB", async () => {
    const { env, shots } = setup();
    const a = await createShot(env);
    const b = await createShot(env);
    expect(a.id).not.toBe(b.id);
    expect(a.token).not.toBe(b.token);

    // Token A: two trace batches. Token B: one log batch.
    for (let i = 0; i < 2; i++) {
      const resp = await handleRequest(push(a.token, "traces"), env);
      expect(resp.status).toBe(200);
      expect(resp.headers.get("x-burstel-shot-id")).toBe(a.id);
    }
    const respB = await handleRequest(push(b.token, "logs"), env);
    expect(respB.status).toBe(200);
    expect(respB.headers.get("x-burstel-shot-id")).toBe(b.id);

    // Neither token reached the other shot's recorder.
    expect(shots.shots.get(a.id)!.container.ingestBodies).toEqual([
      { signal: "traces", bytes: 2 },
      { signal: "traces", bytes: 2 },
    ]);
    expect(shots.shots.get(b.id)!.container.ingestBodies).toEqual([{ signal: "logs", bytes: 2 }]);

    // Both freeze independently with distinct artifact keys and states.
    const frozenA = (await (await handleRequest(post(`/api/shots/${a.id}/stop`, {}), env)).json()) as {
      state: string;
      freeze?: { artifact_key: string };
    };
    const frozenB = (await (await handleRequest(post(`/api/shots/${b.id}/stop`, {}), env)).json()) as {
      state: string;
      freeze?: { artifact_key: string };
    };
    expect(frozenA.state).toBe("frozen");
    expect(frozenB.state).toBe("frozen");
    expect(frozenA.freeze?.artifact_key).toBe(`shots/${a.id}/telemetry.duckdb`);
    expect(frozenB.freeze?.artifact_key).toBe(`shots/${b.id}/telemetry.duckdb`);
  });

  test("an unknown or admin token cannot reach any shot", async () => {
    const { env } = setup();
    const a = await createShot(env);
    expect((await handleRequest(push("blt_bogus_token", "traces"), env)).status).toBe(401);
    expect((await handleRequest(push(TOKEN, "traces"), env)).status).toBe(401);
    // The real token still works after the rejections.
    expect((await handleRequest(push(a.token, "traces"), env)).headers.get("x-burstel-shot-id")).toBe(a.id);
  });
});

describe("MAX_CONCURRENT_RECORDING cap", () => {
  test("defaults to the documented value when the var is unset", () => {
    expect(DEFAULT_MAX_CONCURRENT_RECORDING).toBe(10);
  });

  test("a shot beyond the cap is rejected with a retryable 503 capacity_exhausted and no partial state", async () => {
    const { env, shots } = setup({ max: 2 });
    await createShot(env);
    await createShot(env);
    const before = shots.shots.size;

    const third = await handleRequest(post("/api/shots", { metadata: {} }), env);
    expect(third.status).toBe(503);
    const body = (await third.json()) as { code: string; retryable: boolean };
    expect(body.code).toBe("capacity_exhausted");
    expect(body.retryable).toBe(true);

    // No ShotDO/container was created for the rejected shot.
    expect(shots.shots.size).toBe(before);
  });

  test("stopping a shot frees a slot for the next start", async () => {
    const { env, shots } = setup({ max: 2 });
    const a = await createShot(env);
    await createShot(env);
    expect((await handleRequest(post("/api/shots", { metadata: {} }), env)).status).toBe(503);

    expect((await handleRequest(post(`/api/shots/${a.id}/stop`, {}), env)).status).toBe(200);

    const c = await createShot(env);
    expect(c.id).toBeDefined();
    expect(shots.shots.size).toBe(3);
  });

  test("concurrent starts within the cap both succeed; the extra one is capped", async () => {
    const { env } = setup({ max: 2 });
    const results = await Promise.all([
      handleRequest(post("/api/shots", { metadata: { n: 1 } }), env),
      handleRequest(post("/api/shots", { metadata: { n: 2 } }), env),
      handleRequest(post("/api/shots", { metadata: { n: 3 } }), env),
    ]);
    const statuses = results.map((r) => r.status).sort();
    expect(statuses).toEqual([201, 201, 503]);
  });
});
