// Spec 014: shots record concurrently; the application-level
// `MAX_CONCURRENT_RECORDING` cap is gone. Concurrency is bounded only by the
// explicit backend container capacity controls.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import type { IngestCore } from "../src/ingest-core";
import { FakeArtifacts, FakeBucket, FakeIngestNamespace, makeCore, makeIngest } from "./fakes";
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

function setup(): Setup {
  const shots = new FakeShotNamespace();
  const ingest = makeIngest();
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    BUCKET: new FakeBucket(),
    BURSTEL_API_TOKEN: TOKEN,
    PUBLIC_BASE_URL: "https://burstel.example",
  };
  return { env, shots, ingest: ingest.core };
}

function post(path: string, body: unknown): Request {
  return new Request(`https://burstel.example${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` },
    body: JSON.stringify(body),
  });
}

async function createShot(env: AppEnv): Promise<string> {
  const resp = await handleRequest(post("/api/shots", { metadata: {} }), env);
  expect(resp.status).toBe(201);
  return String(((await resp.json()) as { shot_id: string }).shot_id);
}

describe("concurrent recording (no application cap)", () => {
  test("many shots record simultaneously and all are listed", async () => {
    const { env } = setup();
    const ids: string[] = [];
    for (let i = 0; i < 6; i++) ids.push(await createShot(env));

    const listed = (await (await handleRequest(
      new Request("https://burstel.example/api/shots", { headers: { authorization: `Bearer ${TOKEN}` } }),
      env,
    )).json()) as { total: number };
    expect(listed.total).toBe(6);
  });

  test("concurrent starts all succeed", async () => {
    const { env } = setup();
    const results = await Promise.all([
      handleRequest(post("/api/shots", { metadata: { n: 1 } }), env),
      handleRequest(post("/api/shots", { metadata: { n: 2 } }), env),
      handleRequest(post("/api/shots", { metadata: { n: 3 } }), env),
      handleRequest(post("/api/shots", { metadata: { n: 4 } }), env),
    ]);
    expect(results.map((r) => r.status)).toEqual([201, 201, 201, 201]);
  });

  test("stopping a shot keeps the others recording", async () => {
    const { env } = setup();
    const a = await createShot(env);
    await createShot(env);
    expect((await handleRequest(post(`/api/shots/${a}/stop`, {}), env)).status).toBe(200);
    const status = (await handleRequest(
      new Request("https://burstel.example/api/shots", { headers: { authorization: `Bearer ${TOKEN}` } }),
      env,
    )).json() as Promise<{ shots: Array<{ state: string }> }>;
    const states = (await status).shots.map((s) => s.state);
    expect(states.filter((s) => s === "recording").length).toBe(1);
    expect(states.filter((s) => s === "frozen").length).toBe(1);
  });
});
