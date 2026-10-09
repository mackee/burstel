import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/app";
import { makeCore } from "./fakes";
import type { TestHarness } from "./fakes";

const TOKEN = "test-api-token";

/** In-memory stand-in for a Durable Object namespace: name -> ShotDO. */
class FakeNamespace implements ShotNamespaceLike {
  readonly shots = new Map<string, TestHarness>();

  getByName(name: string): DoStubLike {
    let harness = this.shots.get(name);
    if (!harness) {
      harness = makeCore(name);
      this.shots.set(name, harness);
    }
    const core = harness.core;
    return { fetch: (request: Request) => core.handleInternal(request) };
  }
}

function makeEnv(namespace = new FakeNamespace()): { env: AppEnv; namespace: FakeNamespace } {
  return {
    env: { SHOT_DO: namespace, BURSTEL_API_TOKEN: TOKEN, PUBLIC_BASE_URL: "https://burstel.example" },
    namespace,
  };
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

function get(path: string, token: string | null = TOKEN): Request {
  const headers: Record<string, string> = {};
  if (token) headers["authorization"] = `Bearer ${token}`;
  return new Request(`https://burstel.example${path}`, { headers });
}

async function createShot(env: AppEnv): Promise<{ id: string; body: Record<string, unknown> }> {
  const resp = await handleRequest(post("/api/shots", { metadata: { scenario: "bench" } }), env);
  expect(resp.status).toBe(201);
  const body = (await resp.json()) as Record<string, unknown>;
  return { id: String(body.shot_id), body };
}

describe("Worker API", () => {
  test("healthz is reachable without a token", async () => {
    const { env } = makeEnv();
    const resp = await handleRequest(get("/healthz", null), env);
    expect(resp.status).toBe(200);
    expect(((await resp.json()) as { status: string }).status).toBe("ok");
  });

  test("control and ingest APIs reject a missing/invalid token", async () => {
    const { env } = makeEnv();
    expect((await handleRequest(post("/api/shots", {}, null), env)).status).toBe(401);
    expect((await handleRequest(post("/api/shots", {}, "wrong"), env)).status).toBe(401);
    expect((await handleRequest(get("/api/shots/unknown", null), env)).status).toBe(401);
    expect((await handleRequest(post("/shots/unknown/v1/traces", {}, null), env)).status).toBe(401);
  });

  test("creating a shot returns an unguessable id and per-shot OTLP endpoints", async () => {
    const { env, namespace } = makeEnv();
    const { id, body } = await createShot(env);

    expect(id).toMatch(/^[A-Za-z0-9_-]{16,64}$/);
    expect(body.state).toBe("recording");
    expect(body.otlp_base_url).toBe(`https://burstel.example/shots/${id}`);
    expect(body.otlp_endpoints).toEqual({
      traces: `https://burstel.example/shots/${id}/v1/traces`,
      metrics: `https://burstel.example/shots/${id}/v1/metrics`,
      logs: `https://burstel.example/shots/${id}/v1/logs`,
    });
    expect(namespace.shots.has(id)).toBe(true);
  });

  test("two shots map to two independent DO identities", async () => {
    const { env, namespace } = makeEnv();
    const a = await createShot(env);
    const b = await createShot(env);

    expect(a.id).not.toBe(b.id);
    expect(namespace.shots.size).toBe(2);
    expect(namespace.shots.get(a.id)).not.toBe(namespace.shots.get(b.id));
  });

  test("status and stop operate on the addressed shot", async () => {
    const { env } = makeEnv();
    const { id } = await createShot(env);

    const statusResp = await handleRequest(get(`/api/shots/${id}`), env);
    expect(statusResp.status).toBe(200);
    expect(((await statusResp.json()) as Record<string, unknown>).shot_id).toBe(id);

    const stopResp = await handleRequest(post(`/api/shots/${id}/stop`, {}), env);
    expect(stopResp.status).toBe(200);
    const stopped = (await stopResp.json()) as Record<string, unknown>;
    expect(stopped.state).toBe("frozen");
    expect(typeof stopped.sha256).toBe("string");

    const againResp = await handleRequest(post(`/api/shots/${id}/stop`, {}), env);
    const again = (await againResp.json()) as Record<string, unknown>;
    expect(again.sha256).toBe(stopped.sha256);
  });

  test("valid OTLP ingest is proxied and preserves response semantics", async () => {
    const { env } = makeEnv();
    const { id } = await createShot(env);

    const resp = await handleRequest(
      new Request(`https://burstel.example/shots/${id}/v1/traces`, {
        method: "POST",
        headers: { "content-type": "application/x-protobuf", authorization: `Bearer ${TOKEN}` },
        body: new Uint8Array([0x0a, 0x00]),
      }),
      env,
    );

    expect(resp.status).toBe(200);
    expect(resp.headers.get("content-type")).toBe("application/x-protobuf");
    expect(resp.headers.get("x-burstel-shot-id")).toBe(id);
    expect(new Uint8Array(await resp.arrayBuffer()).byteLength).toBe(2);
  });

  test("malformed/unknown routes are rejected", async () => {
    const { env } = makeEnv();
    expect((await handleRequest(get("/api/shots/../../etc/passwd"), env)).status).toBe(404);
    expect((await handleRequest(get("/api/shots/not/a/valid/id/"), env)).status).toBe(404);
    expect((await handleRequest(post("/api/shots/short/stop", {}), env)).status).toBe(400);
    expect((await handleRequest(get("/nope"), env)).status).toBe(404);
  });
});
