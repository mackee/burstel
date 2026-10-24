import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/app";
import type { MaterializerNamespaceLike } from "../src/env";
import { FakeIngestNamespace, makeCore, makeIngest } from "./fakes";
import type { TestHarness } from "./fakes";

const TOKEN = "app-window-token";

class FakeNamespace implements ShotNamespaceLike {
  readonly shots = new Map<string, TestHarness>();
  getByName(name: string): DoStubLike {
    let harness = this.shots.get(name);
    if (!harness) {
      harness = makeCore(name);
      this.shots.set(name, harness);
    }
    const core = harness.core;
    return {
      fetch: (request: Request) =>
        new URL(request.url).pathname === "/internal/prewarm"
          ? Promise.resolve(new Response(JSON.stringify({ prewarm: "scheduled" }), { status: 202 }))
          : core.handleInternal(request),
    };
  }
}

class FakeMaterializerNamespace implements MaterializerNamespaceLike {
  readonly requests: Array<{ name: string; body: Record<string, unknown> }> = [];

  getByName(name: string): DoStubLike {
    return {
      fetch: async (request: Request) => {
        this.requests.push({ name, body: (await request.json()) as Record<string, unknown> });
        return new Response(JSON.stringify({ shot_id: name, state: "scheduled" }), {
          status: 202,
          headers: { "content-type": "application/json" },
        });
      },
    };
  }
}

function makeEnv(): AppEnv {
  return {
    SHOT_DO: new FakeNamespace(),
    INGEST_DO: new FakeIngestNamespace(makeIngest().core),
    MATERIALIZE_DO: new FakeMaterializerNamespace(),
    BURSTEL_API_TOKEN: TOKEN,
    PUBLIC_BASE_URL: "https://burstel.example",
  };
}

function post(path: string, body: unknown, token: string | null = TOKEN): Request {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (token) headers["authorization"] = `Bearer ${token}`;
  return new Request(`https://burstel.example${path}`, { method: "POST", headers, body: JSON.stringify(body) });
}

function get(path: string, token: string | null = TOKEN): Request {
  const headers: Record<string, string> = {};
  if (token) headers["authorization"] = `Bearer ${token}`;
  return new Request(`https://burstel.example${path}`, { headers });
}

describe("REST logical windows", () => {
  test("POST /api/shots/window creates a backdated external window", async () => {
    const env = makeEnv();
    await handleRequest(post("/api/instances", { name: "web-1" }), env);
    const now = Date.now();
    const resp = await handleRequest(
      post("/api/shots/window", {
        name: "bench",
        started_at: new Date(now - 120_000).toISOString(),
        ended_at: new Date(now - 60_000).toISOString(),
        instances: ["web-1"],
      }),
      env,
    );
    expect(resp.status).toBe(201);
    const body = (await resp.json()) as Record<string, unknown>;
    expect(body["kind"]).toBe("window");
    expect((body["window"] as Record<string, unknown>)["provenance"]).toBe("external");

    const id = String(body["shot_id"]);
    const got = await handleRequest(get(`/api/shots/${id}`), env);
    expect(got.status).toBe(200);
    const gotBody = (await got.json()) as Record<string, unknown>;
    expect(gotBody["kind"]).toBe("window");
  });

  test("POST /api/shots with mode=window opens a server-clock marker and stops it", async () => {
    const env = makeEnv();
    await handleRequest(post("/api/instances", { name: "web-1" }), env);
    const started = await handleRequest(post("/api/shots", { mode: "window", instances: ["web-1"] }), env);
    expect(started.status).toBe(201);
    const body = (await started.json()) as Record<string, unknown>;
    expect(body["state"]).toBe("open");

    const id = String(body["shot_id"]);
    const stopped = await handleRequest(post(`/api/shots/${id}/stop`, {}), env);
    expect(stopped.status).toBe(200);
    const stoppedBody = (await stopped.json()) as Record<string, unknown>;
    expect(["ready", "incomplete"]).toContain(String(stoppedBody["state"]));
  });

  test("POST /api/shots/{id}/materialize records a durable retry (admin|member)", async () => {
    const env = makeEnv();
    await handleRequest(post("/api/instances", { name: "web-1" }), env);
    const now = Date.now();
    const created = await handleRequest(
      post("/api/shots/window", {
        started_at: new Date(now - 120_000).toISOString(),
        ended_at: new Date(now - 60_000).toISOString(),
        instances: ["web-1"],
      }),
      env,
    );
    const id = String(((await created.json()) as Record<string, unknown>)["shot_id"]);

    const resp = await handleRequest(post(`/api/shots/${id}/materialize`, { force: true }), env);
    expect(resp.status).toBe(202);
    const body = (await resp.json()) as Record<string, unknown>;
    const coverage = body["coverage"] as Record<string, unknown>;
    expect(coverage["complete"]).toBe(false);
    expect(coverage["materialization_state"]).toBe("pending");

    const coordinator = env.MATERIALIZE_DO as unknown as FakeMaterializerNamespace;
    expect(coordinator.requests.length).toBe(1);
    expect(coordinator.requests[0]?.name).toBe(id);
    expect(coordinator.requests[0]?.body["force"]).toBe(true);
  });

  test("POST /api/shots defaults to a window shot when mode is omitted", async () => {
    const env = makeEnv();
    const resp = await handleRequest(post("/api/shots", { metadata: { scenario: "bench" } }), env);
    expect(resp.status).toBe(201);
    const body = (await resp.json()) as Record<string, unknown>;
    expect(body["kind"]).toBe("window");
    expect(body["state"]).toBe("open");
    // Spec 033: a warm ShotDO is created only to prewarm the analysis
    // container; no recording shot state/container is created on this path.
    const prewarmed = [...(env.SHOT_DO as unknown as FakeNamespace).shots.values()];
    expect(prewarmed.length).toBeLessThanOrEqual(1);
    await expect(prewarmed[0]!.core.status()).rejects.toThrow(/unknown shot/);
  });

  test("explicit mode=recording still works and is flagged deprecated", async () => {
    const env = makeEnv();
    const resp = await handleRequest(post("/api/shots", { mode: "recording", metadata: {} }), env);
    expect(resp.status).toBe(201);
    const body = (await resp.json()) as Record<string, unknown>;
    expect(body["deprecated"]).toBe(true);
    expect(body["kind"]).toBeUndefined();
    expect((body["otlp_endpoints"] as Record<string, string>)["traces"]).toContain("/ingest/v1/traces");
    expect((env.SHOT_DO as unknown as FakeNamespace).shots.size).toBe(1);
  });

  test("unknown instance names are rejected with 400 unknown_instance", async () => {
    const env = makeEnv();
    const rejected = await handleRequest(post("/api/shots", { instances: ["nope"] }), env);
    expect(rejected.status).toBe(400);
    const body = (await rejected.json()) as { code: string; error: string };
    expect(body.code).toBe("unknown_instance");
    expect(body.error).toContain("nope");

    const malformed = await handleRequest(post("/api/shots", { instances: ["Not A Name"] }), env);
    expect(malformed.status).toBe(400);
    expect(((await malformed.json()) as { code: string }).code).toBe("unknown_instance");

    await handleRequest(post("/api/instances", { name: "web-1" }), env);
    const accepted = await handleRequest(post("/api/shots", { instances: ["web-1"] }), env);
    expect(accepted.status).toBe(201);
    expect(((await accepted.json()) as Record<string, unknown>)["kind"]).toBe("window");

    const unknownWindow = await handleRequest(
      post("/api/shots/window", {
        started_at: new Date(Date.now() - 120_000).toISOString(),
        ended_at: new Date(Date.now() - 60_000).toISOString(),
        instances: ["ghost"],
      }),
      env,
    );
    expect(unknownWindow.status).toBe(400);
    expect(((await unknownWindow.json()) as { code: string }).code).toBe("unknown_instance");
  });

  test("rejects an unordered window", async () => {
    const env = makeEnv();
    const now = Date.now();
    const resp = await handleRequest(
      post("/api/shots/window", {
        started_at: new Date(now).toISOString(),
        ended_at: new Date(now - 1000).toISOString(),
      }),
      env,
    );
    expect(resp.status).toBe(400);
  });

  test("retired per-shot ingest tokens return 410 with migration guidance", async () => {
    const env = makeEnv();
    const resp = await handleRequest(post("/api/shots/AAAAAAAAAAAAAAAA/ingest-token", {}), env);
    expect(resp.status).toBe(410);
    const body = (await resp.json()) as { error: string; code: string };
    expect(body.code).toBe("gone");
    expect(body.error).toContain("/ingest/v1/");

    const legacy = await handleRequest(post("/api/ingest-token", {}), env);
    expect(legacy.status).toBe(410);
  });
});
