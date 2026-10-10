import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/app";
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
    return { fetch: (request: Request) => core.handleInternal(request) };
  }
}

function makeEnv(): AppEnv {
  return {
    SHOT_DO: new FakeNamespace(),
    INGEST_DO: new FakeIngestNamespace(makeIngest().core),
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
