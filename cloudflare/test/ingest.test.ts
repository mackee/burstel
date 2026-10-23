// Spec 014: retired per-shot credentials, the legacy admin path and shot
// listing. The new continuous/instance behavior lives in instance.test.ts.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { sha256Hex } from "../src/crypto";
import type { IngestCore } from "../src/ingest-core";
import { FakeArtifacts, FakeBucket, FakeIngestNamespace, makeCore, makeIngest } from "./fakes";
import type { TestHarness } from "./fakes";

const TOKEN = "admin-api-token";

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
  setClock(epochMs: number): void;
  now(): number;
}

function setup(options: { adminToken?: string | null } = {}): Setup {
  let clock = 1_700_000_000_000;
  const now = () => clock;
  const shots = new FakeShotNamespace();
  const ingest = makeIngest({ now });
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    BUCKET: new FakeBucket(),
    PUBLIC_BASE_URL: "https://burstel.example",
  };
  if (options.adminToken !== null) env.BURSTEL_API_TOKEN = options.adminToken ?? TOKEN;
  return { env, shots, ingest: ingest.core, setClock: (v) => (clock = v), now };
}

function request(
  method: string,
  path: string,
  options: { body?: unknown; token?: string | null } = {},
): Request {
  const headers: Record<string, string> = { "content-type": "application/json" };
  const token = options.token === undefined ? TOKEN : options.token;
  if (token) headers["authorization"] = `Bearer ${token}`;
  const init: RequestInit = { method, headers };
  if (options.body !== undefined) init.body = JSON.stringify(options.body);
  return new Request(`https://burstel.example${path}`, init);
}

function admin(method: string, path: string, body?: unknown, token: string | null = TOKEN): Request {
  return request(method, path, { body, token });
}

async function createShot(env: AppEnv, owner?: string): Promise<{ id: string; body: Record<string, unknown> }> {
  const payload: Record<string, unknown> = { mode: "recording", metadata: { scenario: "bench" } };
  if (owner !== undefined) payload.owner = owner;
  const resp = await handleRequest(admin("POST", "/api/shots", payload), env);
  expect(resp.status).toBe(201);
  const body = (await resp.json()) as Record<string, unknown>;
  return { id: String(body.shot_id), body };
}

describe("retired credential endpoints", () => {
  test("the installation-wide token endpoint returns 410 Gone", async () => {
    const { env } = setup();
    for (const method of ["POST", "GET", "DELETE"]) {
      const resp = await handleRequest(admin(method, "/api/ingest-token"), env);
      expect(resp.status).toBe(410);
      expect(((await resp.json()) as { code: string }).code).toBe("gone");
    }
  });

  test("the per-shot ingest-token endpoint returns 410 Gone", async () => {
    const { env } = setup();
    const resp = await handleRequest(admin("POST", "/api/shots/AAAAAAAAAAAAAAAA/ingest-token"), env);
    expect(resp.status).toBe(410);
    expect(((await resp.json()) as { code: string }).code).toBe("gone");
  });
});

describe("legacy per-shot admin path", () => {
  test("the admin token can proxy OTLP to a specific shot", async () => {
    const { env } = setup();
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
    expect(resp.headers.get("x-burstel-shot-id")).toBe(id);
  });

  test("the legacy path fails closed without a valid admin token", async () => {
    const { env } = setup();
    const { id } = await createShot(env);
    const resp = await handleRequest(
      new Request(`https://burstel.example/shots/${id}/v1/traces`, {
        method: "POST",
        headers: { "content-type": "application/x-protobuf" },
        body: new Uint8Array([0x0a, 0x00]),
      }),
      env,
    );
    expect(resp.status).toBe(401);
  });
});

describe("fails closed without an admin token", () => {
  test("admin endpoints 401 and the explicit dev bypass works", async () => {
    const { env } = setup({ adminToken: null });
    expect((await handleRequest(admin("POST", "/api/shots", { metadata: {} }), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", "/api/shots", undefined), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", "/healthz", undefined, null), env)).status).toBe(200);

    const withBypass: AppEnv = { ...env, BURSTEL_DEV_INSECURE_AUTH: "1" };
    expect((await handleRequest(admin("POST", "/api/shots", { metadata: {} }), withBypass)).status).toBe(201);
  });
});

describe("GET /api/shots", () => {
  test("lists newest first, filters by owner and paginates", async () => {
    const { env, setClock } = setup();
    const base = Date.parse("2026-01-01T00:00:00.000Z");
    setClock(base);
    const a = await createShot(env, "alice");
    setClock(base + 1000);
    const b = await createShot(env, "bob");
    setClock(base + 2000);
    const c = await createShot(env, "alice");

    const all = (await (await handleRequest(admin("GET", "/api/shots"), env)).json()) as {
      shots: Array<{ shot_id: string; owner: string; state: string }>;
      total: number;
    };
    expect(all.total).toBe(3);
    expect(all.shots.map((s) => s.shot_id)).toEqual([c.id, b.id, a.id]);
    expect(all.shots.every((s) => s.state === "recording")).toBe(true);

    const filtered = (await (await handleRequest(admin("GET", "/api/shots?owner=alice"), env)).json()) as {
      shots: Array<{ shot_id: string; owner: string }>;
      total: number;
    };
    expect(filtered.total).toBe(2);
    expect(filtered.shots.map((s) => s.shot_id)).toEqual([c.id, a.id]);

    const page = (await (await handleRequest(admin("GET", "/api/shots?limit=1&offset=1"), env)).json()) as {
      shots: Array<{ shot_id: string }>;
      total: number;
    };
    expect(page.total).toBe(3);
    expect(page.shots.map((s) => s.shot_id)).toEqual([b.id]);
  });

  test("existing shots without an owner are attributed to legacy", async () => {
    const { env } = setup();
    const { id } = await createShot(env);
    const listed = (await (
      await handleRequest(admin("GET", `/api/shots?owner=legacy`), env)
    ).json()) as { shots: Array<{ shot_id: string; owner: string }> };
    expect(listed.shots.find((s) => s.shot_id === id)?.owner).toBe("legacy");
  });
});

describe("IngestDO durability", () => {
  test("restart preserves the shot registry, instance registry and counters", async () => {
    let clock = 1_700_000_000_000;
    const now = () => clock;
    const first = makeIngest({ now });
    await first.core.registerShot("shot-restart", "legacy");
    await first.core.instanceRegistry.upsert("web-1");

    const restarted = makeIngest({ now, sql: first.sql });
    await restarted.core.load();
    const status = (await restarted.core.status()) as { recording: number; counters: Record<string, number> };
    expect(status.recording).toBe(1);
    expect(status.counters.registered).toBe(1);
    expect((await restarted.core.instanceRegistry.list()).instances.map((i) => i.name)).toEqual(["web-1"]);
  });
});

// Referenced so the shared crypto helper stays exercised from this suite.
test("sha256Hex is stable", async () => {
  expect(await sha256Hex("abc")).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
});
