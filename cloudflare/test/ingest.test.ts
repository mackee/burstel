// Spec 013, Part A: per-shot push-only ingest credentials.
//
// All tests run against the real Worker request handler, the real IngestCore
// over real in-process SQLite, and the real ShotCore lifecycle over fakes. A
// fake clock makes the 24h boundary deterministic.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { INGEST_TOKEN_GRACE_MS, newIngestToken, sha256Hex } from "../src/ingest-core";
import type { IngestCore } from "../src/ingest-core";
import { handleMcp } from "../src/mcp";
import { FakeArtifacts, FakeIngestNamespace, makeCore, makeIngest } from "./fakes";
import type { BunSql, TestHarness } from "./fakes";

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
  sql: BunSql;
  setClock(epochMs: number): void;
  now(): number;
}

let tokenCounter = 0;
function nextToken(): string {
  tokenCounter += 1;
  return `blt_test_token_${tokenCounter}`;
}

function setup(options: { adminToken?: string | null } = {}): Setup {
  let clock = 1_700_000_000_000;
  const now = () => clock;
  const shots = new FakeShotNamespace();
  const ingest = makeIngest({ now, generateToken: nextToken });
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    PUBLIC_BASE_URL: "https://burstel.example",
  };
  if (options.adminToken !== null) env.BURSTEL_API_TOKEN = options.adminToken ?? TOKEN;
  return { env, shots, ingest: ingest.core, sql: ingest.sql, setClock: (v) => (clock = v), now };
}

function request(
  method: string,
  path: string,
  options: { body?: unknown; token?: string | null; header?: "authorization" | "x-burstel-token" } = {},
): Request {
  const headers: Record<string, string> = { "content-type": "application/json" };
  const token = options.token === undefined ? TOKEN : options.token;
  if (token) headers[options.header ?? "authorization"] = options.header === "x-burstel-token" ? token : `Bearer ${token}`;
  const init: RequestInit = { method, headers };
  if (options.body !== undefined) init.body = JSON.stringify(options.body);
  return new Request(`https://burstel.example${path}`, init);
}

function admin(method: string, path: string, body?: unknown, token: string | null = TOKEN): Request {
  return request(method, path, { body, token });
}

function pushIngest(path: string, token: string | null, contentType = "application/x-protobuf"): Request {
  const headers: Record<string, string> = { "content-type": contentType };
  if (token) headers["x-burstel-token"] = token;
  return new Request(`https://burstel.example${path}`, {
    method: "POST",
    headers,
    body: new Uint8Array([0x0a, 0x00]),
  });
}

async function createShot(
  env: AppEnv,
  owner?: string,
): Promise<{ id: string; token: string; body: Record<string, unknown> }> {
  const payload: Record<string, unknown> = { metadata: { scenario: "bench" } };
  if (owner !== undefined) payload.owner = owner;
  const resp = await handleRequest(admin("POST", "/api/shots", payload), env);
  expect(resp.status).toBe(201);
  const body = (await resp.json()) as Record<string, unknown>;
  return { id: String(body.shot_id), token: String(body.ingest_token), body };
}

describe("per-shot ingest tokens", () => {
  test("creating a shot mints a 256-bit 24h token, disclosed once, digest only", async () => {
    const { env, sql } = setup();
    const { id, token, body } = await createShot(env);

    expect(token.startsWith("blt_")).toBe(true);
    const real = newIngestToken();
    expect(real.slice(4).length).toBeGreaterThanOrEqual(43);
    expect(newIngestToken()).not.toBe(real);

    expect(body.ingest_token_shot_id).toBe(id);
    expect(body.ingest_token_ttl_seconds).toBe(86_400);
    expect(Date.parse(String(body.ingest_token_expires_at)) - Date.parse(String(body.ingest_token_issued_at))).toBe(
      86_400_000,
    );

    // Only the SHA-256 digest is persisted; the raw token never touches storage.
    const row = sql.db
      .query("SELECT digest, shot_id, revoked FROM shot_ingest_token")
      .get() as { digest: string; shot_id: string; revoked: number };
    expect(row.shot_id).toBe(id);
    expect(row.revoked).toBe(0);
    expect(row.digest).not.toBe(token);
    expect(row.digest).toMatch(/^[0-9a-f]{64}$/);
    expect(row.digest).toBe(await sha256Hex(token));

    const columns = sql.db
      .query("SELECT name FROM pragma_table_info('shot_ingest_token')")
      .all() as Array<{ name: string }>;
    const names = columns.map((c) => c.name);
    expect(names).not.toContain("token");
    expect(names).toContain("digest");
  });

  test("the dedicated endpoint mints a fresh token for an existing shot", async () => {
    const { env } = setup();
    const { id, token } = await createShot(env);
    const resp = await handleRequest(admin("POST", `/api/shots/${id}/ingest-token`), env);
    expect(resp.status).toBe(201);
    const issued = (await resp.json()) as Record<string, unknown>;
    expect(issued.shot_id).toBe(id);
    expect(issued.token).not.toBe(token);
    expect(issued.ttl_seconds).toBe(86_400);

    // Both tokens route to the same shot.
    expect((await handleRequest(pushIngest("/ingest/v1/traces", token), env)).headers.get("x-burstel-shot-id")).toBe(id);
    expect((await handleRequest(pushIngest("/ingest/v1/traces", String(issued.token)), env)).headers.get("x-burstel-shot-id")).toBe(id);
  });

  test("accepts at issue+23h59m59s and rejects 401 at issue+24h without touching the shot", async () => {
    const { env, setClock } = setup();
    const { id, token, body } = await createShot(env);
    const expiresAt = Date.parse(String(body.ingest_token_expires_at));

    setClock(expiresAt - 1000); // issue + 23h59m59s
    const ok = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(ok.status).toBe(200);
    expect(ok.headers.get("x-burstel-shot-id")).toBe(id);

    setClock(expiresAt); // exactly issue + 24h
    const expired = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(expired.status).toBe(401);
    expect(((await expired.json()) as { reason: string }).reason).toBe("expired");

    // Expiry never stops or freezes a shot; it keeps recording until admin stops it.
    const status = (await (await handleRequest(admin("GET", `/api/shots/${id}`), env)).json()) as { state: string };
    expect(status.state).toBe("recording");
    const stopped = (await (await handleRequest(admin("POST", `/api/shots/${id}/stop`), env)).json()) as {
      state: string;
    };
    expect(stopped.state).toBe("frozen");
  });

  test("freeze revokes the token after a short grace so late batches drain", async () => {
    const { env, setClock, now } = setup();
    const { id, token } = await createShot(env);
    expect((await handleRequest(pushIngest("/ingest/v1/traces", token), env)).status).toBe(200);

    const stoppedAt = now();
    expect((await handleRequest(admin("POST", `/api/shots/${id}/stop`), env)).status).toBe(200);

    // Within the grace the token still routes, but the shot is frozen: the
    // batch is consciously discarded, not answered with a hard 401.
    setClock(stoppedAt + INGEST_TOKEN_GRACE_MS - 1);
    const late = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(late.status).toBe(200);
    expect(late.headers.get("x-burstel-disposition")).toBe("ignored-no-active-shot");

    // After the grace the token is revoked and fails closed immediately.
    setClock(stoppedAt + INGEST_TOKEN_GRACE_MS + 1);
    const revoked = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(revoked.status).toBe(401);
    expect(((await revoked.json()) as { reason: string }).reason).toBe("revoked");
  });

  test("missing and wrong tokens are rejected and counted, via both header forms", async () => {
    const { env, ingest } = setup();
    const { token } = await createShot(env);
    expect((await handleRequest(pushIngest("/ingest/v1/traces", null), env)).status).toBe(401);
    expect((await handleRequest(pushIngest("/ingest/v1/traces", "nope"), env)).status).toBe(401);

    // Authorization: Bearer is also accepted for exporters.
    const asBearer = new Request("https://burstel.example/ingest/v1/traces", {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/x-protobuf" },
      body: new Uint8Array([0x0a, 0x00]),
    });
    expect((await handleRequest(asBearer, env)).status).toBe(200);

    const status = (await ingest.status()) as { counters: Record<string, number> };
    expect(status.counters.missing).toBeGreaterThanOrEqual(1);
    expect(status.counters.invalid).toBeGreaterThanOrEqual(1);
  });

  test("push token cannot reach admin/REST/MCP and admin token cannot ingest", async () => {
    const { env } = setup();
    const { id, token } = await createShot(env);

    expect((await handleRequest(admin("POST", "/api/shots", { metadata: {} }, token), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", "/api/shots", undefined, token), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", `/api/shots/${id}`, undefined, token), env)).status).toBe(401);
    expect((await handleRequest(admin("POST", `/api/shots/${id}/stop`, {}, token), env)).status).toBe(401);
    expect((await handleRequest(admin("POST", `/api/shots/${id}/ingest-token`, undefined, token), env)).status).toBe(401);

    const mcpResp = await handleMcp(
      new Request("https://burstel.example/mcp", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
      }),
      env,
    );
    expect(mcpResp.status).toBe(401);

    // The admin token is not a push token; it must not authorize /ingest.
    expect((await handleRequest(pushIngest("/ingest/v1/traces", TOKEN), env)).status).toBe(401);
  });

  test("the retired installation-wide token endpoint returns 410 Gone", async () => {
    const { env, sql } = setup();
    for (const method of ["POST", "GET", "DELETE"]) {
      const resp = await handleRequest(admin(method, "/api/ingest-token"), env);
      expect(resp.status).toBe(410);
      expect(((await resp.json()) as { code: string }).code).toBe("gone");
    }
    const rows = sql.db.query("SELECT name FROM sqlite_master WHERE type = 'table'").all() as Array<{ name: string }>;
    expect(rows.map((r) => r.name)).not.toContain("ingest_token");
  });

  test("fails closed without an admin token and honors the explicit dev bypass", async () => {
    const { env } = setup({ adminToken: null });
    expect((await handleRequest(admin("POST", "/api/shots", { metadata: {} }), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", "/api/shots", undefined), env)).status).toBe(401);
    // /healthz stays public even with no admin token configured.
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
    expect(filtered.shots.every((s) => s.owner === "alice")).toBe(true);

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
    expect(listed.shots.map((s) => s.shot_id)).toContain(id);
    expect(listed.shots.find((s) => s.shot_id === id)?.owner).toBe("legacy");
  });
});

describe("IngestDO durability", () => {
  test("restart preserves per-shot tokens, registry and counters", async () => {
    let clock = 1_700_000_000_000;
    const now = () => clock;
    const first = makeIngest({ now, generateToken: nextToken });
    const issued = await first.core.issueShotToken("shot-restart");
    await first.core.reserveShot("shot-restart", "legacy", 10);
    const admitted = await first.core.admit(issued.token, "traces");
    expect(admitted.ok).toBe(true);
    if (admitted.ok) expect(admitted.shot_id).toBe("shot-restart");

    // Simulate DO eviction: a brand-new core over the same SQLite storage.
    const restarted = makeIngest({ now, generateToken: nextToken, sql: first.sql });
    await restarted.core.load();
    const status = (await restarted.core.status()) as { tokens: number; recording: number; counters: Record<string, number> };
    expect(status.tokens).toBe(1);
    expect(status.recording).toBe(1);
    expect(status.counters.accepted).toBe(1);

    clock = Date.parse(issued.expires_at);
    const rejected = await restarted.core.admit(issued.token, "traces");
    expect(rejected.ok).toBe(false);
    if (!rejected.ok) expect(rejected.reason).toBe("expired");
  });
});
