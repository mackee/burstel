// Spec 006: stable OTLP `/ingest` endpoint with a push-only token that expires
// exactly 24h after issue, backed by a SQLite IngestDO coordinator.
//
// All tests run against the real Worker request handler, the real IngestCore
// over real in-process SQLite, and the real ShotCore lifecycle over fakes. A
// fake clock makes the 24h boundary deterministic.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { newIngestToken, sha256Hex } from "../src/ingest-core";
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
  return { env, shots, ingest: ingest.core, sql: ingest.sql, setClock: (v) => (clock = v) };
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

async function createShot(env: AppEnv): Promise<{ id: string; body: Record<string, unknown> }> {
  const resp = await handleRequest(admin("POST", "/api/shots", { metadata: { scenario: "bench" } }), env);
  expect(resp.status).toBe(201);
  const body = (await resp.json()) as Record<string, unknown>;
  return { id: String(body.shot_id), body };
}

async function issue(env: AppEnv): Promise<Record<string, unknown>> {
  const resp = await handleRequest(admin("POST", "/api/ingest-token"), env);
  expect(resp.status).toBe(201);
  return (await resp.json()) as Record<string, unknown>;
}

describe("stable ingest token", () => {
  test("issues a 256-bit token with exact 24h TTL; status never leaks the secret", async () => {
    const { env } = setup();
    const issued = await issue(env);
    const raw = String(issued.token);

    expect(raw.startsWith("blt_")).toBe(true);
    // The real generator carries at least 256 bits (base64url of 32 bytes).
    const real = newIngestToken();
    expect(real.slice(4).length).toBeGreaterThanOrEqual(43);
    expect(newIngestToken()).not.toBe(real);
    expect(issued.ttl_seconds).toBe(86_400);
    expect(Date.parse(String(issued.expires_at)) - Date.parse(String(issued.issued_at))).toBe(86_400_000);

    const resp = await handleRequest(admin("GET", "/api/ingest-token"), env);
    expect(resp.status).toBe(200);
    expect(resp.headers.get("cache-control")).toBe("no-store");
    const status = (await resp.json()) as Record<string, unknown>;
    expect(status.configured).toBe(true);
    expect(status.active).toBe(true);
    expect(status.expires_at).toBe(issued.expires_at);
    expect(status.token).toBeUndefined();
    expect(status.digest).toBeUndefined();
    expect(JSON.stringify(status)).not.toContain(raw);
  });

  test("accepts at issue+23h59m59s and rejects 401 at issue+24h without touching the shot", async () => {
    const { env, setClock } = setup();
    const issued = await issue(env);
    const token = String(issued.token);
    const expiresAt = Date.parse(String(issued.expires_at));
    const { id } = await createShot(env);

    setClock(expiresAt - 1000); // issue + 23h59m59s
    const ok = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(ok.status).toBe(200);
    expect(ok.headers.get("x-burstel-shot-id")).toBe(id);

    setClock(expiresAt); // exactly issue + 24h
    const expired = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(expired.status).toBe(401);
    expect(((await expired.json()) as { reason: string }).reason).toBe("expired");

    // Expiry never stops or freezes a shot; it keeps recording until admin stops it.
    const status = (await (await handleRequest(admin("GET", `/api/shots/${id}`), env)).json()) as {
      state: string;
    };
    expect(status.state).toBe("recording");
    const stopped = (await (
      await handleRequest(admin("POST", `/api/shots/${id}/stop`), env)
    ).json()) as { state: string };
    expect(stopped.state).toBe("frozen");

    const mgmt = (await (await handleRequest(admin("GET", "/api/ingest-token"), env)).json()) as {
      counters: Record<string, number>;
    };
    expect(mgmt.counters.expired).toBeGreaterThanOrEqual(1);
  });

  test("revocation and replacement invalidate the prior token", async () => {
    const { env, setClock } = setup();
    const first = String((await issue(env)).token);
    setClock(1_700_000_000_000 + 1000);
    await handleRequest(admin("DELETE", "/api/ingest-token"), env);

    const revoked = await handleRequest(pushIngest("/ingest/v1/logs", first), env);
    expect(revoked.status).toBe(401);
    expect(((await revoked.json()) as { reason: string }).reason).toBe("revoked");

    const second = String((await issue(env)).token);
    expect(second).not.toBe(first);
    const replaced = await handleRequest(pushIngest("/ingest/v1/logs", first), env);
    expect(replaced.status).toBe(401);
    expect(((await replaced.json()) as { reason: string }).reason).toBe("invalid");
    expect((await handleRequest(pushIngest("/ingest/v1/logs", second), env)).status).toBe(200);
  });

  test("missing and wrong tokens are rejected and counted, via both header forms", async () => {
    const { env } = setup();
    const good = String((await issue(env)).token);
    expect((await handleRequest(pushIngest("/ingest/v1/traces", null), env)).status).toBe(401);
    expect((await handleRequest(pushIngest("/ingest/v1/traces", "nope"), env)).status).toBe(401);
    // Authorization: Bearer is also accepted for exporters.
    const asBearer = request("POST", "/ingest/v1/traces", {});
    asBearer.headers.set("authorization", `Bearer ${good}`);
    asBearer.headers.set("content-type", "application/x-protobuf");
    // Body was serialized as JSON; still forwarded, status is what matters here.
    expect((await handleRequest(asBearer, env)).status).toBe(200);
    const status = (await (await handleRequest(admin("GET", "/api/ingest-token"), env)).json()) as {
      counters: Record<string, number>;
    };
    expect(status.counters.missing).toBeGreaterThanOrEqual(1);
    expect(status.counters.invalid).toBeGreaterThanOrEqual(1);
  });

  test("push token cannot reach admin/REST/MCP and admin token cannot ingest", async () => {
    const { env } = setup();
    const push = String((await issue(env)).token);
    const { id } = await createShot(env);

    expect((await handleRequest(admin("POST", "/api/shots", { metadata: {} }, push), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", "/api/shots", undefined, push), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", `/api/shots/${id}`, undefined, push), env)).status).toBe(401);
    expect((await handleRequest(admin("POST", `/api/shots/${id}/stop`, {}, push), env)).status).toBe(401);
    expect((await handleRequest(admin("GET", "/api/ingest-token", undefined, push), env)).status).toBe(401);
    expect((await handleRequest(admin("DELETE", "/api/ingest-token", undefined, push), env)).status).toBe(401);

    const mcpResp = await handleMcp(
      new Request("https://burstel.example/mcp", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${push}` },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
      }),
      env,
    );
    expect(mcpResp.status).toBe(401);

    // The admin token is not the push token; it must not authorize /ingest.
    expect((await handleRequest(pushIngest("/ingest/v1/traces", TOKEN), env)).status).toBe(401);
  });

  test("fails closed without an admin token and honors the explicit dev bypass", async () => {
    const { env } = setup({ adminToken: null });
    expect((await handleRequest(admin("POST", "/api/ingest-token"), env)).status).toBe(401);
    expect((await handleRequest(admin("POST", "/api/shots", { metadata: {} }), env)).status).toBe(401);
    // /healthz stays public even with no admin token configured.
    expect((await handleRequest(admin("GET", "/healthz", undefined, null), env)).status).toBe(200);

    const withBypass: AppEnv = { ...env, BURSTEL_DEV_INSECURE_AUTH: "1" };
    expect((await handleRequest(admin("POST", "/api/ingest-token"), withBypass)).status).toBe(201);
  });

  test("the stable URL and token capture two sequential shots with distinct backends", async () => {
    const { env, shots } = setup();
    const token = String((await issue(env)).token);

    const first = await createShot(env);
    for (const signal of ["traces", "metrics", "logs"] as const) {
      const resp = await handleRequest(pushIngest(`/ingest/v1/${signal}`, token), env);
      expect(resp.status).toBe(200);
      expect(resp.headers.get("x-burstel-shot-id")).toBe(first.id);
    }
    expect((await handleRequest(admin("POST", `/api/shots/${first.id}/stop`), env)).status).toBe(200);

    const second = await createShot(env);
    const resp = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(resp.status).toBe(200);
    expect(resp.headers.get("x-burstel-shot-id")).toBe(second.id);

    expect(second.id).not.toBe(first.id);
    expect(first.body.otlp_base_url).toBe("https://burstel.example/ingest");
    expect(first.body.otlp_base_url).toBe(second.body.otlp_base_url);
    expect(shots.shots.get(first.id)!.container.ingestBodies).toHaveLength(3);
    expect(shots.shots.get(second.id)!.container.ingestBodies).toHaveLength(1);
  });

  test("background telemetry with no active shot is a 200 discard, not a retry storm", async () => {
    const { env } = setup();
    const token = String((await issue(env)).token);

    const json = await handleRequest(
      pushIngest("/ingest/v1/metrics", token, "application/json"),
      env,
    );
    expect(json.status).toBe(200);
    expect(json.headers.get("x-burstel-disposition")).toBe("ignored-no-active-shot");
    expect(await json.text()).toBe("{}");

    const proto = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(proto.status).toBe(200);
    expect(proto.headers.get("content-type")).toBe("application/x-protobuf");
    expect(proto.headers.get("x-burstel-disposition")).toBe("ignored-no-active-shot");
    expect((await proto.arrayBuffer()).byteLength).toBe(0);

    const status = (await (await handleRequest(admin("GET", "/api/ingest-token"), env)).json()) as {
      counters: Record<string, number>;
    };
    expect(status.counters.discarded_no_active).toBeGreaterThanOrEqual(2);
  });

  test("late data after stop is discarded, never attributed to the next shot", async () => {
    const { env, shots } = setup();
    const token = String((await issue(env)).token);

    const first = await createShot(env);
    await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    await handleRequest(admin("POST", `/api/shots/${first.id}/stop`), env);

    const late = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(late.status).toBe(200);
    expect(late.headers.get("x-burstel-disposition")).toBe("ignored-no-active-shot");

    const second = await createShot(env);
    expect(shots.shots.get(first.id)!.container.ingestBodies).toHaveLength(1);
    expect(shots.shots.get(second.id)!.container.ingestBodies).toHaveLength(0);

    const routed = await handleRequest(pushIngest("/ingest/v1/traces", token), env);
    expect(routed.headers.get("x-burstel-shot-id")).toBe(second.id);
  });

  test("a recording shot is not silently displaced by a new start", async () => {
    const { env } = setup();
    await issue(env);
    const first = await createShot(env);
    const conflict = await handleRequest(admin("POST", "/api/shots", { metadata: {} }), env);
    expect(conflict.status).toBe(409);
    // The original shot is untouched and still recording.
    const status = (await (await handleRequest(admin("GET", `/api/shots/${first.id}`), env)).json()) as {
      state: string;
    };
    expect(status.state).toBe("recording");
  });

  test("rotation is refused while a shot records and allowed after it stops", async () => {
    const { env } = setup();
    expect((await handleRequest(admin("POST", "/api/ingest-token"), env)).status).toBe(201);
    const { id } = await createShot(env);
    expect((await handleRequest(admin("POST", "/api/ingest-token"), env)).status).toBe(409);
    await handleRequest(admin("POST", `/api/shots/${id}/stop`), env);
    expect((await handleRequest(admin("POST", "/api/ingest-token"), env)).status).toBe(201);
  });

  test("concurrent starts serialize: exactly one recording shot becomes active", async () => {
    const { env, ingest } = setup();
    await issue(env);
    const [a, b] = await Promise.all([
      handleRequest(admin("POST", "/api/shots", { metadata: { n: 1 } }), env),
      handleRequest(admin("POST", "/api/shots", { metadata: { n: 2 } }), env),
    ]);
    expect([a.status, b.status].sort()).toEqual([201, 409]);

    const winner = a.status === 201 ? a : b;
    const winnerId = String(((await winner.json()) as { shot_id: string }).shot_id);
    const active = await ingest.active();
    expect(active.state).toBe("recording");
    expect(active.shot_id).toBe(winnerId);
  });
});

describe("IngestDO durability", () => {
  test("restart preserves expiry, active mapping and counters", async () => {
    let clock = 1_700_000_000_000;
    const now = () => clock;
    const first = makeIngest({ now, generateToken: nextToken });
    const issued = await first.core.issueToken();
    await first.core.activate("shot-restart");
    const admitted = await first.core.admit(issued.token, "traces");
    expect(admitted.ok).toBe(true);

    // Simulate DO eviction: a brand-new core over the same SQLite storage.
    const restarted = makeIngest({ now, generateToken: nextToken, sql: first.sql });
    await restarted.core.load();
    const status = await restarted.core.status();
    expect(status.expires_at).toBe(issued.expires_at);
    expect(status.active_shot_id).toBe("shot-restart");
    expect(status.active_state).toBe("recording");
    expect(status.counters.accepted).toBe(1);

    clock = Date.parse(issued.expires_at);
    const rejected = await restarted.core.admit(issued.token, "traces");
    expect(rejected.ok).toBe(false);
    if (!rejected.ok) expect(rejected.reason).toBe("expired");

    const rows = restarted.sql.db
      .query("SELECT value FROM ingest_counters WHERE name = 'expired'")
      .all() as Array<{ value: number }>;
    expect(rows[0]?.value).toBe(1);
  });

  test("only a one-way digest is persisted; the raw token never touches storage", async () => {
    const { env, sql } = setup();
    const issued = await issue(env);
    const raw = String(issued.token);

    const row = sql.db
      .query("SELECT digest, issued_at, expires_at, revoked FROM ingest_token WHERE id = 1")
      .get() as { digest: string };
    expect(row.digest).not.toBe(raw);
    expect(row.digest).toMatch(/^[0-9a-f]{64}$/);
    expect(row.digest).toBe(await sha256Hex(raw));

    const columns = sql.db
      .query("SELECT name FROM pragma_table_info('ingest_token')")
      .all() as Array<{ name: string }>;
    const names = columns.map((c) => c.name);
    expect(names).not.toContain("token");
    expect(names).toContain("digest");
  });
});
