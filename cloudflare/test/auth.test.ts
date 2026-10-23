// Spec 013, Part B: shared multi-user access with per-person tokens.
//
// Deterministic Worker-level tests over the real request handler, the real
// AuthCore/IngestCore/ShotCore over in-process SQLite, and fake containers.
// They pin the capability matrix (REST + MCP), registry denial for `member`,
// revocation, digest-only persistence, constant-time comparison and the retired
// installation-wide token (410).

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import type { AuthCore, AuthTokenIssue } from "../src/auth-core";
import type { IngestCore } from "../src/ingest-core";
import { constantTimeEqual, sha256Hex } from "../src/ingest-core";
import { handleMcp } from "../src/mcp";
import { BunSql, FakeArtifacts, FakeAuthNamespace, FakeBucket, FakeIngestNamespace, makeAuth, makeCore, makeIngest } from "./fakes";
import type { TestHarness } from "./fakes";
import { ShotStore } from "../src/store";

const ENV_ADMIN = "env-admin-token";

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
  auth: AuthCore;
  authSql: BunSql;
  setClock(epochMs: number): void;
  now(): number;
}

let tokenCounter = 0;
function nextToken(): string {
  tokenCounter += 1;
  return `bta_test_${tokenCounter}`;
}
function nextId(): string {
  tokenCounter += 1;
  return `id${tokenCounter.toString().padStart(4, "0")}`;
}

function setup(): Setup {
  let clock = 1_700_000_000_000;
  const now = () => clock;
  const shots = new FakeShotNamespace();
  const ingest = makeIngest({ now });
  const auth = makeAuth({ now, generateToken: nextToken, generateId: nextId });
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    AUTH_DO: new FakeAuthNamespace(auth.core),
    BUCKET: new FakeBucket(),
    BURSTEL_API_TOKEN: ENV_ADMIN,
    PUBLIC_BASE_URL: "https://burstel.example",
  };
  return {
    env,
    shots,
    ingest: ingest.core,
    auth: auth.core,
    authSql: auth.sql,
    setClock: (v) => (clock = v),
    now,
  };
}

function req(
  method: string,
  path: string,
  options: { body?: unknown; token?: string | null } = {},
): Request {
  const headers: Record<string, string> = { "content-type": "application/json" };
  const token = options.token === undefined ? ENV_ADMIN : options.token;
  if (token) headers["authorization"] = `Bearer ${token}`;
  const init: RequestInit = { method, headers };
  if (options.body !== undefined) init.body = JSON.stringify(options.body);
  return new Request(`https://burstel.example${path}`, init);
}

function push(path: string, token: string): Request {
  return new Request(`https://burstel.example${path}`, {
    method: "POST",
    headers: { "content-type": "application/x-protobuf", "x-burstel-token": token },
    body: new Uint8Array([0x0a, 0x00]),
  });
}

async function mint(env: AppEnv, scope: string, owner?: string): Promise<AuthTokenIssue> {
  const resp = await handleRequest(req("POST", "/api/tokens", { body: { scope, owner } }), env);
  if (resp.status !== 201) throw new Error(`mint failed: ${resp.status} ${await resp.text()}`);
  return (await resp.json()) as AuthTokenIssue;
}

async function createShot(
  env: AppEnv,
  token = ENV_ADMIN,
  owner?: string,
): Promise<{ id: string; body: Record<string, unknown> }> {
  const payload: Record<string, unknown> = { mode: "recording", metadata: { scenario: "shared" } };
  if (owner !== undefined) payload.owner = owner;
  const resp = await handleRequest(req("POST", "/api/shots", { body: payload, token }), env);
  if (resp.status !== 201) throw new Error(`create failed: ${resp.status} ${await resp.text()}`);
  const body = (await resp.json()) as Record<string, unknown>;
  return { id: String(body.shot_id), body };
}

async function createInstance(env: AppEnv, name: string, token = ENV_ADMIN): Promise<string> {
  const resp = await handleRequest(req("POST", "/api/instances", { body: { name }, token }), env);
  if (resp.status !== 201) throw new Error(`instance create failed: ${resp.status} ${await resp.text()}`);
  return String(((await resp.json()) as { instance_id: string }).instance_id);
}

async function issueInstanceToken(env: AppEnv, name: string, token = ENV_ADMIN): Promise<string> {
  const resp = await handleRequest(req("POST", `/api/instances/${name}/tokens`, { body: {}, token }), env);
  if (resp.status !== 201) throw new Error(`instance token failed: ${resp.status} ${await resp.text()}`);
  return String(((await resp.json()) as { token: string }).token);
}

interface RpcResponse {
  jsonrpc: string;
  id: number;
  result?: Record<string, unknown>;
  error?: { code: number; message: string };
}

async function mcpRpc(env: AppEnv, body: unknown, token: string): Promise<RpcResponse> {
  const resp = await handleMcp(
    new Request("https://burstel.example/mcp", {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json, text/event-stream", authorization: `Bearer ${token}` },
      body: JSON.stringify(body),
    }),
    env,
  );
  expect(resp.status).toBe(200);
  return (await resp.json()) as RpcResponse;
}

async function mcpCall(
  env: AppEnv,
  name: string,
  args: Record<string, unknown>,
  token: string,
): Promise<{ isError: boolean; data: Record<string, unknown>; text: string }> {
  const body = await mcpRpc(env, { jsonrpc: "2.0", id: 7, method: "tools/call", params: { name, arguments: args } }, token);
  expect(body.error).toBeUndefined();
  const result = body.result as { content: Array<{ type: string; text: string }>; isError?: boolean };
  const text = result.content[0]!.text;
  return { isError: result.isError === true, data: result.isError ? {} : (JSON.parse(text) as Record<string, unknown>), text };
}

describe("token registry (REST)", () => {
  test("env admin mints/lists/revokes; a member token may not manage the registry", async () => {
    const { env } = setup();
    const member = await mint(env, "member", "alice");
    expect(member.token.startsWith("bta_")).toBe(true);
    expect(member.scope).toBe("member");
    expect(member.owner).toBe("alice");
    expect(member.id).toMatch(/^[a-z0-9]+$/);
    expect(member.expires_at).toBeNull();

    // member is denied every registry endpoint with 403, before any DO work.
    expect((await handleRequest(req("POST", "/api/tokens", { body: { scope: "member" }, token: member.token }), env)).status).toBe(403);
    expect((await handleRequest(req("GET", "/api/tokens", { token: member.token }), env)).status).toBe(403);
    expect((await handleRequest(req("DELETE", `/api/tokens/${member.id}`, { token: member.token }), env)).status).toBe(403);

    // admin can list (metadata only) and revoke.
    const listed = await handleRequest(req("GET", "/api/tokens"), env);
    expect(listed.status).toBe(200);
    const listBody = (await listed.json()) as { tokens: Array<Record<string, unknown>> };
    expect(listBody.tokens.map((t) => t.id)).toContain(member.id);
    expect(Object.keys(listBody.tokens[0]!).sort()).toEqual(["created_at", "expires_at", "id", "owner", "revoked", "scope"]);

    const revoked = await handleRequest(req("DELETE", `/api/tokens/${member.id}`), env);
    expect(revoked.status).toBe(200);
    expect((await revoked.json()) as { revoked: boolean; id: string }).toEqual({ revoked: true, id: member.id });
    // Idempotent, and an unknown id is a 404.
    expect((await handleRequest(req("DELETE", `/api/tokens/${member.id}`), env)).status).toBe(200);
    expect((await handleRequest(req("DELETE", "/api/tokens/does-not-exist"), env)).status).toBe(404);
  });

  test("only digests are persisted and list never returns values", async () => {
    const { env, authSql } = setup();
    const member = await mint(env, "member", "alice");

    const row = authSql.db
      .query("SELECT digest, scope, owner FROM auth_token WHERE id = ?")
      .get(member.id) as { digest: string; scope: string; owner: string };
    expect(row.digest).toBe(await sha256Hex(member.token));
    expect(row.digest).toMatch(/^[0-9a-f]{64}$/);
    expect(row.digest).not.toBe(member.token);

    const columns = authSql.db.query("SELECT name FROM pragma_table_info('auth_token')").all() as Array<{ name: string }>;
    const names = columns.map((c) => c.name);
    expect(names).not.toContain("token");
    expect(names).toContain("digest");

    const listed = (await (await handleRequest(req("GET", "/api/tokens"), env)).json()) as unknown;
    const serialized = JSON.stringify(listed);
    expect(serialized).not.toContain(member.token);
    expect(serialized).not.toContain(row.digest);
  });

  test("a revoked token fails immediately", async () => {
    const { env } = setup();
    const member = await mint(env, "member", "alice");
    expect((await handleRequest(req("GET", "/api/shots", { token: member.token }), env)).status).toBe(200);

    expect((await handleRequest(req("DELETE", `/api/tokens/${member.id}`), env)).status).toBe(200);
    expect((await handleRequest(req("GET", "/api/shots", { token: member.token }), env)).status).toBe(401);
    expect((await handleRequest(req("POST", "/api/shots", { body: {}, token: member.token }), env)).status).toBe(401);
  });

  test("an optional expires_at is enforced at the boundary", async () => {
    const { env, setClock } = setup();
    const member = await mint(env, "member", "alice");
    const withTtl = await handleRequest(
      req("POST", "/api/tokens", { body: { scope: "member", owner: "bob", ttl_seconds: 60 } }),
      env,
    );
    const bob = (await withTtl.json()) as AuthTokenIssue;
    expect(Date.parse(bob.expires_at!)).toBe(Date.parse(bob.created_at) + 60_000);

    const expiresAt = Date.parse(bob.expires_at!);
    setClock(expiresAt - 1);
    expect((await handleRequest(req("GET", "/api/shots", { token: bob.token }), env)).status).toBe(200);
    setClock(expiresAt);
    expect((await handleRequest(req("GET", "/api/shots", { token: bob.token }), env)).status).toBe(401);
    // The non-expiring token still works.
    expect((await handleRequest(req("GET", "/api/shots", { token: member.token }), env)).status).toBe(200);
  });

  test("an invalid scope is rejected", async () => {
    const { env } = setup();
    const resp = await handleRequest(req("POST", "/api/tokens", { body: { scope: "superuser" } }), env);
    expect(resp.status).toBe(400);
    // No token was registered; validation happens before any DO work.
    const listed = (await (await handleRequest(req("GET", "/api/tokens"), env)).json()) as { tokens: unknown[] };
    expect(listed.tokens).toEqual([]);
  });

  test("unauthenticated requests fail closed", async () => {
    const { env } = setup();
    expect((await handleRequest(req("GET", "/api/tokens", { token: null }), env)).status).toBe(401);
    expect((await handleRequest(req("GET", "/api/shots", { token: null }), env)).status).toBe(401);
    expect((await handleRequest(req("GET", "/api/tokens", { token: "nope" }), env)).status).toBe(401);

    // A non-admin token with no registry configured cannot be resolved.
    const noRegistry: AppEnv = { ...env, AUTH_DO: undefined };
    expect((await handleRequest(req("GET", "/api/shots", { token: "some-token" }), noRegistry)).status).toBe(401);
    expect((await handleRequest(req("GET", "/api/shots", { token: ENV_ADMIN }), noRegistry)).status).toBe(200);
  });
});

describe("constant-time comparison", () => {
  test("equal strings match; differing content or length does not", () => {
    expect(constantTimeEqual("abcdef", "abcdef")).toBe(true);
    expect(constantTimeEqual("abcdef", "abcdeg")).toBe(false);
    expect(constantTimeEqual("abcdef", "abcde")).toBe(false);
    expect(constantTimeEqual("", "")).toBe(true);
  });
});

describe("capability matrix (REST)", () => {
  test("two member tokens operate on every shot with no ownership restriction", async () => {
    const { env } = setup();
    const alice = await mint(env, "member", "alice");
    const bob = await mint(env, "member", "bob");

    const shotA = await createShot(env, alice.token, "alice");
    const shotB = await createShot(env, bob.token, "bob");
    expect(shotA.id).not.toBe(shotB.id);

    // Each token lists both shots (owner is attribution only).
    const listed = (await (await handleRequest(req("GET", "/api/shots", { token: alice.token }), env)).json()) as {
      shots: Array<{ shot_id: string; owner: string }>;
    };
    expect(listed.shots.map((s) => s.shot_id).sort()).toEqual([shotA.id, shotB.id].sort());

    // Alice manages operational instance credentials and stops Bob's shot;
    // Bob queries and analyzes Alice's shot. Both must succeed.
    await createInstance(env, "web-1", alice.token);
    const issuedToken = await issueInstanceToken(env, "web-1", alice.token);
    expect(issuedToken.startsWith("bit_")).toBe(true);
    expect((await handleRequest(req("GET", "/api/instances/web-1/tokens", { token: bob.token }), env)).status).toBe(200);

    expect((await handleRequest(req("POST", `/api/shots/${shotB.id}/stop`, { body: {}, token: alice.token }), env)).status).toBe(200);
    expect((await handleRequest(req("POST", `/api/shots/${shotA.id}/stop`, { body: {}, token: bob.token }), env)).status).toBe(200);
    expect((await handleRequest(req("POST", `/api/shots/${shotA.id}/query`, { body: { sql: "SELECT 1" }, token: bob.token }), env)).status).toBe(200);
    expect((await handleRequest(req("POST", `/api/shots/${shotA.id}/analyze`, { body: {}, token: bob.token }), env)).status).toBe(200);
    expect((await handleRequest(req("GET", `/api/shots/${shotA.id}/schema`, { token: bob.token }), env)).status).toBe(200);
    expect(
      (await handleRequest(req("POST", `/api/shots/${shotB.id}/compare`, { body: { other: shotA.id }, token: alice.token }), env)).status,
    ).toBe(200);

    // A member may not manage the registry even when operating on its own shots.
    expect((await handleRequest(req("GET", "/api/tokens", { token: bob.token }), env)).status).toBe(403);
  });

  test("scope separation: instance tokens cannot reach /api or /mcp", async () => {
    const { env } = setup();
    await createInstance(env, "web-1");
    const token = await issueInstanceToken(env, "web-1");
    expect((await handleRequest(req("GET", "/api/shots", { token }), env)).status).toBe(401);
    expect((await handleRequest(req("GET", "/api/instances", { token }), env)).status).toBe(401);
    expect((await handleRequest(req("GET", "/api/tokens", { token }), env)).status).toBe(401);
    const mcp = await handleMcp(
      new Request("https://burstel.example/mcp", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
      }),
      env,
    );
    expect(mcp.status).toBe(401);
  });
});

describe("MCP surface", () => {
  test("member gets the full operational tool set and is denied the registry tools", async () => {
    const { env } = setup();
    const member = await mint(env, "member", "alice");

    const listed = await mcpRpc(env, { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }, member.token);
    const names = (listed.result?.tools as Array<{ name: string }>).map((t) => t.name);
    for (const name of [
      "shots_list",
      "shot_start",
      "shot_stop",
      "shot_get",
      "shot_query",
      "shot_analyze",
      "shot_schema",
      "shot_compare",
      "instance_create",
      "instance_list",
      "instance_token_issue",
      "instance_token_list",
      "instance_token_revoke",
      "token_mint",
      "token_list",
      "token_revoke",
    ]) {
      expect(names).toContain(name);
    }

    const started = await mcpCall(env, "shot_start", { mode: "recording", metadata: { x: 1 }, owner: "alice" }, member.token);
    expect(started.isError).toBe(false);
    const shotId = String(started.data["shot_id"]);

    const created = await mcpCall(env, "instance_create", { name: "web-1" }, member.token);
    expect(created.isError).toBe(false);
    const ingest = await mcpCall(env, "instance_token_issue", { name: "web-1" }, member.token);
    expect(ingest.isError).toBe(false);
    expect(String(ingest.data["token"])).toMatch(/^bit_/);

    const list = await mcpCall(env, "shots_list", {}, member.token);
    expect(list.isError).toBe(false);
    expect((list.data["shots"] as unknown[]).length).toBeGreaterThan(0);

    // Continuous ingest with the instance token is journalled (no shot routing).
    const pushed = await handleRequest(push("/ingest/v1/traces", String(ingest.data["token"])), env);
    expect(pushed.status).toBe(200);
    expect(pushed.headers.get("x-burstel-instance")).toBe("web-1");

    await mcpCall(env, "shot_stop", { shot_id: shotId }, member.token);
    expect((await mcpCall(env, "shot_get", { shot_id: shotId }, member.token)).data["state"]).toBe("frozen");
    expect((await mcpCall(env, "shot_query", { shot_id: shotId, sql: "SELECT 1" }, member.token)).isError).toBe(false);
    expect((await mcpCall(env, "shot_analyze", { shot_id: shotId }, member.token)).isError).toBe(false);
    expect((await mcpCall(env, "shot_schema", { shot_id: shotId }, member.token)).isError).toBe(false);

    const other = await mcpCall(env, "shot_start", { mode: "recording", metadata: {} }, member.token);
    const otherId = String(other.data["shot_id"]);
    await mcpCall(env, "shot_stop", { shot_id: otherId }, member.token);
    const cmp = await mcpCall(env, "shot_compare", { left_shot_id: shotId, right_shot_id: otherId }, member.token);
    expect(cmp.isError).toBe(false);

    // Registry tools are denied with a 403 tool error, before any DO work.
    for (const [name, args] of [
      ["token_mint", { scope: "member", owner: "eve" }],
      ["token_list", {}],
      ["token_revoke", { id: "whatever" }],
    ] as const) {
      const denied = await mcpCall(env, name, args as Record<string, unknown>, member.token);
      expect(denied.isError).toBe(true);
      expect(denied.text).toContain("403");
    }
  });

  test("an admin can mint a member token over MCP and the new token works", async () => {
    const { env } = setup();
    const minted = await mcpCall(env, "token_mint", { scope: "member", owner: "carol" }, ENV_ADMIN);
    expect(minted.isError).toBe(false);
    const token = String(minted.data["token"]);
    expect(token.startsWith("bta_")).toBe(true);

    const started = await mcpCall(env, "shot_start", { mode: "recording", metadata: {} }, token);
    expect(started.isError).toBe(false);
    const shotId = String(started.data["shot_id"]);
    expect(String((await mcpCall(env, "shot_get", { shot_id: shotId }, token)).data["shot_id"])).toBe(shotId);

    const listed = await mcpCall(env, "token_list", {}, ENV_ADMIN);
    expect(listed.isError).toBe(false);
    const ids = (listed.data["tokens"] as Array<{ id: string }>).map((t) => t.id);
    expect(ids).toContain(String(minted.data["id"]));

    const revoked = await mcpCall(env, "token_revoke", { id: String(minted.data["id"]) }, ENV_ADMIN);
    expect(revoked.isError).toBe(false);
    // The revoked token now fails authentication at the MCP boundary.
    const afterRevoke = await handleMcp(
      new Request("https://burstel.example/mcp", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
      }),
      env,
    );
    expect(afterRevoke.status).toBe(401);
  });
});

describe("installation-wide token migration", () => {
  test("the retired endpoint returns 410 Gone for admin and member", async () => {
    const { env } = setup();
    const member = await mint(env, "member", "alice");
    for (const token of [ENV_ADMIN, member.token]) {
      for (const method of ["POST", "GET", "DELETE"]) {
        const resp = await handleRequest(req(method, "/api/ingest-token", { token }), env);
        expect(resp.status).toBe(410);
        expect(((await resp.json()) as { code: string }).code).toBe("gone");
      }
    }
  });
});

describe("shot owner migration", () => {
  test("a pre-013 shot_state row with no owner migrates to owner=legacy", () => {
    const sql = new BunSql();
    sql.db.exec(
      `CREATE TABLE shot_state (
         shot_id       TEXT PRIMARY KEY,
         state         TEXT NOT NULL,
         metadata_json TEXT NOT NULL DEFAULT '{}',
         freeze_json   TEXT NOT NULL DEFAULT '{}',
         created_at    INTEGER NOT NULL,
         started_at    INTEGER,
         stopping_at   INTEGER,
         stopped_at    INTEGER,
         size_bytes    INTEGER,
         sha256        TEXT,
         last_error    TEXT
       )`,
    );
    sql.db.exec(`INSERT INTO shot_state (shot_id, state, created_at) VALUES ('old-shot', 'frozen', 1)`);

    const store = new ShotStore(sql);
    store.migrate();
    expect(store.get("old-shot")?.owner).toBe("legacy");
    // Idempotent: a second migrate is a no-op.
    store.migrate();
    expect(store.get("old-shot")?.owner).toBe("legacy");
  });

  test("a shot created without an owner is attributed to legacy", async () => {
    const { env } = setup();
    const shot = await createShot(env, ENV_ADMIN);
    const listed = (await (await handleRequest(req("GET", "/api/shots?owner=legacy"), env)).json()) as {
      shots: Array<{ shot_id: string; owner: string }>;
    };
    expect(listed.shots.find((s) => s.shot_id === shot.id)?.owner).toBe("legacy");
  });
});
