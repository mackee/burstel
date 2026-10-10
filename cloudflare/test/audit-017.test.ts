// Independent acceptance audit for spec 017.
//
// This suite is deliberately separate from the spec-014/015/016 unit tests. It
// adversarially re-verifies the acceptance contracts on the real request
// handler, the real IngestCore/InstanceCore/AuthCore over in-process SQLite and
// an in-memory R2 bucket, and it closes the *migration* gap: upgrading a
// pre-016 `shot_registry` and pre-existing instance/person credential rows must
// survive `load()` without loss.
//
// Contracts covered here:
//   1. exactly-24h credential boundary at +/-1ms, persisted across a restart;
//   2. digest-only persistence and no secret leakage in any list/response;
//   3. admin|member|push scope separation in both directions (REST + MCP);
//   4. durable R2 ACK ordering (write + ledger before 200) and fail-closed 5xx;
//   5. non-shot idle intake is retained, never silently discarded;
//   6. bounded rotation grace boundary;
//   7. legacy migration: a pre-016 shot registry and existing credential rows.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import { AuthCore } from "../src/auth-core";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { IngestCore } from "../src/ingest-core";
import { INSTANCE_TOKEN_TTL_MS } from "../src/instance-core";
import { handleMcp } from "../src/mcp";
import { sha256HexBytes } from "../src/crypto";
import {
  BunSql,
  FakeArtifacts,
  FakeAuthNamespace,
  FakeBucket,
  FakeIngestNamespace,
  makeAuth,
  makeCore,
  makeIngest,
} from "./fakes";
import type { TestHarness } from "./fakes";

const ENV_ADMIN = "audit-017-admin";

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
  bucket: FakeBucket;
  ingest: IngestCore;
  ingestSql: BunSql;
  auth: AuthCore;
  authSql: BunSql;
  setClock(epochMs: number): void;
  now(): number;
}

function setup(): Setup {
  let clock = 1_700_000_000_000;
  const now = () => clock;
  const bucket = new FakeBucket();
  const ingest = makeIngest({ now });
  const auth = makeAuth({ now });
  const env: AppEnv = {
    SHOT_DO: new FakeShotNamespace(),
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    AUTH_DO: new FakeAuthNamespace(auth.core),
    BUCKET: bucket,
    BURSTEL_API_TOKEN: ENV_ADMIN,
    PUBLIC_BASE_URL: "https://burstel.example",
  };
  return {
    env,
    bucket,
    ingest: ingest.core,
    ingestSql: ingest.sql,
    auth: auth.core,
    authSql: auth.sql,
    setClock: (v) => (clock = v),
    now,
  };
}

function api(
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

function push(token: string | null, signal = "traces", body = new Uint8Array([0x0a, 0x00])): Request {
  const headers: Record<string, string> = { "content-type": "application/x-protobuf" };
  if (token) headers["x-burstel-token"] = token;
  return new Request(`https://burstel.example/ingest/v1/${signal}`, { method: "POST", headers, body });
}

async function createInstance(env: AppEnv, name: string): Promise<void> {
  const resp = await handleRequest(api("POST", "/api/instances", { body: { name } }), env);
  expect(resp.status).toBe(201);
}

async function issue(env: AppEnv, name: string, opts: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
  const resp = await handleRequest(api("POST", `/api/instances/${name}/tokens`, { body: opts }), env);
  expect(resp.status).toBe(201);
  return (await resp.json()) as Record<string, unknown>;
}

describe("audit 017: exactly-24h credential boundary", () => {
  test("accepts at expires-1ms, rejects at expires, and the boundary survives a restart", async () => {
    const { env, ingestSql, setClock, now } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const token = String(issued.token);
    const expiresAt = Date.parse(String(issued.expires_at));
    expect(expiresAt - Date.parse(String(issued.issued_at))).toBe(INSTANCE_TOKEN_TTL_MS);

    setClock(expiresAt - 1);
    expect((await handleRequest(push(token), env)).status).toBe(200);

    setClock(expiresAt);
    const expired = await handleRequest(push(token), env);
    expect(expired.status).toBe(401);
    expect(((await expired.json()) as { reason: string }).reason).toBe("expired");

    // A rehydrated coordinator over the same storage keeps the exact expiry.
    const restarted = new IngestCore({ sql: ingestSql, now });
    await restarted.load();
    setClock(expiresAt - 1);
    expect((await restarted.instanceRegistry.admit(token)).ok).toBe(true);
    setClock(expiresAt);
    expect((await restarted.instanceRegistry.admit(token)).ok).toBe(false);
  });
});

describe("audit 017: digest-only storage and no secret leakage", () => {
  test("no raw instance or person token is persisted or returned", async () => {
    const { env, ingestSql, authSql } = setup();
    await createInstance(env, "web-1");
    const instanceToken = String((await issue(env, "web-1")).token);

    const personResp = await handleRequest(api("POST", "/api/tokens", { body: { scope: "member", owner: "alice" } }), env);
    expect(personResp.status).toBe(201);
    const personToken = String(((await personResp.json()) as { token: string }).token);

    // Raw SQLite tables must hold only digests, never the secret material.
    const instanceDump = JSON.stringify(ingestSql.db.query("SELECT * FROM instance_token").all());
    const personDump = JSON.stringify(authSql.db.query("SELECT * FROM auth_token").all());
    expect(instanceDump).not.toContain(instanceToken);
    expect(personDump).not.toContain(personToken);

    // No list/response leaks a value.
    const instances = await (await handleRequest(api("GET", "/api/instances"), env)).text();
    const instanceTokens = await (await handleRequest(api("GET", "/api/instances/web-1/tokens"), env)).text();
    const personTokens = await (await handleRequest(api("GET", "/api/tokens"), env)).text();
    for (const body of [instances, instanceTokens, personTokens]) {
      expect(body).not.toContain(instanceToken);
      expect(body).not.toContain(personToken);
      expect(body).not.toContain("bit_");
      expect(body).not.toContain("bta_");
    }
  });
});

describe("audit 017: scope separation", () => {
  test("a push token reaches /ingest only; admin/member reach /api and /mcp only", async () => {
    const { env } = setup();
    await createInstance(env, "web-1");
    const instanceToken = String((await issue(env, "web-1")).token);
    const memberResp = await handleRequest(api("POST", "/api/tokens", { body: { scope: "member", owner: "alice" } }), env);
    const memberToken = String(((await memberResp.json()) as { token: string }).token);

    // Push token is rejected everywhere but /ingest.
    expect((await handleRequest(api("GET", "/api/shots", { token: instanceToken }), env)).status).toBe(401);
    expect((await handleRequest(api("GET", "/api/instances", { token: instanceToken }), env)).status).toBe(401);
    expect((await handleRequest(api("GET", "/api/tokens", { token: instanceToken }), env)).status).toBe(401);
    const mcpPush = await handleMcp(
      new Request("https://burstel.example/mcp", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${instanceToken}` },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
      }),
      env,
    );
    expect(mcpPush.status).toBe(401);

    // Admin and member tokens are rejected on /ingest.
    expect((await handleRequest(push(ENV_ADMIN), env)).status).toBe(401);
    expect((await handleRequest(push(memberToken), env)).status).toBe(401);

    // The push token works on /ingest.
    expect((await handleRequest(push(instanceToken), env)).status).toBe(200);
  });

  test("member is denied the person registry but may manage instance credentials", async () => {
    const { env } = setup();
    const memberResp = await handleRequest(api("POST", "/api/tokens", { body: { scope: "member", owner: "alice" } }), env);
    const memberToken = String(((await memberResp.json()) as { token: string }).token);

    expect((await handleRequest(api("GET", "/api/tokens", { token: memberToken }), env)).status).toBe(403);
    expect((await handleRequest(api("POST", "/api/tokens", { body: { scope: "member" }, token: memberToken }), env)).status).toBe(403);
    expect((await handleRequest(api("POST", "/api/instances", { body: { name: "web-1" }, token: memberToken }), env)).status).toBe(201);
    expect((await handleRequest(api("POST", "/api/instances/web-1/tokens", { body: {}, token: memberToken }), env)).status).toBe(201);
  });
});

describe("audit 017: durable ACK ordering and failure injection", () => {
  test("the ledger row exists only after 200, and an R2 failure is a retryable 503", async () => {
    const { env, bucket, ingest } = setup();
    await createInstance(env, "web-1");
    const token = String((await issue(env, "web-1")).token);

    // Non-shot idle intake is retained, never discarded.
    expect((await handleRequest(push(token, "traces"), env)).status).toBe(200);
    const afterOk = await ingest.listJournal({ limit: 10, offset: 0 });
    expect(afterOk.total).toBe(1);

    // R2 failure: no 200, no ledger row, no orphan.
    bucket.failPut = new Error("injected R2 outage");
    const failed = await handleRequest(push(token, "metrics"), env);
    expect(failed.status).toBe(503);
    expect(((await failed.json()) as { code: string }).code).toBe("journal_write_failed");
    expect((await ingest.listJournal({ limit: 10, offset: 0 })).total).toBe(1);
    bucket.failPut = undefined;
  });

  test("a ledger-commit failure is 503 but the raw bytes are durable and reconciliation recovers them", async () => {
    const { env, bucket, ingest } = setup();
    await createInstance(env, "web-1");
    const token = String((await issue(env, "web-1")).token);
    const payload = new Uint8Array([0x0a, 0x02, 0x07, 0x09]);

    const original = ingest.commitJournal;
    ingest.commitJournal = (() => {
      throw new Error("injected ledger outage");
    }) as typeof ingest.commitJournal;

    const failed = await handleRequest(push(token, "logs", payload), env);
    expect(failed.status).toBe(503);
    expect((await ingest.listJournal({ limit: 10, offset: 0 })).total).toBe(0);
    // The original bytes are durable in R2 despite the failed ACK.
    const key = [...bucket.objects.keys()][0]!;
    expect([...bucket.objects.get(key)!.bytes]).toEqual([...payload]);

    ingest.commitJournal = original;
    const recovered = await ingest.reconcileJournal([
      {
        key,
        instance_id: key.split("/")[1]!,
        instance: "web-1",
        signal: "logs",
        size: payload.byteLength,
        sha256: await sha256HexBytes(payload),
        content_type: "application/x-protobuf",
        content_encoding: "",
        received_at: Date.now(),
      },
    ]);
    expect(recovered).toEqual({ reconciled: 1, skipped: 0 });
    // Idempotent replay never double counts.
    const replay = await ingest.reconcileJournal([
      {
        key,
        instance_id: key.split("/")[1]!,
        instance: "web-1",
        signal: "logs",
        size: payload.byteLength,
        sha256: await sha256HexBytes(payload),
        content_type: "application/x-protobuf",
        content_encoding: "",
        received_at: Date.now(),
      },
    ]);
    expect(replay).toEqual({ reconciled: 0, skipped: 1 });
    expect((await ingest.listJournal({ limit: 10, offset: 0 })).total).toBe(1);
  });
});

describe("audit 017: bounded rotation grace", () => {
  test("the previous token works until exactly the grace instant, then is revoked", async () => {
    const { env, setClock, now } = setup();
    await createInstance(env, "web-1");
    const first = await issue(env, "web-1");
    const rotated = await issue(env, "web-1", { rotate: true, overlap_seconds: 60 });
    const rotatedAt = now();

    setClock(rotatedAt + 59_999);
    expect((await handleRequest(push(String(first.token)), env)).status).toBe(200);
    setClock(rotatedAt + 60_000);
    expect((await handleRequest(push(String(first.token)), env)).status).toBe(401);
    expect((await handleRequest(push(String(rotated.token)), env)).status).toBe(200);
  });
});

describe("audit 017: legacy migration", () => {
  test("a pre-016 shot_registry is migrated additively and legacy rows survive", async () => {
    const sql = new BunSql();
    // A shot registry as it existed before spec 016: no kind/name/window columns.
    sql.db.exec(
      `CREATE TABLE shot_registry (
         shot_id    TEXT PRIMARY KEY,
         owner      TEXT NOT NULL DEFAULT 'legacy',
         created_at INTEGER NOT NULL,
         state      TEXT NOT NULL DEFAULT 'recording',
         stopped_at INTEGER,
         size_bytes INTEGER,
         sha256     TEXT
       )`,
    );
    sql.db.exec(`INSERT INTO shot_registry (shot_id, owner, created_at, state) VALUES ('legacy-shot', 'alice', 1, 'frozen')`);

    const core = new IngestCore({ sql, now: () => 2_000_000_000_000 });
    await core.load();

    const record = core.getShotRecord("legacy-shot");
    expect(record?.kind).toBe("recording");
    expect(record?.owner).toBe("alice");
    expect(record?.state).toBe("frozen");
    expect(record?.window).toBeNull();

    // The migrated table also accepts new window shots.
    const window = await core.startWindowShot({ shot_id: "new-window", owner: "bob", instances: ["web-1"] });
    expect(window.kind).toBe("window");
    const page = await core.listShots({ limit: 10, offset: 0 });
    expect(page.shots.map((s) => s.shot_id).sort()).toEqual(["legacy-shot", "new-window"]);
  });

  test("existing instance and person credential rows survive a reload", async () => {
    const sql = new BunSql();
    const firstIngest = new IngestCore({ sql, now: () => 1_700_000_000_000 });
    await firstIngest.load();
    await firstIngest.instanceRegistry.upsert("web-1");
    const issued = await firstIngest.instanceRegistry.issue("web-1");

    const firstAuth = new AuthCore({ sql, now: () => 1_700_000_000_000 });
    await firstAuth.load();
    const minted = await firstAuth.mint({ scope: "admin", owner: "root" });

    // Simulate an upgrade: new cores over the same storage (CREATE TABLE IF NOT
    // EXISTS only, so rows must be untouched).
    const restarted = new IngestCore({ sql, now: () => 1_700_000_000_000 });
    await restarted.load();
    const restartedAuth = new AuthCore({ sql, now: () => 1_700_000_000_000 });
    await restartedAuth.load();

    expect((await restarted.instanceRegistry.admit(issued.token)).ok).toBe(true);
    expect((await restarted.instanceRegistry.list()).instances.map((i) => i.name)).toEqual(["web-1"]);
    expect((await restartedAuth.authorize(minted.token)).ok).toBe(true);
    expect((await restartedAuth.list()).tokens.map((t) => t.id)).toEqual([minted.id]);
  });

  test("the retired ingest-token endpoints return 410 with migration guidance", async () => {
    const { env } = setup();
    const noAuth = await handleRequest(api("POST", "/api/ingest-token", { token: null }), env);
    expect(noAuth.status).toBe(410);
    expect(((await noAuth.json()) as { code: string }).code).toBe("gone");

    const withAuth = await handleRequest(api("POST", "/api/shots/AAAAAAAAAAAAAAAA/ingest-token", { body: {} }), env);
    expect(withAuth.status).toBe(410);
    expect((await withAuth.text()).toLowerCase()).toContain("instance");
  });
});
