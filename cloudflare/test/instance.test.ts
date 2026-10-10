// Spec 014: named instances and continuous durable OTLP intake.
//
// Deterministic Worker-level tests over the real request handler, the real
// IngestCore/InstanceCore over in-process SQLite, a fake clock and an in-memory
// R2 bucket. They pin:
//   * named instances with immutable ids and 24h push-only tokens;
//   * continuous ingest with no shots and across arbitrary shot events;
//   * no cross-instance attribution;
//   * the durability boundary (R2 raw journal + ledger BEFORE OTLP 200);
//   * expiration, revocation and bounded rotation overlap;
//   * the auth matrix (instance token vs admin/member credential, REST + MCP);
//   * malformed protobuf/gzip/JSON rejection;
//   * R2/ledger failure handling and reconciliation.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { sha256Hex, sha256HexBytes } from "../src/crypto";
import type { IngestCore } from "../src/ingest-core";
import { INSTANCE_TOKEN_TTL_MS, newInstanceToken } from "../src/instance-core";
import { handleMcp } from "../src/mcp";
import {
  FakeArtifacts,
  FakeAuthNamespace,
  FakeBucket,
  FakeIngestNamespace,
  makeAuth,
  makeCore,
  makeIngest,
} from "./fakes";
import type { TestHarness } from "./fakes";

const ENV_ADMIN = "instance-admin-token";

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
  bucket: FakeBucket;
  ingest: IngestCore;
  setClock(epochMs: number): void;
  now(): number;
}

let counter = 0;
function nextToken(): string {
  counter += 1;
  return `bit_test_${counter}`;
}
function nextInstanceId(): string {
  counter += 1;
  return `inst${counter.toString().padStart(8, "0")}`;
}
function nextTokenId(): string {
  counter += 1;
  return `tok${counter.toString().padStart(8, "0")}`;
}
function nextAuthToken(): string {
  counter += 1;
  return `bta_test_${counter}`;
}
function nextAuthId(): string {
  counter += 1;
  return `person${counter.toString().padStart(4, "0")}`;
}

function setup(): Setup {
  let clock = 1_700_000_000_000;
  const now = () => clock;
  const shots = new FakeShotNamespace();
  const bucket = new FakeBucket();
  const ingest = makeIngest({
    now,
    generateToken: nextToken,
    generateInstanceId: nextInstanceId,
    generateTokenId: nextTokenId,
  });
  const auth = makeAuth({ now, generateToken: nextAuthToken, generateId: nextAuthId });
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    AUTH_DO: new FakeAuthNamespace(auth.core),
    BUCKET: bucket,
    BURSTEL_API_TOKEN: ENV_ADMIN,
    PUBLIC_BASE_URL: "https://burstel.example",
  };
  return { env, shots, bucket, ingest: ingest.core, setClock: (v) => (clock = v), now };
}

function req(
  method: string,
  path: string,
  options: { body?: unknown; token?: string | null; rawBody?: Uint8Array; contentType?: string } = {},
): Request {
  const headers: Record<string, string> = {};
  const token = options.token === undefined ? ENV_ADMIN : options.token;
  if (token) headers["authorization"] = `Bearer ${token}`;
  const init: RequestInit = { method, headers };
  if (options.rawBody !== undefined) {
    headers["content-type"] = options.contentType ?? "application/x-protobuf";
    init.body = options.rawBody;
  } else if (options.body !== undefined) {
    headers["content-type"] = "application/json";
    init.body = JSON.stringify(options.body);
  }
  return new Request(`https://burstel.example${path}`, init);
}

function push(token: string | null, signal = "traces", body = new Uint8Array([0x0a, 0x00])): Request {
  const headers: Record<string, string> = { "content-type": "application/x-protobuf" };
  if (token) headers["x-burstel-token"] = token;
  return new Request(`https://burstel.example/ingest/v1/${signal}`, { method: "POST", headers, body });
}

async function createInstance(env: AppEnv, name: string): Promise<Record<string, unknown>> {
  const resp = await handleRequest(req("POST", "/api/instances", { body: { name } }), env);
  expect(resp.status).toBe(201);
  return (await resp.json()) as Record<string, unknown>;
}

async function issue(env: AppEnv, name: string, opts: { rotate?: boolean; overlap_seconds?: number } = {}): Promise<Record<string, unknown>> {
  const resp = await handleRequest(req("POST", `/api/instances/${name}/tokens`, { body: opts }), env);
  expect(resp.status).toBe(201);
  return (await resp.json()) as Record<string, unknown>;
}

async function gzip(bytes: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

describe("named instances and credentials", () => {
  test("create is an idempotent upsert with an immutable instance_id", async () => {
    const { env } = setup();
    const a = await createInstance(env, "  Web-1 ");
    expect(a.name).toBe("web-1");
    expect(typeof a.instance_id).toBe("string");
    expect(String(a.instance_id).length).toBeGreaterThan(0);

    const again = await createInstance(env, "web-1");
    expect(again.instance_id).toBe(a.instance_id);

    const listed = (await (await handleRequest(req("GET", "/api/instances"), env)).json()) as {
      instances: Array<Record<string, unknown>>;
    };
    expect(listed.instances.map((i) => i.name)).toEqual(["web-1"]);
    // No secret is ever present in instance metadata.
    expect(JSON.stringify(listed)).not.toContain("bit_");
  });

  test("rejects path tricks and invalid names", async () => {
    const { env } = setup();
    for (const name of ["../etc", "a/b", "..", "", "-", "UPPER CASE", "a".repeat(64)]) {
      const resp = await handleRequest(req("POST", "/api/instances", { body: { name } }), env);
      expect(resp.status).toBe(400);
    }
  });

  test("issues a 256-bit 24h token once, digest-only", async () => {
    const { env, ingest } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const token = String(issued.token);
    expect(token.startsWith("bit_")).toBe(true);
    const real = newInstanceToken();
    expect(real.slice(4).length).toBeGreaterThanOrEqual(43);
    expect(newInstanceToken()).not.toBe(real);
    expect(issued.ttl_seconds).toBe(86_400);
    expect(Date.parse(String(issued.expires_at)) - Date.parse(String(issued.issued_at))).toBe(INSTANCE_TOKEN_TTL_MS);

    const stored = await ingest.instanceRegistry.listTokens("web-1");
    expect(stored.tokens).toHaveLength(1);
    expect(JSON.stringify(stored)).not.toContain(token);

    // Response is never cached.
    const resp = await handleRequest(req("POST", "/api/instances/web-1/tokens", { body: {} }), env);
    expect(resp.headers.get("cache-control")).toBe("no-store");
  });

  test("accepts at issue+23h59m59s and rejects 401 at exactly 24h", async () => {
    const { env, setClock } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const token = String(issued.token);
    const expiresAt = Date.parse(String(issued.expires_at));

    setClock(expiresAt - 1000);
    const ok = await handleRequest(push(token), env);
    expect(ok.status).toBe(200);
    expect(ok.headers.get("x-burstel-instance")).toBe("web-1");
    expect(ok.headers.get("x-burstel-instance-id")).toBe(String(issued.instance_id));

    setClock(expiresAt);
    const expired = await handleRequest(push(token), env);
    expect(expired.status).toBe(401);
    expect(((await expired.json()) as { reason: string }).reason).toBe("expired");
  });

  test("revocation is immediate; rotation is bounded and instance-scoped", async () => {
    const { env, setClock, now } = setup();
    await createInstance(env, "web-1");
    await createInstance(env, "db-1");
    const web = await issue(env, "web-1");
    const db = await issue(env, "db-1");

    // Immediate revocation.
    const revoked = await handleRequest(req("DELETE", `/api/instances/web-1/tokens/${web.id}`), env);
    expect(revoked.status).toBe(200);
    expect((await handleRequest(push(String(web.token)), env)).status).toBe(401);
    // Another instance is untouched.
    expect((await handleRequest(push(String(db.token)), env)).status).toBe(200);

    // Rotation: previous token keeps working for the bounded overlap, then dies.
    const rotated = await issue(env, "db-1", { rotate: true, overlap_seconds: 60 });
    const rotatedAt = now();
    expect((await handleRequest(push(String(db.token)), env)).status).toBe(200);
    setClock(rotatedAt + 59_000);
    expect((await handleRequest(push(String(db.token)), env)).status).toBe(200);
    setClock(rotatedAt + 61_000);
    expect((await handleRequest(push(String(db.token)), env)).status).toBe(401);
    expect((await handleRequest(push(String(rotated.token)), env)).status).toBe(200);
  });

  test("rotation overlap is bounded even when a huge value is requested", async () => {
    const { env, setClock, now } = setup();
    await createInstance(env, "web-1");
    const first = await issue(env, "web-1");
    await issue(env, "web-1", { rotate: true, overlap_seconds: 999_999 });
    const at = now();
    setClock(at + 3_600_000); // cap is 1h
    expect((await handleRequest(push(String(first.token)), env)).status).toBe(401);
  });

  test("unknown instance and unknown token fail closed", async () => {
    const { env } = setup();
    expect((await handleRequest(req("POST", "/api/instances/nope/tokens", { body: {} }), env)).status).toBe(404);
    expect((await handleRequest(req("GET", "/api/instances/nope/tokens"), env)).status).toBe(404);
    expect((await handleRequest(push("bit_bogus"), env)).status).toBe(401);
    expect((await handleRequest(push(null), env)).status).toBe(401);
  });
});

describe("durable continuous OTLP intake", () => {
  test("two named instances ingest concurrently with no shots and no cross attribution", async () => {
    const { env, shots, bucket, ingest } = setup();
    await createInstance(env, "web-1");
    await createInstance(env, "db-1");
    const web = await issue(env, "web-1");
    const db = await issue(env, "db-1");

    // Get the real ids for attribution assertions.
    const webMeta = (await (await handleRequest(req("GET", "/api/instances"), env)).json()) as {
      instances: Array<{ name: string; instance_id: string }>;
    };
    const webId = webMeta.instances.find((i) => i.name === "web-1")!.instance_id;

    const responses = await Promise.all([
      handleRequest(push(String(web.token), "traces"), env),
      handleRequest(push(String(db.token), "metrics"), env),
      handleRequest(push(String(web.token), "logs"), env),
    ]);
    expect(responses.map((r) => r.status)).toEqual([200, 200, 200]);

    // No shots were created by continuous ingest.
    expect(shots.shots.size).toBe(0);
    expect(bucket.objects.size).toBe(3);

    const ledger = await ingest.listJournal({ limit: 50, offset: 0 });
    expect(ledger.total).toBe(3);
    // R2 keys and ledger rows carry the trusted instance id, not a shot.
    for (const entry of ledger.entries) {
      expect(entry.key.startsWith(`journal/${entry.instance_id}/`)).toBe(true);
      expect(entry.instance_id).toBe(entry.instance === "web-1" ? webId : entry.instance_id);
    }
    const webEntries = ledger.entries.filter((e) => e.instance === "web-1");
    expect(webEntries).toHaveLength(2);
    expect(new Set(webEntries.map((e) => e.instance_id))).toEqual(new Set([webId]));
    // Distinct payloads never merge.
    expect(new Set(ledger.entries.map((e) => e.key)).size).toBe(3);
  });

  test("valid OTLP is persisted byte-for-byte before 200 with trusted instance fields", async () => {
    const { env, bucket, ingest } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const payload = new Uint8Array([0x0a, 0x02, 0x01, 0x02]);

    // User OTel resource labels are carried in the body and never touched.
    const resp = await handleRequest(push(String(issued.token), "traces", payload), env);
    expect(resp.status).toBe(200);
    expect(resp.headers.get("x-burstel-disposition")).toBe("journaled");

    const keys = [...bucket.objects.keys()];
    expect(keys).toHaveLength(1);
    const object = bucket.objects.get(keys[0]!)!;
    expect([...object.bytes]).toEqual([...payload]);
    expect(object.customMetadata?.instance_id).toBe(String(issued.instance_id));
    expect(object.customMetadata?.instance).toBe("web-1");
    expect(object.customMetadata?.signal).toBe("traces");

    const ledger = await ingest.listJournal({ limit: 10, offset: 0 });
    expect(ledger.entries[0]!.sha256).toBe(await sha256HexBytes(payload));
  });

  test("gzip bodies are verified and the original compressed bytes are preserved", async () => {
    const { env, bucket } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const raw = new Uint8Array([0x0a, 0x00]);
    const compressed = await gzip(raw);

    const request = new Request("https://burstel.example/ingest/v1/traces", {
      method: "POST",
      headers: {
        "content-type": "application/x-protobuf",
        "content-encoding": "gzip",
        "x-burstel-token": String(issued.token),
      },
      body: compressed,
    });
    const resp = await handleRequest(request, env);
    expect(resp.status).toBe(200);
    const keys = [...bucket.objects.keys()];
    expect([...bucket.objects.get(keys[0]!)!.bytes]).toEqual([...compressed]);
    expect(bucket.objects.get(keys[0]!)!.customMetadata?.content_encoding).toBe("gzip");
  });

  test("continued ingest survives arbitrary shot events", async () => {
    const { env, ingest } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");

    expect((await handleRequest(push(String(issued.token)), env)).status).toBe(200);
    // Start and stop shots arbitrarily; the instance token is unaffected.
    await handleRequest(req("POST", "/api/shots", { body: { metadata: {} } }), env);
    const shot = (await (await handleRequest(req("POST", "/api/shots", { body: { metadata: {} } }), env)).json()) as {
      shot_id: string;
    };
    await handleRequest(req("POST", `/api/shots/${shot.shot_id}/stop`, { body: {} }), env);
    expect((await handleRequest(push(String(issued.token)), env)).status).toBe(200);

    const ledger = await ingest.listJournal({ limit: 10, offset: 0 });
    expect(ledger.total).toBe(2);
  });

  test("malformed bodies are rejected with 4xx and nothing is persisted", async () => {
    const { env, bucket } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const token = String(issued.token);

    // Truncated protobuf: field 1 declares 5 bytes but provides none.
    expect((await handleRequest(push(token, "traces", new Uint8Array([0x0a, 0x05])), env)).status).toBe(400);

    // Invalid gzip.
    const badGzip = new Request("https://burstel.example/ingest/v1/logs", {
      method: "POST",
      headers: { "content-type": "application/x-protobuf", "content-encoding": "gzip", "x-burstel-token": token },
      body: new Uint8Array([0x00, 0x01, 0x02, 0x03]),
    });
    expect((await handleRequest(badGzip, env)).status).toBe(400);

    // Malformed JSON.
    const badJson = new Request("https://burstel.example/ingest/v1/metrics", {
      method: "POST",
      headers: { "content-type": "application/json", "x-burstel-token": token },
      body: "{not json",
    });
    expect((await handleRequest(badJson, env)).status).toBe(400);

    // Empty body and unsupported content type.
    expect((await handleRequest(push(token, "traces", new Uint8Array()), env)).status).toBe(400);
    const wrongType = new Request("https://burstel.example/ingest/v1/traces", {
      method: "POST",
      headers: { "content-type": "text/plain", "x-burstel-token": token },
      body: new Uint8Array([0x0a, 0x00]),
    });
    expect((await handleRequest(wrongType, env)).status).toBe(415);

    expect(bucket.objects.size).toBe(0);
  });

  test("an oversized body is rejected with 413 before any durable write", async () => {
    const { env, bucket } = setup();
    env.MAX_INGEST_BYTES = "4";
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const resp = await handleRequest(push(String(issued.token), "traces", new Uint8Array([0x0a, 0x02, 1, 2, 3, 4])), env);
    expect(resp.status).toBe(413);
    expect(bucket.objects.size).toBe(0);
  });
});

describe("durability failures and reconciliation", () => {
  test("an R2 write failure returns a retryable 503 and commits no ledger row", async () => {
    const { env, bucket, ingest } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    bucket.failPut = new Error("R2 unavailable");

    const resp = await handleRequest(push(String(issued.token)), env);
    expect(resp.status).toBe(503);
    expect(((await resp.json()) as { code: string }).code).toBe("journal_write_failed");
    const ledger = await ingest.listJournal({ limit: 10, offset: 0 });
    expect(ledger.total).toBe(0);
  });

  test("a ledger commit failure returns 503 and reconciliation recovers the durable object", async () => {
    const { env, bucket, ingest } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");

    const original = ingest.commitJournal;
    ingest.commitJournal = (() => {
      throw new Error("ledger unavailable");
    }) as typeof ingest.commitJournal;

    const resp = await handleRequest(push(String(issued.token)), env);
    expect(resp.status).toBe(503);
    // The raw bytes are durable even though the ACK failed.
    expect(bucket.objects.size).toBe(1);
    expect((await ingest.listJournal({ limit: 10, offset: 0 })).total).toBe(0);

    ingest.commitJournal = original;
    const key = [...bucket.objects.keys()][0]!;
    const bytes = bucket.objects.get(key)!.bytes;
    const reconciled = await ingest.reconcileJournal([
      {
        key,
        instance_id: String(issued.instance_id),
        instance: "web-1",
        signal: "traces",
        size: bytes.byteLength,
        sha256: await sha256HexBytes(bytes),
        content_type: "application/x-protobuf",
        content_encoding: "",
        received_at: Date.now(),
      },
    ]);
    expect(reconciled.reconciled).toBe(1);
    expect((await ingest.listJournal({ limit: 10, offset: 0 })).total).toBe(1);
  });

  test("a lost ledger write is recovered by reconciliation without re-writing the payload", async () => {    const { env, bucket, ingest } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");

    // Simulate raw bytes durable in R2 but the ledger commit lost.
    const key = `journal/${issued.instance_id}/traces/1700000000000-orphan.bin`;
    const bytes = new Uint8Array([0x0a, 0x00]);
    await bucket.put(key, bytes);

    const before = await ingest.listJournal({ limit: 10, offset: 0 });
    expect(before.total).toBe(0);

    const reconciled = await ingest.reconcileJournal([
      {
        key,
        instance_id: String(issued.instance_id),
        instance: "web-1",
        signal: "traces",
        size: bytes.byteLength,
        sha256: await sha256HexBytes(bytes),
        content_type: "application/x-protobuf",
        content_encoding: "",
        received_at: 1_700_000_000_000,
      },
    ]);
    expect(reconciled).toEqual({ reconciled: 1, skipped: 0 });

    // Idempotent: a replay does not double count.
    const replay = await ingest.reconcileJournal([
      {
        key,
        instance_id: String(issued.instance_id),
        instance: "web-1",
        signal: "traces",
        size: bytes.byteLength,
        sha256: await sha256HexBytes(bytes),
        content_type: "application/x-protobuf",
        content_encoding: "",
        received_at: 1_700_000_000_000,
      },
    ]);
    expect(replay).toEqual({ reconciled: 0, skipped: 1 });
    expect((await ingest.listJournal({ limit: 10, offset: 0 })).total).toBe(1);
  });
});

describe("instance auth matrix", () => {
  test("an instance token cannot reach /api, /mcp or the legacy shot path", async () => {
    const { env } = setup();
    await createInstance(env, "web-1");
    const issued = await issue(env, "web-1");
    const token = String(issued.token);

    expect((await handleRequest(req("GET", "/api/shots", { token }), env)).status).toBe(401);
    expect((await handleRequest(req("GET", "/api/instances", { token }), env)).status).toBe(401);
    expect((await handleRequest(req("POST", "/api/instances/web-1/tokens", { body: {}, token }), env)).status).toBe(401);
    expect((await handleRequest(req("GET", "/api/tokens", { token }), env)).status).toBe(401);

    const legacy = await handleRequest(
      new Request("https://burstel.example/shots/AAAAAAAAAAAAAAAA/v1/traces", {
        method: "POST",
        headers: { "content-type": "application/x-protobuf", "x-burstel-token": token },
        body: new Uint8Array([0x0a, 0x00]),
      }),
      env,
    );
    expect(legacy.status).toBe(401);

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

  test("the admin credential cannot be used for OTLP ingest", async () => {
    const { env } = setup();
    expect((await handleRequest(push(ENV_ADMIN), env)).status).toBe(401);
    expect((await handleRequest(push(null), env)).status).toBe(401);
  });

  test("admin and member may manage instance credentials; only admin may manage person tokens", async () => {
    const { env } = setup();
    const memberResp = await handleRequest(req("POST", "/api/tokens", { body: { scope: "member", owner: "alice" } }), env);
    expect(memberResp.status).toBe(201);
    const member = (await memberResp.json()) as { token: string };

    // member can create an instance and issue/list/revoke its token.
    const created = await handleRequest(req("POST", "/api/instances", { body: { name: "web-1" }, token: member.token }), env);
    expect(created.status).toBe(201);
    const issued = await handleRequest(req("POST", "/api/instances/web-1/tokens", { body: {}, token: member.token }), env);
    expect(issued.status).toBe(201);
    const tokenId = String(((await issued.json()) as { id: string }).id);
    expect((await handleRequest(req("GET", "/api/instances/web-1/tokens", { token: member.token }), env)).status).toBe(200);
    expect((await handleRequest(req("DELETE", `/api/instances/web-1/tokens/${tokenId}`, { token: member.token }), env)).status).toBe(200);

    // member is denied person-token management.
    expect((await handleRequest(req("POST", "/api/tokens", { body: { scope: "member" }, token: member.token }), env)).status).toBe(403);
    expect((await handleRequest(req("GET", "/api/tokens", { token: member.token }), env)).status).toBe(403);
  });
});

describe("registry durability", () => {
  test("restart preserves instances, tokens and the journal ledger", async () => {
    let clock = 1_700_000_000_000;
    const now = () => clock;
    const first = makeIngest({ now, generateToken: nextToken, generateInstanceId: nextInstanceId, generateTokenId: nextTokenId });
    await first.core.instanceRegistry.upsert("web-1");
    const issued = await first.core.instanceRegistry.issue("web-1");
    await first.core.commitJournal({
      key: `journal/x/traces/1-abc.bin`,
      instance_id: issued.instance_id,
      instance: "web-1",
      signal: "traces",
      size: 2,
      sha256: await sha256Hex("xx"),
      content_type: "application/x-protobuf",
      content_encoding: "",
      received_at: 1,
    });

    // Simulate DO eviction: a fresh core over the same SQLite storage.
    const restarted = makeIngest({
      now,
      sql: first.sql,
      generateToken: nextToken,
      generateInstanceId: nextInstanceId,
      generateTokenId: nextTokenId,
    });
    await restarted.core.load();
    const instances = await restarted.core.instanceRegistry.list();
    expect(instances.instances.map((i) => i.name)).toEqual(["web-1"]);
    expect((await restarted.core.instanceRegistry.admit(issued.token)).ok).toBe(true);
    expect((await restarted.core.listJournal({ limit: 10, offset: 0 })).total).toBe(1);
  });
});
