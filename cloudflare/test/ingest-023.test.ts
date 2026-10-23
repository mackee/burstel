// Spec 023 acceptance tests: DO-free ingest, KV admission, late-write guard,
// compaction-time discovery and revocation exclusion.
//
// These exercise the real Worker handler, the real IngestCore/InstanceCore over
// in-process SQLite, the real CompactorCore, an in-memory R2/KV fake and a fake
// container. They pin exactly the acceptance list from the spec.

import { describe, expect, test } from "bun:test";

import { admissionCacheSize, resetAdmissionCache } from "../src/admission";
import { RecordingAnalytics } from "../src/analytics";
import { handleRequest } from "../src/app";
import { CompactorCore } from "../src/compactor-core";
import type { AppEnv } from "../src/env";
import type { IngestCore } from "../src/ingest-core";
import { sha256HexBytes } from "../src/crypto";
import { autoBackfillInstanceTokens } from "../src/tools";
import {
  FakeBucket,
  FakeCompactorContainer,
  FakeCompactorNamespace,
  FakeIngestNamespace,
  FakeKV,
  makeIngest,
} from "./fakes";

const ADMIN = "spec-023-admin";
const HOUR = 60 * 60 * 1000;

interface Harness {
  env: AppEnv;
  bucket: FakeBucket;
  kv: FakeKV;
  namespace: FakeIngestNamespace;
  ingest: IngestCore;
  sql: { exec(q: string): { toArray(): Array<Record<string, unknown>> } };
  analytics: RecordingAnalytics;
  clock: { now: number };
  setClock(ms: number): void;
}

let counter = 0;
function next(prefix: string): string {
  counter += 1;
  return `${prefix}${counter.toString().padStart(6, "0")}`;
}

function makeHarness(options: { lateWriteLimitMs?: number; withKv?: boolean } = {}): Harness {
  resetAdmissionCache();
  const clock = { now: 1_700_000_000_000 };
  const now = () => clock.now;
  const bucket = new FakeBucket();
  const kv = new FakeKV();
  const analytics = new RecordingAnalytics();
  const { core, sql } = makeIngest({
    now,
    bucket,
    generateToken: () => `bit_${next("token")}`,
    generateInstanceId: () => next("inst"),
    generateTokenId: () => next("tok"),
  });
  const namespace = new FakeIngestNamespace(core);
  const env: AppEnv = {
    SHOT_DO: { getByName: () => ({ fetch: async () => new Response("{}", { status: 200 }) }) },
    INGEST_DO: namespace,
    BUCKET: bucket,
    BURSTEL_API_TOKEN: ADMIN,
    PUBLIC_BASE_URL: "https://burstel.example",
    INGEST_ANALYTICS: analytics,
    now,
    LATE_WRITE_LIMIT_MS: String(options.lateWriteLimitMs ?? 45_000),
  };
  if (options.withKv !== false) env.INSTANCE_TOKENS = kv;
  return {
    env,
    bucket,
    kv,
    namespace,
    ingest: core,
    sql,
    analytics,
    clock,
    setClock: (ms) => {
      clock.now = ms;
    },
  };
}

function api(method: string, path: string, body?: unknown, token: string | null = ADMIN): Request {
  const headers: Record<string, string> = {};
  if (token) headers["authorization"] = `Bearer ${token}`;
  const init: RequestInit = { method, headers };
  if (body !== undefined) {
    headers["content-type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  return new Request(`https://burstel.example${path}`, init);
}

async function createInstance(h: Harness, name: string): Promise<void> {
  const resp = await handleRequest(api("POST", "/api/instances", { name }), h.env);
  expect(resp.status).toBe(201);
}

async function issue(h: Harness, name: string): Promise<{ token: string; id: string; instance_id: string; expires_at: string }> {
  const resp = await handleRequest(api("POST", `/api/instances/${name}/tokens`, {}), h.env);
  expect(resp.status).toBe(201);
  return (await resp.json()) as { token: string; id: string; instance_id: string; expires_at: string };
}

function push(token: string | null, signal = "traces", body = new Uint8Array([0x0a, 0x00])): Request {
  const headers: Record<string, string> = { "content-type": "application/x-protobuf" };
  if (token) headers["x-burstel-token"] = token;
  return new Request(`https://burstel.example/ingest/v1/${signal}`, { method: "POST", headers, body });
}

async function seedJournal(
  h: Harness,
  opts: {
    instanceId: string;
    instance: string;
    tokenId: string;
    receivedAt: number;
    signal?: string;
    bytes?: Uint8Array;
  },
): Promise<{ key: string; sha: string; size: number }> {
  const signal = opts.signal ?? "traces";
  const bytes = opts.bytes ?? new TextEncoder().encode(`seed-${next("obj")}`);
  const sha = await sha256HexBytes(bytes);
  const key = `journal/${opts.instanceId}/${signal}/${opts.receivedAt}-${next("rand")}.bin`;
  await h.bucket.put(key, bytes, {
    sha256: sha,
    customMetadata: {
      sha256: sha,
      signal,
      instance_id: opts.instanceId,
      instance: opts.instance,
      token_id: opts.tokenId,
      received_at: String(opts.receivedAt),
      content_encoding: "",
      content_type: "application/x-protobuf",
    },
  });
  return { key, sha, size: bytes.byteLength };
}

function makeCompactor(
  h: Harness,
  options: { graceMs?: number; lookbackMs?: number; batchObjects?: number; maxBatchesPerTick?: number; windowMs?: number } = {},
): CompactorCore {
  const graceMs = options.graceMs ?? 90_000;
  return new CompactorCore({
    ingest: new FakeCompactorNamespace(h.ingest),
    bucket: h.bucket,
    container: new FakeCompactorContainer(),
    holder: "spec-023",
    now: () => h.clock.now,
    config: {
      graceMs,
      lookbackMs: options.lookbackMs ?? Math.max(2 * graceMs, 1),
      ...(options.batchObjects !== undefined ? { batchObjects: options.batchObjects } : {}),
      ...(options.maxBatchesPerTick !== undefined ? { maxBatchesPerTick: options.maxBatchesPerTick } : {}),
      ...(options.windowMs !== undefined ? { windowMs: options.windowMs } : {}),
    },
  });
}

describe("D1: KV token admission", () => {
  test("a KV miss falls back to the authoritative ControlDO and backfills KV", async () => {
    const h = makeHarness();
    await createInstance(h, "web-1");
    const issued = await issue(h, "web-1");

    // Simulate a pre-023 token: no KV entry, no memory entry.
    h.kv.values.clear();
    resetAdmissionCache();
    const before = h.namespace.admitCalls();

    const resp = await handleRequest(push(issued.token), h.env);
    expect(resp.status).toBe(200);
    expect(h.namespace.admitCalls()).toBe(before + 1);
    expect(h.kv.values.size).toBe(1);
    const cached = [...h.kv.values.values()][0]!;
    expect(JSON.parse(cached)).toMatchObject({
      instance_id: issued.instance_id,
      instance_name: "web-1",
      token_id: issued.id,
    });

    // The next miss is served by KV without another ControlDO admission.
    resetAdmissionCache();
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(200);
    expect(h.namespace.admitCalls()).toBe(before + 1);
  });

  test("negative results are never cached in memory or KV", async () => {
    const h = makeHarness();
    resetAdmissionCache();
    const before = h.namespace.admitCalls();

    expect((await handleRequest(push("bit_does_not_exist"), h.env)).status).toBe(401);
    expect((await handleRequest(push("bit_does_not_exist"), h.env)).status).toBe(401);

    expect(h.namespace.admitCalls()).toBe(before + 2);
    expect(admissionCacheSize()).toBe(0);
    expect(h.kv.values.size).toBe(0);
    expect(h.kv.putCalls).toBe(0);
  });

  test("expiry is evaluated locally and exactly at expires_at", async () => {
    const h = makeHarness();
    await createInstance(h, "web-1");
    const issued = await issue(h, "web-1");
    const expiresAt = Date.parse(issued.expires_at);

    h.setClock(expiresAt - 1);
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(200);

    // Same cached grant: at expires_at it must be rejected without a DO call.
    const before = h.namespace.admitCalls();
    h.setClock(expiresAt);
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(401);
    expect(h.namespace.admitCalls()).toBe(before);

    // The KV path applies the same exact boundary after a memory flush.
    resetAdmissionCache();
    h.setClock(expiresAt - 1);
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(200);
    resetAdmissionCache();
    h.setClock(expiresAt);
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(401);
  });

  test("a one-shot backfill populates KV for existing bit_ tokens", async () => {
    const h = makeHarness();
    const instance = await h.ingest.instanceRegistry.upsert("web-1");
    const issued = await h.ingest.instanceRegistry.issue("web-1");
    expect(h.kv.values.size).toBe(0);

    const resp = await handleRequest(api("POST", "/api/instance-tokens/backfill"), h.env);
    expect(resp.status).toBe(200);
    expect(await resp.json()).toMatchObject({ backfilled: 1, failed: 0 });
    expect(h.kv.values.size).toBe(1);

    resetAdmissionCache();
    const before = h.namespace.admitCalls();
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(200);
    expect(h.namespace.admitCalls()).toBe(before);
    expect(h.bucket.objects.size).toBe(1);
    expect([...h.bucket.objects.values()][0]!.customMetadata?.instance_id).toBe(instance.instance_id);
  });

  test("a KV outage falls back to the authoritative ControlDO", async () => {
    const h = makeHarness();
    await createInstance(h, "web-1");
    const issued = await issue(h, "web-1");
    resetAdmissionCache();
    h.kv.failGet = new Error("KV unavailable");
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(200);
  });

  test("the Cron backfill warms KV without waiting for an admission miss (R2)", async () => {
    const h = makeHarness();
    const instance = await h.ingest.instanceRegistry.upsert("web-1");
    const issued = await h.ingest.instanceRegistry.issue("web-1");
    // Simulate an upgrade: authoritative state exists, KV is empty, memory cold.
    h.kv.values.clear();
    resetAdmissionCache();

    const result = await autoBackfillInstanceTokens(h.namespace, h.kv);
    expect(result).toMatchObject({ backfilled: 1, failed: 0 });
    expect(result.skipped).toBeUndefined();
    expect(h.kv.values.size).toBe(1);

    const before = h.namespace.admitCalls();
    expect((await handleRequest(push(issued.token), h.env)).status).toBe(200);
    expect(h.namespace.admitCalls()).toBe(before);
    expect([...h.bucket.objects.values()][0]!.customMetadata?.instance_id).toBe(instance.instance_id);
  });

  test("the Cron backfill is best effort and idempotent", async () => {
    const h = makeHarness();
    await h.ingest.instanceRegistry.upsert("web-1");
    await h.ingest.instanceRegistry.issue("web-1");
    const first = await autoBackfillInstanceTokens(h.namespace, h.kv);
    const second = await autoBackfillInstanceTokens(h.namespace, h.kv);
    expect(first.backfilled).toBe(1);
    expect(second.backfilled).toBe(1);
    expect(h.kv.values.size).toBe(1);
    expect(await autoBackfillInstanceTokens(h.namespace, undefined)).toMatchObject({
      backfilled: 0,
      failed: 0,
      skipped: "kv_unconfigured",
    });
  });
});

describe("D2: ledger-free ingest and late-write guard", () => {
  test("an accepted export writes one object and emits an Analytics Engine counter", async () => {
    const h = makeHarness();
    await createInstance(h, "web-1");
    const issued = await issue(h, "web-1");

    expect((await handleRequest(push(issued.token), h.env)).status).toBe(200);
    expect(h.bucket.objects.size).toBe(1);
    expect(await h.ingest.listJournal({ limit: 10, offset: 0 }).then((r) => r.total)).toBe(0);

    const accepted = h.analytics.points.find((p) => p.blobs?.[0] === "accepted")!;
    expect(accepted.blobs).toMatchObject(["accepted", "", "traces", issued.instance_id, "web-1", issued.id]);
    expect(accepted.doubles?.[0]).toBe(1);
    expect(accepted.doubles?.[1]).toBeGreaterThan(0);

    // Rejections are counted with a stable reason, never as an accepted byte.
    const rejected = await handleRequest(push("bit_nope", "logs"), h.env);
    expect(rejected.status).toBe(401);
    const point = h.analytics.points.at(-1)!;
    expect(point.blobs?.[0]).toBe("rejected");
    expect(point.blobs?.[1]).toBe("invalid_token_invalid");
  });

  test("a PUT that completes after the late-write limit deletes the object and returns 503", async () => {
    const h = makeHarness({ lateWriteLimitMs: 5 });
    await createInstance(h, "web-1");
    const issued = await issue(h, "web-1");

    const originalPut = h.bucket.put.bind(h.bucket);
    h.bucket.put = async (key, value, options) => {
      const result = await originalPut(key, value, options);
      h.clock.now += 10; // simulate a slow write
      return result;
    };

    const resp = await handleRequest(push(issued.token), h.env);
    expect(resp.status).toBe(503);
    expect(((await resp.json()) as { code: string }).code).toBe("late_write");
    expect(h.bucket.objects.size).toBe(0);
    expect(h.analytics.points.at(-1)!.blobs?.[1]).toBe("late_write");
    expect(await h.ingest.journalStats().then((s) => s.objects)).toBe(0);
  });
});

describe("D3: compaction-time discovery", () => {
  test("a cross-instance time bucket becomes one segment; late objects inside the lookback are re-listed", async () => {
    const h = makeHarness();
    const t = h.clock.now;
    await seedJournal(h, { instanceId: "inst-a", instance: "a", tokenId: "tok-a", receivedAt: t - 130_000 });
    const first = await makeCompactor(h).tick();
    expect(first.ok).toBe(true);
    expect(first.segments_published).toBe(1);
    expect(first.discovered_objects).toBe(1);

    // Lands after the previous run's cursor but inside the 2x grace lookback.
    await seedJournal(h, { instanceId: "inst-b", instance: "b", tokenId: "tok-b", receivedAt: t - 120_000 });
    const second = await makeCompactor(h).tick();
    expect(second.ok).toBe(true);
    expect(second.segments_published).toBe(1);
    expect(second.discovered_objects).toBe(1);

    const segments = await h.ingest.listSegments({ limit: 10, offset: 0 });
    expect(segments.total).toBe(2);
    // Two instances in one bucket -> one cross-instance segment (D3.3).
    const rows = h.sql
      .exec("SELECT DISTINCT instance_id FROM segment_coverage ORDER BY instance_id")
      .toArray()
      .map((r) => String(r.instance_id));
    expect(rows).toEqual(["inst-a", "inst-b"]);
  });

  test("objects written with a token revoked before receipt are excluded, retained for audit and then GC'd (R5)", async () => {
    const h = makeHarness();
    const t = h.clock.now;
    h.setClock(t - 300_000);
    await h.ingest.instanceRegistry.upsert("web-1");
    const issued = await h.ingest.instanceRegistry.issue("web-1");
    // Revoke well before receipt so the object is provably written with a dead
    // token, then restore the compaction clock.
    h.setClock(t - 200_000);
    await h.ingest.instanceRegistry.revoke("web-1", issued.id);
    h.setClock(t);

    const bad = await seedJournal(h, {
      instanceId: issued.instance_id,
      instance: "web-1",
      tokenId: issued.id,
      receivedAt: t - 130_000,
    });
    const good = await seedJournal(h, {
      instanceId: issued.instance_id,
      instance: "web-1",
      tokenId: "tok-still-live",
      receivedAt: t - 129_000,
    });

    const report = await makeCompactor(h).tick();
    expect(report.ok).toBe(true);
    expect(report.excluded_revoked).toBe(1);
    expect(report.segments_published).toBe(1);
    // Spec 024 R5: kept in place for the default 7-day audit window.
    expect(report.revoked_deleted).toBe(0);
    expect(await h.bucket.get(bad.key)).not.toBeNull();
    expect(await h.bucket.get(good.key)).not.toBeNull();

    const covered = h.sql
      .exec("SELECT journal_key FROM segment_coverage")
      .toArray()
      .map((r) => String(r.journal_key));
    expect(covered).toEqual([good.key]);
    expect(covered).not.toContain(bad.key);
    expect(await h.ingest.listExcluded(10).then((r) => r.total)).toBe(1);
    // R7 review: retained-but-excluded objects are not "pending" work.
    expect(await h.ingest.journalStats().then((s) => s.pending)).toBe(0);
    expect(
      await h.ingest.pendingJournalInRange(["inst-a"], 0, Number.MAX_SAFE_INTEGER).then((r) => r.pending_journal),
    ).toBe(0);
    expect((await h.ingest.compactionStatus()).counters as Record<string, number>).toMatchObject({ excluded_revoked: 1 });

    // After the audit window the object is deleted and the exclusion row is
    // consumed; the good object is untouched.
    h.setClock(t + 7 * 24 * 60 * 60 * 1000 + 1);
    const swept = await makeCompactor(h).tick();
    expect(swept.revoked_deleted).toBe(1);
    expect(await h.bucket.get(bad.key)).toBeNull();
    expect(await h.ingest.listExcluded(10).then((r) => r.total)).toBe(0);
  });

  test("a duplicate segment publish heals coverage atoms lost after the manifest row (R7)", async () => {
    const h = makeHarness();
    const t = h.clock.now;
    const obj = await seedJournal(h, {
      instanceId: "inst-a",
      instance: "a",
      tokenId: "tok-a",
      receivedAt: t - 130_000,
    });
    const bytes = new TextEncoder().encode("segment-bytes");
    const sha = await sha256HexBytes(bytes);
    const segmentKey = `segments/all/${t - 130_000}-${t - 130_000}-abcdef0123456789.duckdb`;
    await h.bucket.put(segmentKey, bytes, { sha256: sha, customMetadata: { sha256: sha } });
    const input = {
      segment_key: segmentKey,
      instance_id: "all",
      object_key: segmentKey,
      size_bytes: bytes.byteLength,
      sha256: sha,
      first_received_at: t - 130_000,
      last_received_at: t - 130_000,
      request_count: 1,
      record_count: 1,
      coverage: [{ journal_key: obj.key, signal: "traces", received_at: t - 130_000, instance_id: "inst-a" }],
    };

    const first = await h.ingest.publishSegment(input);
    expect(first.committed).toBe(true);
    // Simulate a crash between the manifest write and the coverage atoms.
    h.sql.exec(`DELETE FROM segment_coverage WHERE journal_key = '${obj.key}'`);
    expect(h.sql.exec("SELECT COUNT(*) AS n FROM segment_coverage").toArray()[0]!.n).toBe(0);

    const again = await h.ingest.publishSegment(input);
    expect(again.duplicate).toBe(true);
    expect(again.covered_new).toBe(1);
    expect(await h.ingest.coverageLookup([obj.key])).toEqual({ covered: [obj.key], excluded: [] });
  });

  test("a revocation-lookup failure fails the tick instead of compacting revoked data (R7)", async () => {
    const h = makeHarness();
    const t = h.clock.now;
    h.setClock(t - 300_000);
    await h.ingest.instanceRegistry.upsert("web-1");
    const issued = await h.ingest.instanceRegistry.issue("web-1");
    h.setClock(t - 200_000);
    await h.ingest.instanceRegistry.revoke("web-1", issued.id);
    h.setClock(t);
    const bad = await seedJournal(h, {
      instanceId: issued.instance_id,
      instance: "web-1",
      tokenId: issued.id,
      receivedAt: t - 130_000,
    });

    const failingNamespace = {
      getByName: () => ({
        fetch: async (request: Request): Promise<Response> => {
          if (new URL(request.url).pathname === "/internal/instance/token/revocations") {
            throw new Error("revocation lookup unavailable");
          }
          return h.ingest.handleInternal(request);
        },
      }),
    };
    const compactor = new CompactorCore({
      ingest: failingNamespace,
      bucket: h.bucket,
      container: new FakeCompactorContainer(),
      holder: "spec-024",
      now: () => h.clock.now,
      config: { graceMs: 90_000 },
    });
    const report = await compactor.tick();
    expect(report.ok).toBe(false);
    expect(report.segments_published).toBe(0);
    // Fail closed: the potentially revoked object is never published.
    expect(await h.bucket.get(bad.key)).not.toBeNull();
    expect(await h.ingest.listSegments({ limit: 10, offset: 0 }).then((r) => r.total)).toBe(0);
  });

  test("a batch-capped run never advances the cursor past uncompacted objects", async () => {
    const h = makeHarness();
    const t = h.clock.now;
    for (let i = 0; i < 3; i++) {
      await seedJournal(h, {
        instanceId: "inst-a",
        instance: "a",
        tokenId: "tok-a",
        receivedAt: t - 130_000 + i,
      });
    }
    // Two objects discovered but only one batch allowed, one bucket per ms.
    const options = { batchObjects: 2, maxBatchesPerTick: 1, windowMs: 1 };
    const first = await makeCompactor(h, options).tick();
    expect(first.segments_published).toBe(1);
    expect(first.discovered_objects).toBe(2);
    const second = await makeCompactor(h, options).tick();
    expect(second.segments_published).toBe(1);
    const third = await makeCompactor(h, options).tick();
    expect(third.segments_published).toBe(1);
    expect((await h.ingest.listSegments({ limit: 10, offset: 0 })).total).toBe(3);
    expect((await h.ingest.journalStats()).pending).toBe(0);
  });

  test("pending disclosure comes from the R2 listing, not a ledger", async () => {
    const h = makeHarness();
    const t = h.clock.now;
    await seedJournal(h, { instanceId: "inst-a", instance: "a", tokenId: "tok-a", receivedAt: t - 60_000 });
    const pending = await h.ingest.pendingJournalInRange(["inst-a"], t - 120_000, t - 30_000);
    expect(pending.pending_journal).toBe(1);
    expect(await h.ingest.journalStats().then((s) => s.pending)).toBe(1);
  });
});
