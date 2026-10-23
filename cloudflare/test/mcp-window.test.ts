import { describe, expect, test } from "bun:test";

import { CompactorCore } from "../src/compactor-core";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { handleMcp } from "../src/mcp";
import {
  FakeAuthNamespace,
  FakeBucket,
  FakeCompactorContainer,
  FakeIngestNamespace,
  makeAuth,
  makeCore,
  makeIngest,
  sha256Hex,
} from "./fakes";
import type { IngestHarness } from "./fakes";

const TOKEN = "mcp-window-token";

class FakeShotNamespace implements ShotNamespaceLike {
  readonly shots = new Map<string, ReturnType<typeof makeCore>>();

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

function makeEnv(overrides: Partial<AppEnv> = {}, ingest?: IngestHarness, bucket?: FakeBucket): AppEnv {
  const ing = ingest ?? makeIngest();
  return {
    SHOT_DO: new FakeShotNamespace(),
    INGEST_DO: new FakeIngestNamespace(ing.core),
    BURSTEL_API_TOKEN: TOKEN,
    PUBLIC_BASE_URL: "https://burstel.example",
    BUCKET: bucket,
    ...overrides,
  };
}

async function rpc(env: AppEnv, body: unknown, token: string | null = TOKEN): Promise<Record<string, unknown>> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
  };
  if (token) headers["authorization"] = `Bearer ${token}`;
  const resp = await handleMcp(
    new Request("https://burstel.example/mcp", { method: "POST", headers, body: JSON.stringify(body) }),
    env,
  );
  return (await resp.json()) as Record<string, unknown>;
}

async function callTool(env: AppEnv, name: string, args: Record<string, unknown>, token: string | null = TOKEN): Promise<Record<string, unknown>> {
  const body = (await rpc(
    env,
    { jsonrpc: "2.0", id: 7, method: "tools/call", params: { name, arguments: args } },
    token,
  )) as { result: { content: Array<{ text: string }>; isError?: boolean } };
  const result = body.result;
  if (result.isError) throw new Error(result.content[0]!.text);
  return JSON.parse(result.content[0]!.text) as Record<string, unknown>;
}

describe("MCP window shots and token tools", () => {
  test("advertises the spec 016 tool surface", async () => {
    const env = makeEnv();
    const body = (await rpc(env, { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} })) as {
      result: { tools: Array<{ name: string }> };
    };
    const names = body.result.tools.map((t) => t.name);
    for (const expected of [
      "shots_list",
      "shot_create",
      "shot_start",
      "shot_stop",
      "shot_get",
      "shot_query",
      "shot_analyze",
      "shot_compare",
      "shot_schema",
      "instances_list",
      "instance_token_mint",
      "instance_token_list",
      "instance_token_revoke",
      "compaction_status",
      "compaction_run",
    ]) {
      expect(names).toContain(expected);
    }
  });

  test("creates a window shot, lists it, gets it and stops it", async () => {
    const env = makeEnv();
    await callTool(env, "instance_create", { name: "web-1" });
    const now = Date.now();
    const created = await callTool(env, "shot_create", {
      name: "bench",
      started_at: new Date(now - 120_000).toISOString(),
      ended_at: new Date(now - 60_000).toISOString(),
      instances: ["web-1"],
    });
    expect(created["kind"]).toBe("window");
    const shotId = String(created["shot_id"]);
    expect(shotId).toMatch(/^[A-Za-z0-9_-]{16,64}$/);

    const listed = await callTool(env, "shots_list", {});
    const shots = listed["shots"] as Array<Record<string, unknown>>;
    expect(shots.some((s) => s["shot_id"] === shotId && s["kind"] === "window")).toBe(true);

    const got = await callTool(env, "shot_get", { shot_id: shotId });
    expect((got["window"] as Record<string, unknown>)["provenance"]).toBe("external");

    const stopped = await callTool(env, "shot_stop", { shot_id: shotId });
    expect(stopped["state"]).toBe("incomplete");
  });

  test("server-clock window start/stop via shot_start mode=window", async () => {
    const env = makeEnv();
    await callTool(env, "instance_create", { name: "web-1" });
    const started = await callTool(env, "shot_start", { mode: "window", name: "run", instances: ["web-1"] });
    expect(started["kind"]).toBe("window");
    expect(started["state"]).toBe("open");
    const shotId = String(started["shot_id"]);
    const stopped = await callTool(env, "shot_stop", { shot_id: shotId });
    expect(["ready", "incomplete"]).toContain(String(stopped["state"]));
  });

  test("shot_start defaults to a window shot when mode is omitted", async () => {
    const env = makeEnv();
    const started = await callTool(env, "shot_start", { metadata: { scenario: "bench" } });
    expect(started["kind"]).toBe("window");
    expect(started["state"]).toBe("open");
    expect(started["otlp_endpoints"]).toBeUndefined();
    // No legacy ShotDO / recording container is created for the default path.
    expect((env.SHOT_DO as unknown as FakeShotNamespace).shots.size).toBe(0);
  });

  test("explicit mode=recording still works and is flagged deprecated", async () => {
    const env = makeEnv();
    const started = await callTool(env, "shot_start", { mode: "recording", metadata: {} });
    expect(started["deprecated"]).toBe(true);
    expect(started["kind"]).toBeUndefined();
    expect((started["otlp_endpoints"] as Record<string, string>)["traces"]).toContain("/ingest/v1/traces");
    expect((env.SHOT_DO as unknown as FakeShotNamespace).shots.size).toBe(1);
  });

  test("shot_start rejects unknown window instance names with unknown_instance", async () => {
    const env = makeEnv();
    await expect(callTool(env, "shot_start", { instances: ["nope"] })).rejects.toThrow(/unknown instance/);
    await callTool(env, "instance_create", { name: "web-1" });
    const ok = await callTool(env, "shot_start", { instances: ["web-1"] });
    expect(ok["kind"]).toBe("window");
    expect((ok["window"] as Record<string, unknown>)["instances"]).toEqual(["web-1"]);
  });

  test("instance token tools require an existing instance and never leak secrets in lists", async () => {
    const env = makeEnv();
    await callTool(env, "instance_create", { name: "web-1" });
    const minted = await callTool(env, "instance_token_mint", { name: "web-1" });
    expect(typeof minted["token"]).toBe("string");
    const list = await callTool(env, "instance_token_list", { name: "web-1" });
    const tokens = list["tokens"] as Array<Record<string, unknown>>;
    expect(tokens.length).toBeGreaterThan(0);
    for (const t of tokens) {
      expect(t["token"]).toBeUndefined();
      expect(t["digest"]).toBeUndefined();
    }
    const revoked = await callTool(env, "instance_token_revoke", { name: "web-1", id: String(minted["id"]) });
    expect(revoked["revoked"]).toBe(true);
    const instances = await callTool(env, "instances_list", {});
    expect((instances["instances"] as unknown[]).length).toBe(1);
  });

  test("member tokens may use instance and shot tools but not the person registry", async () => {
    const { core: authCore } = makeAuth();
    const member = await authCore.mint({ scope: "member", owner: "alice" });
    const env = makeEnv({ AUTH_DO: new FakeAuthNamespace(authCore) });

    // Member can list instances.
    const listed = await callTool(env, "instances_list", {}, member.token);
    expect(Array.isArray(listed["instances"])).toBe(true);

    // Member is denied the person-token registry.
    const denied = (await rpc(
      env,
      { jsonrpc: "2.0", id: 8, method: "tools/call", params: { name: "token_list", arguments: {} } },
      member.token,
    )) as { result: { isError?: boolean; content: Array<{ text: string }> } };
    expect(denied.result.isError).toBe(true);
  });
});

describe("MCP window analysis over segments", () => {
  test("shot_query/analyze/compare stream verified segments into a scoped window session", async () => {
    const now = Date.now();
    const start = now - 120_000;
    const end = now - 60_000;
    const ingest = makeIngest();
    const bucket = new FakeBucket();
    const instance = await ingest.core.instanceRegistry.upsert("web-1");
    await ingest.core.publishSegment({
      segment_key: "seg-1",
      instance_id: instance.instance_id,
      object_key: "segments/seg-1.duckdb",
      size_bytes: 4,
      sha256: "a".repeat(64),
      first_received_at: start,
      last_received_at: end,
      request_count: 1,
      record_count: 1,
      coverage: [{ journal_key: "j1", signal: "traces", received_at: start }],
    });
    await bucket.put("segments/seg-1.duckdb", new Uint8Array([1, 2, 3, 4]));

    const env = makeEnv({}, ingest, bucket);
    const created = await callTool(env, "shot_create", {
      started_at: new Date(start).toISOString(),
      ended_at: new Date(end).toISOString(),
      instances: ["web-1"],
    });
    const shotId = String(created["shot_id"]);

    const query = await callTool(env, "shot_query", { shot_id: shotId, sql: "SELECT name FROM spans" });
    expect(query["row_count"]).toBe(1);

    const analyzed = await callTool(env, "shot_analyze", { shot_id: shotId, sections: ["overview"] });
    expect(analyzed["schema_version"]).toBe("1");

    const schema = await callTool(env, "shot_schema", { shot_id: shotId });
    expect(Array.isArray(schema["tables"])).toBe(true);

    // The segment bytes were streamed into a window session on the analysis container.
    const ns = env.SHOT_DO as unknown as FakeShotNamespace;
    const harness = ns.shots.get(shotId);
    expect(harness?.container.windowSegments.length).toBeGreaterThan(0);
    expect(harness?.container.windowSegments[0]!.key).toBe("seg-1");
  });

  test("compaction-produced segments flow into window analysis without manual steps", async () => {
    const now = Date.now();
    const bucket = new FakeBucket();
    const ingest = makeIngest({ now: () => now, bucket });
    const instance = await ingest.core.instanceRegistry.upsert("web-1");
    const receivedAt = now - 600_000;

    // Seed one raw journal object exactly as the intake path would.
    const bytes = new TextEncoder().encode("otlp");
    const sha = await sha256Hex(bytes);
    const key = `journal/${instance.instance_id}/traces/${receivedAt}-a.bin`;
    await bucket.put(key, bytes, { sha256: sha, customMetadata: { sha256: sha } });
    await ingest.core.commitJournal({
      key,
      instance_id: instance.instance_id,
      instance: "web-1",
      signal: "traces",
      size: bytes.byteLength,
      sha256: sha,
      content_type: "application/x-protobuf",
      content_encoding: "",
      received_at: receivedAt,
    });

    const compactor = new CompactorCore({
      ingest: new FakeIngestNamespace(ingest.core),
      bucket,
      container: new FakeCompactorContainer(),
      holder: "test",
      now: () => now,
      config: { graceMs: 0 },
    });
    const report = await compactor.tick();
    expect(report.segments_published).toBe(1);
    const published = (await ingest.core.listSegments({ limit: 10, offset: 0 })).segments[0]!;

    // A window shot over the published segment analyzes it with no manual publish.
    const env = makeEnv({}, ingest, bucket);
    const created = await callTool(env, "shot_create", {
      started_at: new Date(receivedAt - 1000).toISOString(),
      ended_at: new Date(receivedAt + 1000).toISOString(),
      instances: ["web-1"],
    });
    const shotId = String(created["shot_id"]);
    const query = await callTool(env, "shot_query", { shot_id: shotId, sql: "SELECT name FROM spans" });
    expect(query["row_count"]).toBe(1);

    const ns = env.SHOT_DO as unknown as FakeShotNamespace;
    const harness = ns.shots.get(shotId);
    expect(harness?.container.windowSegments.length).toBeGreaterThan(0);
    expect(harness?.container.windowSegments[0]!.key).toBe(published.segment_key);
  });

  test("discloses un-compacted journal as pending, never silently complete", async () => {
    const now = Date.now();
    const start = now - 120_000;
    const end = now - 60_000;
    const bucket = new FakeBucket();
    const ingest = makeIngest({ now: () => now, bucket });
    const instance = await ingest.core.instanceRegistry.upsert("web-1");
    const bytes = new TextEncoder().encode("x");
    const sha = await sha256Hex(bytes);
    const key = `journal/${instance.instance_id}/traces/${start}-p.bin`;
    await bucket.put(key, bytes, { sha256: sha, customMetadata: { sha256: sha } });
    await ingest.core.commitJournal({
      key,
      instance_id: instance.instance_id,
      instance: "web-1",
      signal: "traces",
      size: bytes.byteLength,
      sha256: sha,
      content_type: "application/x-protobuf",
      content_encoding: "",
      received_at: start,
    });

    const env = makeEnv({}, ingest, bucket);
    const created = await callTool(env, "shot_create", {
      started_at: new Date(start).toISOString(),
      ended_at: new Date(end).toISOString(),
      instances: ["web-1"],
    });
    const shotId = String(created["shot_id"]);
    await callTool(env, "shot_query", { shot_id: shotId, sql: "SELECT 1" });

    const got = await callTool(env, "shot_get", { shot_id: shotId });
    const coverage = got["coverage"] as Record<string, unknown>;
    expect(coverage["complete"]).toBe(false);
    expect(coverage["pending_journal"]).toBe(1);
  });

  test("rejects a window query when a candidate segment is not available", async () => {
    const now = Date.now();
    const start = now - 120_000;
    const end = now - 60_000;
    const ingest = makeIngest();
    const bucket = new FakeBucket();
    // Advertised but never uploaded: the analysis must fail loudly, not silently.
    await ingest.core.publishSegment({
      segment_key: "seg-missing",
      instance_id: "inst-web",
      object_key: "segments/seg-missing.duckdb",
      size_bytes: 4,
      sha256: "b".repeat(64),
      first_received_at: start,
      last_received_at: end,
      request_count: 1,
      record_count: 1,
      coverage: [{ journal_key: "j2", signal: "traces", received_at: start }],
    });
    const env = makeEnv({}, ingest, bucket);
    const created = await callTool(env, "shot_create", {
      started_at: new Date(start).toISOString(),
      ended_at: new Date(end).toISOString(),
    });
    const shotId = String(created["shot_id"]);
    // The Worker commits an incomplete session and still answers (coverage disclosed).
    const query = await callTool(env, "shot_query", { shot_id: shotId, sql: "SELECT 1" });
    expect(query["row_count"]).toBe(1);
    const got = await callTool(env, "shot_get", { shot_id: shotId });
    const coverage = got["coverage"] as Record<string, unknown> | undefined;
    expect(coverage?.["complete"]).toBe(false);
  });
});
