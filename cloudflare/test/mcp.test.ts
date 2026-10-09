import { describe, expect, test } from "bun:test";

import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { handleMcp } from "../src/mcp";
import { FakeArtifacts, FakeIngestNamespace, makeCore, makeIngest } from "./fakes";
import type { TestHarness } from "./fakes";

const TOKEN = "mcp-api-token";

class FakeNamespace implements ShotNamespaceLike {
  readonly shots = new Map<string, TestHarness>();
  /** Shared R2-like bucket across all ShotDOs. */
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

function makeEnv(): AppEnv {
  return {
    SHOT_DO: new FakeNamespace(),
    INGEST_DO: new FakeIngestNamespace(makeIngest().core),
    BURSTEL_API_TOKEN: TOKEN,
    PUBLIC_BASE_URL: "https://burstel.example",
  };
}

async function mcp(env: AppEnv, body: unknown, token: string | null = TOKEN): Promise<Response> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
  };
  if (token) headers["authorization"] = `Bearer ${token}`;
  return handleMcp(
    new Request("https://burstel.example/mcp", { method: "POST", headers, body: JSON.stringify(body) }),
    env,
  );
}

interface RpcResponse {
  jsonrpc: string;
  id: number;
  result?: Record<string, unknown>;
  error?: { code: number; message: string };
}

async function rpc(env: AppEnv, body: unknown, token: string | null = TOKEN): Promise<RpcResponse> {
  const resp = await mcp(env, body, token);
  expect(resp.status).toBe(200);
  return (await resp.json()) as RpcResponse;
}

async function callTool(env: AppEnv, name: string, args: Record<string, unknown>): Promise<Record<string, unknown>> {
  const body = await rpc(env, {
    jsonrpc: "2.0",
    id: 99,
    method: "tools/call",
    params: { name, arguments: args },
  });
  expect(body.error).toBeUndefined();
  const result = body.result as { content: Array<{ type: string; text: string }>; isError?: boolean };
  expect(result.content[0]!.type).toBe("text");
  if (result.isError) throw new Error(result.content[0]!.text);
  const parsed = JSON.parse(result.content[0]!.text) as Record<string, unknown>;
  return parsed;
}

describe("MCP server", () => {
  test("initialize returns a valid MCP envelope", async () => {
    const env = makeEnv();
    const body = await rpc(env, {
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "test", version: "1" } },
    });
    expect(body.result?.protocolVersion).toBe("2025-06-18");
    const serverInfo = body.result?.serverInfo as { name: string; version: string };
    expect(serverInfo.name).toBe("burstel");
    expect(body.result?.capabilities).toBeDefined();
  });

  test("tools/list advertises every shot tool with a JSON schema", async () => {
    const env = makeEnv();
    const body = await rpc(env, { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} });
    const tools = body.result?.tools as Array<{ name: string; description: string; inputSchema: Record<string, unknown> }>;
    const names = tools.map((t) => t.name);
    for (const expected of ["shot_start", "shot_stop", "shot_query", "shot_analyze", "shot_compare", "shot_get", "shot_schema"]) {
      expect(names).toContain(expected);
    }
    const query = tools.find((t) => t.name === "shot_query")!;
    expect(query.inputSchema["type"]).toBe("object");
    expect(query.description.length).toBeGreaterThan(0);
  });

  test("full start/stop/query/analyze/schema/compare flow through tools/call", async () => {
    const env = makeEnv();

    const started = await callTool(env, "shot_start", { metadata: { commit: "abc123", scenario: "bench" } });
    const shotId = String(started["shot_id"]);
    expect(shotId).toMatch(/^[A-Za-z0-9_-]{16,64}$/);
    expect((started["otlp_endpoints"] as Record<string, string>)["traces"]).toContain("/ingest/v1/traces");

    const stopped = await callTool(env, "shot_stop", { shot_id: shotId });
    expect(stopped["state"]).toBe("frozen");
    expect(typeof stopped["sha256"]).toBe("string");

    const got = await callTool(env, "shot_get", { shot_id: shotId });
    expect(got["shot_id"]).toBe(shotId);
    expect(got["state"]).toBe("frozen");

    const queried = await callTool(env, "shot_query", { shot_id: shotId, sql: "SELECT 1", max_rows: 10 });
    expect(queried["row_count"]).toBe(1);

    const analyzed = await callTool(env, "shot_analyze", { shot_id: shotId, sections: ["latency"], top_n: 5 });
    expect(analyzed["schema_version"]).toBe("1");

    const schema = await callTool(env, "shot_schema", { shot_id: shotId });
    expect(Array.isArray(schema["tables"])).toBe(true);

    // A second shot to compare against.
    const other = await callTool(env, "shot_start", { metadata: { scenario: "bench" } });
    const otherId = String(other["shot_id"]);
    await callTool(env, "shot_stop", { shot_id: otherId });

    const comparison = await callTool(env, "shot_compare", {
      left_shot_id: shotId,
      right_shot_id: otherId,
      top_n: 5,
    });
    expect(comparison["sample_sizes"]).toBeDefined();
    expect(Array.isArray(comparison["disclosure"])).toBe(true);
  });

  test("tools/call surfaces a tool error for an unknown shot", async () => {
    const env = makeEnv();
    const body = await rpc(env, {
      jsonrpc: "2.0",
      id: 5,
      method: "tools/call",
      params: { name: "shot_get", arguments: { shot_id: "AAAAAAAAAAAAAAAA" } },
    });
    const result = body.result as { isError?: boolean; content: Array<{ text: string }> };
    expect(result.isError).toBe(true);
  });

  test("MCP requires the same bearer token as the REST API", async () => {
    const env = makeEnv();
    const resp = await mcp(env, { jsonrpc: "2.0", id: 1, method: "tools/list" }, null);
    expect(resp.status).toBe(401);
    const wrong = await mcp(env, { jsonrpc: "2.0", id: 1, method: "tools/list" }, "wrong");
    expect(wrong.status).toBe(401);
  });
});
