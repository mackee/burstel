// Spec 034: materializer container request timeout scales with part size.
import { describe, expect, test } from "bun:test";

import type { ContainerControl } from "../src/container";
import {
  MATERIALIZE_PART_MAX_TIMEOUT_MS,
  MATERIALIZE_PART_TIMEOUT_MS,
  MaterializerCore,
  materializerPartTimeoutMs,
} from "../src/materializer-core";

class FakeContainer implements ContainerControl {
  runningFlag = false;
  started = false;
  readonly requests: Array<{ path: string; signal?: AbortSignal | null }> = [];

  get running(): boolean {
    return this.runningFlag;
  }

  start(): void {
    this.started = true;
    this.runningFlag = true;
  }

  async ensureReady(): Promise<void> {
    this.runningFlag = true;
  }

  async keepAlive(): Promise<void> {
    return;
  }

  async destroy(): Promise<void> {
    this.runningFlag = false;
  }

  async request(path: string, init?: RequestInit): Promise<Response> {
    this.requests.push({ path, signal: init?.signal ?? null });
    return new Response(JSON.stringify({ part_key: "shots/s/part-0.duckdb", size_bytes: 1 }), {
      status: 201,
      headers: { "content-type": "application/json" },
    });
  }
}

function requestBody(bytes: number): Request {
  return new Request("https://do/internal/materialize", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      shot_id: "shot-timeout",
      part_index: 0,
      part_total: 1,
      objects: [{ key: "journal/x", signal: "traces", size: bytes }],
    }),
  });
}

describe("materializer part timeout", () => {
  test("scales with part input bytes and honours the base + ceiling", () => {
    // Defaults: 120 s base, +500 ms per MiB, 10 min ceiling.
    expect(materializerPartTimeoutMs(0)).toBe(MATERIALIZE_PART_TIMEOUT_MS);
    expect(materializerPartTimeoutMs(50 * 1024 * 1024)).toBe(MATERIALIZE_PART_TIMEOUT_MS + 50 * 500);
    expect(materializerPartTimeoutMs(10 * 1024 * 1024 * 1024)).toBe(MATERIALIZE_PART_MAX_TIMEOUT_MS);
    expect(materializerPartTimeoutMs(1024, 5_000, 100, 60_000)).toBe(5_100);
  });

  test("uses MATERIALIZE_PART_TIMEOUT_MS as the base and logs the effective timeout", async () => {
    const container = new FakeContainer();
    const logs: Array<{ message: string; fields?: Record<string, unknown> }> = [];
    const core = new MaterializerCore({
      container,
      holder: "materializer-0",
      config: {
        containerTimeoutMs: 100_000,
        timeoutPerMiBMs: 1_000,
        maxTimeoutMs: 600_000,
        containerKeepAliveMs: 1_000,
      },
      log: (message, fields) => logs.push({ message, fields }),
    });

    const resp = await core.handleInternal(requestBody(32 * 1024 * 1024));
    expect(resp.status).toBe(201);
    expect(container.started).toBe(true);
    expect(container.requests.map((r) => r.path)).toEqual(["/v1/materialize"]);

    const timeoutLog = logs.find((l) => l.message === "materialize.part.timeout");
    expect(timeoutLog).toBeDefined();
    expect(timeoutLog?.fields?.["input_bytes"]).toBe(32 * 1024 * 1024);
    expect(timeoutLog?.fields?.["timeout_ms"]).toBe(132_000);
  });

  test("keeps a bigger part inside a longer deadline than the 60 s compaction timeout", async () => {
    const container = new FakeContainer();
    const logs: Array<{ message: string; fields?: Record<string, unknown> }> = [];
    const core = new MaterializerCore({
      container,
      holder: "materializer-1",
      config: { containerTimeoutMs: 120_000, timeoutPerMiBMs: 500, maxTimeoutMs: 600_000 },
      log: (message, fields) => logs.push({ message, fields }),
    });
    await core.handleInternal(requestBody(1 * 1024 * 1024 * 1024));
    expect(logs.find((l) => l.message === "materialize.part.timeout")?.fields?.["timeout_ms"]).toBe(600_000);
  });
});
