import { describe, expect, test } from "bun:test";

import { HttpError } from "../src/errors";
import type { DoStubLike, ShotNamespaceLike } from "../src/env";
import { createTools } from "../src/tools";
import { FakeContainer, FakeIngestNamespace, makeCore, makeIngest } from "./fakes";

const WINDOW = {
  start: new Date(1_700_000_000_000).toISOString(),
  end: new Date(1_700_000_060_000).toISOString(),
  provenance: "server",
  clock_trust: "server",
  instances: ["web-1"],
};

const PART = {
  object_key: "shots/AAAAAAAAAAAAAAAA/part-0.duckdb",
  size_bytes: 4096,
  sha256: "c".repeat(64),
  first_received_at: 1_700_000_000_000,
  last_received_at: 1_700_000_060_000,
};

const ANALYSIS_ENV = {
  R2_ACCESS_KEY_ID: "key",
  R2_SECRET_ACCESS_KEY: "secret",
  R2_ACCOUNT_ID: "account",
  R2_BUCKET: "burstel-shots",
};

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

function preloadBody(spans: number) {
  return { window: WINDOW, parts: [PART], spans, parts_total: 1 };
}

describe("spec 036 preload path", () => {
  test("preload uses the window-session path and needs no frozen artifact", async () => {
    const container = new FakeContainer();
    const h = makeCore("shot-win-preload", { container, analysisEnv: ANALYSIS_ENV, analysisEgress: true });

    await h.core.preloadAnalysis(preloadBody(10));

    expect(container.preloads.length).toBe(1);
    expect(container.preloads[0]!["download"]).toBe(true);
    // Spec 036: the manifest totals ride along so the commit self-check can
    // refuse a partially attached session.
    expect(container.preloads[0]!["expected_parts"]).toBe(1);
    expect(container.preloads[0]!["expected_spans"]).toBe(10);
    // No frozen artifact exists for a window shot and none is ever restored.
    expect(h.artifacts.stored.size).toBe(0);
    expect(container.restored.size).toBe(0);
    expect(container.requests.some((request) => request.path === "/internal/restore")).toBe(false);
  });

  test("analyze waits for an in-flight preload instead of failing window_not_committed", async () => {
    const gate = deferred();
    const started = deferred();
    const container = new FakeContainer();
    const original = container.request.bind(container);
    container.request = async (path: string, init?: RequestInit) => {
      if (path === "/internal/window/preload") {
        started.resolve();
        await gate.promise;
      }
      return original(path, init);
    };
    const h = makeCore("shot-race", { container, analysisEnv: ANALYSIS_ENV, analysisEgress: true });

    const preload = h.core.preloadAnalysis(preloadBody(10));
    await started.promise;

    // Release the preload shortly after analyze starts waiting.
    setTimeout(() => gate.resolve(), 25);
    const analyzed = await h.core.windowAnalyze("w-shot-race", "shot-race", {});
    expect(analyzed["state"]).toBeUndefined();
    expect(analyzed["sections"]).toBeDefined();
    // The analyze request ran only after the preload committed the session.
    const paths = container.requests.map((request) => request.path);
    expect(paths.indexOf("/internal/window/preload")).toBeLessThan(paths.indexOf("/internal/window/analyze"));
    await preload;
  });

  test("returns 202 preparing when the preload outlives the bounded wait", async () => {
    const gate = deferred();
    const started = deferred();
    const container = new FakeContainer();
    const original = container.request.bind(container);
    container.request = async (path: string, init?: RequestInit) => {
      if (path === "/internal/window/preload") {
        started.resolve();
        await gate.promise;
      }
      return original(path, init);
    };
    const h = makeCore("shot-slow", {
      container,
      analysisEnv: ANALYSIS_ENV,
      analysisEgress: true,
      preloadWaitMs: 30,
    });

    const preload = h.core.preloadAnalysis(preloadBody(10));
    await started.promise;
    const analyzed = await h.core.windowAnalyze("w-shot-slow", "shot-slow", {});
    expect(analyzed["state"]).toBe("preparing");
    expect(analyzed["retry_after_ms"]).toBe(5000);
    // Never a partial/error query while the session is still downloading.
    expect(container.requests.some((request) => request.path === "/internal/window/analyze")).toBe(false);
    gate.resolve();
    await preload;
  });
});

describe("spec 036 commit invariant in the Worker", () => {
  async function prepareShot(spans: number): Promise<{ ingest: ReturnType<typeof makeIngest>; shotId: string }> {
    const ingest = makeIngest({ now: () => 1_700_000_100_000 });
    const started = await ingest.core.startWindowShot({
      shot_id: "AAAAAAAAAAAAAAAA",
      owner: "tester",
      instances: ["web-1"],
    });
    await ingest.core.stopWindowShot(started.shot_id, 1_700_000_060_000);
    await ingest.core.setMaterialization(started.shot_id, {
      state: "complete",
      parts_done: 1,
      parts_total: 1,
      spans,
      started_at: 1_700_000_000_000,
      finished_at: 1_700_000_070_000,
      parts: [{ ...PART, index: 0, object_count: 1, request_count: 1, record_count: spans, span_count: spans, log_count: 0, metric_count: 0 }],
    });
    return { ingest, shotId: started.shot_id };
  }

  class CommitStub implements DoStubLike {
    readonly calls: Array<{ path: string; body: Record<string, unknown> }> = [];

    constructor(
      private readonly commit: Record<string, unknown>,
      private readonly commitStatus = 200,
    ) {}

    async fetch(request: Request): Promise<Response> {
      const path = new URL(request.url).pathname;
      const raw = request.method === "GET" ? "" : await request.text();
      const body = raw ? (JSON.parse(raw) as Record<string, unknown>) : {};
      this.calls.push({ path, body });
      const jsonResponse = (status: number, value: unknown) =>
        new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
      if (path === "/internal/window/open") {
        const keys = ((body["catalog"] as Array<{ key?: string }>) ?? []).map((entry) => entry.key);
        return jsonResponse(200, {
          session_id: body["session_id"],
          selection: { selected: [], missing: [], expected: keys.length, justification: "test" },
          cached_keys: keys,
          timings: { attach_ms: 1, materialize_ms: 0 },
        });
      }
      if (path === "/internal/window/commit") return jsonResponse(this.commitStatus, this.commit);
      if (path === "/internal/window/analyze") return jsonResponse(200, { shot_id: "AAAAAAAAAAAAAAAA", sections: [] });
      return jsonResponse(200, { released: true });
    }
  }

  test("a raced (uncommitted) session maps to 202 preparing, never a hard 400", async () => {
    const { ingest, shotId } = await prepareShot(10);
    const stub = new CommitStub(
      { error: "window session has not been committed", code: "window_not_committed", retryable: true },
      409,
    );
    const tools = createTools({
      namespace: { getByName: () => stub },
      ingest: new FakeIngestNamespace(ingest.core),
      origin: "https://burstel.example",
      partDownload: true,
      materializer: { getByName: () => stub },
    });

    const result = await tools.analyze(shotId);
    expect(result["state"]).toBe("preparing");
    expect(result["retry_after_ms"]).toBe(5000);
    expect(stub.calls.some((call) => call.path === "/internal/window/analyze")).toBe(false);
    // The raced session is released so the next attempt starts clean.
    expect(stub.calls.some((call) => call.path === "/internal/window/release")).toBe(true);
  });

  test("passes the manifest totals to open and commit and reports the self-check", async () => {
    const { ingest, shotId } = await prepareShot(10);
    const stub = new CommitStub({
      expected_segments: 1,
      available_segments: 1,
      missing_segments: [],
      complete: true,
      verified: true,
      attached_parts: 1,
      attached_spans: 10,
      expected_parts: 1,
      expected_spans: 10,
      view_spans: 10,
      part_rows: [{ key: PART.object_key, spans: 10, log_records: 0, metric_points: 0, batches: 0 }],
    });
    const namespace: ShotNamespaceLike = { getByName: () => stub };
    const tools = createTools({
      namespace,
      ingest: new FakeIngestNamespace(ingest.core),
      origin: "https://burstel.example",
      partDownload: true,
      materializer: { getByName: () => stub },
    });

    const result = await tools.analyze(shotId);
    expect(result["shot_id"]).toBe("AAAAAAAAAAAAAAAA");
    const open = stub.calls.find((call) => call.path === "/internal/window/open");
    expect(open?.body["expected_parts"]).toBe(1);
    expect(open?.body["expected_spans"]).toBe(10);
    const commit = stub.calls.find((call) => call.path === "/internal/window/commit");
    expect(commit?.body["expected_parts"]).toBe(1);
    expect(commit?.body["expected_spans"]).toBe(10);
  });

  test("a partially attached session fails with 409 analysis_incomplete and never returns rows", async () => {
    const { ingest, shotId } = await prepareShot(10);
    const stub = new CommitStub({
      expected_segments: 1,
      available_segments: 1,
      missing_segments: [],
      complete: true,
      verified: true,
      attached_parts: 1,
      attached_spans: 6,
      expected_parts: 1,
      expected_spans: 10,
      view_spans: 6,
    });
    const tools = createTools({
      namespace: { getByName: () => stub },
      ingest: new FakeIngestNamespace(ingest.core),
      origin: "https://burstel.example",
      partDownload: true,
      materializer: { getByName: () => stub },
    });

    const error = await tools.analyze(shotId).catch((err: unknown) => err);
    expect(error).toBeInstanceOf(HttpError);
    expect((error as HttpError).status).toBe(409);
    expect((error as HttpError).code).toBe("analysis_incomplete");
    // No analyze row query ran and the session was released (discarded).
    expect(stub.calls.some((call) => call.path === "/internal/window/analyze")).toBe(false);
    expect(stub.calls.some((call) => call.path === "/internal/window/release")).toBe(true);
    // Coverage is recorded as incomplete, never `complete: true`.
    const coverage = ingest.core.getShotRecord(shotId)?.coverage;
    expect(coverage?.complete).toBe(false);
  });
});
