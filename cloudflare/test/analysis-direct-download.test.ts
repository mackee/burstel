import { describe, expect, test } from "bun:test";

import type { DoStubLike, ShotNamespaceLike } from "../src/env";
import { createTools } from "../src/tools";
import { FakeIngestNamespace, makeIngest } from "./fakes";

const PART = {
  index: 0,
  object_key: "shots/AAAAAAAAAAAAAAAA/part-0.duckdb",
  size_bytes: 4096,
  sha256: "c".repeat(64),
  object_count: 1,
  request_count: 1,
  record_count: 10,
  span_count: 10,
  log_count: 0,
  metric_count: 0,
  first_received_at: 1_700_000_000_000,
  last_received_at: 1_700_000_060_000,
};

async function prepareShot(): Promise<{ ingest: ReturnType<typeof makeIngest>; shotId: string }> {
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
    spans: 10,
    started_at: 1_700_000_000_000,
    finished_at: 1_700_000_070_000,
    parts: [PART],
  });
  return { ingest, shotId: started.shot_id };
}

class CaptureStub implements DoStubLike {
  readonly calls: Array<{ path: string; body: Record<string, unknown> }> = [];

  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    const raw = request.method === "GET" ? "" : await request.text();
    const body = raw ? (JSON.parse(raw) as Record<string, unknown>) : {};
    this.calls.push({ path, body });
    if (path === "/internal/window/open") {
      const keys = ((body["catalog"] as Array<{ key?: string }>) ?? []).map((entry) => entry.key);
      return new Response(
        JSON.stringify({
          session_id: body["session_id"],
          selection: { selected: [], missing: [], expected: keys.length, justification: "test" },
          cached_keys: keys,
          timings: { attach_ms: 1, materialize_ms: 0 },
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
    if (path === "/internal/window/commit") {
      return new Response(
        JSON.stringify({ expected_segments: 1, available_segments: 1, missing_segments: [], complete: true,
          verified: true, attached_parts: 1, attached_spans: 10, expected_parts: 1, expected_spans: 10, view_spans: 10 }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
    if (path === "/internal/window/analyze") {
      return new Response(JSON.stringify({ shot_id: "AAAAAAAAAAAAAAAA", sections: [] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    return new Response(JSON.stringify({ released: true }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }
}

describe("spec 035 direct part download in window analysis", () => {
  test("passes download=true to the container and never streams part bytes", async () => {
    const { ingest, shotId } = await prepareShot();
    const stub = new CaptureStub();
    const namespace: ShotNamespaceLike = { getByName: () => stub };
    const tools = createTools({
      namespace,
      ingest: new FakeIngestNamespace(ingest.core),
      origin: "https://burstel.example",
      partDownload: true,
      materializer: { getByName: () => stub },
    });

    await tools.analyze(shotId);

    const open = stub.calls.find((call) => call.path === "/internal/window/open");
    expect(open).toBeDefined();
    expect(open!.body["download"]).toBe(true);
    const catalog = open!.body["catalog"] as Array<Record<string, unknown>>;
    expect(catalog.length).toBe(1);
    expect(catalog[0]!["key"]).toBe(PART.object_key);
    expect(stub.calls.some((call) => call.path === "/internal/window/segment")).toBe(false);
  });

  test("the legacy streaming path still requires the bucket binding", async () => {
    const { ingest, shotId } = await prepareShot();
    const stub = new CaptureStub();
    const tools = createTools({
      namespace: { getByName: () => stub },
      ingest: new FakeIngestNamespace(ingest.core),
      origin: "https://burstel.example",
      materializer: { getByName: () => stub },
    });
    await expect(tools.analyze(shotId)).rejects.toThrow(/object bucket binding/);
  });
});
