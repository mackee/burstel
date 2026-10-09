import { describe, expect, test } from "bun:test";

import { ARTIFACT_NAME, METADATA_NAME } from "../src/core";
import { FakeArtifacts, FakeContainer, makeCore } from "./fakes";

function otlpRequest(bytes = new Uint8Array([0x0a, 0x02, 0x01, 0x02])): Request {
  return new Request("https://worker.test/v1/traces", {
    method: "POST",
    headers: { "content-type": "application/x-protobuf" },
    body: bytes,
  });
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("ShotCore lifecycle", () => {
  test("create boots the container and enters recording", async () => {
    const h = makeCore("shot-a");
    const info = await h.core.create({ scenario: "bench" });

    expect(info.state).toBe("recording");
    expect(info.metadata).toEqual({ scenario: "bench" });
    expect(info.otlp_path).toBe("/shots/shot-a");
    expect(h.container.started).toBe(true);
    expect(h.container.startEnv?.["BURSTEL_INTERNAL_TOKEN"]).toBe("internal-secret");
    expect(h.container.startEnv?.["BURSTEL_ADDR"]).toBe(":8080");
  });

  test("create is idempotent but rejects metadata drift", async () => {
    const h = makeCore("shot-idem");
    await h.core.create({ a: 1 });
    const again = await h.core.create({ a: 1 });
    expect(again.state).toBe("recording");

    await expect(h.core.create({ a: 2 })).rejects.toThrow(/already exists/);
  });

  test("records accepted batches as counters", async () => {
    const h = makeCore("shot-count");
    await h.core.create({});
    const resp = await h.core.ingest("traces", otlpRequest());
    expect(resp.status).toBe(200);
    expect(resp.headers.get("content-type")).toBe("application/x-protobuf");

    const info = await h.core.status();
    expect(info.counters.batches.traces).toBe(1);
    expect(h.container.ingestBodies).toHaveLength(1);
    expect(h.container.ingestBodies[0]!.signal).toBe("traces");
  });

  test("container readiness failure marks the shot failed and can be retried", async () => {
    const container = new FakeContainer({ readyError: new Error("container did not start") });
    const h = makeCore("shot-startfail", { container });

    await expect(h.core.create({})).rejects.toThrow(/container start failed/);
    expect((await h.core.status()).state).toBe("failed");

    container.readyError = undefined;
    const retried = await h.core.create({});
    expect(retried.state).toBe("recording");
  });

  test("double stop is idempotent and reads the artifact once", async () => {
    const h = makeCore("shot-double");
    await h.core.create({});
    await h.core.ingest("traces", otlpRequest());

    const first = await h.core.stop();
    const second = await h.core.stop();

    expect(first.state).toBe("frozen");
    expect(second.state).toBe("frozen");
    expect(second.sha256).toBe(first.sha256);
    expect(second.size_bytes).toBe(first.size_bytes);
    expect(h.container.destroyed).toBe(true);
    expect(h.container.artifactReads).toBe(1);
  });

  test("frozen artifact bytes match the reported content length", async () => {
    const artifactBytes = new Uint8Array(4096);
    for (let i = 0; i < artifactBytes.length; i++) artifactBytes[i] = i % 251;
    const container = new FakeContainer({ artifactBytes });
    const h = makeCore("shot-len", { container });
    await h.core.create({});

    const res = await h.core.stop();
    const key = `shots/shot-len/${ARTIFACT_NAME}`;
    const stored = h.artifacts.stored.get(key);

    expect(stored).toBeDefined();
    expect(stored!.byteLength).toBe(artifactBytes.byteLength);
    expect(res.size_bytes).toBe(artifactBytes.byteLength);
    expect(res.freeze?.artifact_size).toBe(artifactBytes.byteLength);
    expect(res.freeze?.artifact_key).toBe(key);
  });

  test("a failed R2 write never produces a frozen shot and can be retried", async () => {
    const artifacts = new FakeArtifacts();
    const h = makeCore("shot-r2fail", { artifacts });
    await h.core.create({});

    artifacts.failPut = new Error("R2 temporarily unavailable");
    await expect(h.core.stop()).rejects.toThrow(/freeze failed/);

    expect((await h.core.status()).state).toBe("failed");
    expect(artifacts.stored.size).toBe(0);
    // The container must survive a failed freeze: its disk has the only copy.
    expect(h.container.destroyed).toBe(false);

    artifacts.failPut = undefined;
    const res = await h.core.stop();
    expect(res.state).toBe("frozen");
    expect(artifacts.stored.size).toBe(1);
    expect(artifacts.json.has(`shots/shot-r2fail/${METADATA_NAME}`)).toBe(true);
  });

  test("stop drains in-flight ingestion and refuses new traffic", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const container = new FakeContainer({ ingestGate: () => gate });
    const h = makeCore("shot-race", { container, drainTimeoutMs: 5000 });
    await h.core.create({});

    const writers = Array.from({ length: 8 }, () => h.core.ingest("traces", otlpRequest()));
    await sleep(20); // let every writer reach the gate

    const stopPromise = h.core.stop();

    // New ingestion after stop has been requested must be refused, not lost.
    await expect(h.core.ingest("logs", otlpRequest())).rejects.toThrow(/stopping/);

    release();
    const settled = await Promise.all(writers);
    expect(settled.every((r) => r.status === 200)).toBe(true);

    const result = await stopPromise;
    expect(result.state).toBe("frozen");
    expect(container.ingestBodies).toHaveLength(8);
    expect(result.counters.batches.traces).toBe(8);
  });

  test("persisted lifecycle metadata survives a DO restart", async () => {
    const h = makeCore("shot-rehydrate");
    await h.core.create({ env: "ci", run: 42 });
    await h.core.ingest("traces", otlpRequest());
    const stopped = await h.core.stop();

    // Simulate DO eviction: a brand new core over the same SQLite storage.
    const restarted = makeCore("shot-rehydrate", { sql: h.sql });
    await restarted.core.load();
    const info = await restarted.core.status();

    expect(info.state).toBe("frozen");
    expect(info.metadata).toEqual({ env: "ci", run: 42 });
    expect(info.sha256).toBe(stopped.sha256);
    expect(info.size_bytes).toBe(stopped.size_bytes);
    expect(info.freeze?.artifact_key).toBe(`shots/shot-rehydrate/${ARTIFACT_NAME}`);
  });

  test("two shots use independent storage and artifact keys", async () => {
    const a = makeCore("shot-A");
    const b = makeCore("shot-B");
    await a.core.create({});
    await b.core.create({});
    await a.core.ingest("traces", otlpRequest());
    await b.core.ingest("logs", otlpRequest());

    const ra = await a.core.stop();
    const rb = await b.core.stop();

    expect(ra.freeze?.artifact_key).toBe(`shots/shot-A/${ARTIFACT_NAME}`);
    expect(rb.freeze?.artifact_key).toBe(`shots/shot-B/${ARTIFACT_NAME}`);
    expect(a.artifacts).not.toBe(b.artifacts);
    expect(a.core.shotId).not.toBe(b.core.shotId);
  });

  test("stop before recording is a conflict", async () => {
    const h = makeCore("shot-created");
    await expect(h.core.stop()).rejects.toThrow(/unknown shot/);
  });
});
