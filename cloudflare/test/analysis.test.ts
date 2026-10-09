import { describe, expect, test } from "bun:test";

import { FakeArtifacts, FakeContainer, makeCore } from "./fakes";

function bytes(seed: number, n = 64): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = (seed + i) % 251;
  return out;
}

describe("frozen shot analysis", () => {
  test("restores the frozen R2 artifact before querying, with the recording container gone", async () => {
    const artifact = bytes(7, 128);
    const container = new FakeContainer({ artifactBytes: artifact });
    const h = makeCore("shot-analysis", { container });

    await h.core.create({ scenario: "bench" });
    const frozen = await h.core.stop();
    expect(frozen.state).toBe("frozen");
    // The original recording container is destroyed at freeze.
    expect(container.destroyed).toBe(true);
    expect(container.running).toBe(false);

    const result = await h.core.query("SELECT name FROM spans");
    expect(result.row_count).toBe(1);
    // A fresh analysis container was started and the exact frozen bytes restored.
    expect(container.running).toBe(true);
    const restored = container.restored.get("shot-analysis");
    expect(restored).toBeDefined();
    expect([...restored!.bytes]).toEqual([...artifact]);
    expect(restored!.sha).toBe(frozen.sha256!);
    expect(container.startEnv?.["BURSTEL_ANALYSIS"]).toBe("1");
  });

  test("restore is skipped when the verified snapshot is already loaded", async () => {
    const container = new FakeContainer();
    const h = makeCore("shot-reuse", { container });
    await h.core.create({});
    await h.core.stop();

    await h.core.query("SELECT 1");
    const first = container.restored.get("shot-reuse");
    expect(first).toBeDefined();
    // Second query reuses the same restored snapshot (same sha).
    await h.core.query("SELECT 1");
    expect(container.restored.get("shot-reuse")?.sha).toBe(first!.sha);
  });

  test("rejects analysis of a shot that is not frozen", async () => {
    const h = makeCore("shot-recording");
    await h.core.create({});
    await expect(h.core.query("SELECT 1")).rejects.toThrow(/not frozen/);
    await expect(h.core.analyze()).rejects.toThrow(/not frozen/);
  });

  test("reports a missing frozen artifact instead of querying stale data", async () => {
    const h = makeCore("shot-missing");
    await h.core.create({});
    const frozen = await h.core.stop();
    const key = frozen.freeze?.artifact_key as string;
    h.artifacts.stored.delete(key);
    await expect(h.core.query("SELECT 1")).rejects.toThrow(/missing/);
  });

  test("detects a tampered snapshot via the declared checksum", async () => {
    const h = makeCore("shot-tamper", { container: new FakeContainer({ artifactBytes: bytes(1) }) });
    await h.core.create({});
    const frozen = await h.core.stop();
    const key = frozen.freeze?.artifact_key as string;
    h.artifacts.stored.set(key, bytes(200));
    await expect(h.core.query("SELECT 1")).rejects.toThrow(/restore failed/);
  });

  test("analyze and schema use the restored snapshot", async () => {
    const h = makeCore("shot-sections");
    await h.core.create({});
    await h.core.stop();

    const analyzed = await h.core.analyze({ sections: ["latency"], topN: 5 });
    expect(analyzed.schema_version).toBe("1");
    expect(Array.isArray(analyzed.sections)).toBe(true);

    const schema = await h.core.schema();
    expect(schema.schema_version).toBe("1");
    expect(Array.isArray(schema.tables)).toBe(true);
  });

  test("compare restores both frozen shots into one analysis container", async () => {
    const artifacts = new FakeArtifacts();
    const left = makeCore("shot-left", {
      artifacts,
      container: new FakeContainer({ artifactBytes: bytes(10, 96) }),
    });
    const right = makeCore("shot-right", {
      artifacts,
      container: new FakeContainer({ artifactBytes: bytes(20, 96) }),
    });
    await left.core.create({});
    await right.core.create({});
    await left.core.stop();
    const rightFrozen = await right.core.stop();

    const comparison = await left.core.compare(
      { shot_id: "shot-right", freeze: rightFrozen.freeze! },
      5,
    );
    expect(comparison.sample_sizes).toBeDefined();
    // Both artifacts were restored into the left analysis container.
    expect(left.container.restored.has("shot-left")).toBe(true);
    expect(left.container.restored.has("other-shot-right")).toBe(true);
    const other = left.container.restored.get("other-shot-right");
    expect(other!.sha).toBe(rightFrozen.sha256!);
  });
});
