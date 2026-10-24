// Regression tests for spec 028: the compactor Container must receive the
// distinct internal token the Worker uses for `Authorization: Bearer ...`.
//
// The secure-mode bug shipped unnoticed because unit tests either faked the
// container without inspecting its start env or used insecure dev auth. These
// tests assert the exact env mapping and then drive the real orchestration
// against a fake container that records what it was started with.

import { describe, expect, test } from "bun:test";

import type { CompactorConfig } from "../src/compactor-core";
import { CompactorCore } from "../src/compactor-core";
import { resolveCompactorContainerEnv } from "../src/compactor-env";
import {
  FakeBucket,
  FakeCompactorContainer,
  FakeIngestNamespace,
  makeIngest,
  sha256Hex,
} from "./fakes";

const SECURE_TOKEN = "compactor-internal-token-028";
const ADMIN_TOKEN = "admin-token-028";

describe("compactor container env (spec 028)", () => {
  test("secure mode injects the distinct internal token", () => {
    const { env, internal } = resolveCompactorContainerEnv(
      { BURSTEL_INTERNAL_TOKEN: SECURE_TOKEN, BURSTEL_API_TOKEN: ADMIN_TOKEN },
      8080,
    );
    expect(internal).toEqual({ ok: true, token: SECURE_TOKEN, insecure: false });
    expect(env).toEqual({
      BURSTEL_ADDR: ":8080",
      BURSTEL_COMPACT_WORK_DIR: "/work",
      BURSTEL_INTERNAL_TOKEN: SECURE_TOKEN,
    });
  });

  test("a missing or non-distinct token never reaches the container", () => {
    const missing = resolveCompactorContainerEnv({ BURSTEL_API_TOKEN: ADMIN_TOKEN }, 8080);
    expect(missing.internal.ok).toBe(false);
    expect(missing.env.BURSTEL_INTERNAL_TOKEN).toBeUndefined();
    expect(missing.env.BURSTEL_DEV_INSECURE_AUTH).toBeUndefined();

    const sameAsAdmin = resolveCompactorContainerEnv(
      { BURSTEL_INTERNAL_TOKEN: ADMIN_TOKEN, BURSTEL_API_TOKEN: ADMIN_TOKEN },
      8080,
    );
    expect(sameAsAdmin.internal.ok).toBe(false);
    expect(sameAsAdmin.env.BURSTEL_INTERNAL_TOKEN).toBeUndefined();
  });

  test("insecure dev mode opts in and does not inject a token", () => {
    const { env, internal } = resolveCompactorContainerEnv(
      { BURSTEL_DEV_INSECURE_AUTH: "1", BURSTEL_INTERNAL_TOKEN: SECURE_TOKEN },
      8080,
    );
    expect(internal).toEqual({ ok: true, token: SECURE_TOKEN, insecure: true });
    expect(env.BURSTEL_INTERNAL_TOKEN).toBeUndefined();
    expect(env.BURSTEL_DEV_INSECURE_AUTH).toBe("1");
  });
});

async function seedObject(bucket: FakeBucket, receivedAt: number): Promise<void> {
  const bytes = new TextEncoder().encode(`spec-028-${receivedAt}`);
  const sha = await sha256Hex(bytes);
  await bucket.put(`journal/inst-028/traces/${receivedAt}-028.bin`, bytes, {
    sha256: sha,
    httpMetadata: { contentType: "application/x-protobuf" },
    customMetadata: {
      sha256: sha,
      signal: "traces",
      instance_id: "inst-028",
      instance: "inst-028",
      received_at: String(receivedAt),
      token_id: "tok-028",
      content_encoding: "",
      content_type: "application/x-protobuf",
    },
  });
}

function makeCompactor(
  containerEnv: Record<string, string>,
  internalToken: string | undefined,
): { bucket: FakeBucket; container: FakeCompactorContainer; compactor: CompactorCore; now: number } {
  const bucket = new FakeBucket();
  const now = 1_700_000_000_000;
  const { core: ingest } = makeIngest({ now: () => now, bucket });
  const container = new FakeCompactorContainer();
  const config: Partial<CompactorConfig> = { graceMs: 90_000, windowMs: 60_000 };
  const compactor = new CompactorCore({
    ingest: new FakeIngestNamespace(ingest),
    bucket,
    container,
    containerEnv,
    internalToken,
    internalReady: true,
    holder: "spec-028",
    now: () => now,
    config,
    scheduler: { set: () => undefined },
  });
  return { bucket, container, compactor, now };
}

describe("compactor start env (spec 028)", () => {
  test("a secure-mode tick starts the container with BURSTEL_INTERNAL_TOKEN", async () => {
    const { env } = resolveCompactorContainerEnv(
      { BURSTEL_INTERNAL_TOKEN: SECURE_TOKEN, BURSTEL_API_TOKEN: ADMIN_TOKEN },
      8080,
    );
    const h = makeCompactor(env, SECURE_TOKEN);
    await seedObject(h.bucket, h.now - 600_000);

    const report = await h.compactor.tick();
    expect(report.ok).toBe(true);
    expect(report.segments_published).toBe(1);
    expect(h.container.startEnv).toBeDefined();
    expect(h.container.startEnv!.BURSTEL_INTERNAL_TOKEN).toBe(SECURE_TOKEN);
    expect(h.container.startEnv!.BURSTEL_DEV_INSECURE_AUTH).toBeUndefined();
  });

  test("an insecure-mode tick opts the container in without a token", async () => {
    const { env } = resolveCompactorContainerEnv(
      { BURSTEL_DEV_INSECURE_AUTH: "1", BURSTEL_INTERNAL_TOKEN: SECURE_TOKEN },
      8080,
    );
    const h = makeCompactor(env, undefined);
    await seedObject(h.bucket, h.now - 600_000);

    const report = await h.compactor.tick();
    expect(report.ok).toBe(true);
    expect(report.segments_published).toBe(1);
    expect(h.container.startEnv).toBeDefined();
    expect(h.container.startEnv!.BURSTEL_INTERNAL_TOKEN).toBeUndefined();
    expect(h.container.startEnv!.BURSTEL_DEV_INSECURE_AUTH).toBe("1");
  });
});

describe("materializer container env (spec 034)", () => {
  test("forwards only explicitly configured part limits and pipeline concurrency", () => {
    const { env } = resolveCompactorContainerEnv(
      {
        BURSTEL_INTERNAL_TOKEN: SECURE_TOKEN,
        MATERIALIZE_MAX_PART_BYTES: "1073741824",
        MATERIALIZE_CONCURRENCY: "32",
        MATERIALIZE_DECODE_WORKERS: "4",
        MATERIALIZE_APPEND_SHARDS: "2",
      },
      8080,
    );
    expect(env.MATERIALIZE_MAX_PART_BYTES).toBe("1073741824");
    expect(env.MATERIALIZE_CONCURRENCY).toBe("32");
    expect(env.MATERIALIZE_DECODE_WORKERS).toBe("4");
    expect(env.MATERIALIZE_APPEND_SHARDS).toBe("2");
  });

  test("does not invent unset tuning values", () => {
    const { env } = resolveCompactorContainerEnv({ BURSTEL_INTERNAL_TOKEN: SECURE_TOKEN }, 8080);
    expect(env.MATERIALIZE_MAX_PART_BYTES).toBeUndefined();
    expect(env.MATERIALIZE_CONCURRENCY).toBeUndefined();
  });
});
