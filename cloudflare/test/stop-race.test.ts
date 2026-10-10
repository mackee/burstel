// Spec 014: the legacy per-shot admin path simply proxies to the addressed
// ShotDO. Unlike the retired per-shot push path it does not discard on a
// pre-commit rejection: whatever the shot boundary returns is propagated so the
// caller can decide. Continuous instance ingest is independent of shots.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { json } from "../src/http";
import { FakeBucket, FakeIngestNamespace, makeIngest } from "./fakes";

const ADMIN = "admin-token";

/** A ShotDO stub that answers with a replaceable canned response. */
class CannedShotNamespace implements ShotNamespaceLike {
  calls = 0;
  constructor(public make: () => Response) {}

  getByName(): DoStubLike {
    return {
      fetch: async () => {
        this.calls += 1;
        return this.make();
      },
    };
  }
}

function setup(make: () => Response) {
  const ingest = makeIngest();
  const shots = new CannedShotNamespace(make);
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    BUCKET: new FakeBucket(),
    PUBLIC_BASE_URL: "https://burstel.example",
    BURSTEL_API_TOKEN: ADMIN,
  };
  return { env, shots };
}

function legacyPush(shotId: string, token: string, signal = "traces"): Request {
  return new Request(`https://burstel.example/shots/${shotId}/v1/${signal}`, {
    method: "POST",
    headers: { "content-type": "application/x-protobuf", authorization: `Bearer ${token}` },
    body: new Uint8Array([0x0a, 0x02, 0x01, 0x02]),
  });
}

describe("legacy per-shot path propagation", () => {
  test("a recorder 200 is returned with the addressed shot id", async () => {
    const { env, shots } = setup(() =>
      new Response(new Uint8Array([0x0a, 0x00]), {
        status: 200,
        headers: { "content-type": "application/x-protobuf" },
      }),
    );
    const resp = await handleRequest(legacyPush("shotAaaaaaaaaaaaaaa", ADMIN), env);
    expect(resp.status).toBe(200);
    expect(resp.headers.get("x-burstel-shot-id")).toBe("shotAaaaaaaaaaaaaaa");
    expect(shots.calls).toBe(1);
  });

  test("a pre-commit 409 is propagated, not silently discarded", async () => {
    const { env } = setup(() =>
      json(409, { error: "shot is stopping", code: "shot_stopping", retryable: false }),
    );
    const resp = await handleRequest(legacyPush("shotBbbbbbbbbbbbbbb", ADMIN), env);
    expect(resp.status).toBe(409);
    expect(((await resp.json()) as { code: string }).code).toBe("shot_stopping");
  });

  test("a genuine downstream 503 is propagated so the caller retries", async () => {
    const { env } = setup(() =>
      json(503, { error: "storage is unavailable", code: "unavailable", retryable: true }),
    );
    const resp = await handleRequest(legacyPush("shotCcccccccccccccc", ADMIN), env);
    expect(resp.status).toBe(503);
    expect(((await resp.json()) as { retryable: boolean }).retryable).toBe(true);
  });

  test("the legacy path is admin-only", async () => {
    const { env, shots } = setup(() => json(200, { ok: true }));
    const resp = await handleRequest(legacyPush("shotDdddddddddddddd", "not-admin"), env);
    expect(resp.status).toBe(401);
    expect(shots.calls).toBe(0);
  });
});
