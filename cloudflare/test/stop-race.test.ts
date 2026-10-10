// Spec 013, Part A: the shot-stop race at the stable `/ingest` boundary.
//
// A request that is rejected *before* it reaches the recorder (shot stopping /
// not recording) is provably uncommitted and must be acknowledged with a valid
// OTLP ExportResponse and discarded. A genuine downstream 503 (storage failure,
// down container, write timeout) is ambiguous and must be propagated so the
// exporter retries. The target shot is chosen by the per-shot token, so a
// discard is never re-attributed to another shot.

import { describe, expect, test } from "bun:test";

import { handleRequest } from "../src/app";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "../src/env";
import { json } from "../src/http";
import { FakeIngestNamespace, makeIngest } from "./fakes";

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

let tokenCounter = 0;
function nextToken(): string {
  tokenCounter += 1;
  return `blt_race_${tokenCounter}`;
}

function push(token: string): Request {
  return new Request("https://burstel.example/ingest/v1/traces", {
    method: "POST",
    headers: { "content-type": "application/x-protobuf", "x-burstel-token": token },
    body: new Uint8Array([0x0a, 0x02, 0x01, 0x02]),
  });
}

async function armed(make: () => Response) {
  const ingest = makeIngest({ now: () => 1_700_000_000_000, generateToken: nextToken });
  const shots = new CannedShotNamespace(make);
  const env: AppEnv = {
    SHOT_DO: shots,
    INGEST_DO: new FakeIngestNamespace(ingest.core),
    PUBLIC_BASE_URL: "https://burstel.example",
    BURSTEL_API_TOKEN: ADMIN,
  };
  const issued = await ingest.core.issueShotToken("shot-1");
  return { env, ingest: ingest.core, shots, token: issued.token };
}

describe("shot-stop race at the stable /ingest boundary", () => {
  test("a shot_stopping rejection is discarded with a valid OTLP response", async () => {
    const { env, shots, token } = await armed(() =>
      json(
        409,
        { error: "shot is stopping", code: "shot_stopping", retryable: false },
        { "x-burstel-disposition": "ignored-no-active-shot" },
      ),
    );

    const resp = await handleRequest(push(token), env);
    expect(resp.status).toBe(200);
    expect(resp.headers.get("x-burstel-disposition")).toBe("ignored-no-active-shot");
    expect(shots.calls).toBe(1);
  });

  test("a genuine downstream 503 is propagated and keeps routing to that shot", async () => {
    const { env, shots, token } = await armed(() =>
      json(503, { error: "storage is unavailable", code: "unavailable", retryable: true }),
    );

    const resp = await handleRequest(push(token), env);
    expect(resp.status).toBe(503);
    expect(resp.headers.get("x-burstel-disposition")).toBeNull();
    expect(((await resp.json()) as { code: string }).code).toBe("unavailable");
    expect(shots.calls).toBe(1);
  });

  test("a bare 409 (definite pre-commit) is discarded, a 500 is not", async () => {
    const bare409 = await armed(() =>
      json(409, { error: "shot is not recording", code: "not_recording", retryable: false }),
    );
    const d = await handleRequest(push(bare409.token), bare409.env);
    expect(d.status).toBe(200);
    expect(d.headers.get("x-burstel-disposition")).toBe("ignored-no-active-shot");

    const serverError = await armed(() =>
      json(500, { error: "internal container token is not configured", code: "internal_token_missing" }),
    );
    const e = await handleRequest(push(serverError.token), serverError.env);
    expect(e.status).toBe(500);
    expect(e.headers.get("x-burstel-disposition")).toBeNull();
  });

  test("a token for another shot routes to its own shot, not the discarded one", async () => {
    const { env, ingest, shots, token } = await armed(() =>
      json(
        409,
        { error: "shot is stopping", code: "shot_stopping", retryable: false },
        { "x-burstel-disposition": "ignored-no-active-shot" },
      ),
    );

    expect((await handleRequest(push(token), env)).status).toBe(200);
    expect(shots.calls).toBe(1);

    // A second per-shot token routes to shot-2, which accepts the batch.
    const second = await ingest.issueShotToken("shot-2");
    shots.make = () =>
      new Response(new Uint8Array([0x0a, 0x00]), {
        status: 200,
        headers: { "content-type": "application/x-protobuf" },
      });
    const routed = await handleRequest(push(second.token), env);
    expect(routed.status).toBe(200);
    expect(routed.headers.get("x-burstel-shot-id")).toBe("shot-2");
    expect(shots.calls).toBe(2);
  });
});
