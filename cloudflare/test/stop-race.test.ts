// Spec 008, finding F-2: the shot-stop race at the stable `/ingest` boundary.
//
// A request that is rejected *before* it reaches the recorder (shot stopping /
// not recording) is provably uncommitted and must be acknowledged with a valid
// OTLP ExportResponse and discarded. A genuine downstream 503 (storage failure,
// down container, write timeout) is ambiguous and must be propagated so the
// exporter retries. These tests pin both directions at the Worker boundary with
// a canned ShotDO, a real IngestDO coordinator and a deterministic token.

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
  // Issue before activating: rotation is refused while a shot records.
  const issued = await ingest.core.issueToken();
  await ingest.core.activate("shot-1");
  return { env, ingest: ingest.core, shots, token: issued.token };
}

describe("shot-stop race at the stable /ingest boundary", () => {
  test("a shot_stopping rejection is discarded with a valid OTLP response", async () => {
    const { env, ingest, shots, token } = await armed(() =>
      json(
        409,
        { error: "shot is stopping", code: "shot_stopping", retryable: false },
        { "x-burstel-disposition": "ignored-no-active-shot" },
      ),
    );

    const resp = await handleRequest(push(token), env);
    expect(resp.status).toBe(200);
    expect(resp.headers.get("x-burstel-disposition")).toBe("ignored-no-active-shot");
    // The forwarder reconciles the stale active mapping so late data is never
    // attributed to the next shot.
    expect((await ingest.active()).shot_id).toBeNull();
  });

  test("a genuine downstream 503 is propagated and keeps the shot active", async () => {
    const { env, ingest, token } = await armed(() =>
      json(503, { error: "storage is unavailable", code: "unavailable", retryable: true }),
    );

    const resp = await handleRequest(push(token), env);
    expect(resp.status).toBe(503);
    expect(resp.headers.get("x-burstel-disposition")).toBeNull();
    // No false success: the coordinator still points at the shot so the
    // exporter's retry is routed correctly.
    expect((await ingest.active()).shot_id).toBe("shot-1");
    expect(((await resp.json()) as { code: string }).code).toBe("unavailable");
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
    expect((await serverError.ingest.active()).shot_id).toBe("shot-1");
  });

  test("after a discard the next shot receives the traffic, not the old one", async () => {
    const { env, ingest, shots, token } = await armed(() =>
      json(
        409,
        { error: "shot is stopping", code: "shot_stopping", retryable: false },
        { "x-burstel-disposition": "ignored-no-active-shot" },
      ),
    );

    expect((await handleRequest(push(token), env)).status).toBe(200);
    expect(shots.calls).toBe(1);

    // A new shot starts recording and is now the stable target; the canned DO
    // returns a real OTLP 200 so the request is committed to shot-2.
    await ingest.activate("shot-2");
    shots.make = () =>
      new Response(new Uint8Array([0x0a, 0x00]), {
        status: 200,
        headers: { "content-type": "application/x-protobuf" },
      });
    const routed = await handleRequest(push(token), env);
    expect(routed.status).toBe(200);
    expect(routed.headers.get("x-burstel-shot-id")).toBe("shot-2");
    expect(shots.calls).toBe(2);
    expect((await ingest.active()).shot_id).toBe("shot-2");
  });
});
