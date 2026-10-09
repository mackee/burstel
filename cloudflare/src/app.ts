// Public HTTP surface of the Worker.
//
// Only this whitelist is reachable from the internet. The Go recorder's
// `/internal/*`, artifact and analysis endpoints are never exposed: the Worker
// routes them privately to the ShotDO, which talks to the container over its
// TCP port.
//
//   GET    /healthz                         (public)
//   POST   /api/ingest-token                (admin) issue/replace push token
//   GET    /api/ingest-token                (admin) token status, never the secret
//   DELETE /api/ingest-token                (admin) revoke push token
//   POST   /api/shots                       (admin)
//   GET    /api/shots/{id}                  (admin)
//   POST   /api/shots/{id}/stop             (admin)
//   POST   /api/shots/{id}/query            (admin)
//   POST   /api/shots/{id}/analyze          (admin)
//   GET    /api/shots/{id}/schema           (admin)
//   POST   /api/shots/{id}/compare          (admin)
//   POST   /ingest/v1/{traces,metrics,logs} (push token only)
//   POST   /shots/{id}/v1/{traces,metrics,logs} (admin, legacy per-shot path)
//   POST   /mcp                             (admin)

import { adminAuthenticate, extractToken } from "./auth";
import { DO_ORIGIN } from "./env";
import type { AppEnv, DoStubLike, IngestNamespaceLike, ShotNamespaceLike } from "./env";
import { badRequest, HttpError, unauthorized } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { INGEST_DO_NAME } from "./ingest-core";
import type { IngestAdmitResult } from "./ingest-core";
import { isShotId } from "./ids";
import { handleMcp } from "./mcp";
import {
  analyzeShot,
  compareShots,
  getShot,
  queryShot,
  schemaShot,
  startShot,
  stopShot,
} from "./tools";
import type { ToolContext } from "./tools";

export type { AppEnv, DoStubLike, IngestNamespaceLike, ShotNamespaceLike } from "./env";

export async function handleRequest(request: Request, env: AppEnv): Promise<Response> {
  try {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "GET" && path === "/healthz") {
      return json(200, { status: "ok", service: "burstel" });
    }

    // Stable OTLP ingest: authenticated by the push-only token, never by the
    // admin token.
    const signal = ingestSignal(path);
    if (signal) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      return await handleIngest(request, env, signal);
    }

    // Everything below is admin-scoped and fails closed when no admin token is
    // configured (unless the explicit local-development bypass is enabled).
    if (!adminAuthenticate(request, env)) {
      throw unauthorized();
    }

    if (path === "/mcp") {
      return await handleMcp(request, env);
    }

    if (path === "/api/ingest-token") {
      return await handleIngestToken(request, env);
    }

    const ctx = toolContext(env, url);

    if (request.method === "POST" && path === "/api/shots") {
      const body = await readJson<{ metadata?: Record<string, unknown> }>(request);
      return json(201, await startShot(ctx, body.metadata ?? {}));
    }

    const statusMatch = /^\/api\/shots\/([^/]+)$/.exec(path);
    if (statusMatch) {
      if (request.method !== "GET") throw badRequest("method not allowed");
      return json(200, await getShot(ctx, requireShotId(statusMatch[1]!)));
    }

    const stopMatch = /^\/api\/shots\/([^/]+)\/stop$/.exec(path);
    if (stopMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      return json(200, await stopShot(ctx, requireShotId(stopMatch[1]!)));
    }

    const queryMatch = /^\/api\/shots\/([^/]+)\/query$/.exec(path);
    if (queryMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      const body = await readJson<{ sql?: string; max_rows?: number }>(request);
      if (typeof body.sql !== "string") throw badRequest("sql is required");
      return json(200, await queryShot(ctx, requireShotId(queryMatch[1]!), body.sql, body.max_rows));
    }

    const analyzeMatch = /^\/api\/shots\/([^/]+)\/analyze$/.exec(path);
    if (analyzeMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      const body = await readJson<{ sections?: string[]; top_n?: number }>(request);
      return json(200, await analyzeShot(ctx, requireShotId(analyzeMatch[1]!), body.sections, body.top_n));
    }

    const schemaMatch = /^\/api\/shots\/([^/]+)\/schema$/.exec(path);
    if (schemaMatch) {
      if (request.method !== "GET") throw badRequest("method not allowed");
      return json(200, await schemaShot(ctx, requireShotId(schemaMatch[1]!)));
    }

    const compareMatch = /^\/api\/shots\/([^/]+)\/compare$/.exec(path);
    if (compareMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      const body = await readJson<{ other?: string; top_n?: number }>(request);
      if (typeof body.other !== "string") throw badRequest("other shot id is required");
      return json(
        200,
        await compareShots(ctx, requireShotId(compareMatch[1]!), requireShotId(body.other), body.top_n),
      );
    }

    // Legacy per-shot path, kept for backwards compatibility. Admin token only.
    const ingestMatch = /^\/shots\/([^/]+)\/v1\/(traces|metrics|logs)$/.exec(path);
    if (ingestMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      return await forwardLegacyIngest(env, requireShotId(ingestMatch[1]!), ingestMatch[2]!, request);
    }

    return json(404, { error: "not found" });
  } catch (error) {
    return errorResponse(error);
  }
}

function ingestSignal(path: string): "traces" | "metrics" | "logs" | null {
  const match = /^\/ingest\/v1\/(traces|metrics|logs)$/.exec(path);
  if (!match) return null;
  return match[1] as "traces" | "metrics" | "logs";
}

function toolContext(env: AppEnv, url: URL): ToolContext {
  return {
    namespace: env.SHOT_DO,
    ingest: env.INGEST_DO,
    origin: url.origin,
    publicBaseUrl: env.PUBLIC_BASE_URL,
  };
}

// ------------------------------------------------------------ push ingest

async function handleIngest(
  request: Request,
  env: AppEnv,
  signal: "traces" | "metrics" | "logs",
): Promise<Response> {
  const ingest = ingestStub(env);
  const token = extractToken(request);
  const admitted = await ingestJson<IngestAdmitResult>(ingest, "/internal/admit", "POST", { token, signal });
  if (!admitted.ok) {
    return json(401, {
      error: `ingest token ${admitted.reason}`,
      code: `ingest_token_${admitted.reason}`,
      reason: admitted.reason,
    });
  }
  const shotId = admitted.active_shot_id;
  if (!shotId) {
    return discardResponse(request, "ignored-no-active-shot");
  }
  const stub = env.SHOT_DO.getByName(shotId);
  const resp = await forwardIngest(stub, signal, request);
  if (resp.status === 409) {
    // The active shot is no longer recording (stop won the race, or the
    // coordinator was stale after a restart). Reconcile and consciously
    // discard rather than attributing the data to a different shot.
    await ingestJson(ingest, "/internal/detach", "POST", { shot_id: shotId, late: true }).catch(() => undefined);
    return discardResponse(request, "ignored-no-active-shot");
  }
  const out = new Response(resp.body, resp);
  out.headers.set("x-burstel-shot-id", shotId);
  return out;
}

/** A valid OTLP/HTTP 200 that deliberately discards data with no active shot. */
export function discardResponse(request: Request, disposition: string): Response {
  const contentType = request.headers.get("content-type") ?? "";
  const headers: Record<string, string> = {
    "x-burstel-disposition": disposition,
    "cache-control": "no-store",
  };
  if (contentType.includes("application/json")) {
    return new Response("{}", { status: 200, headers: { ...headers, "content-type": "application/json" } });
  }
  // An empty proto message is a valid Export{Trace,Metric,Log}ServiceResponse.
  return new Response(new Uint8Array(0), {
    status: 200,
    headers: { ...headers, "content-type": "application/x-protobuf" },
  });
}

async function forwardIngest(stub: DoStubLike, signal: string, request: Request): Promise<Response> {
  const headers = new Headers();
  const contentType = request.headers.get("content-type");
  if (contentType) headers.set("content-type", contentType);
  const contentEncoding = request.headers.get("content-encoding");
  if (contentEncoding) headers.set("content-encoding", contentEncoding);

  const init = {
    method: "POST",
    headers,
    body: request.body ?? undefined,
    duplex: "half",
  } as RequestInit;
  return stub.fetch(new Request(`${DO_ORIGIN}/internal/ingest/${signal}`, init));
}

async function forwardLegacyIngest(
  env: AppEnv,
  shotId: string,
  signal: string,
  request: Request,
): Promise<Response> {
  const resp = await forwardIngest(env.SHOT_DO.getByName(shotId), signal, request);
  const out = new Response(resp.body, resp);
  out.headers.set("x-burstel-shot-id", shotId);
  return out;
}

// --------------------------------------------------------- token lifecycle

async function handleIngestToken(request: Request, env: AppEnv): Promise<Response> {
  const ingest = ingestStub(env);
  if (request.method === "POST") {
    const issued = await ingestJson<Record<string, unknown>>(ingest, "/internal/token/issue", "POST");
    return json(201, issued);
  }
  if (request.method === "GET") {
    const status = await ingestJson<Record<string, unknown>>(ingest, "/internal/token/status", "GET");
    return json(200, status);
  }
  if (request.method === "DELETE") {
    const revoked = await ingestJson<Record<string, unknown>>(ingest, "/internal/token/revoke", "DELETE");
    return json(200, revoked);
  }
  throw badRequest("method not allowed");
}

// ---------------------------------------------------------------- helpers

function ingestStub(env: AppEnv): DoStubLike {
  return env.INGEST_DO.getByName(INGEST_DO_NAME);
}

async function ingestJson<T>(
  stub: DoStubLike,
  path: string,
  method: string,
  body?: unknown,
): Promise<T> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.headers = { "content-type": "application/json" };
    init.body = JSON.stringify(body);
  }
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}${path}`, init));
  if (resp.status >= 200 && resp.status < 300) {
    return (await resp.json()) as T;
  }
  let message = `ingest coordinator request failed with status ${resp.status}`;
  let code = "error";
  try {
    const parsed = (await resp.json()) as { error?: string; code?: string };
    if (parsed.error) message = parsed.error;
    if (parsed.code) code = parsed.code;
  } catch {
    // keep defaults
  }
  throw new HttpError(resp.status, message, false, code);
}

function requireShotId(raw: string): string {
  if (!isShotId(raw)) throw badRequest("invalid shot id");
  return raw;
}
