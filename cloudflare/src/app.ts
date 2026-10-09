// Public HTTP surface of the Worker.
//
// Only this whitelist is reachable from the internet. The Go recorder's
// `/internal/*`, artifact and analysis endpoints are never exposed: the Worker
// routes them privately to the ShotDO, which talks to the container over its
// TCP port.
//
//   GET  /healthz
//   POST /api/shots
//   GET  /api/shots/{id}
//   POST /api/shots/{id}/stop
//   POST /api/shots/{id}/query
//   POST /api/shots/{id}/analyze
//   GET  /api/shots/{id}/schema
//   POST /api/shots/{id}/compare
//   POST /shots/{id}/v1/{traces,metrics,logs}
//   POST /mcp

import { authenticate } from "./auth";
import { DO_ORIGIN } from "./env";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "./env";
import { badRequest, unauthorized } from "./errors";
import { errorResponse, json, readJson } from "./http";
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

export type { AppEnv, DoStubLike, ShotNamespaceLike } from "./env";

export async function handleRequest(request: Request, env: AppEnv): Promise<Response> {
  try {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "GET" && path === "/healthz") {
      return json(200, { status: "ok", service: "burstel" });
    }

    if (!authenticate(request, env.BURSTEL_API_TOKEN)) {
      throw unauthorized();
    }

    if (path === "/mcp") {
      return await handleMcp(request, env);
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

    const ingestMatch = /^\/shots\/([^/]+)\/v1\/(traces|metrics|logs)$/.exec(path);
    if (ingestMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      return await forwardIngest(env, requireShotId(ingestMatch[1]!), ingestMatch[2]!, request);
    }

    return json(404, { error: "not found" });
  } catch (error) {
    return errorResponse(error);
  }
}

function toolContext(env: AppEnv, url: URL): ToolContext {
  return { namespace: env.SHOT_DO, origin: url.origin, publicBaseUrl: env.PUBLIC_BASE_URL };
}

async function forwardIngest(
  env: AppEnv,
  shotId: string,
  signal: string,
  request: Request,
): Promise<Response> {
  const stub = env.SHOT_DO.getByName(shotId);
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
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}/internal/ingest/${signal}`, init));
  const out = new Response(resp.body, resp);
  out.headers.set("x-burstel-shot-id", shotId);
  return out;
}

function requireShotId(raw: string): string {
  if (!isShotId(raw)) throw badRequest("invalid shot id");
  return raw;
}
