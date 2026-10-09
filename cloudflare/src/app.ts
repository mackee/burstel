// Public HTTP surface of the Worker.
//
// Only this whitelist is reachable from the internet. The Go recorder's
// `/internal/*`, artifact and query endpoints are never exposed: the Worker
// routes them privately to the ShotDO, which talks to the container over its
// TCP port.
//
//   GET  /healthz
//   POST /api/shots
//   GET  /api/shots/{id}
//   POST /api/shots/{id}/stop
//   POST /shots/{id}/v1/{traces,metrics,logs}

import { authenticate } from "./auth";
import { badRequest, unauthorized } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { isShotId, newShotId } from "./ids";

/** Structural subset of `DurableObjectStub`. */
export interface DoStubLike {
  fetch(request: Request): Promise<Response>;
}

/** Structural subset of `DurableObjectNamespace<ShotDO>`. */
export interface ShotNamespaceLike {
  getByName(name: string): DoStubLike;
}

export interface AppEnv {
  SHOT_DO: ShotNamespaceLike;
  BURSTEL_API_TOKEN?: string;
  PUBLIC_BASE_URL?: string;
}

const DO_ORIGIN = "https://shot-do.internal";

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

    if (request.method === "POST" && path === "/api/shots") {
      return await createShot(request, env, url);
    }

    const statusMatch = /^\/api\/shots\/([^/]+)$/.exec(path);
    if (statusMatch) {
      if (request.method !== "GET") throw badRequest("method not allowed");
      return await forwardControl(env, requireShotId(statusMatch[1]!), "GET", "/internal/status", url, undefined);
    }

    const stopMatch = /^\/api\/shots\/([^/]+)\/stop$/.exec(path);
    if (stopMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      const shotId = requireShotId(stopMatch[1]!);
      return await forwardControl(env, shotId, "POST", "/internal/stop", url, JSON.stringify({ shot_id: shotId }));
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

async function createShot(request: Request, env: AppEnv, url: URL): Promise<Response> {
  const body = await readJson<{ metadata?: Record<string, unknown> }>(request);
  const shotId = newShotId();
  const stub = env.SHOT_DO.getByName(shotId);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/create`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ shot_id: shotId, metadata: body.metadata ?? {} }),
    }),
  );
  if (resp.status !== 201 && resp.status !== 200) return resp;
  const info = (await resp.json()) as Record<string, unknown>;
  return json(201, withOtlpEndpoints(info, env, url, shotId));
}

async function forwardControl(
  env: AppEnv,
  shotId: string,
  method: string,
  doPath: string,
  url: URL,
  body: string | undefined,
): Promise<Response> {
  const stub = env.SHOT_DO.getByName(shotId);
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.headers = { "content-type": "application/json" };
    init.body = body;
  }
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}${doPath}`, init));
  if (resp.status !== 200) return resp;
  const info = (await resp.json()) as Record<string, unknown>;
  return json(200, withOtlpEndpoints(info, env, url, shotId));
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

function withOtlpEndpoints(
  info: Record<string, unknown>,
  env: AppEnv,
  url: URL,
  shotId: string,
): Record<string, unknown> {
  const base = (env.PUBLIC_BASE_URL ?? url.origin).replace(/\/+$/, "");
  const shotBase = `${base}/shots/${shotId}`;
  return {
    ...info,
    otlp_base_url: shotBase,
    otlp_endpoints: {
      traces: `${shotBase}/v1/traces`,
      metrics: `${shotBase}/v1/metrics`,
      logs: `${shotBase}/v1/logs`,
    },
  };
}

function requireShotId(raw: string): string {
  if (!isShotId(raw)) throw badRequest("invalid shot id");
  return raw;
}
