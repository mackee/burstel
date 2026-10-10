// Public HTTP surface of the Worker.
//
// Only this whitelist is reachable from the internet. The Go recorder's
// `/internal/*`, artifact and analysis endpoints are never exposed: the Worker
// routes them privately to the ShotDO, which talks to the container over its
// TCP port.
//
//   GET    /healthz                         (public)
//   POST   /api/ingest-token                (authenticated) retired; 410 Gone
//   POST   /api/tokens                      (admin) mint a per-person token
//   GET    /api/tokens                      (admin) list token metadata
//   DELETE /api/tokens/{id}                 (admin) revoke a token
//   POST   /api/shots                       (admin|member)
//   GET    /api/shots                       (admin|member) list, optional ?owner=
//   GET    /api/shots/{id}                  (admin|member)
//   POST   /api/shots/{id}/ingest-token     (admin|member) mint per-shot push token
//   POST   /api/shots/{id}/stop             (admin|member)
//   POST   /api/shots/{id}/query            (admin|member)
//   POST   /api/shots/{id}/analyze          (admin|member)
//   GET    /api/shots/{id}/schema           (admin|member)
//   POST   /api/shots/{id}/compare          (admin|member)
//   POST   /ingest/v1/{traces,metrics,logs} (per-shot push token)
//   POST   /shots/{id}/v1/{traces,metrics,logs} (admin|member, legacy per-shot path)
//   POST   /mcp                             (admin|member)
//
// Spec 013, Part B: `/api/*` and `/mcp` authenticate with `BURSTEL_API_TOKEN`
// (admin) or a registered per-person token (`admin` | `member`). Only `admin`
// may manage the token registry.

import { extractToken } from "./auth";
import { resolvePrincipal, isRegistryAdmin } from "./authz";
import { DO_ORIGIN, maxConcurrentRecording, pagination } from "./env";
import type { AppEnv, DoStubLike, IngestNamespaceLike, ShotNamespaceLike } from "./env";
import { badRequest, forbidden, HttpError, notFound, unauthorized } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { INGEST_DO_NAME } from "./ingest-core";
import type { IngestAdmitResult } from "./ingest-core";
import { isShotId } from "./ids";
import { handleMcp } from "./mcp";
import {
  analyzeShot,
  compareShots,
  getShot,
  listShots,
  listTokens,
  mintShotIngestToken,
  mintToken,
  queryShot,
  revokeToken,
  schemaShot,
  startShot,
  stopShot,
} from "./tools";
import type { ToolContext } from "./tools";

export type { AppEnv, AuthNamespaceLike, DoStubLike, IngestNamespaceLike, ShotNamespaceLike } from "./env";

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

    // Everything below is scoped to an authenticated principal and fails
    // closed when no credential is configured (unless the explicit
    // local-development bypass is enabled).
    const auth = await resolvePrincipal(request, env);
    if (!auth.ok) throw unauthorized();
    const principal = auth.principal;

    if (path === "/mcp") {
      return await handleMcp(request, env, principal);
    }

    // Retired in spec 013: the installation-wide 24h push token is gone. Point
    // collectors at the per-shot endpoint so the change is explicit.
    if (path === "/api/ingest-token") {
      return json(410, {
        error:
          "the installation-wide ingest token was removed; mint a per-shot token with POST /api/shots/{id}/ingest-token",
        code: "gone",
      });
    }

    const ctx = toolContext(env, url);

    // Token registry: admin-only (spec 013, Part B). A member token is denied
    // here with 403 before any DO work.
    if (path === "/api/tokens") {
      if (!isRegistryAdmin(principal)) throw forbidden();
      if (request.method === "POST") {
        const body = await readJson<{
          scope?: unknown;
          owner?: unknown;
          expires_at?: unknown;
          ttl_seconds?: unknown;
        }>(request);
        return json(
          201,
          await mintToken(ctx, {
            scope: typeof body.scope === "string" ? body.scope : "",
            owner: typeof body.owner === "string" ? body.owner : undefined,
            expires_at: typeof body.expires_at === "string" ? body.expires_at : undefined,
            ttl_seconds: typeof body.ttl_seconds === "number" ? body.ttl_seconds : undefined,
          }),
        );
      }
      if (request.method === "GET") return json(200, await listTokens(ctx));
      throw badRequest("method not allowed");
    }

    const tokenMatch = /^\/api\/tokens\/([^/]+)$/.exec(path);
    if (tokenMatch) {
      if (!isRegistryAdmin(principal)) throw forbidden();
      if (request.method !== "DELETE") throw badRequest("method not allowed");
      const result = await revokeToken(ctx, decodeURIComponent(tokenMatch[1]!));
      if (!result.revoked) throw notFound("token not found");
      return json(200, { revoked: true, id: result.id });
    }

    if (path === "/api/shots") {
      if (request.method === "GET") {
        const { limit, offset } = pagination(url);
        const owner = url.searchParams.get("owner") ?? undefined;
        return json(200, await listShots(ctx, { owner, limit, offset }));
      }
      if (request.method === "POST") {
        const body = await readJson<{ metadata?: Record<string, unknown>; owner?: unknown }>(request);
        const owner = typeof body.owner === "string" ? body.owner : undefined;
        return json(201, await startShot(ctx, body.metadata ?? {}, owner));
      }
      throw badRequest("method not allowed");
    }

    const ingestTokenMatch = /^\/api\/shots\/([^/]+)\/ingest-token$/.exec(path);
    if (ingestTokenMatch) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      return json(201, await mintShotIngestToken(ctx, requireShotId(ingestTokenMatch[1]!)));
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
    auth: env.AUTH_DO,
    origin: url.origin,
    publicBaseUrl: env.PUBLIC_BASE_URL,
    maxConcurrentRecording: maxConcurrentRecording(env),
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
  // The token is bound to exactly one shot, so it routes there directly.
  const shotId = admitted.shot_id;
  if (!shotId) {
    return discardResponse(request, "ignored-no-active-shot");
  }
  const stub = env.SHOT_DO.getByName(shotId);
  const resp = await forwardIngest(stub, signal, request);
  if (isDefinitelyUnaccepted(resp)) {
    // The ShotDO rejected the request *before* forwarding any bytes to the
    // recorder (stop won the race, or the shot is no longer recording). That is
    // provably uncommitted, so acknowledge with a valid OTLP ExportResponse and
    // consciously discard; never attribute it to a different shot. A genuine
    // 503 (storage failure, a down container, a write timeout) carries no
    // marker and is propagated below so the exporter retries.
    // A lost shot (its recording container stopped) is discarded the same way
    // but with its own marker so operators can tell the two apart.
    const disposition =
      resp.headers.get("x-burstel-disposition") === "ignored-shot-lost" ? "ignored-shot-lost" : "ignored-no-active-shot";
    await resp.body?.cancel().catch(() => undefined);
    return discardResponse(request, disposition);
  }
  const out = new Response(resp.body, resp);
  out.headers.set("x-burstel-shot-id", shotId);
  return out;
}

/**
 * True only for a definite, pre-commit rejection from the ShotDO boundary: it
 * carries the disposition marker, or is a 409 (every 409 the boundary emits for
 * ingest is produced before any recorder write). Genuine downstream failures —
 * notably a 503 storage/container error — must never be mistaken for this.
 */
function isDefinitelyUnaccepted(resp: Response): boolean {
  const disposition = resp.headers.get("x-burstel-disposition");
  return disposition === "ignored-no-active-shot" || disposition === "ignored-shot-lost" || resp.status === 409;
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
  let retryable = false;
  try {
    const parsed = (await resp.json()) as { error?: string; code?: string; retryable?: boolean };
    if (parsed.error) message = parsed.error;
    if (parsed.code) code = parsed.code;
    retryable = parsed.retryable === true;
  } catch {
    // keep defaults
  }
  throw new HttpError(resp.status, message, retryable, code);
}

function requireShotId(raw: string): string {
  if (!isShotId(raw)) throw badRequest("invalid shot id");
  return raw;
}
