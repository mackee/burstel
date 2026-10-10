// Public HTTP surface of the Worker.
//
// Only this whitelist is reachable from the internet. The Go recorder's
// `/internal/*`, artifact and analysis endpoints are never exposed: the Worker
// routes them privately to the ShotDO, which talks to the container over its
// TCP port.
//
//   GET    /healthz                         (public)
//   POST   /api/ingest-token                retired; 410 Gone
//   POST   /api/instances                   (admin|member) create/upsert an instance
//   GET    /api/instances                   (admin|member) list instance metadata
//   POST   /api/instances/{name}/tokens     (admin|member) issue/rotate an instance token
//   GET    /api/instances/{name}/tokens      (admin|member) list instance token metadata
//   DELETE /api/instances/{name}/tokens/{id} (admin|member) revoke an instance token
//   POST   /api/tokens                      (admin) mint a per-person token
//   GET    /api/tokens                      (admin) list per-person token metadata
//   DELETE /api/tokens/{id}                 (admin) revoke a per-person token
//   POST   /api/shots                       (admin|member)
//   GET    /api/shots                       (admin|member) list, optional ?owner=
//   GET    /api/shots/{id}                  (admin|member)
//   POST   /api/shots/{id}/stop             (admin|member)
//   POST   /api/shots/{id}/query            (admin|member)
//   POST   /api/shots/{id}/analyze          (admin|member)
//   GET    /api/shots/{id}/schema           (admin|member)
//   POST   /api/shots/{id}/compare          (admin|member)
//   POST   /ingest/v1/{traces,metrics,logs}  (push-only instance token)
//   POST   /shots/{id}/v1/{traces,metrics,logs} (admin|member, legacy per-shot path)
//   POST   /mcp                             (admin|member)
//
// Spec 014: `/ingest/v1/*` authenticates a push-only *instance* token and
// durably journals the raw export to R2 before acknowledging it. Instance
// credentials are managed by `admin` or `member`; only `admin` may manage
// *person* tokens. Neither the admin credential nor an instance token may
// cross the other boundary.

import { resolvePrincipal, isRegistryAdmin } from "./authz";
import { DO_ORIGIN, pagination } from "./env";
import type { AppEnv, DoStubLike, ShotNamespaceLike } from "./env";
import { badRequest, forbidden, notFound, unauthorized } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { handleInstanceIngest } from "./intake";
import { isShotId } from "./ids";
import { handleMcp } from "./mcp";
import type { Signal } from "./store";
import {
  analyzeShot,
  compareShots,
  createInstance,
  getShot,
  issueInstanceToken,
  listInstanceTokens,
  listInstances,
  listShots,
  listTokens,
  mintToken,
  queryShot,
  revokeInstanceToken,
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

    // Stable OTLP ingest: authenticated by a push-only instance token, never by
    // the admin credential. Durable journaling happens before the 200.
    const signal = ingestSignal(path);
    if (signal) {
      if (request.method !== "POST") throw badRequest("method not allowed");
      return await handleInstanceIngest(request, env, signal);
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

    // Retired installation-wide token. Point operators at named instances.
    if (path === "/api/ingest-token") {
      return json(410, {
        error:
          "the installation-wide ingest token was removed; create a named instance with POST /api/instances and issue a token with POST /api/instances/{name}/tokens",
        code: "gone",
      });
    }

    const ctx = toolContext(env, url);

    // -------------------------------------------------- instance registry
    // admin and member may manage operational instance credentials.
    if (path === "/api/instances") {
      if (request.method === "POST") {
        const body = await readJson<{ name?: unknown }>(request);
        if (typeof body.name !== "string" || body.name.trim() === "") throw badRequest("name is required");
        return json(201, await createInstance(ctx, body.name));
      }
      if (request.method === "GET") return json(200, await listInstances(ctx));
      throw badRequest("method not allowed");
    }

    const instanceTokensMatch = /^\/api\/instances\/([^/]+)\/tokens$/.exec(path);
    if (instanceTokensMatch) {
      const name = decodeURIComponent(instanceTokensMatch[1]!);
      if (request.method === "POST") {
        const body = await readJson<{ rotate?: unknown; overlap_seconds?: unknown }>(request);
        return json(
          201,
          await issueInstanceToken(ctx, name, {
            rotate: body.rotate === true,
            overlap_seconds: typeof body.overlap_seconds === "number" ? body.overlap_seconds : undefined,
          }),
        );
      }
      if (request.method === "GET") return json(200, await listInstanceTokens(ctx, name));
      throw badRequest("method not allowed");
    }

    const instanceTokenMatch = /^\/api\/instances\/([^/]+)\/tokens\/([^/]+)$/.exec(path);
    if (instanceTokenMatch) {
      if (request.method !== "DELETE") throw badRequest("method not allowed");
      const name = decodeURIComponent(instanceTokenMatch[1]!);
      const id = decodeURIComponent(instanceTokenMatch[2]!);
      const result = await revokeInstanceToken(ctx, name, id);
      if (!result.revoked) throw notFound("token not found");
      return json(200, { revoked: true, id: result.id });
    }

    // ---------------------------------------------- person token registry
    // Admin-only (spec 013, Part B). A member token is denied here with 403
    // before any DO work.
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

    // Superseded per-shot ingest token (spec 013) — explicit 410 Gone.
    const legacyIngestTokenMatch = /^\/api\/shots\/([^/]+)\/ingest-token$/.exec(path);
    if (legacyIngestTokenMatch) {
      return json(410, {
        error:
          "per-shot ingest tokens were removed; configure one named instance token for POST /ingest/v1/{traces,metrics,logs}",
        code: "gone",
      });
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

function ingestSignal(path: string): Signal | null {
  const match = /^\/ingest\/v1\/(traces|metrics|logs)$/.exec(path);
  if (!match) return null;
  return match[1] as Signal;
}

function toolContext(env: AppEnv, url: URL): ToolContext {
  return {
    namespace: env.SHOT_DO,
    ingest: env.INGEST_DO,
    auth: env.AUTH_DO,
    origin: url.origin,
    publicBaseUrl: env.PUBLIC_BASE_URL,
  };
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

function requireShotId(raw: string): string {
  if (!isShotId(raw)) throw badRequest("invalid shot id");
  return raw;
}
