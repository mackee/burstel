// Shot tools shared by the REST API and the MCP server.
//
// Every tool is a thin, authenticated proxy to a per-shot ShotDO. The DO owns
// the R2 artifact, the analysis container and the read-only DuckDB session, so
// MCP callers cannot bypass the same boundary the REST API uses.

import { DO_ORIGIN } from "./env";
import type { ShotNamespaceLike } from "./env";
import { badRequest, conflict, HttpError } from "./errors";
import { isShotId, newShotId } from "./ids";

export interface ToolContext {
  namespace: ShotNamespaceLike;
  /** Origin of the incoming request, used to advertise OTLP endpoints. */
  origin: string;
  publicBaseUrl?: string;
}

export interface FreezeRef {
  artifact_key: string;
  artifact_size: number;
  artifact_sha256: string;
  [key: string]: unknown;
}

export interface ShotInfo {
  shot_id: string;
  state: string;
  metadata?: Record<string, unknown>;
  freeze?: FreezeRef;
  [key: string]: unknown;
}

export function createTools(ctx: ToolContext) {
  return {
    start: (metadata: Record<string, unknown> = {}) => startShot(ctx, metadata),
    stop: (shotId: string) => stopShot(ctx, shotId),
    get: (shotId: string) => getShot(ctx, shotId),
    query: (shotId: string, sql: string, maxRows?: number) => queryShot(ctx, shotId, sql, maxRows),
    analyze: (shotId: string, sections?: string[], topN?: number) => analyzeShot(ctx, shotId, sections, topN),
    schema: (shotId: string) => schemaShot(ctx, shotId),
    compare: (leftId: string, rightId: string, topN?: number) => compareShots(ctx, leftId, rightId, topN),
  };
}

// ---------------------------------------------------------------- start/stop

export async function startShot(
  ctx: ToolContext,
  metadata: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const shotId = newShotId();
  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/create`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ shot_id: shotId, metadata }),
    }),
  );
  const info = await parseJsonOrThrow(resp);
  return withOtlpEndpoints(info, ctx, shotId);
}

export async function stopShot(ctx: ToolContext, shotId: string): Promise<Record<string, unknown>> {
  requireShotId(shotId);
  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/stop`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ shot_id: shotId }),
    }),
  );
  return parseJsonOrThrow(resp);
}

export async function getShot(ctx: ToolContext, shotId: string): Promise<ShotInfo> {
  requireShotId(shotId);
  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}/internal/status`));
  return (await parseJsonOrThrow(resp)) as ShotInfo;
}

// ---------------------------------------------------------------- analysis

export async function queryShot(
  ctx: ToolContext,
  shotId: string,
  sql: string,
  maxRows?: number,
): Promise<Record<string, unknown>> {
  requireShotId(shotId);
  if (typeof sql !== "string" || sql.trim() === "") throw badRequest("sql is required");
  const stub = ctx.namespace.getByName(shotId);
  return doJson(stub, "/internal/analysis/query", "POST", { sql, max_rows: maxRows });
}

export async function analyzeShot(
  ctx: ToolContext,
  shotId: string,
  sections?: string[],
  topN?: number,
): Promise<Record<string, unknown>> {
  requireShotId(shotId);
  const stub = ctx.namespace.getByName(shotId);
  return doJson(stub, "/internal/analysis/analyze", "POST", { sections, top_n: topN });
}

export async function schemaShot(ctx: ToolContext, shotId: string): Promise<Record<string, unknown>> {
  requireShotId(shotId);
  const stub = ctx.namespace.getByName(shotId);
  return doJson(stub, "/internal/analysis/schema", "GET");
}

export async function compareShots(
  ctx: ToolContext,
  leftId: string,
  rightId: string,
  topN?: number,
): Promise<Record<string, unknown>> {
  requireShotId(leftId);
  requireShotId(rightId);
  if (leftId === rightId) throw badRequest("left and right shot ids must differ");

  const right = await getShot(ctx, rightId);
  if (right.state !== "frozen" || !right.freeze) {
    throw conflict(`shot ${rightId} is not frozen`);
  }
  const leftStub = ctx.namespace.getByName(leftId);
  return doJson(leftStub, "/internal/analysis/compare", "POST", {
    other: { shot_id: rightId, freeze: right.freeze },
    top_n: topN,
  });
}

// ---------------------------------------------------------------- helpers

async function doJson(
  stub: { fetch(request: Request): Promise<Response> },
  path: string,
  method: string,
  body?: unknown,
): Promise<Record<string, unknown>> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.headers = { "content-type": "application/json" };
    init.body = JSON.stringify(body);
  }
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}${path}`, init));
  return parseJsonOrThrow(resp);
}

export async function parseJsonOrThrow(resp: Response): Promise<Record<string, unknown>> {
  if (resp.status >= 200 && resp.status < 300) {
    return (await resp.json()) as Record<string, unknown>;
  }
  let message = `shot request failed with status ${resp.status}`;
  let code = "error";
  let retryable = false;
  try {
    const body = (await resp.json()) as { error?: string; code?: string; retryable?: boolean };
    if (body.error) message = body.error;
    if (body.code) code = body.code;
    retryable = body.retryable === true;
  } catch {
    // keep the default message
  }
  throw new HttpError(resp.status, message, retryable, code);
}

function requireShotId(value: string): string {
  if (typeof value !== "string" || !isShotId(value)) throw badRequest("invalid shot id");
  return value;
}

export function withOtlpEndpoints(
  info: Record<string, unknown>,
  ctx: ToolContext,
  shotId: string,
): Record<string, unknown> {
  const base = (ctx.publicBaseUrl ?? ctx.origin).replace(/\/+$/, "");
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
