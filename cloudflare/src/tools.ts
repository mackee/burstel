// Shot tools shared by the REST API and the MCP server.
//
// Every tool is a thin, authenticated proxy to a per-shot ShotDO. The DO owns
// the R2 artifact, the analysis container and the read-only DuckDB session, so
// MCP callers cannot bypass the same boundary the REST API uses.
//
// Shot creation/stop also coordinates the installation-wide IngestDO so the
// stable `/ingest` endpoint always targets the active (recording) shot.

import { DO_ORIGIN } from "./env";
import type { DoStubLike, IngestNamespaceLike, ShotNamespaceLike } from "./env";
import { badRequest, conflict, HttpError } from "./errors";
import { INGEST_DO_NAME } from "./ingest-core";
import { isShotId, newShotId } from "./ids";

export interface ToolContext {
  namespace: ShotNamespaceLike;
  /** Installation-wide push-token / active-shot coordinator. */
  ingest: IngestNamespaceLike;
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
  // A recording shot must never be silently displaced by the stable endpoint.
  await ensureNoRecordingShot(ctx);

  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/create`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ shot_id: shotId, metadata }),
    }),
  );
  const info = await parseJsonOrThrow(resp);

  const activated = await activateShot(ctx, shotId, stub);
  if (!activated) {
    throw conflict("another shot is already recording; stop it before starting a new one");
  }
  return withIngestEndpoints(info, ctx, shotId);
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
  let result: Record<string, unknown>;
  try {
    result = await parseJsonOrThrow(resp);
  } catch (error) {
    // A lost shot will never record again: release the stable endpoint so the
    // next shot can start, then surface the terminal failure.
    if (error instanceof HttpError && error.code === "shot_lost") {
      await ingestCall(ctx, "/internal/detach", "POST", { shot_id: shotId }).catch(() => undefined);
    }
    throw error;
  }
  // Detach ingress only after the shot has frozen, so in-flight requests are
  // attributed to the old shot and late arrivals are discarded, not leaked into
  // the next shot.
  await ingestCall(ctx, "/internal/detach", "POST", { shot_id: shotId }).catch(() => undefined);
  return result;
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

// ---------------------------------------------------------------- ingest coordination

function ingestStub(ctx: ToolContext): DoStubLike {
  return ctx.ingest.getByName(INGEST_DO_NAME);
}

async function ingestCall<T>(
  ctx: ToolContext,
  path: string,
  method: string,
  body?: unknown,
): Promise<T> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.headers = { "content-type": "application/json" };
    init.body = JSON.stringify(body);
  }
  const resp = await ingestStub(ctx).fetch(new Request(`${DO_ORIGIN}${path}`, init));
  return (await parseJsonOrThrow(resp)) as T;
}

async function ensureNoRecordingShot(ctx: ToolContext): Promise<void> {
  const active = await ingestCall<{ shot_id: string | null }>(ctx, "/internal/active", "GET");
  if (!active.shot_id) return;

  const info = await peekShot(ctx, active.shot_id);
  if (info && info.state === "recording") {
    throw conflict("another shot is already recording; stop it before starting a new one");
  }
  // Stale mapping (e.g. after a crash between freeze and detach): reconcile it.
  await ingestCall(ctx, "/internal/detach", "POST", { shot_id: active.shot_id }).catch(() => undefined);
}

async function peekShot(ctx: ToolContext, shotId: string): Promise<ShotInfo | null> {
  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}/internal/status`));
  if (resp.status < 200 || resp.status >= 300) return null;
  return (await resp.json()) as ShotInfo;
}

async function activateShot(ctx: ToolContext, shotId: string, stub: DoStubLike): Promise<boolean> {
  try {
    await ingestCall(ctx, "/internal/activate", "POST", { shot_id: shotId });
    return true;
  } catch (error) {
    // Best-effort rollback so a losing concurrent start cannot linger as a
    // recording orphan with no way to receive the stable endpoint.
    await stub
      .fetch(
        new Request(`${DO_ORIGIN}/internal/stop`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ shot_id: shotId }),
        }),
      )
      .catch(() => undefined);
    if (error instanceof HttpError && error.code === "active_shot_recording") return false;
    throw error;
  }
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

/**
 * Advertises the *stable* `/ingest` base URL plus the deprecated per-shot path
 * for backwards compatibility. The push token itself is never included.
 */
export function withIngestEndpoints(
  info: Record<string, unknown>,
  ctx: ToolContext,
  shotId: string,
): Record<string, unknown> {
  const base = (ctx.publicBaseUrl ?? ctx.origin).replace(/\/+$/, "");
  const ingest = `${base}/ingest`;
  const legacy = `${base}/shots/${shotId}`;
  return {
    ...info,
    otlp_base_url: ingest,
    otlp_endpoints: {
      traces: `${ingest}/v1/traces`,
      metrics: `${ingest}/v1/metrics`,
      logs: `${ingest}/v1/logs`,
    },
    legacy_otlp_base_url: legacy,
    legacy_otlp_endpoints: {
      traces: `${legacy}/v1/traces`,
      metrics: `${legacy}/v1/metrics`,
      logs: `${legacy}/v1/logs`,
    },
    ingest_token_endpoint: `${base}/api/ingest-token`,
  };
}
