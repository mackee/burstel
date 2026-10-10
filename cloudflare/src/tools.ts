// Shot tools shared by the REST API and the MCP server.
//
// Every tool is a thin, authenticated proxy to a per-shot ShotDO. The DO owns
// the R2 artifact, the analysis container and the read-only DuckDB session, so
// MCP callers cannot bypass the same boundary the REST API uses.
//
// Spec 013 Part A: shots record concurrently. The stable `/ingest` endpoint no
// longer targets a single active shot; each per-shot ingest token routes to its
// own shot. The coordinator (`IngestDO`) holds the shot registry, enforces the
// `MAX_CONCURRENT_RECORDING` cap and persists per-shot token digests.

import { DO_ORIGIN } from "./env";
import type { DoStubLike, IngestNamespaceLike, ShotNamespaceLike } from "./env";
import { badRequest, capacityExhausted, conflict, HttpError } from "./errors";
import { INGEST_DO_NAME } from "./ingest-core";
import type { ShotIngestTokenIssue } from "./ingest-core";
import { isShotId, newShotId } from "./ids";

export interface ToolContext {
  namespace: ShotNamespaceLike;
  /** Installation-wide coordinator: shot registry, per-shot tokens and cap. */
  ingest: IngestNamespaceLike;
  /** Origin of the incoming request, used to advertise OTLP endpoints. */
  origin: string;
  publicBaseUrl?: string;
  /** Maximum number of shots that may record at once (spec 013, Part A). */
  maxConcurrentRecording: number;
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
  owner?: string;
  metadata?: Record<string, unknown>;
  created_at?: string;
  started_at?: string;
  stopped_at?: string;
  size_bytes?: number;
  sha256?: string;
  counters?: Record<string, unknown>;
  freeze?: FreezeRef;
  [key: string]: unknown;
}

/** A shot registry row as returned by the coordinator. */
interface RegisteredShot {
  shot_id: string;
  owner: string;
  created_at: number;
  state: string;
  stopped_at: number | null;
  size_bytes: number | null;
  sha256: string | null;
}

export function createTools(ctx: ToolContext) {
  return {
    start: (metadata: Record<string, unknown> = {}, owner?: string) => startShot(ctx, metadata, owner),
    stop: (shotId: string) => stopShot(ctx, shotId),
    get: (shotId: string) => getShot(ctx, shotId),
    query: (shotId: string, sql: string, maxRows?: number) => queryShot(ctx, shotId, sql, maxRows),
    analyze: (shotId: string, sections?: string[], topN?: number) => analyzeShot(ctx, shotId, sections, topN),
    schema: (shotId: string) => schemaShot(ctx, shotId),
    compare: (leftId: string, rightId: string, topN?: number) => compareShots(ctx, leftId, rightId, topN),
    list: (opts: { owner?: string; limit?: number; offset?: number } = {}) => listShots(ctx, opts),
    issueIngestToken: (shotId: string) => mintShotIngestToken(ctx, shotId),
  };
}

// ---------------------------------------------------------------- start/stop

/**
 * Creates and starts a shot, reserving a concurrency slot first so exceeding
 * `MAX_CONCURRENT_RECORDING` returns a retryable `503 capacity_exhausted`
 * before any ShotDO or container state exists, and mints its per-shot ingest
 * token (disclosed once).
 */
export async function startShot(
  ctx: ToolContext,
  metadata: Record<string, unknown>,
  owner?: string,
): Promise<Record<string, unknown>> {
  const shotId = newShotId();
  const principal = normalizeOwner(owner);

  await reserveRecordingSlot(ctx, shotId, principal);

  let info: Record<string, unknown>;
  try {
    info = await createShot(ctx, shotId, metadata, principal);
  } catch (error) {
    // Roll the reservation back so a failed start never leaks capacity.
    await ingestCall(ctx, "/internal/shot/release", "POST", { shot_id: shotId }).catch(() => undefined);
    throw error;
  }

  const issued = await issueShotToken(ctx, shotId);
  return withIngestEndpoints(info, ctx, shotId, issued);
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
    // A lost shot will never record again: release its slot and revoke its
    // token so the next shot can start.
    if (error instanceof HttpError && error.code === "shot_lost") {
      await ingestCall(ctx, "/internal/shot-token/revoke", "POST", { shot_id: shotId }).catch(() => undefined);
      await ingestCall(ctx, "/internal/shot/state", "POST", { shot_id: shotId, state: "failed" }).catch(
        () => undefined,
      );
    }
    throw error;
  }
  // Revoke the per-shot token (with a short grace) and record the terminal
  // state only after the shot has frozen, so late but already-admitted batches
  // are attributed to this shot and consciously discarded, never leaked.
  await ingestCall(ctx, "/internal/shot-token/revoke", "POST", { shot_id: shotId }).catch(() => undefined);
  await ingestCall(ctx, "/internal/shot/state", "POST", {
    shot_id: shotId,
    state: String(result.state ?? "frozen"),
    stopped_at: epochMs(result.stopped_at),
    size_bytes: numberOrUndefined(result.size_bytes),
    sha256: typeof result.sha256 === "string" ? result.sha256 : undefined,
  }).catch(() => undefined);
  return result;
}

export async function getShot(ctx: ToolContext, shotId: string): Promise<ShotInfo> {
  requireShotId(shotId);
  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}/internal/status`));
  return (await parseJsonOrThrow(resp)) as ShotInfo;
}

// ---------------------------------------------------------------- listing

/**
 * Lists shots newest first, optionally filtered by attribution-only `owner`.
 * Metadata comes from the coordinator registry; the live per-shot status is
 * merged in for the current page.
 */
export async function listShots(
  ctx: ToolContext,
  opts: { owner?: string; limit?: number; offset?: number } = {},
): Promise<Record<string, unknown>> {
  const limit = clamp(opts.limit ?? 50, 1, 200);
  const offset = Math.max(0, opts.offset ?? 0);
  const page = await ingestCall<{ shots: RegisteredShot[]; total: number }>(ctx, "/internal/shots/list", "POST", {
    owner: opts.owner,
    limit,
    offset,
  });
  const shots: Record<string, unknown>[] = [];
  for (const entry of page.shots) {
    const info = await peekShot(ctx, entry.shot_id);
    shots.push(summarizeShot(entry, info));
  }
  return { shots, total: page.total, limit, offset };
}

/** Mints a per-shot ingest token (disclosed once) after confirming the shot exists. */
export async function mintShotIngestToken(
  ctx: ToolContext,
  shotId: string,
): Promise<ShotIngestTokenIssue> {
  await getShot(ctx, shotId);
  return issueShotToken(ctx, shotId);
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

// ------------------------------------------------------- ingest coordination

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

async function reserveRecordingSlot(ctx: ToolContext, shotId: string, owner: string): Promise<void> {
  const max = ctx.maxConcurrentRecording;
  // Reconcile stale `recording` entries only when the registry is at/over the
  // cap, so the common path is a single coordinator call.
  const { shots } = await ingestCall<{ shots: Array<{ shot_id: string }> }>(ctx, "/internal/shots/recording", "GET");
  if (shots.length >= max) {
    const stale: string[] = [];
    for (const { shot_id } of shots) {
      const info = await peekShot(ctx, shot_id);
      if (!info || info.state !== "recording") stale.push(shot_id);
    }
    if (stale.length > 0) {
      await ingestCall(ctx, "/internal/shot/release", "POST", { shot_ids: stale });
    }
  }
  // The coordinator enforces the cap atomically (single-threaded per DO).
  await ingestCall(ctx, "/internal/shot/reserve", "POST", { shot_id: shotId, owner, max });
}

async function createShot(
  ctx: ToolContext,
  shotId: string,
  metadata: Record<string, unknown>,
  owner: string,
): Promise<Record<string, unknown>> {
  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/create`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ shot_id: shotId, metadata, owner }),
    }),
  );
  return parseJsonOrThrow(resp);
}

async function issueShotToken(ctx: ToolContext, shotId: string): Promise<ShotIngestTokenIssue> {
  return ingestCall<ShotIngestTokenIssue>(ctx, "/internal/shot-token/issue", "POST", { shot_id: shotId });
}

async function peekShot(ctx: ToolContext, shotId: string): Promise<ShotInfo | null> {
  const stub = ctx.namespace.getByName(shotId);
  const resp = await stub.fetch(new Request(`${DO_ORIGIN}/internal/status`));
  if (resp.status < 200 || resp.status >= 300) return null;
  return (await resp.json()) as ShotInfo;
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

/** Attribution-only owner label; never an authorization boundary. */
function normalizeOwner(owner: string | undefined): string {
  const value = (owner ?? "").trim();
  return value === "" ? "legacy" : value;
}

function summarizeShot(entry: RegisteredShot, info: ShotInfo | null): Record<string, unknown> {
  if (info) {
    return {
      shot_id: entry.shot_id,
      state: info.state,
      owner: info.owner ?? entry.owner,
      created_at: info.created_at,
      started_at: info.started_at,
      stopped_at: info.stopped_at,
      size_bytes: info.size_bytes,
      sha256: info.sha256,
      counters: info.counters,
    };
  }
  return {
    shot_id: entry.shot_id,
    state: entry.state,
    owner: entry.owner,
    created_at: isoOrUndefined(entry.created_at),
    stopped_at: isoOrUndefined(entry.stopped_at ?? undefined),
    size_bytes: entry.size_bytes ?? undefined,
    sha256: entry.sha256 ?? undefined,
  };
}

function epochMs(value: unknown): number | undefined {
  if (typeof value !== "string" || value === "") return undefined;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function numberOrUndefined(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function isoOrUndefined(ms: number | undefined): string | undefined {
  return ms === undefined ? undefined : new Date(ms).toISOString();
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

/**
 * Advertises the stable `/ingest` base URL plus the deprecated per-shot admin
 * path. The per-shot push token, when freshly minted, is disclosed here exactly
 * once; the raw token is never included in any other response.
 */
export function withIngestEndpoints(
  info: Record<string, unknown>,
  ctx: ToolContext,
  shotId: string,
  issued?: ShotIngestTokenIssue,
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
    ingest_token_endpoint: `${base}/api/shots/${shotId}/ingest-token`,
    ...(issued
      ? {
          ingest_token: issued.token,
          ingest_token_shot_id: issued.shot_id,
          ingest_token_issued_at: issued.issued_at,
          ingest_token_expires_at: issued.expires_at,
          ingest_token_ttl_seconds: issued.ttl_seconds,
        }
      : {}),
  };
}
