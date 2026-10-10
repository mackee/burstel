// Shot, instance and token tools shared by the REST API and the MCP server.
//
// Every tool is a thin, authenticated proxy to a Durable Object. The ShotDO owns
// the R2 artifact, the analysis container and the read-only DuckDB session; the
// IngestDO owns the named-instance registry, instance credentials and the
// durable raw-journal ledger. MCP callers cannot bypass the same boundaries the
// REST API uses.
//
// Spec 014: named instances with 256-bit push-only tokens replaced the per-shot
// ingest tokens and the recording cap. `/ingest/v1/*` now journals raw OTLP
// durably for an instance and never targets a shot.

import { AUTH_DO_NAME, isScope } from "./auth-core";
import type { AuthTokenIssue, AuthTokenMetadata } from "./auth-core";
import { DO_ORIGIN } from "./env";
import type { AuthNamespaceLike, DoStubLike, IngestNamespaceLike, ShotNamespaceLike } from "./env";
import { badRequest, conflict, HttpError } from "./errors";
import { INGEST_DO_NAME, isValidInstanceName, normalizeInstanceName } from "./ingest-core";
import type { InstanceMetadata, InstanceTokenIssue, InstanceTokenMetadata } from "./ingest-core";
import { isShotId, newShotId } from "./ids";

export interface ToolContext {
  namespace: ShotNamespaceLike;
  /** Installation-wide coordinator: instance registry, journal ledger, shots. */
  ingest: IngestNamespaceLike;
  /** Multi-user token registry (spec 013, Part B); optional in pure unit tests. */
  auth?: AuthNamespaceLike;
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
    mintToken: (input: MintTokenInput) => mintToken(ctx, input),
    listTokens: () => listTokens(ctx),
    revokeToken: (id: string) => revokeToken(ctx, id),
    createInstance: (name: string) => createInstance(ctx, name),
    listInstances: () => listInstances(ctx),
    issueInstanceToken: (name: string, opts?: { rotate?: boolean; overlap_seconds?: number }) =>
      issueInstanceToken(ctx, name, opts),
    listInstanceTokens: (name: string) => listInstanceTokens(ctx, name),
    revokeInstanceToken: (name: string, id: string) => revokeInstanceToken(ctx, name, id),
  };
}

/** Body accepted when minting a per-person token (REST or MCP). */
export interface MintTokenInput {
  scope: string;
  owner?: string;
  /** Absolute expiry as an ISO-8601 timestamp. */
  expires_at?: string;
  /** Relative expiry in seconds. */
  ttl_seconds?: number;
}

// ---------------------------------------------------------------- start/stop

/**
 * Creates and starts a shot. Spec 014 removed the application-level recording
 * cap, so this always registers the shot and starts recording; concurrency is
 * bounded only by the explicit backend container capacity controls.
 */
export async function startShot(
  ctx: ToolContext,
  metadata: Record<string, unknown>,
  owner?: string,
): Promise<Record<string, unknown>> {
  const shotId = newShotId();
  const principal = normalizeOwner(owner);

  await ingestCall(ctx, "/internal/shot/register", "POST", { shot_id: shotId, owner: principal });

  let info: Record<string, unknown>;
  try {
    info = await createShot(ctx, shotId, metadata, principal);
  } catch (error) {
    // Roll the registration back so a failed start never leaves a phantom row.
    await ingestCall(ctx, "/internal/shot/release", "POST", { shot_id: shotId }).catch(() => undefined);
    throw error;
  }

  return withIngestEndpoints(info, ctx);
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
    // A lost shot will never record again: mark its registry row terminal.
    if (error instanceof HttpError && error.code === "shot_lost") {
      await ingestCall(ctx, "/internal/shot/state", "POST", { shot_id: shotId, state: "failed" }).catch(
        () => undefined,
      );
    }
    throw error;
  }
  // Record the terminal state only after the shot has frozen, so listing stays
  // true. Ingestion is independent of the shot lifecycle (spec 014).
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

// -------------------------------------------------------- instance registry

/** Creates or returns the named logical instance. Requires admin|member. */
export async function createInstance(ctx: ToolContext, name: string): Promise<InstanceMetadata> {
  const normalized = normalizeInstanceName(name);
  if (!isValidInstanceName(normalized)) throw badRequest("invalid instance name");
  return ingestCall<InstanceMetadata>(ctx, "/internal/instance/upsert", "POST", { name: normalized });
}

/** Lists instance metadata/status (never secrets). */
export async function listInstances(ctx: ToolContext): Promise<{ instances: InstanceMetadata[] }> {
  return ingestCall<{ instances: InstanceMetadata[] }>(ctx, "/internal/instance/list", "GET");
}

/**
 * Issues a 24h push-only instance token (disclosed once). With `rotate` the
 * previous active token(s) for *this* instance are revoked with a bounded
 * overlap; other instances are never touched.
 */
export async function issueInstanceToken(
  ctx: ToolContext,
  name: string,
  opts: { rotate?: boolean; overlap_seconds?: number } = {},
): Promise<InstanceTokenIssue> {
  const normalized = normalizeInstanceName(name);
  if (!isValidInstanceName(normalized)) throw badRequest("invalid instance name");
  return ingestCall<InstanceTokenIssue>(ctx, "/internal/instance/token/issue", "POST", {
    name: normalized,
    rotate: opts.rotate === true,
    overlap_seconds: opts.overlap_seconds,
  });
}

/** Lists instance token metadata (never values). */
export async function listInstanceTokens(
  ctx: ToolContext,
  name: string,
): Promise<{ tokens: InstanceTokenMetadata[] }> {
  const normalized = normalizeInstanceName(name);
  if (!isValidInstanceName(normalized)) throw badRequest("invalid instance name");
  return ingestCall<{ tokens: InstanceTokenMetadata[] }>(ctx, "/internal/instance/token/list", "POST", {
    name: normalized,
  });
}

/** Revokes one instance token immediately (idempotent). */
export async function revokeInstanceToken(
  ctx: ToolContext,
  name: string,
  id: string,
): Promise<{ revoked: boolean; id: string }> {
  const normalized = normalizeInstanceName(name);
  if (!isValidInstanceName(normalized)) throw badRequest("invalid instance name");
  if (typeof id !== "string" || id.trim() === "") throw badRequest("token id is required");
  return ingestCall<{ revoked: boolean; id: string }>(ctx, "/internal/instance/token/revoke", "POST", {
    name: normalized,
    id,
  });
}

// -------------------------------------------------------- token registry

/** Mints a per-person token (disclosed once). Scope is validated here and again in the DO. */
export async function mintToken(ctx: ToolContext, input: MintTokenInput): Promise<AuthTokenIssue> {
  if (!isScope(input.scope)) throw badRequest("scope must be admin or member");
  const result = await authCall<AuthTokenIssue>(ctx, "/internal/mint", "POST", input);
  return result;
}

/** Lists registry metadata (never values or digests), newest first. */
export async function listTokens(ctx: ToolContext): Promise<{ tokens: AuthTokenMetadata[] }> {
  return authCall<{ tokens: AuthTokenMetadata[] }>(ctx, "/internal/list", "GET");
}

/** Revokes a token immediately and idempotently. */
export async function revokeToken(ctx: ToolContext, id: string): Promise<{ revoked: boolean; id: string }> {
  if (typeof id !== "string" || id.trim() === "") throw badRequest("token id is required");
  return authCall<{ revoked: boolean; id: string }>(ctx, "/internal/revoke", "POST", { id });
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
  return (await parseJsonOrThrow(resp)) as unknown as T;
}

function authStub(ctx: ToolContext): DoStubLike {
  if (!ctx.auth) {
    throw new HttpError(500, "token registry is not configured", false, "auth_registry_unavailable");
  }
  return ctx.auth.getByName(AUTH_DO_NAME);
}

async function authCall<T>(
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
  const resp = await authStub(ctx).fetch(new Request(`${DO_ORIGIN}${path}`, init));
  return (await parseJsonOrThrow(resp)) as unknown as T;
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
 * Advertises the stable instance ingest endpoints and the deprecated per-shot
 * admin path. The per-shot push token is gone (spec 014): one named instance
 * token configures OTLP for a whole day regardless of shots.
 */
export function withIngestEndpoints(info: Record<string, unknown>, ctx: ToolContext): Record<string, unknown> {
  const base = (ctx.publicBaseUrl ?? ctx.origin).replace(/\/+$/, "");
  const ingest = `${base}/ingest`;
  const shotId = String(info.shot_id ?? "");
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
    instance_endpoint: `${base}/api/instances`,
    ingest_note:
      "configure one named instance token for the stable /ingest endpoints; ingestion is continuous and not tied to this shot",
  };
}
