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
import type { R2BucketLike } from "./artifacts";
import { COMPACTOR_DO_NAME } from "./compactor-core";
import { DO_ORIGIN } from "./env";
import type {
  AuthNamespaceLike,
  CompactorNamespaceLike,
  DoStubLike,
  IngestNamespaceLike,
  ShotNamespaceLike,
} from "./env";
import { badRequest, conflict, HttpError } from "./errors";
import { INGEST_DO_NAME, isValidInstanceName, normalizeInstanceName } from "./ingest-core";
import type { CreateWindowShotInput, InstanceMetadata, InstanceTokenIssue, InstanceTokenMetadata, SegmentManifestRow, StartWindowShotInput, WindowShotRecord } from "./ingest-core";
import { isShotId, newShotId } from "./ids";
import { selectSegments } from "./shot-window";
import type { ShotWindow, WindowSegmentMeta } from "./shot-window";

export interface ToolContext {
  namespace: ShotNamespaceLike;
  /** Installation-wide coordinator: instance registry, journal ledger, shots. */
  ingest: IngestNamespaceLike;
  /** Multi-user token registry (spec 013, Part B); optional in pure unit tests. */
  auth?: AuthNamespaceLike;
  /** Compaction coordinator (spec 018); optional in pure unit tests. */
  compactor?: CompactorNamespaceLike;
  /** Origin of the incoming request, used to advertise OTLP endpoints. */
  origin: string;
  publicBaseUrl?: string;
  /**
   * Durable object bucket (spec 014/015). Required for window-shot analysis so
   * the Worker can stream verified segment objects into the analysis container.
   */
  bucket?: R2BucketLike;
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
  kind?: "window" | "recording";
  name?: string | null;
  window?: { start_ms: number; end_ms: number; provenance: string; clock_trust: string; instances: string[]; name?: string } | null;
  coverage?: Record<string, unknown> | null;
}

export function createTools(ctx: ToolContext) {
  return {
    start: (metadata: Record<string, unknown> = {}, owner?: string, opts: { mode?: string; name?: string; instances?: string[] } = {}) =>
      startShot(ctx, metadata, owner, opts),
    stop: (shotId: string, endedAtMs?: number) => dispatchStop(ctx, shotId, endedAtMs),
    get: (shotId: string) => dispatchGet(ctx, shotId),
    query: (shotId: string, sql: string, maxRows?: number) => queryShot(ctx, shotId, sql, maxRows),
    analyze: (shotId: string, sections?: string[], topN?: number) => analyzeShot(ctx, shotId, sections, topN),
    schema: (shotId: string) => schemaShot(ctx, shotId),
    compare: (leftId: string, rightId: string, topN?: number) => compareShots(ctx, leftId, rightId, topN),
    list: (opts: { owner?: string; limit?: number; offset?: number } = {}) => listShots(ctx, opts),
    createWindow: (input: CreateWindowShotInput) => createWindowShot(ctx, input),
    startWindow: (input: StartWindowShotInput) => startWindowShot(ctx, input),
    stopWindow: (shotId: string, endedAtMs?: number) => stopWindowShot(ctx, shotId, endedAtMs),
    shotRecord: (shotId: string) => getShotRecord(ctx, shotId),
    mintToken: (input: MintTokenInput) => mintToken(ctx, input),
    listTokens: () => listTokens(ctx),
    revokeToken: (id: string) => revokeToken(ctx, id),
    createInstance: (name: string) => createInstance(ctx, name),
    listInstances: () => listInstances(ctx),
    issueInstanceToken: (name: string, opts?: { rotate?: boolean; overlap_seconds?: number }) =>
      issueInstanceToken(ctx, name, opts),
    listInstanceTokens: (name: string) => listInstanceTokens(ctx, name),
    revokeInstanceToken: (name: string, id: string) => revokeInstanceToken(ctx, name, id),
    compactionStatus: () => compactionStatus(ctx),
    compactionRun: (opts: { gc?: boolean } = {}) => compactionRun(ctx, opts),
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
  opts: { mode?: string; name?: string; instances?: string[] } = {},
): Promise<Record<string, unknown>> {
  const shotId = newShotId();
  const principal = normalizeOwner(owner);

  if (opts.mode === "window") {
    const record = await startWindowShot(ctx, {
      shot_id: shotId,
      owner: principal,
      name: opts.name,
      instances: opts.instances,
      metadata,
    });
    return windowShotInfo(record, ctx);
  }

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

/** Creates an externally supplied, backdated complete window shot. */
export async function createWindowShot(ctx: ToolContext, input: CreateWindowShotInput): Promise<WindowShotRecord> {
  if (typeof input.shot_id !== "string" || !isShotId(input.shot_id)) throw badRequest("invalid shot id");
  return ingestCall<WindowShotRecord>(ctx, "/internal/shot/window/create", "POST", input);
}

/** Opens a server-clock window marker. */
export async function startWindowShot(ctx: ToolContext, input: StartWindowShotInput): Promise<WindowShotRecord> {
  if (typeof input.shot_id !== "string" || !isShotId(input.shot_id)) throw badRequest("invalid shot id");
  return ingestCall<WindowShotRecord>(ctx, "/internal/shot/window/start", "POST", input);
}

/** Stops and finalizes a server-clock window marker (idempotent). */
export async function stopWindowShot(
  ctx: ToolContext,
  shotId: string,
  endedAtMs?: number,
): Promise<WindowShotRecord> {
  requireShotId(shotId);
  return ingestCall<WindowShotRecord>(ctx, "/internal/shot/window/stop", "POST", {
    shot_id: shotId,
    ended_at_ms: endedAtMs,
  });
}

/** Reads one coordinator registry row (window or legacy), or null when absent. */
export async function getShotRecord(ctx: ToolContext, shotId: string): Promise<WindowShotRecord | null> {
  requireShotId(shotId);
  try {
    return await ingestCall<WindowShotRecord>(ctx, "/internal/shot/get", "POST", { shot_id: shotId });
  } catch (error) {
    if (error instanceof HttpError && error.status === 404) return null;
    throw error;
  }
}

/** Dispatches shot_get to the window registry or the legacy ShotDO. */
export async function dispatchGet(ctx: ToolContext, shotId: string): Promise<Record<string, unknown>> {
  const record = await getShotRecord(ctx, shotId);
  if (record && record.kind === "window") return windowShotInfo(record, ctx);
  return getShot(ctx, shotId);
}

/** Dispatches shot_stop to the window registry or the legacy ShotDO. */
export async function dispatchStop(
  ctx: ToolContext,
  shotId: string,
  endedAtMs?: number,
): Promise<Record<string, unknown>> {
  const record = await getShotRecord(ctx, shotId);
  if (record && record.kind === "window") {
    return windowShotInfo(await stopWindowShot(ctx, shotId, endedAtMs), ctx);
  }
  return stopShot(ctx, shotId);
}

/** Serializes a window shot for API/MCP callers (metadata only; no secrets). */
export function windowShotInfo(record: WindowShotRecord, ctx: ToolContext): Record<string, unknown> {
  const base = (ctx.publicBaseUrl ?? ctx.origin).replace(/\/+$/, "");
  return {
    shot_id: record.shot_id,
    kind: "window",
    name: record.name ?? undefined,
    state: record.state,
    owner: record.owner,
    window: record.window
      ? {
          start: new Date(record.window.start_ms).toISOString(),
          end: new Date(record.window.end_ms).toISOString(),
          provenance: record.window.provenance,
          clock_trust: record.window.clock_trust,
          instances: record.window.instances,
        }
      : undefined,
    query_watermark: record.query_watermark_ms === null ? undefined : new Date(record.query_watermark_ms).toISOString(),
    coverage: record.coverage ?? undefined,
    created_at: new Date(record.created_at).toISOString(),
    stopped_at: record.stopped_at === null ? undefined : new Date(record.stopped_at).toISOString(),
    otlp_base_url: `${base}/ingest`,
    ingest_note:
      "this is a logical window marker; ingestion is continuous and not tied to this shot, so no per-shot token is issued",
  };
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

// --------------------------------------------------------- compaction

/** Compaction backlog/health (spec 018). admin|member; never returns secrets. */
export async function compactionStatus(ctx: ToolContext): Promise<Record<string, unknown>> {
  const stub = compactorStub(ctx);
  return doJson(stub, "/internal/status", "GET");
}

/**
 * Triggers one bounded compaction tick (or an explicit GC pass). Admin only:
 * it mutates the journal/segment lifecycle, so it must not be reachable by a
 * push-only instance token or an arbitrary member.
 */
export async function compactionRun(
  ctx: ToolContext,
  opts: { gc?: boolean } = {},
): Promise<Record<string, unknown>> {
  const stub = compactorStub(ctx);
  return doJson(stub, opts.gc === true ? "/internal/gc" : "/internal/trigger", "POST");
}

function compactorStub(ctx: ToolContext): DoStubLike {
  if (!ctx.compactor) {
    throw new HttpError(503, "compaction coordinator is not configured", true, "compactor_unavailable");
  }
  return ctx.compactor.getByName(COMPACTOR_DO_NAME);
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
  const record = await getShotRecord(ctx, shotId);
  if (record && record.kind === "window") {
    return withWindowSession(ctx, record, async (sessionId) => {
      const stub = ctx.namespace.getByName(record.shot_id);
      return doJson(stub, "/internal/window/query", "POST", { session_id: sessionId, sql, max_rows: maxRows });
    });
  }
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
  const record = await getShotRecord(ctx, shotId);
  if (record && record.kind === "window") {
    return withWindowSession(ctx, record, async (sessionId) => {
      const stub = ctx.namespace.getByName(record.shot_id);
      return doJson(stub, "/internal/window/analyze", "POST", {
        session_id: sessionId,
        shot_id: record.shot_id,
        sections,
        top_n: topN,
      });
    });
  }
  const stub = ctx.namespace.getByName(shotId);
  return doJson(stub, "/internal/analysis/analyze", "POST", { sections, top_n: topN });
}

export async function schemaShot(ctx: ToolContext, shotId: string): Promise<Record<string, unknown>> {
  requireShotId(shotId);
  const record = await getShotRecord(ctx, shotId);
  const stub = ctx.namespace.getByName(shotId);
  if (record && record.kind === "window") {
    return doJson(stub, "/internal/window/schema", "GET");
  }
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

  const leftRecord = await getShotRecord(ctx, leftId);
  const rightRecord = await getShotRecord(ctx, rightId);
  if ((leftRecord?.kind === "window") !== (rightRecord?.kind === "window")) {
    throw badRequest("cannot compare a logical window shot with a legacy frozen shot");
  }
  if (leftRecord?.kind === "window" && rightRecord?.kind === "window") {
    const leftStub = ctx.namespace.getByName(leftId);
    return withWindowSessions(ctx, leftStub, [
      { record: leftRecord, sessionId: `left-${leftId}` },
      { record: rightRecord, sessionId: `right-${rightId}` },
    ], async () => {
      return doJson(leftStub, "/internal/window/compare", "POST", {
        left_session_id: `left-${leftId}`,
        right_session_id: `right-${rightId}`,
        top_n: topN,
      });
    }, [`left-${leftId}`, `right-${rightId}`]);
  }

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

// -------------------------------------------------- window analysis driver

/** Converts a stored window into the Go analysis wire shape. */
function windowWire(window: ShotWindow): Record<string, unknown> {
  return {
    start: new Date(window.start_ms).toISOString(),
    end: new Date(window.end_ms).toISOString(),
    provenance: window.provenance,
    clock_trust: window.clock_trust,
    instances: window.instances,
    name: window.name,
  };
}

function segmentMetaWire(seg: SegmentManifestRow, instanceName: string): Record<string, unknown> {
  return {
    key: seg.segment_key,
    instance_id: seg.instance_id,
    instance: instanceName,
    status: seg.status,
    size: seg.size_bytes,
    sha256: seg.sha256,
    first_received_at: seg.first_received_at,
    last_received_at: seg.last_received_at,
    event_min_ns: seg.event_min_ns,
    event_max_ns: seg.event_max_ns,
  };
}

/** Lists every advertised segment (bounded pagination). */
async function listAllSegments(ctx: ToolContext): Promise<SegmentManifestRow[]> {
  const out: SegmentManifestRow[] = [];
  let offset = 0;
  for (let page = 0; page < 10; page++) {
    const res = await ingestCall<{ segments: SegmentManifestRow[]; total: number }>(ctx, "/internal/segment/list", "POST", {
      limit: 200,
      offset,
    });
    out.push(...res.segments);
    if (out.length >= res.total || res.segments.length === 0) break;
    offset += res.segments.length;
  }
  return out;
}

/** Streams the selected segments of one window into a session on `stub`. */
async function openWindowSession(
  ctx: ToolContext,
  stub: { fetch(request: Request): Promise<Response> },
  sessionId: string,
  window: ShotWindow,
  catalog: SegmentManifestRow[],
): Promise<{ selected: number; available: number; missing: string[]; pending_journal: number }> {
  if (!ctx.bucket) throw new HttpError(503, "window analysis requires the object bucket binding", true, "bucket_unavailable");
  const instances = await ingestCall<{ instances: InstanceMetadata[] }>(ctx, "/internal/instance/list", "GET").catch(
    () => ({ instances: [] as InstanceMetadata[] }),
  );
  const nameById = new Map(instances.instances.map((i) => [i.instance_id, i.name]));
  const metas: WindowSegmentMeta[] = catalog.map((s) => ({
    segment_key: s.segment_key,
    instance_id: s.instance_id,
    instance: nameById.get(s.instance_id) ?? "",
    first_received_at: s.first_received_at,
    last_received_at: s.last_received_at,
    event_min_ns: s.event_min_ns,
    event_max_ns: s.event_max_ns,
    status: s.status,
  }));
  const selection = selectSegments(window, metas);
  const byKey = new Map(catalog.map((s) => [s.segment_key, s]));
  await doJson(stub, "/internal/window/open", "POST", {
    session_id: sessionId,
    window: windowWire(window),
    catalog: catalog.map((s) => segmentMetaWire(s, nameById.get(s.instance_id) ?? "")),
  });
  const missing = [...selection.missing];
  let available = 0;
  for (const sel of selection.selected) {
    const row = byKey.get(sel.segment_key);
    if (!row) {
      missing.push(sel.segment_key);
      continue;
    }
    const object = await ctx.bucket.get(row.object_key);
    if (!object || !object.body) {
      missing.push(sel.segment_key);
      continue;
    }
    await doRawJson(stub, "/internal/window/segment", {
      "content-type": "application/octet-stream",
      "x-burstel-session-id": sessionId,
      "x-burstel-segment-key": row.segment_key,
      "x-burstel-segment-instance": "",
      "x-burstel-size": String(row.size_bytes),
      "x-burstel-sha256": row.sha256,
    }, object.body);
    available += 1;
  }
  await doJson(stub, "/internal/window/commit", "POST", { session_id: sessionId });
  // Disclose un-compacted journal data in the window: never claim empty/complete
  // when the journal still holds data that has not become a segment yet.
  const windowNames = new Set(window.instances);
  const instanceIds =
    windowNames.size === 0
      ? []
      : instances.instances.filter((i) => windowNames.has(i.name)).map((i) => i.instance_id);
  const pending = await ingestCall<{ pending_journal: number }>(ctx, "/internal/compaction/pending", "POST", {
    instance_ids: instanceIds,
    from_ms: window.start_ms,
    to_ms: window.end_ms,
  }).catch(() => ({ pending_journal: 0 }));
  return { selected: selection.selected.length, available, missing, pending_journal: pending.pending_journal };
}

/** Runs `run` against a freshly committed window session, then releases it. */
async function withWindowSession<T>(
  ctx: ToolContext,
  record: WindowShotRecord,
  run: (sessionId: string) => Promise<T>,
): Promise<T> {
  if (!record.window) throw badRequest("shot is not a window shot");
  const stub = ctx.namespace.getByName(record.shot_id);
  const sessionId = `w-${record.shot_id}`;
  const catalog = await listAllSegments(ctx);
  const result = await openWindowSession(ctx, stub, sessionId, record.window, catalog);
  await syncCoverage(ctx, record, catalog, result);
  try {
    return await run(sessionId);
  } finally {
    await doJson(stub, "/internal/window/release", "POST", { session_id: sessionId }).catch(() => undefined);
  }
}

async function withWindowSessions<T>(
  ctx: ToolContext,
  stub: { fetch(request: Request): Promise<Response> },
  sessions: Array<{ record: WindowShotRecord; sessionId: string }>,
  run: () => Promise<T>,
  releaseIds: string[],
): Promise<T> {
  if (!ctx.bucket) throw new HttpError(503, "window analysis requires the object bucket binding", true, "bucket_unavailable");
  const catalog = await listAllSegments(ctx);
  for (const session of sessions) {
    if (!session.record.window) throw badRequest("shot is not a window shot");
    const result = await openWindowSession(ctx, stub, session.sessionId, session.record.window, catalog);
    await syncCoverage(ctx, session.record, catalog, result);
  }
  try {
    return await run();
  } finally {
    for (const id of releaseIds) {
      await doJson(stub, "/internal/window/release", "POST", { session_id: id }).catch(() => undefined);
    }
  }
}

/** Persists the coverage computed for a window shot (best effort). */
async function syncCoverage(
  ctx: ToolContext,
  record: WindowShotRecord,
  catalog: SegmentManifestRow[],
  result: { missing?: string[]; pending_journal?: number },
): Promise<void> {
  const missing = result.missing ?? [];
  const pending = result.pending_journal ?? 0;
  try {
    await ingestCall(ctx, "/internal/shot/window/coverage", "POST", {
      shot_id: record.shot_id,
      coverage: {
        expected_segments: catalog.length,
        available_segments: catalog.length - missing.length,
        missing_segments: missing,
        instances: record.window?.instances ?? [],
        pending_journal: pending,
        complete: missing.length === 0 && pending === 0,
      },
    });
  } catch {
    // Coverage disclosure is best-effort metadata; never fail the analysis.
  }
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

async function doRawJson(
  stub: { fetch(request: Request): Promise<Response> },
  path: string,
  headers: Record<string, string>,
  body: ReadableStream<Uint8Array> | ArrayBuffer | Uint8Array,
): Promise<Record<string, unknown>> {
  const init = { method: "POST", headers, body, duplex: "half" } as RequestInit;
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
  const window = entry.window
    ? {
        start: new Date(entry.window.start_ms).toISOString(),
        end: new Date(entry.window.end_ms).toISOString(),
        provenance: entry.window.provenance,
        clock_trust: entry.window.clock_trust,
        instances: entry.window.instances,
      }
    : undefined;
  if (info) {
    return {
      shot_id: entry.shot_id,
      kind: entry.kind ?? "recording",
      state: info.state,
      owner: info.owner ?? entry.owner,
      name: entry.name ?? undefined,
      window,
      coverage: entry.coverage ?? undefined,
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
    kind: entry.kind ?? "recording",
    state: entry.state,
    owner: entry.owner,
    name: entry.name ?? undefined,
    window,
    coverage: entry.coverage ?? undefined,
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
