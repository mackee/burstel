// Shot-time parallel materialization client/helpers.
//
// Spec 031 introduced the journal -> part plan and the materializer containers.
// Spec 032 moves the durable driver into `MaterializeDO` (see
// materialize-driver.ts): the Worker only records/schedules a run and may wait
// for the record to reach a terminal state. The legacy direct path below is
// kept for pure unit tests and for installations without the coordinator
// binding; production routes through the Durable Object alarm.

import { DO_ORIGIN } from "./env";
import type { CompactorNamespaceLike, IngestNamespaceLike, MaterializerNamespaceLike } from "./env";
import { HttpError } from "./errors";
import type {
  MaterializationPlan,
  MaterializationRecord,
  WindowShotRecord,
} from "./ingest-core";
import { chunkMaterializationPlan, materializeChunk } from "./materialize-part";
import type { MaterializeChunk } from "./materialize-part";

export {
  MATERIALIZE_MAX_PARTS,
  MATERIALIZE_PART_TARGET_SPANS,
  MATERIALIZE_SPANS_PER_KIB,
  chunkMaterializationPlan,
  estimatePlanSpans,
  materializeChunk,
  materializerPoolName,
} from "./materialize-part";
export type { MaterializeChunk } from "./materialize-part";

/**
 * Spec 033: fixed materializer pool size. It must stay at least one instance
 * below the CompactorDO `max_instances` so continuous compaction always has a
 * slot (`P <= max_instances - 1`).
 */
export const MATERIALIZE_PARALLELISM = 3;
export const MATERIALIZE_WAIT_MS = 90_000;
export const MATERIALIZE_MAX_INSTANCES = 4;
/** How long a pool member keeps its container warm for the next shot. */
export const MATERIALIZE_KEEPALIVE_MS = 10 * 60 * 1000;
/** Backoff after "no container instance" before a queued part is retried. */
export const MATERIALIZE_CAPACITY_BACKOFF_MS = 10_000;

export interface MaterializeOptions {
  parallelism?: number;
  maxInstances?: number;
  waitMs?: number;
  force?: boolean;
  /** Test seam; production uses Date.now. */
  now?: () => number;
}

export interface MaterializeContext {
  ingest: IngestNamespaceLike;
  compactor?: CompactorNamespaceLike;
  /** Spec 032 alarm-driven coordinator, keyed by shot id. */
  materializer?: MaterializerNamespaceLike;
  materialize?: MaterializeOptions;
}

/** The coordinator request body accepted by `MaterializeDO`. */
export interface MaterializeCommand {
  shot_id: string;
  force?: boolean;
  restart?: boolean;
}

/** Starts (or joins) materialization and waits up to `waitMs` for completion. */
export async function ensureMaterialized(
  ctx: MaterializeContext,
  record: WindowShotRecord,
  options: MaterializeOptions = {},
): Promise<WindowShotRecord> {
  const merged = { ...ctx.materialize, ...options };
  if (ctx.materializer) return ensureMaterializedViaCoordinator(ctx, record, merged);
  return ensureMaterializedDirect(ctx, record, merged);
}

async function ensureMaterializedDirect(
  ctx: MaterializeContext,
  record: WindowShotRecord,
  merged: MaterializeOptions,
): Promise<WindowShotRecord> {
  const status = record.materialization;
  if (!merged.force && status?.state === "complete") return record;
  if (status?.state === "running") {
    return waitForMaterialization(ctx, record.shot_id, merged.waitMs ?? MATERIALIZE_WAIT_MS);
  }
  if (!ctx.compactor) return record;
  const started = await materializeShot(ctx, record, merged);
  if (started.materialization?.state === "complete") return started;
  return waitForMaterialization(ctx, record.shot_id, merged.waitMs ?? MATERIALIZE_WAIT_MS);
}

/**
 * Spec 032 lazy path: a stale `running` (or a lost `pending`) run is restarted
 * by asking its coordinator DO; this Worker request only polls the durable
 * record. A terminal `failed` run reports its reason instead of silently
 * pretending the window is complete.
 */
async function ensureMaterializedViaCoordinator(
  ctx: MaterializeContext,
  record: WindowShotRecord,
  merged: MaterializeOptions,
): Promise<WindowShotRecord> {
  const status = record.materialization;
  if (!merged.force && status?.state === "complete") return record;
  if (!merged.force && status?.state === "failed") throw materializationFailed(record);
  const now = (merged.now ?? (() => Date.now()))();
  const stale = status?.state === "running" && isMaterializationLeaseStale(status, now);
  if (status === null || status === undefined || status.state === "pending" || stale || merged.force === true) {
    await requestMaterialization(ctx, record.shot_id, {
      force: merged.force === true,
      restart: stale,
    });
  }
  const waited = await waitForMaterialization(ctx, record.shot_id, merged.waitMs ?? MATERIALIZE_WAIT_MS);
  if (waited.materialization?.state === "failed") throw materializationFailed(waited);
  return waited;
}

/** True when a `running` run's lease has expired (or a legacy run has none). */
export function isMaterializationLeaseStale(record: MaterializationRecord, nowMs: number): boolean {
  if (record.state !== "running") return false;
  if (typeof record.lease_expires_at !== "number" || !Number.isFinite(record.lease_expires_at)) return true;
  return record.lease_expires_at <= nowMs;
}

function materializationFailed(record: WindowShotRecord): HttpError {
  const reason = record.materialization?.error ?? "unknown error";
  return new HttpError(
    409,
    `materialization for shot ${record.shot_id} failed: ${reason}; retry with \`shot materialize ${record.shot_id}\``,
    false,
    "materialization_failed",
  );
}

/** Records/schedules a run on the coordinator DO. Durable; never waits. */
export async function requestMaterialization(
  ctx: MaterializeContext,
  shotId: string,
  command: { force?: boolean; restart?: boolean } = {},
): Promise<Record<string, unknown>> {
  const stub = materializerStub(ctx, shotId);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/materialize`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        shot_id: shotId,
        force: command.force === true,
        restart: command.restart === true,
      }),
    }),
  );
  const text = await resp.text();
  if (resp.status < 200 || resp.status >= 300) {
    let message = `materialize schedule failed with status ${resp.status}`;
    let code = "materialize_schedule_failed";
    try {
      const parsed = JSON.parse(text) as { error?: string; code?: string };
      if (parsed.error) message = parsed.error;
      if (parsed.code) code = parsed.code;
    } catch {
      // keep default
    }
    throw new HttpError(resp.status, message, resp.status >= 500, code);
  }
  return text.trim() === "" ? {} : (JSON.parse(text) as Record<string, unknown>);
}

/**
 * Spec 033: ask the coordinator to prewarm its fixed materializer pool. Only a
 * durable flag is written here; the DO alarm starts the containers.
 */
export async function prewarmMaterialization(ctx: MaterializeContext, shotId: string): Promise<void> {
  if (!ctx.materializer) return;
  const stub = ctx.materializer.getByName(shotId);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/prewarm`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ shot_id: shotId }),
    }),
  );
  await resp.body?.cancel().catch(() => undefined);
  if (resp.status < 200 || resp.status >= 300) {
    throw new HttpError(resp.status, `materializer prewarm failed with status ${resp.status}`, true, "prewarm_failed");
  }
}

/** Convenience wrapper used by shot_stop and the explicit materialize action. */
export async function scheduleMaterialization(
  ctx: MaterializeContext,
  record: WindowShotRecord,
  options: { force?: boolean; restart?: boolean } = {},
): Promise<void> {
  if (!ctx.materializer) return;
  if (!record.window) return;
  await requestMaterialization(ctx, record.shot_id, options);
}

/** Runs one materialization attempt directly (legacy/test path). Idempotent per part key. */
export async function materializeShot(
  ctx: MaterializeContext,
  record: WindowShotRecord,
  options: MaterializeOptions = {},
): Promise<WindowShotRecord> {
  const compactor = ctx.compactor;
  if (!compactor) return record;
  if (!record.window) throw new HttpError(400, "shot is not a window shot", false, "bad_request");
  const now = options.now ?? (() => Date.now());
  const waitMs = options.waitMs ?? MATERIALIZE_WAIT_MS;
  const save = (materialization: MaterializationRecord) =>
    ingestCall<WindowShotRecord>(ctx.ingest, "/internal/shot/materialization", "POST", {
      shot_id: record.shot_id,
      materialization,
    });

  if (!options.force && record.materialization?.state === "complete") return record;

  const plan = await ingestCall<MaterializationPlan>(ctx.ingest, "/internal/shot/materialize-plan", "POST", {
    shot_id: record.shot_id,
  });
  const parallelism = Math.max(
    1,
    Math.min(
      Math.floor(options.parallelism ?? MATERIALIZE_PARALLELISM) || MATERIALIZE_PARALLELISM,
      Math.floor(options.maxInstances ?? MATERIALIZE_MAX_INSTANCES) || MATERIALIZE_MAX_INSTANCES,
    ),
  );
  const chunks: MaterializeChunk[] = chunkMaterializationPlan(plan.objects, parallelism);
  const startedAt = now();
  if (chunks.length === 0) {
    return save({
      state: "complete",
      parts_done: 0,
      parts_total: 0,
      spans: 0,
      started_at: startedAt,
      finished_at: startedAt,
      parts: [],
      error: null,
    });
  }

  await save({
    state: "running",
    parts_done: 0,
    parts_total: chunks.length,
    spans: 0,
    started_at: startedAt,
    finished_at: null,
    parts: [],
    error: null,
  });
  // The compaction yield flag is TTL-bounded: a crashed coordinator cannot
  // stall background retention forever.
  await ingestCall(ctx.ingest, "/internal/materialization/begin", "POST", {
    shot_id: record.shot_id,
    ttl_ms: Math.max(60_000, waitMs + 120_000),
  }).catch(() => undefined);

  try {
    const results = await Promise.all(
      chunks.map((chunk) =>
        materializeChunk(compactor, record.shot_id, chunk, chunks.length, plan.excluded),
      ),
    );
    results.sort((a, b) => a.index - b.index);
    const spans = results.reduce((n, part) => n + part.span_count, 0);
    return await save({
      state: "complete",
      parts_done: results.length,
      parts_total: chunks.length,
      spans,
      started_at: startedAt,
      finished_at: now(),
      parts: results,
      error: null,
    });
  } catch (error) {
    await save({
      state: "failed",
      parts_done: 0,
      parts_total: chunks.length,
      spans: 0,
      started_at: startedAt,
      finished_at: now(),
      parts: [],
      error: error instanceof Error ? error.message : String(error),
    }).catch(() => undefined);
    throw error;
  } finally {
    await ingestCall(ctx.ingest, "/internal/materialization/end", "POST", {
      shot_id: record.shot_id,
    }).catch(() => undefined);
  }
}

/** Polls the shot record until materialization is terminal or the deadline. */
export async function waitForMaterialization(
  ctx: MaterializeContext,
  shotId: string,
  timeoutMs: number,
): Promise<WindowShotRecord> {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  for (;;) {
    const record = await ingestCall<WindowShotRecord>(ctx.ingest, "/internal/shot/get", "POST", { shot_id: shotId });
    const state = record.materialization?.state;
    if (state === "complete" || state === "failed" || state === undefined) return record;
    if (Date.now() >= deadline) {
      throw new HttpError(
        504,
        `materialization for shot ${shotId} did not complete within ${timeoutMs}ms (state=${state})`,
        true,
        "materialization_timeout",
      );
    }
    await sleep(200);
  }
}

function materializerStub(ctx: MaterializeContext, shotId: string) {
  if (!ctx.materializer) {
    throw new HttpError(503, "materialization coordinator is not configured", true, "materializer_unavailable");
  }
  return ctx.materializer.getByName(shotId);
}

async function ingestCall<T>(
  ingest: IngestNamespaceLike,
  path: string,
  method: string,
  body?: unknown,
): Promise<T> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.headers = { "content-type": "application/json" };
    init.body = JSON.stringify(body);
  }
  const resp = await ingest.getByName("ingest-primary").fetch(new Request(`${DO_ORIGIN}${path}`, init));
  const text = await resp.text();
  if (resp.status < 200 || resp.status >= 300) {
    let message = `${path} failed with status ${resp.status}`;
    let code = "error";
    try {
      const parsed = JSON.parse(text) as { error?: string; code?: string };
      if (parsed.error) message = parsed.error;
      if (parsed.code) code = parsed.code;
    } catch {
      // keep defaults
    }
    throw new HttpError(resp.status, message, resp.status >= 500, code);
  }
  return JSON.parse(text) as T;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
