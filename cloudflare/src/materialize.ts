// Shot-time parallel materialization driver (spec 031 D3).
//
// The ControlDO enumerates the journal objects in a shot's receive-time range.
// This module splits that bounded list into contiguous, byte-balanced chunks and
// assigns each chunk to a distinct materializer Container (`CompactorDO` name
// `materializer-<shot>-<i>`). Each container reads `journal/` and writes
// `shots/<shot>/part-<i>.duckdb` directly through its bucket-scoped R2
// credential, so the Worker never relays object bytes.
//
// The shot record is the durable state machine: `pending` -> `running` ->
// `complete` (or `failed`). Part keys are deterministic, so re-running a shot
// replaces its parts instead of duplicating spans.

import { DO_ORIGIN } from "./env";
import type { CompactorNamespaceLike, DoStubLike, IngestNamespaceLike } from "./env";
import { HttpError } from "./errors";
import type {
  MaterializationObject,
  MaterializationPart,
  MaterializationPlan,
  MaterializationRecord,
  WindowShotRecord,
} from "./ingest-core";

export const MATERIALIZE_PARALLELISM = 4;
export const MATERIALIZE_WAIT_MS = 90_000;
export const MATERIALIZE_MAX_INSTANCES = 4;

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
  materialize?: MaterializeOptions;
}

/** One contiguous, byte-balanced chunk of journal objects. */
export interface MaterializeChunk {
  index: number;
  bytes: number;
  objects: MaterializationObject[];
}

/**
 * Splits journal objects (already sorted by receive time) into at most
 * `parallelism` contiguous chunks, balancing total bytes. Contiguity keeps each
 * part's receive-time range coherent for diagnostics.
 */
export function chunkMaterializationPlan(
  objects: MaterializationObject[],
  parallelism: number,
): MaterializeChunk[] {
  if (objects.length === 0) return [];
  const parts = Math.max(1, Math.min(Math.floor(parallelism) || 1, objects.length));
  const totalBytes = objects.reduce((n, o) => n + Math.max(0, o.size), 0);
  const target = Math.max(1, Math.ceil(totalBytes / parts));
  const chunks: MaterializeChunk[] = [];
  let current: MaterializeChunk = { index: 0, bytes: 0, objects: [] };
  for (let i = 0; i < objects.length; i++) {
    const object = objects[i]!;
    // Leave at least one object for every remaining chunk so no part is empty.
    const partsLeft = parts - chunks.length;
    const objectsLeft = objects.length - i;
    const mustSplit =
      current.objects.length > 0 &&
      partsLeft > 1 &&
      current.bytes + Math.max(0, object.size) > target &&
      objectsLeft >= partsLeft;
    if (mustSplit) {
      chunks.push(current);
      current = { index: chunks.length, bytes: 0, objects: [] };
    }
    current.objects.push(object);
    current.bytes += Math.max(0, object.size);
  }
  if (current.objects.length > 0) chunks.push(current);
  for (let i = 0; i < chunks.length; i++) chunks[i]!.index = i;
  return chunks;
}

/** Starts (or joins) materialization and waits up to `waitMs` for completion. */
export async function ensureMaterialized(
  ctx: MaterializeContext,
  record: WindowShotRecord,
  options: MaterializeOptions = {},
): Promise<WindowShotRecord> {
  const merged = { ...ctx.materialize, ...options };
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

/** Runs one materialization attempt for a shot. Idempotent per part key. */
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
  const chunks = chunkMaterializationPlan(plan.objects, parallelism);
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

  const started = now();
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

async function materializeChunk(
  compactor: CompactorNamespaceLike,
  shotId: string,
  chunk: MaterializeChunk,
  partTotal: number,
  excluded: string[],
): Promise<MaterializationPart> {
  const stub: DoStubLike = compactor.getByName(`materializer-${shotId}-${chunk.index}`);
  const resp = await stub.fetch(
    new Request(`${DO_ORIGIN}/internal/materialize`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        shot_id: shotId,
        part_index: chunk.index,
        part_total: partTotal,
        objects: chunk.objects,
        excluded,
      }),
    }),
  );
  const text = await resp.text();
  if (resp.status < 200 || resp.status >= 300) {
    let message = `materializer part ${chunk.index} failed with status ${resp.status}`;
    try {
      const body = JSON.parse(text) as { error?: string };
      if (body.error) message = body.error;
    } catch {
      // keep the status message
    }
    throw new HttpError(503, message, true, "materialize_failed");
  }
  let raw: Record<string, unknown>;
  try {
    raw = JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new HttpError(503, `materializer part ${chunk.index} returned invalid JSON`, true, "materialize_failed");
  }
  return {
    index: chunk.index,
    object_key: String(raw.part_key ?? ""),
    size_bytes: num(raw.size_bytes),
    sha256: String(raw.sha256 ?? ""),
    object_count: num(raw.object_count),
    request_count: num(raw.request_count),
    record_count: num(raw.record_count),
    span_count: num(raw.span_count),
    log_count: num(raw.log_count),
    metric_count: num(raw.metric_count),
    first_received_at: num(raw.first_received_at),
    last_received_at: num(raw.last_received_at),
  };
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

function num(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
