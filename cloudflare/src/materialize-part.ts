// Pure part-planning and part-execution helpers for shot materialization.
//
// Spec 031 introduced the byte-balanced chunking and the direct-to-R2
// materializer request. Spec 032 keeps those primitives but moves the *driver*
// (planning, leases, retries and terminal state) into a Durable Object alarm;
// this module stays free of any Durable Object runtime so both the driver and
// pure unit tests can use it.
//
// Spec 034 changes the planning basis: parts are sized by an *estimated span
// count* (from the stored journal bytes and a measured spans-per-KiB ratio),
// not by bytes alone. A byte-balanced 3-way split produced ~1.34M-span parts
// that could not be built inside the timeout; a 500k-span ceiling keeps every
// part inside the build budget while still saturating the container pool.

import { DO_ORIGIN } from "./env";
import type { CompactorNamespaceLike, DoStubLike } from "./env";
import { HttpError } from "./errors";
import type { MaterializationObject, MaterializationPart } from "./ingest-core";

/** Default ceiling for the estimated span count of one part (spec 034 R4). */
export const MATERIALIZE_PART_TARGET_SPANS = 500_000;
/**
 * Measured spans per KiB of stored (gzip) journal bytes. The staging evidence
 * (196 MB gzip -> ~8.0M spans, spec 034) gives ~40 spans/KiB.
 */
export const MATERIALIZE_SPANS_PER_KIB = 40;
/** The Go service rejects more than 64 parts for one shot. */
export const MATERIALIZE_MAX_PARTS = 64;

/** One contiguous, provenance-balanced chunk of journal objects. */
export interface MaterializeChunk {
  index: number;
  bytes: number;
  /** Estimated spans in this chunk (bytes x measured ratio). */
  spans: number;
  objects: MaterializationObject[];
}

export interface PartPlanOptions {
  /** Hard target for estimated spans per part (default 500k). */
  targetSpans?: number;
  /** Measured spans per KiB of stored journal bytes (default 40). */
  spansPerKiB?: number;
  /** Upper bound on the number of parts (default 64). */
  maxParts?: number;
}

function positiveInt(value: number | undefined, fallback: number, max: number): number {
  const n = typeof value === "number" && Number.isFinite(value) ? Math.floor(value) : fallback;
  if (n <= 0) return fallback;
  return Math.min(n, max);
}

/** Estimated spans for a set of journal objects. */
export function estimatePlanSpans(
  objects: MaterializationObject[],
  spansPerKiB: number = MATERIALIZE_SPANS_PER_KIB,
): number {
  const ratio = positiveInt(spansPerKiB, MATERIALIZE_SPANS_PER_KIB, 1_000_000);
  const bytes = objects.reduce((n, o) => n + Math.max(0, o.size), 0);
  return (bytes / 1024) * ratio;
}

/**
 * Splits journal objects (already sorted by receive time) into contiguous
 * chunks so that each chunk's *estimated span count* stays at or below
 * `targetSpans`, while creating at least `parallelism` chunks so the fixed
 * materializer pool stays busy. Contiguity keeps each part's receive-time range
 * coherent for diagnostics.
 */
export function chunkMaterializationPlan(
  objects: MaterializationObject[],
  parallelism: number,
  options: PartPlanOptions = {},
): MaterializeChunk[] {
  if (objects.length === 0) return [];
  const targetSpans = positiveInt(options.targetSpans, MATERIALIZE_PART_TARGET_SPANS, 1_000_000_000);
  const spansPerKiB = positiveInt(options.spansPerKiB, MATERIALIZE_SPANS_PER_KIB, 1_000_000);
  const maxParts = positiveInt(options.maxParts, MATERIALIZE_MAX_PARTS, MATERIALIZE_MAX_PARTS);

  const totalBytes = objects.reduce((n, o) => n + Math.max(0, o.size), 0);
  const totalSpans = estimatePlanSpans(objects, spansPerKiB);
  const minParts = Math.max(1, Math.floor(parallelism) || 1);
  const rowParts = Math.max(1, Math.ceil(totalSpans / targetSpans));
  const parts = Math.max(1, Math.min(Math.max(minParts, rowParts), maxParts, objects.length));
  const targetBytesPerPart = Math.max(1, totalBytes / parts);
  const targetSpansPerPart = Math.max(1, totalSpans / parts);

  const chunks: MaterializeChunk[] = [];
  let current: MaterializeChunk = { index: 0, bytes: 0, spans: 0, objects: [] };
  for (let i = 0; i < objects.length; i++) {
    const object = objects[i]!;
    const objectSpans = (Math.max(0, object.size) / 1024) * spansPerKiB;
    // Leave at least one object for every remaining chunk so no part is empty.
    const partsLeft = parts - chunks.length;
    const objectsLeft = objects.length - i;
    const mustSplit =
      current.objects.length > 0 &&
      partsLeft > 1 &&
      objectsLeft >= partsLeft - 1 &&
      (current.spans + objectSpans > targetSpansPerPart || current.bytes + Math.max(0, object.size) > targetBytesPerPart);
    if (mustSplit) {
      chunks.push(current);
      current = { index: chunks.length, bytes: 0, spans: 0, objects: [] };
    }
    current.objects.push(object);
    current.bytes += Math.max(0, object.size);
    current.spans += objectSpans;
  }
  if (current.objects.length > 0) chunks.push(current);
  for (let i = 0; i < chunks.length; i++) chunks[i]!.index = i;
  return chunks;
}

/**
 * Fixed materializer pool member name (spec 033). The pool is
 * `materializer-0 .. materializer-(P-1)`: deterministic names are what let a
 * later shot reuse an already-warm container instead of exhausting the
 * CompactorDO instance limit with one DO name per shot.
 */
export function materializerPoolName(member: number): string {
  return `materializer-${member}`;
}

/**
 * Runs one part on a pool member Container. The container reads the journal
 * objects and writes `shots/<shot>/part-<i>.duckdb` directly through its
 * bucket-scoped R2 credential, so no object bytes pass through the Worker.
 */
export async function materializeChunk(
  compactor: CompactorNamespaceLike,
  shotId: string,
  chunk: MaterializeChunk,
  partTotal: number,
  excluded: string[],
  poolMember: number = chunk.index,
): Promise<MaterializationPart> {
  const stub: DoStubLike = compactor.getByName(materializerPoolName(poolMember));
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

function num(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}
