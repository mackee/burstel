// Pure part-planning and part-execution helpers for shot materialization.
//
// Spec 031 introduced the byte-balanced chunking and the direct-to-R2
// materializer request. Spec 032 keeps those primitives but moves the *driver*
// (planning, leases, retries and terminal state) into a Durable Object alarm;
// this module stays free of any Durable Object runtime so both the driver and
// pure unit tests can use it.

import { DO_ORIGIN } from "./env";
import type { CompactorNamespaceLike, DoStubLike } from "./env";
import { HttpError } from "./errors";
import type { MaterializationObject, MaterializationPart } from "./ingest-core";

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
      // After pushing `current`, one of the remaining target parts is the new
      // chunk being started now, so only `partsLeft - 1` parts still need a
      // future object.
      objectsLeft >= partsLeft - 1;
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
