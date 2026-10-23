// Durable continuous OTLP/HTTP intake (spec 014), retrofitted for a DO-free
// hot path (spec 023, D1/D2).
//
// The stable `/ingest/v1/{traces,metrics,logs}` endpoints authenticate a
// push-only *instance* token (token -> instance, never token -> shot) and
// durably journal the raw export into R2 before acknowledging it with a valid
// OTLP ExportResponse.
//
// Hot path (no Durable Object call on a cache hit):
//   1. admit the instance token from memory -> KV -> authoritative ControlDO;
//   2. validate the body (protobuf/gzip/JSON) — malformed input is rejected;
//   3. PUT the original, unmodified bytes to a unique, server-generated R2
//      object (`journal/{instance_id}/{signal}/{received_at}-{id}.bin`) with
//      `token_id` in custom metadata (used by compaction-time revocation);
//   4. late-write guard: if the PUT completed after
//      `received_at + LATE_WRITE_LIMIT_MS`, delete it and answer 503 so the
//      exporter retries with a fresh `received_at`;
//   5. only then return OTLP 200.
//
// There is no `journal_ledger` write and no `/internal/journal/commit` on the
// ingest path. Counters go to Workers Analytics Engine (best effort) and the
// compactor discovers work by listing R2 (spec 023, D3).
//
// If step 3 fails the request fails closed with a retryable 5xx, so an upstream
// exporter retries. Duplicate delivery under upstream retry is bounded and
// documented: each accepted export is a distinct, immutable object, and
// compaction de-duplicates by key/sha256 against the manifest.

import { admitInstanceToken } from "./admission";
import { recordIngestPoint } from "./analytics";
import { extractToken } from "./auth";
import { sha256HexBytes } from "./crypto";
import { JOURNAL_PREFIX, lateWriteLimitMs, maxIngestBytes } from "./env";
import type { AppEnv } from "./env";
import { badRequest, HttpError } from "./errors";
import { json } from "./http";
import { randomHexId } from "./crypto";
import type { Signal } from "./store";

const PROTOBUF_TYPES = new Set(["application/x-protobuf", "application/protobuf", "application/vnd.google.protobuf"]);
const JSON_TYPES = new Set(["application/json"]);

/** Handles one ingestion request against the stable instance endpoint. */
export async function handleInstanceIngest(
  request: Request,
  env: AppEnv,
  signal: Signal,
): Promise<Response> {
  const clock = env.now ?? (() => Date.now());
  const receivedAt = clock();
  const token = extractToken(request);
  const admitted = await admitInstanceToken(env, token, receivedAt);
  if (!admitted.ok) {
    recordIngestPoint(env, {
      status: "rejected",
      reason: `invalid_token_${admitted.reason}`,
      signal,
      bytes: 0,
      received_at: receivedAt,
      duration_ms: clock() - receivedAt,
    });
    return json(401, {
      error: `instance token ${admitted.reason}`,
      code: `instance_token_${admitted.reason}`,
      reason: admitted.reason,
    });
  }

  const contentType = normalizeContentType(request.headers.get("content-type"));
  const contentEncoding = normalizeContentEncoding(request.headers.get("content-encoding"));
  let bytes = new Uint8Array(0);

  try {
    // Respect the request size bound before buffering anything durable.
    const declared = Number(request.headers.get("content-length") ?? "");
    const limit = maxIngestBytes(env);
    if (Number.isFinite(declared) && declared > limit) {
      throw new HttpError(413, `OTLP body exceeds the ${limit}-byte limit`, false, "payload_too_large");
    }
    bytes = new Uint8Array(await request.arrayBuffer());
    if (bytes.byteLength === 0) throw badRequest("empty OTLP body");
    if (bytes.byteLength > limit) {
      throw new HttpError(413, `OTLP body exceeds the ${limit}-byte limit`, false, "payload_too_large");
    }

    // Validate before persisting so malformed input never becomes durable.
    await validateBody(bytes, contentType, contentEncoding, signal);

    if (!env.BUCKET) {
      throw new HttpError(503, "durable journal bucket is not configured", true, "journal_unavailable");
    }

    const key = journalKey(admitted.instance_id, signal, receivedAt);
    const sha256 = await sha256HexBytes(bytes);

    // Durable raw journal write, preserving the original bytes and encoding.
    try {
      const object = await env.BUCKET.put(key, bytes, {
        sha256,
        httpMetadata: { contentType: contentType || "application/octet-stream" },
        customMetadata: {
          signal,
          instance_id: admitted.instance_id,
          instance: admitted.instance_name,
          token_id: admitted.token_id,
          received_at: String(receivedAt),
          sha256,
          content_encoding: contentEncoding,
          content_type: contentType,
        },
      });
      if (!object) throw new Error("R2 PUT returned null");
    } catch (error) {
      throw new HttpError(
        503,
        `durable journal write failed: ${error instanceof Error ? error.message : String(error)}`,
        true,
        "journal_write_failed",
      );
    }

    // Late-write guard: a very slow PUT may land after the compactor could have
    // finalized (and later re-listed) the time bucket. Delete it and let the
    // exporter retry, so every acknowledged object is safely inside grace.
    const completedAt = clock();
    const lateLimit = lateWriteLimitMs(env);
    if (lateLimit > 0 && completedAt - receivedAt > lateLimit) {
      await env.BUCKET.delete?.(key).catch(() => undefined);
      throw new HttpError(
        503,
        `journal write completed ${completedAt - receivedAt}ms after receipt, above the ${lateLimit}ms limit`,
        true,
        "late_write",
      );
    }

    recordIngestPoint(env, {
      status: "accepted",
      signal,
      instance_id: admitted.instance_id,
      instance: admitted.instance_name,
      token_id: admitted.token_id,
      bytes: bytes.byteLength,
      received_at: receivedAt,
      duration_ms: completedAt - receivedAt,
    });

    return exportResponse(contentType, {
      "x-burstel-instance-id": admitted.instance_id,
      "x-burstel-instance": admitted.instance_name,
      "x-burstel-disposition": "journaled",
    });
  } catch (error) {
    const reason = error instanceof HttpError ? error.code : "internal";
    recordIngestPoint(env, {
      status: "rejected",
      reason,
      signal,
      instance_id: admitted.instance_id,
      instance: admitted.instance_name,
      token_id: admitted.token_id,
      bytes: bytes.byteLength,
      received_at: receivedAt,
      duration_ms: clock() - receivedAt,
    });
    throw error;
  }
}

/** Server-generated, unique, path-safe R2 key for one accepted export. */
export function journalKey(instanceId: string, signal: Signal, receivedAt: number): string {
  return `${JOURNAL_PREFIX}/${instanceId}/${signal}/${receivedAt}-${randomHexId(12)}.bin`;
}

function exportResponse(contentType: string, headers: Record<string, string>): Response {
  if (JSON_TYPES.has(contentType)) {
    return new Response("{}", {
      status: 200,
      headers: { "content-type": "application/json", "cache-control": "no-store", ...headers },
    });
  }
  // An empty proto message is a valid Export{Trace,Metric,Log}ServiceResponse.
  return new Response(new Uint8Array(0), {
    status: 200,
    headers: { "content-type": "application/x-protobuf", "cache-control": "no-store", ...headers },
  });
}

function normalizeContentType(raw: string | null): string {
  return (raw ?? "").split(";")[0]!.trim().toLowerCase();
}

function normalizeContentEncoding(raw: string | null): string {
  return (raw ?? "").trim().toLowerCase();
}

/**
 * Validates an OTLP/HTTP body. Follows OTLP/HTTP: only protobuf and JSON are
 * accepted, an optional gzip content-encoding is verified by decompressing, and
 * malformed input is rejected instead of being acknowledged.
 */
export async function validateBody(
  bytes: Uint8Array,
  contentType: string,
  contentEncoding: string,
  signal: Signal,
): Promise<void> {
  if (contentEncoding !== "" && contentEncoding !== "gzip") {
    throw new HttpError(415, `unsupported content-encoding ${contentEncoding}`, false, "unsupported_encoding");
  }
  if (!PROTOBUF_TYPES.has(contentType) && !JSON_TYPES.has(contentType)) {
    throw new HttpError(415, `unsupported content-type ${contentType || "(missing)"}`, false, "unsupported_media_type");
  }

  let decoded = bytes;
  if (contentEncoding === "gzip") {
    try {
      decoded = await gunzip(bytes);
    } catch {
      throw badRequest("invalid gzip body");
    }
  }
  if (decoded.byteLength === 0) throw badRequest("empty OTLP body");

  if (JSON_TYPES.has(contentType)) {
    try {
      const parsed = JSON.parse(new TextDecoder().decode(decoded));
      if (parsed === null || typeof parsed !== "object") throw new Error("not an object");
    } catch {
      throw badRequest("invalid OTLP/JSON body");
    }
    return;
  }

  if (!isWellFormedProtobuf(decoded)) {
    throw badRequest(`invalid OTLP/${signal} protobuf body`);
  }
}

async function gunzip(bytes: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/**
 * Minimal structural check of a protobuf message: walks the top-level fields
 * and rejects truncated varints, invalid wire types, invalid field numbers and
 * length-delimited fields whose declared length overruns the buffer. This
 * catches obviously malformed bodies without pulling in a full protobuf
 * runtime.
 */
export function isWellFormedProtobuf(bytes: Uint8Array): boolean {
  let i = 0;
  while (i < bytes.length) {
    const tag = readVarint(bytes, i);
    if (tag === null) return false;
    i = tag.next;
    const field = tag.value >>> 3;
    const wire = tag.value & 7;
    if (field === 0) return false;
    switch (wire) {
      case 0: {
        const value = readVarint(bytes, i);
        if (value === null) return false;
        i = value.next;
        break;
      }
      case 1:
        i += 8;
        break;
      case 2: {
        const len = readVarint(bytes, i);
        if (len === null) return false;
        i = len.next;
        if (i + len.value > bytes.length) return false;
        i += len.value;
        break;
      }
      case 5:
        i += 4;
        break;
      default:
        return false;
    }
    if (i > bytes.length) return false;
  }
  return true;
}

function readVarint(bytes: Uint8Array, start: number): { value: number; next: number } | null {
  let value = 0;
  let shift = 0;
  let i = start;
  while (i < bytes.length) {
    const b = bytes[i]!;
    value |= (b & 0x7f) << shift;
    i += 1;
    if ((b & 0x80) === 0) return { value: value >>> 0, next: i };
    shift += 7;
    if (shift > 63) return null;
  }
  return null;
}
