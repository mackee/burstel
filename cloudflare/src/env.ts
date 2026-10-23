// Shared Worker/DO types used by the HTTP app, the MCP server and the tools.

import type { R2BucketLike } from "./artifacts";

/** Structural subset of `DurableObjectStub`. */
export interface DoStubLike {
  fetch(request: Request): Promise<Response>;
}

/** Structural subset of `DurableObjectNamespace<ShotDO>`. */
export interface ShotNamespaceLike {
  getByName(name: string): DoStubLike;
}

/** Structural subset of the ControlDO (formerly IngestDO) namespace. */
export interface IngestNamespaceLike {
  getByName(name: string): DoStubLike;
}

/** Structural subset of `DurableObjectNamespace<AuthDO>`. */
export interface AuthNamespaceLike {
  getByName(name: string): DoStubLike;
}

/** Structural subset of `DurableObjectNamespace<CompactorDO>`. */
export interface CompactorNamespaceLike {
  getByName(name: string): DoStubLike;
}

/**
 * Structural subset of a Workers KV namespace (spec 023, D1).
 *
 * Only the token-admission surface is modeled. `cacheTtl` is the platform edge
 * cache (minimum 60 s); a deletion therefore propagates within the KV
 * propagation window, never instantly.
 */
export interface KVNamespaceLike {
  get(key: string, options?: { type?: "text" | "json"; cacheTtl?: number }): Promise<unknown>;
  put(
    key: string,
    value: string,
    options?: { expiration?: number; expirationTtl?: number; metadata?: unknown },
  ): Promise<void>;
  delete(key: string): Promise<void>;
}

/** Structural subset of a Workers Analytics Engine dataset binding. */
export interface AnalyticsEngineLike {
  writeDataPoint(point: { indexes?: string[]; doubles?: number[]; blobs?: string[] }): void;
}

export interface AppEnv {
  SHOT_DO: ShotNamespaceLike;
  /**
   * The authoritative control-plane Durable Object (spec 023, D4): instances,
   * tokens, shots, segment manifest, compaction lease/pins. It is never on the
   * ingest hot path.
   */
  INGEST_DO: IngestNamespaceLike;
  /**
   * Multi-user token registry (spec 013, Part B). Optional so pure unit tests
   * that only exercise the env admin credential can omit it; when absent, any
   * non-admin bearer token fails closed (`401`).
   */
  AUTH_DO?: AuthNamespaceLike;
  /**
   * Compaction coordinator Durable Object (spec 018). Optional so pure unit
   * tests that never touch compaction can omit it; when absent the
   * `compaction_status`/`compaction_run` tools fail closed.
   */
  COMPACTOR_DO?: CompactorNamespaceLike;
  /**
   * Durable raw-journal bucket for continuous OTLP intake (spec 014). The
   * trusted R2 bucket binding; every acknowledged export is a durable object.
   * Optional so pure unit tests can omit it.
   */
  BUCKET?: R2BucketLike;
  /**
   * Instance-token admission cache (spec 023, D1). Key `sha256(token)`, value
   * `{instance_id, instance_name, token_id, expires_at}`. Optional: when absent
   * admission goes straight to the authoritative ControlDO (no caching).
   */
  INSTANCE_TOKENS?: KVNamespaceLike;
  /**
   * Ingest counters (spec 023, D2): accepted/rejected-by-reason, bytes and
   * `last_received_at` per instance. Optional so local/unit runs need no
   * Analytics Engine binding; a write failure never fails an ingest.
   */
  INGEST_ANALYTICS?: AnalyticsEngineLike;
  BURSTEL_API_TOKEN?: string;
  /**
   * Trusted local-development escape hatch. When set to `1`/`true`, admin
   * endpoints accept requests even if `BURSTEL_API_TOKEN` is unset. Never set
   * this in production.
   */
  BURSTEL_DEV_INSECURE_AUTH?: string;
  PUBLIC_BASE_URL?: string;
  /**
   * Maximum accepted OTLP body size in bytes for a single export (spec 014).
   * Over-limit requests are rejected with `413` before any durable write.
   */
  MAX_INGEST_BYTES?: string;
  /** Compaction finalize lag (spec 018/023). Default 90 000 ms. */
  COMPACTION_GRACE_MS?: string;
  /**
   * Late-write guard (spec 023, D2.2). Defaults to `COMPACTION_GRACE_MS / 2`.
   * An R2 PUT that completes later than `received_at + LATE_WRITE_LIMIT_MS`
   * is deleted and answered with a retryable 503.
   */
  LATE_WRITE_LIMIT_MS?: string;
  /**
   * Injectable clock (epoch ms). Production leaves it unset (`Date.now`); the
   * deterministic unit tests pin expiry and late-write boundaries with it.
   */
  now?: () => number;
}

/** Internal origin used for requests the Worker makes to a ShotDO/ControlDO. */
export const DO_ORIGIN = "https://shot-do.internal";

/** R2 key prefix for the durable raw OTLP journal (never the shot archive). */
export const JOURNAL_PREFIX = "journal";

/** Default maximum accepted OTLP body size (8 MiB). */
export const DEFAULT_MAX_INGEST_BYTES = 8 * 1024 * 1024;

/** Default compaction finalize lag (90 s). */
export const DEFAULT_COMPACTION_GRACE_MS = 90_000;

/** Parses `MAX_INGEST_BYTES`, falling back to the default. */
export function maxIngestBytes(env: { MAX_INGEST_BYTES?: string }): number {
  const raw = env.MAX_INGEST_BYTES?.trim();
  if (!raw) return DEFAULT_MAX_INGEST_BYTES;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_MAX_INGEST_BYTES;
}

/** Parses `COMPACTION_GRACE_MS`, falling back to the default. */
export function compactionGraceMs(env: { COMPACTION_GRACE_MS?: string }): number {
  const raw = env.COMPACTION_GRACE_MS?.trim();
  if (!raw) return DEFAULT_COMPACTION_GRACE_MS;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_COMPACTION_GRACE_MS;
}

/**
 * Late-write budget (spec 023, D2.2): `LATE_WRITE_LIMIT_MS` when configured,
 * otherwise half the compaction grace. A non-positive value disables the guard.
 */
export function lateWriteLimitMs(env: { LATE_WRITE_LIMIT_MS?: string; COMPACTION_GRACE_MS?: string }): number {
  const raw = env.LATE_WRITE_LIMIT_MS?.trim();
  if (raw) {
    const parsed = Number.parseInt(raw, 10);
    if (Number.isFinite(parsed) && parsed >= 0) return parsed;
  }
  return Math.max(1, Math.floor(compactionGraceMs(env) / 2));
}

/** Bounds for `GET /api/shots` pagination. */
export const SHOTS_PAGE_DEFAULT = 50;
export const SHOTS_PAGE_MAX = 200;

/** Parses `limit`/`offset` query params with sane clamps. */
export function pagination(url: URL): { limit: number; offset: number } {
  const limitRaw = Number.parseInt(url.searchParams.get("limit") ?? "", 10);
  const offsetRaw = Number.parseInt(url.searchParams.get("offset") ?? "", 10);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, SHOTS_PAGE_MAX) : SHOTS_PAGE_DEFAULT;
  const offset = Number.isFinite(offsetRaw) && offsetRaw > 0 ? offsetRaw : 0;
  return { limit, offset };
}
