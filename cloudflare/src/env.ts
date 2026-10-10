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

/** Structural subset of `DurableObjectNamespace<IngestDO>`. */
export interface IngestNamespaceLike {
  getByName(name: string): DoStubLike;
}

/** Structural subset of `DurableObjectNamespace<AuthDO>`. */
export interface AuthNamespaceLike {
  getByName(name: string): DoStubLike;
}

export interface AppEnv {
  SHOT_DO: ShotNamespaceLike;
  INGEST_DO: IngestNamespaceLike;
  /**
   * Multi-user token registry (spec 013, Part B). Optional so pure unit tests
   * that only exercise the env admin credential can omit it; when absent, any
   * non-admin bearer token fails closed (`401`).
   */
  AUTH_DO?: AuthNamespaceLike;
  /**
   * Durable raw-journal bucket for continuous OTLP intake (spec 014). The
   * trusted R2 bucket binding; every acknowledged export is committed here
   * before the OTLP 200 is returned. Optional so pure unit tests can omit it.
   */
  BUCKET?: R2BucketLike;
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
}

/** Internal origin used for requests the Worker makes to a ShotDO/IngestDO. */
export const DO_ORIGIN = "https://shot-do.internal";

/** R2 key prefix for the durable raw OTLP journal (never the shot archive). */
export const JOURNAL_PREFIX = "journal";

/** Default maximum accepted OTLP body size (8 MiB). */
export const DEFAULT_MAX_INGEST_BYTES = 8 * 1024 * 1024;

/** Parses `MAX_INGEST_BYTES`, falling back to the default. */
export function maxIngestBytes(env: { MAX_INGEST_BYTES?: string }): number {
  const raw = env.MAX_INGEST_BYTES?.trim();
  if (!raw) return DEFAULT_MAX_INGEST_BYTES;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_MAX_INGEST_BYTES;
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
