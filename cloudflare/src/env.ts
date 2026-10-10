// Shared Worker/DO types used by the HTTP app, the MCP server and the tools.

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

export interface AppEnv {
  SHOT_DO: ShotNamespaceLike;
  INGEST_DO: IngestNamespaceLike;
  BURSTEL_API_TOKEN?: string;
  /**
   * Trusted local-development escape hatch. When set to `1`/`true`, admin
   * endpoints accept requests even if `BURSTEL_API_TOKEN` is unset. Never set
   * this in production.
   */
  BURSTEL_DEV_INSECURE_AUTH?: string;
  PUBLIC_BASE_URL?: string;
  /**
   * Maximum number of shots that may record concurrently (spec 013, Part A).
   * A Worker var; defaults to {@link DEFAULT_MAX_CONCURRENT_RECORDING} when
   * unset or invalid. Exceeding it yields a retryable `503 capacity_exhausted`.
   */
  MAX_CONCURRENT_RECORDING?: string;
}

/** Internal origin used for requests the Worker makes to a ShotDO/IngestDO. */
export const DO_ORIGIN = "https://shot-do.internal";

/** Default cap when `MAX_CONCURRENT_RECORDING` is absent or invalid. */
export const DEFAULT_MAX_CONCURRENT_RECORDING = 10;

/** Parses the `MAX_CONCURRENT_RECORDING` Worker var, falling back to the default. */
export function maxConcurrentRecording(env: { MAX_CONCURRENT_RECORDING?: string }): number {
  const raw = env.MAX_CONCURRENT_RECORDING?.trim();
  if (!raw) return DEFAULT_MAX_CONCURRENT_RECORDING;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_MAX_CONCURRENT_RECORDING;
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
