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
}

/** Internal origin used for requests the Worker makes to a ShotDO/IngestDO. */
export const DO_ORIGIN = "https://shot-do.internal";
