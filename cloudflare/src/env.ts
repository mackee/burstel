// Shared Worker/DO types used by the HTTP app, the MCP server and the tools.

/** Structural subset of `DurableObjectStub`. */
export interface DoStubLike {
  fetch(request: Request): Promise<Response>;
}

/** Structural subset of `DurableObjectNamespace<ShotDO>`. */
export interface ShotNamespaceLike {
  getByName(name: string): DoStubLike;
}

export interface AppEnv {
  SHOT_DO: ShotNamespaceLike;
  BURSTEL_API_TOKEN?: string;
  PUBLIC_BASE_URL?: string;
}

/** Internal origin used for requests the Worker makes to a ShotDO. */
export const DO_ORIGIN = "https://shot-do.internal";
