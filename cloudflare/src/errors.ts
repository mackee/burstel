// HTTP-facing error type used by the Worker app and the ShotDO core.

export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly retryable = false,
    readonly code = "error",
    /**
     * True only when the request is provably *not* committed downstream (it was
     * rejected before any durable write), so a caller may safely answer with a
     * valid OTLP ExportResponse and discard the payload. Never set this when a
     * write may have been applied or its outcome is unknown.
     */
    readonly definitelyUnaccepted = false,
  ) {
    super(message);
    this.name = "HttpError";
  }
}

export function badRequest(message: string): HttpError {
  return new HttpError(400, message, false, "bad_request");
}

export function unauthorized(): HttpError {
  return new HttpError(401, "missing or invalid bearer token", false, "unauthorized");
}

export function notFound(message: string): HttpError {
  return new HttpError(404, message, false, "not_found");
}

export function conflict(message: string): HttpError {
  return new HttpError(409, message, false, "conflict");
}

export function unavailable(message: string, retryable = true): HttpError {
  return new HttpError(503, message, retryable, "unavailable");
}

/**
 * The shot is stopping, so this request was rejected before it was forwarded
 * to the recorder. Nothing was committed: the caller may acknowledge the OTLP
 * export and consciously discard it.
 */
export function shotStopping(): HttpError {
  return new HttpError(409, "shot is stopping", false, "shot_stopping", true);
}

/**
 * The shot is not recording (created/stopping/frozen/failed), so this request
 * never reached the recorder. Definite non-commitment.
 */
export function notRecording(reason = "shot is not recording"): HttpError {
  return new HttpError(409, reason, false, "not_recording", true);
}

/**
 * Production misconfiguration: the container's distinct internal bearer token
 * is missing or equal to the admin token. Refuse to boot an unguarded
 * container. Not retryable — an operator must fix the configuration.
 */
export function internalTokenUnconfigured(reason: "missing" | "not_distinct"): HttpError {
  return new HttpError(
    500,
    `internal container token is not configured (${reason}); refusing to operate without a distinct BURSTEL_INTERNAL_TOKEN`,
    false,
    `internal_token_${reason}`,
  );
}

/**
 * Container/account capacity exhaustion is retryable: instances free up as
 * other shots stop. Detect the common platform error shapes.
 */
export function isCapacityError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /max(?:imum)?[ _-]?(?:number of )?(?:container )?instances|instance limit|too many (?:container )?instances|no capacity|capacity exceeded|max_instances/i.test(
    message,
  );
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
