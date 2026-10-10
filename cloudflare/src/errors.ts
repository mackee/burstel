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
 * Concurrent-recording capacity is exhausted: at most `MAX_CONCURRENT_RECORDING`
 * shots may record at once (spec 013, Part A). Retryable — instances free up as
 * other shots stop — and produced before any shot/container state is created.
 */
export function capacityExhausted(limit: number): HttpError {
  return new HttpError(
    503,
    `concurrent recording capacity exhausted (max ${limit}); retry after a shot stops`,
    true,
    "capacity_exhausted",
  );
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
 * The shot's recording container stopped, taking its ephemeral DuckDB with it.
 * The shot can never record or freeze again, so this is terminal. The request
 * at hand was rejected before reaching any recorder: definite non-commitment.
 */
export function shotLost(): HttpError {
  return new HttpError(409, LOST_MESSAGE, false, "shot_lost", true);
}

export const LOST_MESSAGE = "recording container stopped; recorded telemetry was lost";

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
