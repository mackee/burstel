// HTTP-facing error type used by the Worker app and the ShotDO core.

export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly retryable = false,
    readonly code = "error",
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
