// Small HTTP helpers shared by the Worker and the ShotDO core.

import { HttpError, isCapacityError, errorMessage, badRequest } from "./errors";

export function json(status: number, body: unknown, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...extraHeaders,
    },
  });
}

/** Converts a thrown error into a JSON error response. */
export function errorResponse(error: unknown): Response {
  if (error instanceof HttpError) {
    return json(error.status, { error: error.message, code: error.code, retryable: error.retryable });
  }
  if (isCapacityError(error)) {
    return json(503, {
      error: `container capacity exhausted: ${errorMessage(error)}`,
      code: "capacity",
      retryable: true,
    });
  }
  return json(503, { error: errorMessage(error), code: "error", retryable: true });
}

export async function readJson<T>(request: Request): Promise<T> {
  const text = await request.text();
  if (text.trim() === "") return {} as T;
  try {
    return JSON.parse(text) as T;
  } catch (error) {
    throw badRequest(`invalid JSON body: ${errorMessage(error)}`);
  }
}
