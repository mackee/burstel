// Bearer-token authentication for the public control and ingest APIs.
//
// The token is supplied to the Worker as the secret `BURSTEL_API_TOKEN`. It is
// never logged and never forwarded to the container.

/** Extracts a token from `Authorization: Bearer ...` or `X-Burstel-Token`. */
export function extractToken(request: Request): string | null {
  const header = request.headers.get("authorization");
  if (header) {
    const match = /^Bearer[ \t]+(.+)$/i.exec(header.trim());
    if (match && match[1] !== undefined) {
      const token = match[1].trim();
      if (token.length > 0) return token;
    }
  }
  // Convenience for OTLP exporters, which can set an arbitrary header via
  // OTEL_EXPORTER_OTLP_HEADERS.
  const xToken = request.headers.get("x-burstel-token");
  if (xToken && xToken.trim().length > 0) return xToken.trim();
  return null;
}

/**
 * Returns true when the request carries the expected token.
 *
 * When no token is configured, authentication is disabled (local `wrangler
 * dev`); production deployments must always set `BURSTEL_API_TOKEN`.
 */
export function authenticate(request: Request, expected: string | undefined): boolean {
  if (!expected) return true;
  const got = extractToken(request);
  if (got === null) return false;
  return constantTimeEqual(got, expected);
}

function constantTimeEqual(a: string, b: string): boolean {
  const enc = new TextEncoder();
  const ab = enc.encode(a);
  const bb = enc.encode(b);
  // Compare a fixed number of bytes; fold the length difference into the
  // accumulator so unequal lengths are still rejected without an early return.
  const len = Math.max(ab.length, bb.length);
  let diff = ab.length ^ bb.length;
  for (let i = 0; i < len; i++) {
    diff |= (ab[i] ?? 0) ^ (bb[i] ?? 0);
  }
  return diff === 0;
}
