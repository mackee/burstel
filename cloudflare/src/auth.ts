// Bearer-token authentication for the public control and ingest APIs.
//
// Two separate credentials exist (spec 006):
//   * `BURSTEL_API_TOKEN` — the global admin/control/analysis/MCP token;
//   * the push-only ingest token — random, admin-issued, valid for 24h, and
//     accepted *only* at POST /ingest/v1/{traces,metrics,logs}.
//
// Neither is ever logged and neither is forwarded to a container.

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
 * A missing expected token always fails: production must always configure one,
 * and tests/local development opt in explicitly via {@link isInsecureDevAuth}.
 */
export function authenticate(request: Request, expected: string | undefined): boolean {
  if (!expected) return false;
  const got = extractToken(request);
  if (got === null) return false;
  return constantTimeEqual(got, expected);
}

export interface AdminAuthEnv {
  BURSTEL_API_TOKEN?: string;
  BURSTEL_DEV_INSECURE_AUTH?: string;
}

/** True only for the trusted, explicit local-development bypass. */
export function isInsecureDevAuth(env: AdminAuthEnv): boolean {
  const value = env.BURSTEL_DEV_INSECURE_AUTH?.trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

/**
 * Admin authentication for control/analysis/MCP and token minting.
 *
 * Fails closed when no admin token is configured, except under the explicitly
 * documented local-development bypass.
 */
export function adminAuthenticate(request: Request, env: AdminAuthEnv): boolean {
  if (env.BURSTEL_API_TOKEN && env.BURSTEL_API_TOKEN.length > 0) {
    return authenticate(request, env.BURSTEL_API_TOKEN);
  }
  return isInsecureDevAuth(env);
}

export function constantTimeEqual(a: string, b: string): boolean {
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
