// Bearer-token primitives for the public control and ingest APIs.
//
// After spec 013 there are three credentials:
//   * `BURSTEL_API_TOKEN` — the env admin/control/analysis/MCP token;
//   * a registered per-person token (`admin` | `member`) — resolved by the
//     `AuthDO` registry in `authz.ts`;
//   * the per-shot push-only ingest token — random, valid for 24h, and accepted
//     *only* at POST /ingest/v1/{traces,metrics,logs}.
//
// None is ever logged and none is forwarded to a container.

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
 * Retained as a low-level primitive; the public surface resolves principals in
 * `authz.ts` (which also accepts registry tokens), so this is not used directly
 * by the request router.
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

export interface InternalAuthEnv extends AdminAuthEnv {
  BURSTEL_INTERNAL_TOKEN?: string;
}

/**
 * Outcome of resolving the container's internal bearer token.
 *
 * Production requires a *distinct*, nonempty `BURSTEL_INTERNAL_TOKEN`. It is
 * never the admin token and never the random push-only token. When it is absent
 * or equal to the admin token the DO fails closed rather than booting an
 * unguarded container or leaking the admin secret.
 */
export type InternalTokenResolution =
  | { ok: true; token: string; insecure: boolean }
  | { ok: false; reason: "missing" | "not_distinct" };

/**
 * Resolves the container internal token from Worker/DO bindings.
 *
 * The legacy `BURSTEL_API_TOKEN` fallback is deliberately gone: an admin token
 * must never be forwarded into a container. The only way to run without an
 * internal token is the explicit local-development flag
 * `BURSTEL_DEV_INSECURE_AUTH`, which is disabled by default.
 */
export function resolveInternalToken(env: InternalAuthEnv): InternalTokenResolution {
  const internal = (env.BURSTEL_INTERNAL_TOKEN ?? "").trim();
  if (isInsecureDevAuth(env)) {
    // Explicit local opt-in. Never promote the admin token; an empty token
    // means the container's internal routes are intentionally ungated in dev.
    return { ok: true, token: internal, insecure: true };
  }
  if (internal.length === 0) return { ok: false, reason: "missing" };
  const admin = env.BURSTEL_API_TOKEN;
  if (typeof admin === "string" && admin.length > 0 && constantTimeEqual(internal, admin)) {
    return { ok: false, reason: "not_distinct" };
  }
  return { ok: true, token: internal, insecure: false };
}

/** True only for the trusted, explicit local-development bypass. */
export function isInsecureDevAuth(env: AdminAuthEnv): boolean {
  const value = env.BURSTEL_DEV_INSECURE_AUTH?.trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

/**
 * Admin authentication for the env credential only.
 *
 * Superseded on the public surface by `resolvePrincipal` (spec 013, Part B),
 * which also accepts registered per-person tokens. Kept as an explicit helper
 * for callers that want the legacy env-admin check; it fails closed when no
 * admin token is configured except under the documented local-development
 * bypass.
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
