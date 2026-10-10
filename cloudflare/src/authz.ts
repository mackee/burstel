// Principal resolution for the public control/MCP surface (spec 013, Part B).
//
// Two credential sources are accepted, both resolving to a {@link Principal}:
//   1. `BURSTEL_API_TOKEN` — the legacy admin credential, resolved in the
//      Worker and never persisted in the registry;
//   2. any registered per-person token — resolved by the `AuthDO`, which stores
//      only digests and returns the scope (`admin` | `member`) and the
//      attribution-only owner.
//
// Deny by default: a missing/unmatched/revoked/expired token resolves to a
// failure; a registry that cannot be reached fails closed.

import { constantTimeEqual, extractToken, isInsecureDevAuth } from "./auth";
import { AUTH_DO_NAME } from "./auth-core";
import type { AuthPrincipal, AuthResult } from "./auth-core";
import { DO_ORIGIN } from "./env";
import type { AppEnv } from "./env";

/** Synthetic principal for the env admin credential. */
export const ENV_ADMIN_ID = "env-admin";
/** Attribution label for shots created with the env admin credential. */
export const ENV_ADMIN_OWNER = "admin";

function envAdmin(): AuthResult {
  return { ok: true, principal: { id: ENV_ADMIN_ID, scope: "admin", owner: ENV_ADMIN_OWNER } };
}

function devAdmin(): AuthResult {
  // The explicit local-development bypass is only ever effective when no env
  // admin token is configured; it grants the admin scope but is never stored.
  return { ok: true, principal: { id: "dev-insecure", scope: "admin", owner: "dev-insecure" } };
}

/**
 * Resolves the caller's principal, or a reason it was rejected.
 *
 * The env admin token is checked first (constant-time). Otherwise a presented
 * token is resolved by the registry. The trusted local-development bypass keeps
 * its previous semantics: it only applies when `BURSTEL_API_TOKEN` is unset.
 */
export async function resolvePrincipal(request: Request, env: AppEnv): Promise<AuthResult> {
  const token = extractToken(request);
  const admin = env.BURSTEL_API_TOKEN;
  const hasAdmin = typeof admin === "string" && admin.length > 0;

  if (hasAdmin && token !== null && constantTimeEqual(token, admin as string)) {
    return envAdmin();
  }

  if (token !== null && env.AUTH_DO) {
    const result = await registryAuthorize(env, token);
    if (result.ok) return result;
    if (!hasAdmin && isInsecureDevAuth(env)) return devAdmin();
    return result;
  }

  if (!hasAdmin && isInsecureDevAuth(env)) return devAdmin();
  return { ok: false, reason: token === null ? "missing" : "invalid" };
}

async function registryAuthorize(env: AppEnv, token: string): Promise<AuthResult> {
  try {
    const resp = await env.AUTH_DO!.getByName(AUTH_DO_NAME).fetch(
      new Request(`${DO_ORIGIN}/internal/authorize`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ token }),
      }),
    );
    if (!resp.ok) return { ok: false, reason: "invalid" };
    return (await resp.json()) as AuthResult;
  } catch {
    // A registry outage must never fall through to an allow.
    return { ok: false, reason: "invalid" };
  }
}

/** True when the principal may manage the token registry. */
export function isRegistryAdmin(principal: AuthPrincipal): boolean {
  return principal.scope === "admin";
}
