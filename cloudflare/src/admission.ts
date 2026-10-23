// Instance-token admission (spec 023, D1).
//
// The ingest hot path authenticates from cache and writes one R2 object. It
// never calls a Durable Object on a cache hit:
//
//   1. isolate memory (positive entries only, `<= 30 s`);
//   2. KV (`sha256(token)` -> grant, `cacheTtl` at the platform minimum);
//   3. authoritative ControlDO (`/internal/instance/admit`) on a miss.
//
// Guarantees and their exact boundaries:
//   * Expiry is exact: an entry is accepted iff `now < expires_at`, evaluated
//     locally from the cached value. At `expires_at` and later it is rejected.
//   * Negative results (missing/invalid/expired/revoked) are never cached in
//     memory or KV, so a newly issued token becomes usable immediately.
//   * Revocation is eventually consistent at the edge: a previously cached
//     positive grant can survive for at most the KV propagation window
//     (memory 30 s + KV `cacheTtl` 60 s). The authoritative ControlDO rejects
//     it immediately, and compaction excludes objects written after the
//     effective revocation instant (D3.2), so revoked data never reaches
//     analysis even if it was briefly accepted at the edge.

import { sha256Hex } from "./crypto";
import { DO_ORIGIN } from "./env";
import type { AppEnv, KVNamespaceLike } from "./env";
import { HttpError } from "./errors";

/** Positive isolate-memory lifetime. Short enough to stay inside the ≤60 s bound. */
export const ADMISSION_MEMORY_TTL_MS = 30_000;

/**
 * KV edge-cache lifetime. Cloudflare documents 60 s as the platform minimum,
 * which also bounds edge revocation propagation.
 */
export const ADMISSION_KV_CACHE_TTL_SECONDS = 60;

/** The cached representation of one issued instance token. */
export interface TokenGrant {
  instance_id: string;
  instance_name: string;
  token_id: string;
  /** Epoch ms. Admission is exactly `now < expires_at`. */
  expires_at: number;
}

export type AdmissionFailureReason = "missing" | "invalid" | "expired" | "revoked";

export interface AdmissionSuccess extends TokenGrant {
  ok: true;
  /** Where the grant was resolved; diagnostics only. */
  source: "memory" | "kv" | "authoritative";
}

export interface AdmissionFailure {
  ok: false;
  reason: AdmissionFailureReason;
}

export type AdmissionResult = AdmissionSuccess | AdmissionFailure;

interface MemoryEntry {
  grant: TokenGrant;
  stored_at: number;
}

const memory = new Map<string, MemoryEntry>();

/** Clears the isolate cache. Production never calls this; tests do. */
export function resetAdmissionCache(): void {
  memory.clear();
}

/** Number of positive entries currently cached. Diagnostics and tests. */
export function admissionCacheSize(): number {
  return memory.size;
}

/** KV key for an instance token: `sha256(token)`, never the secret itself. */
export function instanceTokenKey(token: string): Promise<string> {
  return sha256Hex(token);
}

/** Builds the exact KV value written on issue/backfill. */
export function tokenGrantValue(grant: TokenGrant): TokenGrant {
  return {
    instance_id: grant.instance_id,
    instance_name: grant.instance_name,
    token_id: grant.token_id,
    expires_at: grant.expires_at,
  };
}

/**
 * Writes (or refreshes) one positive grant in KV. Best effort: a KV failure
 * only means the next miss goes to the authoritative ControlDO.
 */
export async function cacheTokenGrant(
  kv: KVNamespaceLike | undefined,
  token: string,
  grant: TokenGrant,
): Promise<boolean> {
  if (!kv) return false;
  try {
    const key = await instanceTokenKey(token);
    await kv.put(key, JSON.stringify(tokenGrantValue(grant)), {
      expiration: Math.max(1, Math.ceil(grant.expires_at / 1000)),
    });
    return true;
  } catch {
    return false;
  }
}

/** Deletes a positive grant for a known token digest (revoke/rotate). */
export async function deleteTokenGrant(kv: KVNamespaceLike | undefined, digest: string): Promise<boolean> {
  if (!kv) return false;
  try {
    await kv.delete(digest);
    return true;
  } catch {
    return false;
  }
}

/**
 * Resolves a push-only instance token.
 *
 * `now` is injectable so expiry and memory TTL are deterministic in tests.
 */
export async function admitInstanceToken(
  env: AppEnv,
  token: string | null,
  now: number = Date.now(),
): Promise<AdmissionResult> {
  if (!token) return { ok: false, reason: "missing" };
  const digest = await instanceTokenKey(token);

  const cached = memory.get(digest);
  if (cached) {
    // Expiry is exact even for a cached grant: never accept at/after it.
    if (now >= cached.grant.expires_at) {
      memory.delete(digest);
      return { ok: false, reason: "expired" };
    }
    if (now - cached.stored_at < ADMISSION_MEMORY_TTL_MS) {
      return { ok: true, ...cached.grant, source: "memory" };
    }
    memory.delete(digest);
  }

  if (env.INSTANCE_TOKENS) {
    let value: TokenGrant | null = null;
    try {
      value = (await env.INSTANCE_TOKENS.get(digest, {
        type: "json",
        cacheTtl: ADMISSION_KV_CACHE_TTL_SECONDS,
      })) as TokenGrant | null;
    } catch {
      // KV unavailable: fall through to the authoritative check, never cache.
      value = null;
    }
    if (value && isGrant(value)) {
      if (now >= value.expires_at) return { ok: false, reason: "expired" };
      const grant: TokenGrant = {
        instance_id: value.instance_id,
        instance_name: value.instance_name,
        token_id: value.token_id,
        expires_at: value.expires_at,
      };
      memory.set(digest, { grant, stored_at: now });
      return { ok: true, ...grant, source: "kv" };
    }
  }

  const authoritative = await admitViaControlDo(env, token);
  if (!authoritative.ok) return { ok: false, reason: authoritative.reason };

  const grant: TokenGrant = {
    instance_id: authoritative.instance_id,
    instance_name: authoritative.instance_name,
    token_id: authoritative.token_id,
    expires_at: authoritative.expires_at,
  };
  memory.set(digest, { grant, stored_at: now });
  // Backfill KV on a miss so the next isolate/edge lookup is fast. Never cache
  // failures, and never let a KV outage fail an otherwise valid ingest.
  await cacheTokenGrant(env.INSTANCE_TOKENS, token, grant);
  return { ok: true, ...grant, source: "authoritative" };
}

function isGrant(value: unknown): value is TokenGrant {
  if (value === null || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.instance_id === "string" &&
    v.instance_id !== "" &&
    typeof v.instance_name === "string" &&
    typeof v.token_id === "string" &&
    typeof v.expires_at === "number" &&
    Number.isFinite(v.expires_at)
  );
}

interface AuthoritativeAdmission {
  ok: boolean;
  reason?: AdmissionFailureReason;
  instance_id?: string;
  instance_name?: string;
  token_id?: string;
  expires_at?: number;
}

async function admitViaControlDo(env: AppEnv, token: string): Promise<{
  ok: true;
  instance_id: string;
  instance_name: string;
  token_id: string;
  expires_at: number;
} | { ok: false; reason: AdmissionFailureReason }> {
  let resp: Response;
  try {
    const stub = env.INGEST_DO.getByName("ingest-primary");
    resp = await stub.fetch(
      new Request(`${DO_ORIGIN}/internal/instance/admit`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ token }),
      }),
    );
  } catch (error) {
    // The authoritative service is unreachable. Fail closed with a retryable
    // server error (surfaced by the caller), never a cached negative.
    throw new HttpError(
      503,
      `instance admission unavailable: ${error instanceof Error ? error.message : String(error)}`,
      true,
      "admission_unavailable",
    );
  }
  if (resp.status >= 500) {
    throw new HttpError(503, `instance admission failed with status ${resp.status}`, true, "admission_unavailable");
  }
  if (resp.status < 200 || resp.status >= 300) return { ok: false, reason: "invalid" };
  let parsed: AuthoritativeAdmission;
  try {
    parsed = (await resp.json()) as AuthoritativeAdmission;
  } catch {
    return { ok: false, reason: "invalid" };
  }
  if (!parsed.ok) return { ok: false, reason: parsed.reason ?? "invalid" };
  if (
    typeof parsed.instance_id !== "string" ||
    typeof parsed.instance_name !== "string" ||
    typeof parsed.token_id !== "string" ||
    typeof parsed.expires_at !== "number"
  ) {
    return { ok: false, reason: "invalid" };
  }
  return {
    ok: true,
    instance_id: parsed.instance_id,
    instance_name: parsed.instance_name,
    token_id: parsed.token_id,
    expires_at: parsed.expires_at,
  };
}
