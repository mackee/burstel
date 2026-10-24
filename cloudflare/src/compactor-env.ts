// Environment handed to the short-lived Go compactor Container (spec 028).
//
// Kept separate from `compactor-do.ts` so the exact env mapping is testable
// without the `cloudflare:workers` runtime: the DO is only wiring, and the
// token gate must not depend on a deployed Worker to be verified.
//
// Spec 028 bug: the secure Worker sends `Authorization: Bearer
// <BURSTEL_INTERNAL_TOKEN>` to the compactor (`compactor-core.ts`), but the
// container env never carried that token. The Go service fails closed with
// `compactor API disabled: BURSTEL_INTERNAL_TOKEN is not configured`, so no
// segment was ever published on a real deployment. `BURSTEL_INTERNAL_TOKEN`
// is therefore injected whenever a secure, distinct token is configured.
// Insecure dev auth intentionally keeps the container ungated instead.

import type { InternalAuthEnv, InternalTokenResolution } from "./auth";
import { resolveInternalToken } from "./auth";

export interface CompactorContainerEnv {
  /** Environment passed to `ContainerControl.start`. */
  env: Record<string, string>;
  /** The same token resolution the coordinator uses for request headers. */
  internal: InternalTokenResolution;
  /** True when a complete bucket-scoped R2 credential was injected. */
  r2: boolean;
}

export interface CompactorR2Env {
  R2_ACCESS_KEY_ID?: string;
  R2_SECRET_ACCESS_KEY?: string;
  R2_ACCOUNT_ID?: string;
  /** Public bucket name; the credential itself is scoped server-side. */
  R2_BUCKET_NAME?: string;
}

/**
 * Builds the compactor Container environment from the Worker/DO bindings.
 *
 * Secure mode: inject the distinct token so the Go service's auth gate matches
 * the `Authorization` header `CompactorCore` sends.
 *
 * Insecure dev mode (`BURSTEL_DEV_INSECURE_AUTH`): set the opt-in flag instead
 * and never inject a token; the container's internal routes are intentionally
 * ungated in that mode.
 */
export function resolveCompactorContainerEnv(
  env: InternalAuthEnv & CompactorR2Env,
  port: number,
): CompactorContainerEnv {
  const internal = resolveInternalToken(env);
  const containerEnv: Record<string, string> = {
    BURSTEL_ADDR: `:${port}`,
    BURSTEL_COMPACT_WORK_DIR: "/work",
  };
  if (internal.ok && !internal.insecure) {
    if (internal.token.length > 0) containerEnv["BURSTEL_INTERNAL_TOKEN"] = internal.token;
  } else if (internal.ok) {
    containerEnv["BURSTEL_DEV_INSECURE_AUTH"] = "1";
  }
  // Spec 031 D1: a bucket-scoped R2 credential is injected into the
  // compactor/materializer container only. It is never exported to the
  // Worker's public surface and never logged.
  let r2 = false;
  const access = env.R2_ACCESS_KEY_ID?.trim() ?? "";
  const secret = env.R2_SECRET_ACCESS_KEY?.trim() ?? "";
  const account = env.R2_ACCOUNT_ID?.trim() ?? "";
  const bucket = env.R2_BUCKET_NAME?.trim() ?? "";
  if (access && secret && account && bucket) {
    containerEnv["R2_ACCESS_KEY_ID"] = access;
    containerEnv["R2_SECRET_ACCESS_KEY"] = secret;
    containerEnv["R2_ACCOUNT_ID"] = account;
    containerEnv["R2_BUCKET"] = bucket;
    r2 = true;
  }
  return { env: containerEnv, internal, r2 };
}
