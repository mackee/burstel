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
export function resolveCompactorContainerEnv(env: InternalAuthEnv, port: number): CompactorContainerEnv {
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
  return { env: containerEnv, internal };
}
