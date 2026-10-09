// Spec 008: the container's internal bearer token must be distinct from the
// admin token, must never fall back to it, and production must fail closed
// when it is missing. These tests pin the resolution rules and prove the admin
// secret never reaches the container.

import { describe, expect, test } from "bun:test";

import { resolveInternalToken } from "../src/auth";

describe("resolveInternalToken", () => {
  test("requires a nonempty internal token in production", () => {
    expect(resolveInternalToken({})).toEqual({ ok: false, reason: "missing" });
    expect(resolveInternalToken({ BURSTEL_API_TOKEN: "admin" })).toEqual({
      ok: false,
      reason: "missing",
    });
    expect(
      resolveInternalToken({ BURSTEL_API_TOKEN: "admin", BURSTEL_INTERNAL_TOKEN: "   " }),
    ).toEqual({ ok: false, reason: "missing" });
  });

  test("rejects an internal token equal to the admin token", () => {
    expect(
      resolveInternalToken({ BURSTEL_API_TOKEN: "admin", BURSTEL_INTERNAL_TOKEN: "admin" }),
    ).toEqual({ ok: false, reason: "not_distinct" });
  });

  test("accepts a distinct, nonempty internal token and trims it", () => {
    expect(
      resolveInternalToken({ BURSTEL_API_TOKEN: "admin", BURSTEL_INTERNAL_TOKEN: "  internal  " }),
    ).toEqual({ ok: true, token: "internal", insecure: false });
  });

  test("the explicit local-dev bypass never promotes the admin token", () => {
    // Missing internal token under the opt-in: ungated in dev, never the admin token.
    expect(
      resolveInternalToken({ BURSTEL_API_TOKEN: "admin", BURSTEL_DEV_INSECURE_AUTH: "1" }),
    ).toEqual({ ok: true, token: "", insecure: true });
    // A provided internal token is still used and still distinct in dev.
    expect(
      resolveInternalToken({
        BURSTEL_API_TOKEN: "admin",
        BURSTEL_INTERNAL_TOKEN: "internal",
        BURSTEL_DEV_INSECURE_AUTH: "true",
      }),
    ).toEqual({ ok: true, token: "internal", insecure: true });
  });
});
