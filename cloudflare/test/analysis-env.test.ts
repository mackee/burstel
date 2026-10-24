import { describe, expect, test } from "bun:test";

import {
  analysisDirectDownload,
  analysisR2Credential,
  resolveAnalysisContainerEnv,
} from "../src/analysis-env";

describe("spec 035 analysis container R2 environment", () => {
  test("direct download is disabled unless the credential is complete", () => {
    expect(analysisDirectDownload({})).toBe(false);
    expect(
      analysisDirectDownload({
        R2_ACCESS_KEY_ID: "key",
        R2_SECRET_ACCESS_KEY: "secret",
        R2_ACCOUNT_ID: "account",
      }),
    ).toBe(false);
    expect(
      analysisR2Credential({
        R2_ACCESS_KEY_ID: "key",
        R2_SECRET_ACCESS_KEY: "secret",
        R2_ACCOUNT_ID: "account",
        R2_BUCKET_NAME: " ",
      }),
    ).toBeNull();
  });

  test("a complete credential is forwarded and enables egress", () => {
    const resolved = resolveAnalysisContainerEnv({
      R2_ACCESS_KEY_ID: "key",
      R2_SECRET_ACCESS_KEY: "secret",
      R2_ACCOUNT_ID: "account",
      R2_BUCKET_NAME: "burstel-shots",
      ANALYSIS_FETCH_PARALLELISM: "16",
    });
    expect(resolved.directDownload).toBe(true);
    expect(resolved.env).toEqual({
      R2_ACCESS_KEY_ID: "key",
      R2_SECRET_ACCESS_KEY: "secret",
      R2_ACCOUNT_ID: "account",
      R2_BUCKET: "burstel-shots",
      BURSTEL_ANALYSIS_FETCH_PARALLELISM: "16",
    });
  });

  test("an incomplete credential never leaks a partial secret", () => {
    const resolved = resolveAnalysisContainerEnv({
      R2_ACCESS_KEY_ID: "key",
      R2_SECRET_ACCESS_KEY: "secret",
    });
    expect(resolved).toEqual({ env: {}, directDownload: false });
  });
});
