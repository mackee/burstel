// Environment and egress policy for the ShotDO analysis container (spec 035).
//
// Zero-copy window analysis downloads materialization parts directly from R2,
// so the analysis container needs both the bucket-scoped S3 credential and
// container internet egress. Both are enabled only when a complete credential
// is configured: without it the Worker keeps streaming parts into the container
// and the container never gets egress.

export interface AnalysisR2Env {
  R2_ACCESS_KEY_ID?: unknown;
  R2_SECRET_ACCESS_KEY?: unknown;
  R2_ACCOUNT_ID?: unknown;
  R2_BUCKET_NAME?: unknown;
  /** Optional bound for concurrent part GETs (Go default 16). */
  ANALYSIS_FETCH_PARALLELISM?: unknown;
}

export interface AnalysisContainerResolution {
  /** Extra environment for the analysis container (never the recorder). */
  env: Record<string, string>;
  /** True when the container may fetch parts directly from R2. */
  directDownload: boolean;
}

function value(raw: unknown): string {
  return typeof raw === "string" ? raw.trim() : "";
}

/** Reads the complete bucket-scoped R2 credential, or null. */
export function analysisR2Credential(
  env: AnalysisR2Env,
): { access: string; secret: string; account: string; bucket: string } | null {
  const access = value(env.R2_ACCESS_KEY_ID);
  const secret = value(env.R2_SECRET_ACCESS_KEY);
  const account = value(env.R2_ACCOUNT_ID);
  const bucket = value(env.R2_BUCKET_NAME);
  if (!access || !secret || !account || !bucket) return null;
  return { access, secret, account, bucket };
}

/** True when direct part download (and therefore analysis egress) is enabled. */
export function analysisDirectDownload(env: AnalysisR2Env): boolean {
  return analysisR2Credential(env) !== null;
}

/** Builds the analysis container environment. Secrets are never logged. */
export function resolveAnalysisContainerEnv(env: AnalysisR2Env): AnalysisContainerResolution {
  const credential = analysisR2Credential(env);
  if (!credential) return { env: {}, directDownload: false };
  const out: Record<string, string> = {
    R2_ACCESS_KEY_ID: credential.access,
    R2_SECRET_ACCESS_KEY: credential.secret,
    R2_ACCOUNT_ID: credential.account,
    R2_BUCKET: credential.bucket,
  };
  const parallelism = value(env.ANALYSIS_FETCH_PARALLELISM);
  if (parallelism !== "") out.BURSTEL_ANALYSIS_FETCH_PARALLELISM = parallelism;
  return { env: out, directDownload: true };
}
