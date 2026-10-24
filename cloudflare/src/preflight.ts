// Pure preflight parsing/validation for the Cloudflare deployment runbook.
//
// This module deliberately uses no Node/Bun runtime APIs so it typechecks under
// the Worker tsconfig (types: []) and can be unit-tested directly. The bash
// entry point (`scripts/cloudflare-preflight.sh`) and the thin runner
// (`scripts/preflight.mjs`) supply the real CLI/file data; nothing here reads
// secrets or talks to the network, and no check ever prints a secret value.

export interface Check {
  name: string;
  ok: boolean;
  required: boolean;
  detail: string;
}

export interface Report {
  ok: boolean;
  checks: Check[];
  failed_required: string[];
}

export interface ContainerConfig {
  class_name?: string;
  image?: string;
  image_build_context?: string;
  instance_type?: string;
  max_instances?: number;
}

export interface DoBinding {
  name?: string;
  class_name?: string;
}

export interface DoExport {
  type?: string;
  storage?: string;
}

export interface R2Binding {
  binding?: string;
  bucket_name?: string;
}

export interface KVBinding {
  binding?: string;
  id?: string;
}

export interface AnalyticsEngineBinding {
  binding?: string;
  dataset?: string;
}

export interface WranglerConfig {
  name?: string;
  main?: string;
  compatibility_date?: string;
  containers?: ContainerConfig[];
  durable_objects?: { bindings?: DoBinding[] };
  exports?: Record<string, DoExport>;
  r2_buckets?: R2Binding[];
  kv_namespaces?: KVBinding[];
  analytics_engine_datasets?: AnalyticsEngineBinding[];
  vars?: Record<string, unknown>;
  triggers?: { crons?: string[] };
  workers_dev?: boolean;
}

export interface WranglerValidationOptions {
  maxInstances?: number;
  expectedBucket?: string;
  expectedWorkerName?: string;
  expectedImage?: string;
  expectedImageBuildContext?: string;
  expectedInstanceType?: string;
  expectedCompactorImage?: string;
  /**
   * Structural lint only: accept the repository's all-zero KV placeholder id.
   * A real deploy preflight must never set this (spec 024, R2).
   */
  allowPlaceholderKv?: boolean;
}

/**
 * Deploy-safe control-plane storage class (spec 024, R1).
 *
 * The code alias is `ControlDO`, but the exported Wrangler class and the
 * `INGEST_DO.class_name` binding must stay `IngestDO` so an existing
 * installation reuses its SQLite storage without a `renamed_classes` migration.
 */
export const LEGACY_CONTROL_DO_CLASS = "IngestDO";
/** Code-level alias for the control-plane role; never a Wrangler class name. */
export const CONTROL_DO_ALIAS = "ControlDO";

/** True for the repo's all-zero / obvious placeholder KV namespace id. */
export function isPlaceholderKvId(id: string | undefined): boolean {
  const value = (id ?? "").trim().toLowerCase();
  if (value === "") return true;
  if (/^0+$/.test(value)) return true;
  return value.includes("placeholder") || value.includes("replace-me") || value.startsWith("<");
}


/** Removes `//` and block comments while preserving string contents. */
export function stripJsonComments(input: string): string {
  let out = "";
  let i = 0;
  let inString = false;
  let inLine = false;
  let inBlock = false;
  while (i < input.length) {
    const c = input[i]!;
    const n = input[i + 1];
    if (inLine) {
      if (c === "\n") {
        inLine = false;
        out += c;
      }
      i += 1;
      continue;
    }
    if (inBlock) {
      if (c === "*" && n === "/") {
        inBlock = false;
        i += 2;
      } else {
        i += 1;
      }
      continue;
    }
    if (inString) {
      out += c;
      if (c === "\\") {
        if (n !== undefined) out += n;
        i += 2;
        continue;
      }
      if (c === '"') inString = false;
      i += 1;
      continue;
    }
    if (c === '"') {
      inString = true;
      out += c;
      i += 1;
      continue;
    }
    if (c === "/" && n === "/") {
      inLine = true;
      i += 2;
      continue;
    }
    if (c === "/" && n === "*") {
      inBlock = true;
      i += 2;
      continue;
    }
    out += c;
    i += 1;
  }
  return out;
}

/** Removes trailing commas before `}`/`]` without touching strings. */
export function removeTrailingCommas(input: string): string {
  let out = "";
  let i = 0;
  let inString = false;
  while (i < input.length) {
    const c = input[i]!;
    if (inString) {
      out += c;
      if (c === "\\") {
        out += input[i + 1] ?? "";
        i += 2;
        continue;
      }
      if (c === '"') inString = false;
      i += 1;
      continue;
    }
    if (c === '"') {
      inString = true;
      out += c;
      i += 1;
      continue;
    }
    if (c === ",") {
      let j = i + 1;
      while (j < input.length && /\s/.test(input[j]!)) j += 1;
      if (input[j] === "}" || input[j] === "]") {
        i += 1;
        continue;
      }
    }
    out += c;
    i += 1;
  }
  return out;
}

/** Parses JSON-with-comments (the `.jsonc` files Wrangler accepts). */
export function parseJsonc(input: string): unknown {
  return JSON.parse(removeTrailingCommas(stripJsonComments(input))) as unknown;
}

export function loadWranglerConfig(text: string): WranglerConfig {
  const raw = parseJsonc(text);
  if (raw === null || typeof raw !== "object") {
    throw new Error("wrangler config is not a JSON object");
  }
  return raw as WranglerConfig;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * The R2 bucket the Worker is configured to use, taken from the parsed
 * Wrangler config (`r2_buckets[BUCKET].bucket_name`). There is no hardcoded
 * bucket name in the validator: the config is the source of truth and the
 * deployment preflight only verifies that the named bucket exists.
 */
export function bucketNameFromConfig(cfg: WranglerConfig): string | undefined {
  const binding = (cfg.r2_buckets ?? []).find((b) => b.binding === "BUCKET");
  const name = binding?.bucket_name;
  if (typeof name !== "string" || name.trim() === "") return undefined;
  return name.trim();
}

export function validateWranglerConfig(
  cfg: WranglerConfig,
  opts: WranglerValidationOptions = {},
): Check[] {
  const maxInstances = opts.maxInstances ?? 10;
  const configuredBucket = bucketNameFromConfig(cfg);
  const expectedBucket = opts.expectedBucket;
  const expectedWorkerName = opts.expectedWorkerName ?? "burstel";
  const expectedImage = opts.expectedImage ?? "../Dockerfile";
  const expectedContext = opts.expectedImageBuildContext ?? "..";
  const expectedInstanceType = opts.expectedInstanceType ?? "standard-1";
  const expectedCompactorImage = opts.expectedCompactorImage ?? "../Dockerfile.compactor";
  const checks: Check[] = [];
  const add = (name: string, ok: boolean, required: boolean, detail: string): void => {
    checks.push({ name, ok, required, detail });
  };

  add(
    "worker_name",
    cfg.name === expectedWorkerName,
    true,
    `name=${cfg.name ?? "<unset>"} (want ${expectedWorkerName})`,
  );
  add(
    "worker_main",
    typeof cfg.main === "string" && cfg.main.endsWith(".ts"),
    true,
    `main=${cfg.main ?? "<unset>"}`,
  );
  add(
    "compatibility_date",
    typeof cfg.compatibility_date === "string" && /^\d{4}-\d{2}-\d{2}$/.test(cfg.compatibility_date),
    true,
    `compatibility_date=${cfg.compatibility_date ?? "<unset>"}`,
  );

  const containers = Array.isArray(cfg.containers) ? cfg.containers : [];
  const shot = containers.filter((c) => c.class_name === "ShotDO");
  add("container_shotdo_singleton", shot.length === 1, true, `ShotDO container entries=${shot.length}`);
  const container = shot[0];
  add(
    "container_image",
    container !== undefined &&
      container.image === expectedImage &&
      container.image_build_context === expectedContext,
    true,
    container
      ? `image=${container.image ?? "<unset>"} context=${container.image_build_context ?? "<unset>"}`
      : "no ShotDO container",
  );
  add(
    "container_instance_type",
    container?.instance_type === expectedInstanceType,
    true,
    `instance_type=${container?.instance_type ?? "<unset>"} (want ${expectedInstanceType})`,
  );
  const max = container?.max_instances;
  add(
    "container_max_instances_bounded",
    typeof max === "number" && Number.isInteger(max) && max >= 1 && max <= maxInstances,
    true,
    `max_instances=${max ?? "<unset>"} must be an integer in 1..${maxInstances}`,
  );

  // Spec 018: a distinct, bounded compactor Container + coordinator DO.
  const compactorContainers = containers.filter((c) => c.class_name === "CompactorDO");
  add(
    "container_compactordo_singleton",
    compactorContainers.length === 1,
    true,
    `CompactorDO container entries=${compactorContainers.length}`,
  );
  const compactor = compactorContainers[0];
  add(
    "container_compactor_image",
    compactor !== undefined &&
      compactor.image === expectedCompactorImage &&
      compactor.image_build_context === expectedContext,
    true,
    compactor
      ? `image=${compactor.image ?? "<unset>"} context=${compactor.image_build_context ?? "<unset>"}`
      : "no CompactorDO container",
  );
  add(
    "container_compactor_max_instances_bounded",
    typeof compactor?.max_instances === "number" &&
      Number.isInteger(compactor.max_instances) &&
      compactor.max_instances >= 1 &&
      compactor.max_instances <= 2,
    true,
    `CompactorDO max_instances=${compactor?.max_instances ?? "<unset>"} must be 1 or 2`,
  );

  const bindings = cfg.durable_objects?.bindings ?? [];
  add(
    "do_binding_compactordo",
    bindings.some((b) => b.name === "COMPACTOR_DO" && b.class_name === "CompactorDO"),
    true,
    "COMPACTOR_DO -> CompactorDO durable object binding",
  );
  add(
    "do_binding_shotdo",
    bindings.some((b) => b.name === "SHOT_DO" && b.class_name === "ShotDO"),
    true,
    "SHOT_DO -> ShotDO durable object binding",
  );
  add(
    "do_binding_ingestdo",
    bindings.some((b) => b.name === "INGEST_DO" && b.class_name === LEGACY_CONTROL_DO_CLASS),
    true,
    `INGEST_DO -> ${LEGACY_CONTROL_DO_CLASS} durable object binding (spec 024 R1; code alias ${CONTROL_DO_ALIAS})`,
  );
  // R1 deploy safety: the binding must resolve to an exported SQLite class
  // whose name is exactly the pre-024 storage class. Renaming it here without a
  // renamed_classes migration would orphan existing DO state.
  const ingestBinding = bindings.find((b) => b.name === "INGEST_DO");
  const ingestExport = ingestBinding?.class_name ? cfg.exports?.[ingestBinding.class_name] : undefined;
  add(
    "do_rename_preserves_storage",
    ingestBinding?.class_name === LEGACY_CONTROL_DO_CLASS &&
      ingestExport?.type === "durable-object" &&
      ingestExport?.storage === "sqlite",
    true,
    `INGEST_DO resolves to ${ingestBinding?.class_name ?? "<missing>"} (${ingestExport?.storage ?? "no export"}); must stay ${LEGACY_CONTROL_DO_CLASS}/sqlite`,
  );
  add(
    "no_unsafe_control_do_rename",
    cfg.exports?.[CONTROL_DO_ALIAS] === undefined,
    true,
    `wrangler must not export ${CONTROL_DO_ALIAS} as a separate Durable Object class (it would create empty storage)`,
  );
  add(
    "do_binding_authdo",
    bindings.some((b) => b.name === "AUTH_DO" && b.class_name === "AuthDO"),
    true,
    "AUTH_DO -> AuthDO durable object binding (spec 013, Part B)",
  );

  const exports = cfg.exports ?? {};
  add(
    "export_shotdo_sqlite",
    exports["ShotDO"]?.type === "durable-object" && exports["ShotDO"]?.storage === "sqlite",
    true,
    `exports.ShotDO=${JSON.stringify(exports["ShotDO"] ?? null)}`,
  );
  add(
    "export_ingestdo_sqlite",
    exports[LEGACY_CONTROL_DO_CLASS]?.type === "durable-object" &&
      exports[LEGACY_CONTROL_DO_CLASS]?.storage === "sqlite",
    true,
    `exports.${LEGACY_CONTROL_DO_CLASS}=${JSON.stringify(exports[LEGACY_CONTROL_DO_CLASS] ?? null)}`,
  );
  add(
    "export_authdo_sqlite",
    exports["AuthDO"]?.type === "durable-object" && exports["AuthDO"]?.storage === "sqlite",
    true,
    `exports.AuthDO=${JSON.stringify(exports["AuthDO"] ?? null)}`,
  );
  add(
    "export_compactordo_sqlite",
    exports["CompactorDO"]?.type === "durable-object" && exports["CompactorDO"]?.storage === "sqlite",
    true,
    `exports.CompactorDO=${JSON.stringify(exports["CompactorDO"] ?? null)}`,
  );
  const crons = cfg.triggers?.crons;
  add(
    "compaction_cron_trigger",
    Array.isArray(crons) && crons.length > 0 && crons.every((c) => typeof c === "string" && c.trim() !== ""),
    true,
    `triggers.crons=${JSON.stringify(crons ?? null)} (the compaction backstop trigger)`,
  );

  add(
    "r2_bucket_binding",
    configuredBucket !== undefined &&
      (expectedBucket === undefined || configuredBucket === expectedBucket),
    true,
    expectedBucket === undefined
      ? `r2_buckets[BUCKET].bucket_name=${configuredBucket ?? "<unset>"} (taken from wrangler config)`
      : `BUCKET -> ${configuredBucket ?? "<unset>"} (want ${expectedBucket})`,
  );

  const kv = cfg.kv_namespaces ?? [];
  const kvBinding = kv.find((b) => b.binding === "INSTANCE_TOKENS");
  add(
    "kv_instance_tokens_binding",
    kvBinding !== undefined && typeof kvBinding.id === "string" && kvBinding.id.trim() !== "",
    true,
    "INSTANCE_TOKENS KV namespace binding (spec 023 D1)",
  );
  // Spec 024 R2: the checked-in all-zero id is a safe repository placeholder,
  // but deploying it would silently break token admission. Fail the real
  // preflight; only an explicit structural lint may accept it.
  add(
    "kv_instance_tokens_not_placeholder",
    opts.allowPlaceholderKv === true || !isPlaceholderKvId(kvBinding?.id),
    true,
    opts.allowPlaceholderKv === true
      ? "structural lint: placeholder KV id accepted only with allowPlaceholderKv"
      : isPlaceholderKvId(kvBinding?.id)
        ? "INSTANCE_TOKENS id is empty/all-zero (placeholder); substitute the real namespace id before deploy"
        : "INSTANCE_TOKENS id is a concrete namespace id",
  );

  const analytics = cfg.analytics_engine_datasets ?? [];
  add(
    "analytics_ingest_binding",
    analytics.some((b) => b.binding === "INGEST_ANALYTICS" && typeof b.dataset === "string" && b.dataset.trim() !== ""),
    true,
    "INGEST_ANALYTICS Analytics Engine dataset (spec 023 D2)",
  );

  const vars = isPlainObject(cfg.vars) ? cfg.vars : {};
  const lateWrite = Number.parseInt(String(vars["LATE_WRITE_LIMIT_MS"] ?? ""), 10);
  add(
    "late_write_limit_configured",
    Number.isFinite(lateWrite) && lateWrite > 0,
    true,
    "LATE_WRITE_LIMIT_MS must be a positive number of milliseconds (spec 023 D2.2)",
  );
  const insecure = vars["BURSTEL_DEV_INSECURE_AUTH"];
  add(
    "no_anonymous_admin",
    insecure === undefined || insecure === false || insecure === 0 || insecure === "0" || insecure === "",
    true,
    "BURSTEL_DEV_INSECURE_AUTH must not be enabled in vars",
  );
  add(
    "no_token_in_vars",
    !("BURSTEL_API_TOKEN" in vars) && !("BURSTEL_INTERNAL_TOKEN" in vars),
    true,
    "admin/internal tokens must be Worker secrets, not plaintext vars",
  );
  // Destructive GC must stay opt-in: a default-on retention/GC can never be
  // shipped accidentally (spec 018).
  const gc = vars["COMPACTION_GC"];
  add(
    "compaction_gc_opt_in",
    gc === undefined || gc === false || gc === 0 || gc === "0" || gc === "",
    true,
    `COMPACTION_GC=${JSON.stringify(gc ?? null)} must be disabled (0/false/empty) by default`,
  );

  return checks;
}

// ----------------------------------------------------------------- whoami

export interface Whoami {
  accountId?: string;
  accountName?: string;
  email?: string;
  authType?: string;
}

export function parseWhoami(text: string): Whoami {
  const raw = JSON.parse(text) as unknown;
  if (!isPlainObject(raw)) return {};
  const out: Whoami = {};
  const account = raw["account"];
  if (isPlainObject(account)) {
    if (typeof account["id"] === "string") out.accountId = account["id"];
    if (typeof account["name"] === "string") out.accountName = account["name"];
  }
  const accounts = raw["accounts"];
  if (!out.accountId && Array.isArray(accounts)) {
    for (const item of accounts) {
      if (isPlainObject(item) && typeof item["id"] === "string") {
        out.accountId = item["id"];
        if (typeof item["name"] === "string") out.accountName = item["name"];
        break;
      }
    }
  }
  if (typeof raw["email"] === "string") out.email = raw["email"];
  if (typeof raw["authType"] === "string") out.authType = raw["authType"];
  return out;
}

export function validateWhoami(whoami: Whoami): Check[] {
  const accountId = whoami.accountId ?? "";
  return [
    {
      name: "authenticated",
      ok: accountId.length > 0,
      required: true,
      detail: accountId ? `account=${whoami.accountName ?? "<unnamed>"}` : "no account id from wrangler whoami",
    },
    {
      name: "account_id_format",
      ok: /^[0-9a-f]{32}$/.test(accountId),
      required: true,
      detail: "Cloudflare account id must be 32 lowercase hex characters",
    },
  ];
}

// --------------------------------------------------------------- secrets

export function parseSecretList(text: string): string[] {
  const raw = JSON.parse(text) as unknown;
  if (!Array.isArray(raw)) return [];
  const names: string[] = [];
  for (const item of raw) {
    if (isPlainObject(item) && typeof item["name"] === "string") names.push(item["name"]);
  }
  return names.sort();
}

export function validateSecrets(
  names: string[],
  required: string[] = ["BURSTEL_API_TOKEN", "BURSTEL_INTERNAL_TOKEN"],
): Check[] {
  return required.map((name) => ({
    name: `secret_present_${name}`,
    ok: names.includes(name),
    required: true,
    detail: `${name} ${names.includes(name) ? "is set" : "is MISSING"} (presence only, value never read)`,
  }));
}

// ------------------------------------------------------------------- r2

export function parseR2BucketList(text: string): string[] {
  const raw = JSON.parse(text) as unknown;
  if (!Array.isArray(raw)) return [];
  const names: string[] = [];
  for (const item of raw) {
    if (isPlainObject(item) && typeof item["name"] === "string") names.push(item["name"]);
  }
  return names.sort();
}

export function validateR2Buckets(names: string[], expected?: string): Check[] {
  const want = (expected ?? "").trim();
  if (want === "") {
    return [
      {
        name: "r2_bucket_exists",
        ok: false,
        required: true,
        detail: "wrangler config has no r2_buckets[BUCKET].bucket_name; cannot verify the bucket",
      },
    ];
  }
  return [
    {
      name: "r2_bucket_exists",
      ok: names.includes(want),
      required: true,
      detail: names.includes(want)
        ? `bucket ${want} already exists (reused, never overwritten)`
        : `bucket ${want} does not exist; create it in an authorized step (never auto-created)`,
    },
  ];
}

// --------------------------------------------------------------- tooling

export interface Tooling {
  dockerCli: boolean;
  dockerDaemon: boolean;
  dockerBuildx: boolean;
  dockerPlatformLinuxAmd64: boolean;
}

export function validateTooling(t: Tooling): Check[] {
  return [
    {
      name: "docker_cli",
      ok: t.dockerCli,
      required: true,
      detail: "docker CLI must be installed to build the linux/amd64 image",
    },
    {
      name: "docker_daemon",
      ok: t.dockerDaemon,
      required: true,
      detail: "docker daemon must be reachable on the build host",
    },
    {
      name: "docker_buildx",
      ok: t.dockerBuildx,
      required: true,
      detail: "docker buildx is required for --platform linux/amd64 --load",
    },
    {
      name: "docker_platform_linux_amd64",
      ok: t.dockerPlatformLinuxAmd64,
      required: true,
      detail: "buildx must be able to build/load linux/amd64",
    },
  ];
}

export interface AccountFacts {
  /** Whether a Worker named `burstel` already exists (collision lookup). */
  workerExists?: boolean;
  /** Whether the account is on Workers Paid (required for Containers). */
  containersPaid?: boolean;
  /** Optional human-readable plan name, for the operator report only. */
  planName?: string;
}

export function validateAccountFacts(facts: AccountFacts): Check[] {
  return [
    {
      name: "no_unexpected_worker_collision",
      ok: facts.workerExists !== true,
      required: false,
      detail:
        facts.workerExists === true
          ? "a Worker named burstel already exists; a deploy updates it in place and never recreates R2/DO resources automatically"
          : facts.workerExists === false
            ? "no existing Worker named burstel"
            : "worker collision status unknown (set CLOUDFLARE_API_TOKEN to check)",
    },
    {
      name: "workers_paid_containers_eligible",
      ok: facts.containersPaid !== false,
      required: true,
      detail:
        facts.containersPaid === true
          ? `Workers Paid/Containers eligible${facts.planName ? ` (${facts.planName})` : ""}`
          : facts.containersPaid === false
            ? "account is NOT on Workers Paid; Containers cannot be deployed (do not auto-upgrade)"
            : "Workers Paid eligibility unknown (set CLOUDFLARE_API_TOKEN to check); confirm manually before deploy",
    },
  ];
}

export function finalize(checks: Check[]): Report {
  const failed = checks.filter((c) => c.required && !c.ok).map((c) => c.name);
  return { ok: failed.length === 0, checks, failed_required: failed };
}
