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

export interface WranglerConfig {
  name?: string;
  main?: string;
  compatibility_date?: string;
  containers?: ContainerConfig[];
  durable_objects?: { bindings?: DoBinding[] };
  exports?: Record<string, DoExport>;
  r2_buckets?: R2Binding[];
  vars?: Record<string, unknown>;
  workers_dev?: boolean;
}

export interface WranglerValidationOptions {
  maxInstances?: number;
  expectedBucket?: string;
  expectedWorkerName?: string;
  expectedImage?: string;
  expectedImageBuildContext?: string;
  expectedInstanceType?: string;
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

export function validateWranglerConfig(
  cfg: WranglerConfig,
  opts: WranglerValidationOptions = {},
): Check[] {
  const maxInstances = opts.maxInstances ?? 10;
  const expectedBucket = opts.expectedBucket ?? "burstel-shots";
  const expectedWorkerName = opts.expectedWorkerName ?? "burstel";
  const expectedImage = opts.expectedImage ?? "../Dockerfile";
  const expectedContext = opts.expectedImageBuildContext ?? "..";
  const expectedInstanceType = opts.expectedInstanceType ?? "standard-1";
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

  const bindings = cfg.durable_objects?.bindings ?? [];
  add(
    "do_binding_shotdo",
    bindings.some((b) => b.name === "SHOT_DO" && b.class_name === "ShotDO"),
    true,
    "SHOT_DO -> ShotDO durable object binding",
  );
  add(
    "do_binding_ingestdo",
    bindings.some((b) => b.name === "INGEST_DO" && b.class_name === "IngestDO"),
    true,
    "INGEST_DO -> IngestDO durable object binding",
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
    exports["IngestDO"]?.type === "durable-object" && exports["IngestDO"]?.storage === "sqlite",
    true,
    `exports.IngestDO=${JSON.stringify(exports["IngestDO"] ?? null)}`,
  );

  const r2 = cfg.r2_buckets ?? [];
  add(
    "r2_bucket_binding",
    r2.some((b) => b.binding === "BUCKET" && b.bucket_name === expectedBucket),
    true,
    `BUCKET -> ${expectedBucket}`,
  );

  const vars = isPlainObject(cfg.vars) ? cfg.vars : {};
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

export function validateR2Buckets(names: string[], expected = "burstel-shots"): Check[] {
  return [
    {
      name: "r2_bucket_exists",
      ok: names.includes(expected),
      required: true,
      detail: names.includes(expected)
        ? `bucket ${expected} already exists (reused, never overwritten)`
        : `bucket ${expected} does not exist; create it in an authorized step (never auto-created)`,
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
