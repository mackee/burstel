import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

import {
  bucketNameFromConfig,
  finalize,
  loadWranglerConfig,
  parseJsonc,
  parseR2BucketList,
  parseSecretList,
  parseWhoami,
  removeTrailingCommas,
  stripJsonComments,
  validateAccountFacts,
  validateR2Buckets,
  validateSecrets,
  validateTooling,
  validateWhoami,
  validateWranglerConfig,
} from "../src/preflight";
import type { WranglerConfig } from "../src/preflight";

function repoFile(rel: string): string {
  return readFileSync(new URL(rel, import.meta.url), "utf8");
}

const REAL_CONFIG = repoFile("../wrangler.jsonc");

describe("JSONC parsing", () => {
  test("strips line/block comments but not comment-like text in strings", () => {
    const input = '{\n  // line\n  "url": "https://x/y//z", /* block */\n  "n": 1\n}';
    const out = stripJsonComments(input);
    expect(out).not.toContain("line");
    expect(out).not.toContain("block");
    expect(out).toContain("https://x/y//z");
  });

  test("removes trailing commas outside strings", () => {
    const out = removeTrailingCommas('{"a": [1,2,],"b": "x,}",}');
    expect(JSON.parse(out)).toEqual({ a: [1, 2], b: "x,}" });
  });

  test("parses the real wrangler.jsonc with comments and trailing commas", () => {
    const parsed = parseJsonc(REAL_CONFIG) as ConfigShape;
    expect(parsed.name).toBe("burstel");
    expect(Array.isArray(parsed.containers)).toBe(true);
  });
});

interface ConfigShape {
  name?: string;
  containers?: unknown[];
}

describe("wrangler configuration validation", () => {
  test("the checked-in wrangler.jsonc satisfies every structural invariant", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    const report = finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true }));
    if (!report.ok) {
      throw new Error(`failed: ${report.failed_required.join(", ")}`);
    }
    expect(report.ok).toBe(true);
    expect(report.checks.length).toBeGreaterThan(8);
    // Port consistency: the DO injects BURSTEL_ADDR with its default port.
    expect(report.failed_required).toEqual([]);
  });

  test("rejects an unbounded max_instances (cost guard)", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.containers![0]!.max_instances = 1000;
    const report = finalize(validateWranglerConfig(cfg, { maxInstances: 10 }));
    expect(report.ok).toBe(false);
    expect(report.failed_required).toContain("container_max_instances_bounded");
  });

  test("requires SQLite-backed DO exports", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.exports!["IngestDO"]!.storage = "key-value";
    const report = finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true }));
    expect(report.failed_required).toContain("export_ingestdo_sqlite");
  });

  test("R1: the binding keeps the legacy IngestDO storage class across the upgrade", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    const binding = cfg.durable_objects!.bindings!.find((b) => b.name === "INGEST_DO");
    expect(binding?.class_name).toBe("IngestDO");
    expect(cfg.exports?.["IngestDO"]).toEqual({ type: "durable-object", storage: "sqlite" });
    expect(cfg.exports?.["ControlDO"]).toBeUndefined();
    const report = finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true }));
    expect(report.failed_required).not.toContain("do_rename_preserves_storage");
    expect(report.failed_required).not.toContain("do_binding_ingestdo");
  });

  test("R1: an unsafe ControlDO rename/orphan is rejected", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    const binding = cfg.durable_objects!.bindings!.find((b) => b.name === "INGEST_DO");
    binding!.class_name = "ControlDO";
    cfg.exports!["ControlDO"] = { type: "durable-object", storage: "sqlite" };
    const report = finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true }));
    expect(report.failed_required).toContain("do_binding_ingestdo");
    expect(report.failed_required).toContain("do_rename_preserves_storage");
    expect(report.failed_required).toContain("no_unsafe_control_do_rename");
  });

  test("R1: pre-023 and 024 configs resolve INGEST_DO to the same storage class", () => {
    const resolve = (cfg: WranglerConfig): string => {
      const binding = cfg.durable_objects?.bindings?.find((b) => b.name === "INGEST_DO");
      const exported = binding?.class_name ? cfg.exports?.[binding.class_name] : undefined;
      return `${binding?.class_name ?? "<missing>"}/${exported?.storage ?? "<missing>"}`;
    };
    // Class name + SQLite storage are the storage identity: if this value is
    // unchanged, Cloudflare reuses the existing DO's stored objects.
    const pre023: WranglerConfig = {
      durable_objects: { bindings: [{ name: "INGEST_DO", class_name: "IngestDO" }] },
      exports: { IngestDO: { type: "durable-object", storage: "sqlite" } },
    };
    const post024 = loadWranglerConfig(REAL_CONFIG);
    expect(resolve(pre023)).toBe("IngestDO/sqlite");
    expect(resolve(post024)).toBe(resolve(pre023));
  });

  test("R2: the all-zero INSTANCE_TOKENS placeholder fails a real preflight", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    expect(cfg.kv_namespaces?.[0]?.id).toBe("00000000000000000000000000000000");
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("kv_instance_tokens_not_placeholder");
    // Structural lint may explicitly opt in.
    expect(
      finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true })).failed_required,
    ).not.toContain("kv_instance_tokens_not_placeholder");
  });

  test("R2: a missing INGEST_ANALYTICS dataset binding fails preflight", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.analytics_engine_datasets = [];
    const report = finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true }));
    expect(report.failed_required).toContain("analytics_ingest_binding");
  });

  test("requires the SQLite AuthDO binding/export (spec 013, Part B)", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.exports!["AuthDO"]!.storage = "key-value";
    cfg.durable_objects!.bindings = cfg.durable_objects!.bindings!.filter((b) => b.name !== "AUTH_DO");
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("export_authdo_sqlite");
    expect(report.failed_required).toContain("do_binding_authdo");
  });

  test("requires a BUCKET r2_buckets entry with a bucket_name", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    // The configured name is the source of truth (spec 030): renaming it is
    // valid, but dropping the binding or the name is not.
    cfg.r2_buckets![0]!.bucket_name = "some-other-bucket";
    expect(finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true })).failed_required).toEqual([]);
    cfg.r2_buckets = [];
    expect(
      finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true })).failed_required,
    ).toContain("r2_bucket_binding");
  });

  test("rejects BURSTEL_DEV_INSECURE_AUTH and plaintext tokens in vars", () => {
    const cfg: WranglerConfig = loadWranglerConfig(REAL_CONFIG);
    cfg.vars = { BURSTEL_DEV_INSECURE_AUTH: "1", BURSTEL_API_TOKEN: "oops" };
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("no_anonymous_admin");
    expect(report.failed_required).toContain("no_token_in_vars");
  });

  test("requires the linux/amd64 Dockerfile build context", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.containers![0]!.image_build_context = "/tmp";
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("container_image");
  });

  test("requires the spec-018 compactor Container, DO, export and cron trigger", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.containers = cfg.containers!.filter((c) => c.class_name !== "CompactorDO");
    cfg.durable_objects!.bindings = cfg.durable_objects!.bindings!.filter((b) => b.name !== "COMPACTOR_DO");
    delete cfg.exports!["CompactorDO"];
    cfg.triggers = { crons: [] };
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("container_compactordo_singleton");
    expect(report.failed_required).toContain("do_binding_compactordo");
    expect(report.failed_required).toContain("export_compactordo_sqlite");
    expect(report.failed_required).toContain("compaction_cron_trigger");
  });

  test("rejects a default-on destructive GC", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.vars = { ...(cfg.vars ?? {}), COMPACTION_GC: "1" };
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("compaction_gc_opt_in");
  });
});

describe("wrangler whoami / secret / r2 parsing", () => {
  test("extracts the account id from either whoami shape", () => {
    const a = parseWhoami(JSON.stringify({ account: { id: "a".repeat(32), name: "X" }, email: "e@x" }));
    expect(a.accountId).toBe("a".repeat(32));
    expect(validateWhoami(a).every((c) => c.ok)).toBe(true);

    const b = parseWhoami(JSON.stringify({ accounts: [{ id: "b".repeat(32), name: "Y" }] }));
    expect(b.accountId).toBe("b".repeat(32));
  });

  test("fails closed on missing/invalid account id", () => {
    expect(finalize(validateWhoami(parseWhoami("{}"))).ok).toBe(false);
    expect(finalize(validateWhoami(parseWhoami('{"account":{"id":"nope"}}'))).ok).toBe(false);
  });

  test("secret list yields names only and never values", () => {
    const raw = JSON.stringify([
      { name: "BURSTEL_API_TOKEN", type: "secret_text" },
      { name: "BURSTEL_INTERNAL_TOKEN", type: "secret_text" },
    ]);
    const names = parseSecretList(raw);
    expect(names).toEqual(["BURSTEL_API_TOKEN", "BURSTEL_INTERNAL_TOKEN"]);
    expect(finalize(validateSecrets(names)).ok).toBe(true);
    expect(finalize(validateSecrets(["BURSTEL_API_TOKEN"])).failed_required).toEqual([
      "secret_present_BURSTEL_INTERNAL_TOKEN",
    ]);
  });

  test("secret list ignores unexpected value-bearing fields", () => {
    const raw = JSON.stringify([{ name: "BURSTEL_API_TOKEN", value: "super-secret" }]);
    const names = parseSecretList(raw);
    expect(names).toEqual(["BURSTEL_API_TOKEN"]);
    expect(JSON.stringify(names)).not.toContain("super-secret");
  });

  test("r2 bucket list presence is required and never created", () => {
    const names = parseR2BucketList(JSON.stringify([{ name: "burstel-shots" }, { name: "other" }]));
    expect(finalize(validateR2Buckets(names, "burstel-shots")).ok).toBe(true);
    expect(finalize(validateR2Buckets([], "burstel-shots")).failed_required).toEqual(["r2_bucket_exists"]);
    // No hardcoded default: without a configured bucket the check fails closed.
    expect(finalize(validateR2Buckets(names)).failed_required).toEqual(["r2_bucket_exists"]);
  });

  test("the expected bucket is read from the wrangler config", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    expect(bucketNameFromConfig(cfg)).toBe(cfg.r2_buckets?.find((b) => b.binding === "BUCKET")?.bucket_name);
    // Renaming the bucket in config must not require a validator change.
    cfg.r2_buckets = [{ binding: "BUCKET", bucket_name: "example-shots" }];
    expect(bucketNameFromConfig(cfg)).toBe("example-shots");
    expect(finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true })).ok).toBe(true);
    expect(parseR2BucketList(JSON.stringify([{ name: "example-shots" }]))).toContain("example-shots");
    // An explicit expectation still acts as a pin.
    expect(
      finalize(validateWranglerConfig(cfg, { allowPlaceholderKv: true, expectedBucket: "other" })).failed_required,
    ).toEqual(["r2_bucket_binding"]);
  });
});

describe("tooling and account facts", () => {
  test("docker/buildx compatibility is a required gate", () => {
    const missing = finalize(
      validateTooling({ dockerCli: false, dockerDaemon: false, dockerBuildx: false, dockerPlatformLinuxAmd64: false }),
    );
    expect(missing.ok).toBe(false);
    const ok = finalize(
      validateTooling({ dockerCli: true, dockerDaemon: true, dockerBuildx: true, dockerPlatformLinuxAmd64: true }),
    );
    expect(ok.ok).toBe(true);
  });

  test("unknown plan eligibility passes with a warning, definite ineligibility fails", () => {
    expect(finalize(validateAccountFacts({})).ok).toBe(true);
    expect(finalize(validateAccountFacts({ containersPaid: true })).ok).toBe(true);
    expect(finalize(validateAccountFacts({ containersPaid: false })).failed_required).toEqual([
      "workers_paid_containers_eligible",
    ]);
    const collision = validateAccountFacts({ workerExists: true }).find(
      (c) => c.name === "no_unexpected_worker_collision",
    );
    expect(collision?.required).toBe(false);
    expect(collision?.ok).toBe(false);
  });
});

describe("cross-file deployment invariants", () => {
  test("Dockerfile advertises 4318 and the DO overrides BURSTEL_ADDR to its 8080 port", () => {
    const dockerfile = repoFile("../../Dockerfile");
    expect(dockerfile).toMatch(/^EXPOSE 4318$/m);
    expect(dockerfile).toMatch(/^ENV BURSTEL_ADDR=:4318/m);

    const shotDo = repoFile("../src/shot-do.ts");
    expect(shotDo).toContain("DEFAULT_CONTAINER_PORT = 8080");
    expect(shotDo).toContain("BURSTEL_ADDR: `:${port}`");
    // The container gets a distinct internal token, never the admin token.
    expect(shotDo).toContain("containerEnv[\"BURSTEL_DEV_INSECURE_AUTH\"]");
  });

  test("Dockerfile.compactor advertises 8080 and the DO injects BURSTEL_ADDR", () => {
    const dockerfile = repoFile("../../Dockerfile.compactor");
    expect(dockerfile).toMatch(/^EXPOSE 8080$/m);
    expect(dockerfile).toMatch(/^ENV BURSTEL_ADDR=:8080/m);
    expect(dockerfile).toContain("USER burstel");
    const compactorEnv = repoFile("../src/compactor-env.ts");
    expect(compactorEnv).toContain("BURSTEL_ADDR: `:${port}`");
    // Spec 028: the secure container env must carry the distinct token.
    expect(compactorEnv).toContain('containerEnv["BURSTEL_INTERNAL_TOKEN"] = internal.token');
    const compactorDo = repoFile("../src/compactor-do.ts");
    expect(compactorDo).toContain("resolveCompactorContainerEnv(env, port)");
    expect(compactorDo).toContain("ctx.storage.setAlarm");
  });

  test(".dev.vars is gitignored so real tokens are never committed", () => {
    const gitignore = repoFile("../../.gitignore");
    expect(gitignore).toContain(".dev.vars");
  });

  test("the real config has no anonymous-admin escape hatch", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    expect(cfg.vars?.["BURSTEL_DEV_INSECURE_AUTH"]).toBeUndefined();
  });
});
