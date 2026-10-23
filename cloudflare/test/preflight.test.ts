import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

import {
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
  test("the checked-in wrangler.jsonc satisfies every required invariant", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    const report = finalize(validateWranglerConfig(cfg));
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
    cfg.exports!["ControlDO"]!.storage = "key-value";
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("export_ingestdo_sqlite");
  });

  test("requires the SQLite AuthDO binding/export (spec 013, Part B)", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.exports!["AuthDO"]!.storage = "key-value";
    cfg.durable_objects!.bindings = cfg.durable_objects!.bindings!.filter((b) => b.name !== "AUTH_DO");
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("export_authdo_sqlite");
    expect(report.failed_required).toContain("do_binding_authdo");
  });

  test("requires the BUCKET -> burstel-shots R2 binding", () => {
    const cfg = loadWranglerConfig(REAL_CONFIG);
    cfg.r2_buckets![0]!.bucket_name = "some-other-bucket";
    const report = finalize(validateWranglerConfig(cfg));
    expect(report.failed_required).toContain("r2_bucket_binding");
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
    expect(finalize(validateR2Buckets(names)).ok).toBe(true);
    expect(finalize(validateR2Buckets([])).failed_required).toEqual(["r2_bucket_exists"]);
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
    const compactorDo = repoFile("../src/compactor-do.ts");
    expect(compactorDo).toContain("BURSTEL_ADDR: `:${port}`");
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
