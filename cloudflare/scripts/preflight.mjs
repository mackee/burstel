#!/usr/bin/env bun
// Thin runner for the pure preflight validator (../src/preflight.ts).
//
// The bash entry point `scripts/cloudflare-preflight.sh` captures read-only CLI
// output into mode-600 temp files and calls this runner. Nothing here prints a
// secret value: only check names, booleans and safe detail strings.
//
// Usage:
//   preflight.mjs --config <wrangler.jsonc> [--whoami <file>] [--secrets <file>]
//                 [--r2 <file>] [--docker-cli 1] [--docker-daemon 1]
//                 [--docker-buildx 1] [--docker-platform-linux-amd64 1]
//                 [--max-instances 10]
import { readFileSync } from "node:fs";

import {
  finalize,
  loadWranglerConfig,
  parseR2BucketList,
  parseSecretList,
  parseWhoami,
  validateAccountFacts,
  validateR2Buckets,
  validateSecrets,
  validateTooling,
  validateWhoami,
  validateWranglerConfig,
} from "../src/preflight.ts";

function arg(name, fallback = undefined) {
  const i = process.argv.indexOf(`--${name}`);
  if (i === -1) return fallback;
  const v = process.argv[i + 1];
  return v === undefined || v.startsWith("--") ? true : v;
}

function flag(name) {
  const v = arg(name, "0");
  return v === true || v === "1" || v === "true";
}

function tri(name) {
  const i = process.argv.indexOf(`--${name}`);
  if (i === -1) return undefined;
  const v = process.argv[i + 1];
  if (v === undefined || v.startsWith("--")) return true;
  if (v === "1" || v === "true") return true;
  if (v === "0" || v === "false") return false;
  return undefined;
}

function readMaybe(path) {
  if (!path || path === true) return undefined;
  return readFileSync(path, "utf8");
}

const checks = [];

const configPath = arg("config");
if (!configPath || configPath === true) {
  console.error("preflight: --config <wrangler.jsonc> is required");
  process.exit(2);
}
const config = loadWranglerConfig(readFileSync(configPath, "utf8"));
const maxInstances = Number(arg("max-instances", "10"));
checks.push(...validateWranglerConfig(config, { maxInstances }));

if (flag("config-only")) {
  const configReport = finalize(checks);
  process.stdout.write(`${JSON.stringify(configReport, null, 2)}\n`);
  process.exit(configReport.ok ? 0 : 1);
}

const whoamiRaw = readMaybe(arg("whoami"));
if (whoamiRaw !== undefined) {
  try {
    checks.push(...validateWhoami(parseWhoami(whoamiRaw)));
  } catch (error) {
    checks.push({ name: "authenticated", ok: false, required: true, detail: `whoami parse failed: ${String(error)}` });
  }
} else {
  checks.push({ name: "authenticated", ok: false, required: true, detail: "wrangler whoami was not captured" });
}

const secretsRaw = readMaybe(arg("secrets"));
if (secretsRaw !== undefined) {
  try {
    checks.push(...validateSecrets(parseSecretList(secretsRaw)));
  } catch (error) {
    checks.push({ name: "secret_list_parse", ok: false, required: true, detail: `secret list parse failed: ${String(error)}` });
  }
} else {
  checks.push({ name: "secret_list_present", ok: false, required: true, detail: "wrangler secret list was not captured" });
}

const r2Raw = readMaybe(arg("r2"));
if (r2Raw !== undefined) {
  try {
    checks.push(...validateR2Buckets(parseR2BucketList(r2Raw)));
  } catch (error) {
    checks.push({ name: "r2_list_parse", ok: false, required: true, detail: `r2 list parse failed: ${String(error)}` });
  }
} else {
  checks.push({ name: "r2_bucket_exists", ok: false, required: true, detail: "wrangler r2 bucket list was not captured" });
}

checks.push(
  ...validateTooling({
    dockerCli: flag("docker-cli"),
    dockerDaemon: flag("docker-daemon"),
    dockerBuildx: flag("docker-buildx"),
    dockerPlatformLinuxAmd64: flag("docker-platform-linux-amd64"),
  }),
);

checks.push(
  ...validateAccountFacts({
    workerExists: tri("worker-exists"),
    containersPaid: tri("containers-paid"),
    planName: typeof arg("plan-name") === "string" ? arg("plan-name") : undefined,
  }),
);

const report = finalize(checks);
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
process.exit(report.ok ? 0 : 1);
