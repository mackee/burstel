import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Spec 030: the public-release guard must stay green for the tracked tree and
// must actually fail on a planted deployment identifier.
const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const SCRIPT = join(REPO_ROOT, "scripts", "check-public.sh");

function runGuard(cwd: string, root?: string) {
  const args = root === undefined ? [SCRIPT] : [SCRIPT, "--root", root];
  return spawnSync("bash", args, { cwd, encoding: "utf8" });
}

function git(cwd: string, ...args: string[]) {
  const result = spawnSync("git", args, { cwd, encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
  }
}

describe("public release guard", () => {
  test("the tracked tree has no deployment identifiers", () => {
    const result = runGuard(REPO_ROOT);
    if (result.status !== 0) {
      throw new Error(`check-public.sh failed:\n${result.stderr}`);
    }
    expect(result.status).toBe(0);
    expect(result.stdout).toContain("check-public: OK");
  });

  test("a planted identifier in a scratch repository fails the guard", () => {
    const dir = mkdtempSync(join(tmpdir(), "burstel-public-"));
    try {
      git(dir, "init", "-q");
      const planted = ["triple", "point"].join("");
      writeFileSync(join(dir, "notes.md"), `deployment host: ${planted}\n`);
      git(dir, "add", "notes.md");
      const result = runGuard(REPO_ROOT, dir);
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("notes.md");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
