// End-to-end bridge test against the *real* Go DuckDB compactor service.
//
// Unlike compactor.test.ts (which fakes the container), this builds and runs
// `cmd/burstel-compact -serve` as a subprocess and drives the real production
// orchestration (`CompactorCore`) through the real bounded HTTP contract, a
// real SQLite IngestCore and the R2 fake. It is skipped when no Go toolchain is
// available (for example a Worker-only CI image); the Go unit tests in
// `internal/compaction` always exercise the same service code.
//
// Run with a toolchain on PATH, e.g.:
//   PATH="$(ls -d /nix/store/*go-1*/bin):$PATH" \
//   CC="$(ls -d /nix/store/*gcc-wrapper*/bin/gcc | head -1)" CGO_ENABLED=1 \
//   bun test test/compactor-go.test.ts

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { CompactorCore } from "../src/compactor-core";
import type { ContainerControl, ContainerStartOptions } from "../src/container";
import { FakeBucket, FakeIngestNamespace, makeIngest, sha256Hex } from "./fakes";

const GO_BIN = process.env.BURSTEL_GO_BIN ?? which("go");
const goAvailable = GO_BIN !== null;
const REPO_ROOT = join(import.meta.dir, "..", "..");

function which(bin: string): string | null {
  const paths = (process.env.PATH ?? "").split(":");
  for (const dir of paths) {
    const candidate = join(dir, bin);
    try {
      if (Bun.file(candidate).size > 0) return candidate;
    } catch {
      // ignore
    }
  }
  return null;
}

class HttpContainer implements ContainerControl {
  runningFlag = false;
  constructor(
    private readonly base: string,
    private readonly token: string,
  ) {}

  get running(): boolean {
    return this.runningFlag;
  }

  start(_options?: ContainerStartOptions): void {
    this.runningFlag = true;
  }

  async ensureReady(): Promise<void> {
    this.runningFlag = true;
  }

  async keepAlive(): Promise<void> {
    // no-op
  }

  async destroy(): Promise<void> {
    this.runningFlag = false;
  }

  async request(path: string, init?: RequestInit): Promise<Response> {
    const headers = new Headers(init?.headers as Record<string, string> | undefined);
    headers.set("authorization", `Bearer ${this.token}`);
    return fetch(`${this.base}${path}`, {
      ...init,
      headers,
      duplex: init?.body ? "half" : undefined,
    } as RequestInit);
  }
}

describe.skipIf(!goAvailable)("compactor Go service bridge (spec 018)", () => {
  let workDir = "";
  let binary = "";
  let server: ReturnType<typeof Bun.spawn> | null = null;
  const token = "go-e2e-internal-token";
  const port = 18080 + Math.floor(Math.random() * 1000);

  beforeAll(async () => {
    workDir = mkdtempSync(join(tmpdir(), "burstel-go-e2e-"));
    binary = join(workDir, "burstel-compact");
    const build = Bun.spawnSync([GO_BIN!, "build", "-o", binary, "./cmd/burstel-compact"], {
      cwd: REPO_ROOT,
      env: { ...process.env, CGO_ENABLED: "1" },
      stdout: "pipe",
      stderr: "pipe",
    });
    if (build.exitCode !== 0) {
      throw new Error(`go build failed: ${build.stderr.toString()}`);
    }
    server = Bun.spawn([binary, "-serve", "-addr", `127.0.0.1:${port}`], {
      env: { ...process.env, BURSTEL_INTERNAL_TOKEN: token, BURSTEL_COMPACT_WORK_DIR: workDir },
      stdout: "pipe",
      stderr: "pipe",
    });
    const deadline = Date.now() + 20_000;
    for (;;) {
      try {
        const resp = await fetch(`http://127.0.0.1:${port}/healthz`);
        if (resp.status === 200) break;
      } catch {
        // not up yet
      }
      if (Date.now() > deadline) throw new Error("compactor service did not become healthy");
      await Bun.sleep(100);
    }
  }, 300_000);

  afterAll(() => {
    server?.kill();
    if (workDir) rmSync(workDir, { recursive: true, force: true });
  });

  test("compacts real OTLP journal objects into a verified R2 segment", async () => {
    const bucket = new FakeBucket();
    const clock = { now: 1_700_000_000_000 };
    const { core: ingest } = makeIngest({ now: () => clock.now, bucket });
    const container = new HttpContainer(`http://127.0.0.1:${port}`, token);
    const compactor = new CompactorCore({
      ingest: new FakeIngestNamespace(ingest),
      bucket,
      container,
      holder: "go-e2e",
      now: () => clock.now,
      config: { graceMs: 90_000 },
    });

    // Empty protobuf messages are valid OTLP export requests; the real Go
    // decoder accepts them and the real DuckDB segment is produced.
    const receivedAt = clock.now - 600_000;
    for (const signal of ["traces", "logs"]) {
      const bytes = new Uint8Array(0);
      const sha = await sha256Hex(bytes);
      const key = `journal/inst-go/${signal}/${receivedAt}-${signal}.bin`;
      await bucket.put(key, bytes, {
        sha256: sha,
        customMetadata: {
          sha256: sha,
          signal,
          instance_id: "inst-go",
          instance: "go-1",
          received_at: String(receivedAt),
          token_id: "tok-go",
          content_encoding: "",
          content_type: "application/x-protobuf",
        },
      });
    }

    const report = await compactor.tick();
    expect(report.ok).toBe(true);
    expect(report.segments_published).toBe(1);

    const segments = await ingest.listSegments({ limit: 10, offset: 0 });
    expect(segments.total).toBe(1);
    const seg = segments.segments[0]!;
    expect(seg.status).toBe("verified");
    const stored = await bucket.get(seg.object_key);
    expect(stored).not.toBeNull();
    expect(stored!.size).toBe(seg.size_bytes);
    expect(stored!.sha256).toBe(seg.sha256);
    // A real DuckDB segment is not empty.
    expect(seg.size_bytes).toBeGreaterThan(0);
  }, 60_000);
});
