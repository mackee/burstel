// Deterministic fakes for the pure ShotCore lifecycle tests.
//
// These let us exercise the real state machine, the real SQLite persistence
// (via bun:sqlite) and the real R2 streaming code paths without a Cloudflare
// account, Docker or a running container.

import { Database } from "bun:sqlite";

import type { ArtifactResult, ArtifactSink, ArtifactSource } from "../src/artifacts";
import type { ContainerControl } from "../src/container";
import type { ShotCoreDeps } from "../src/core";
import { ShotCore } from "../src/core";
import type { SqlCursor, SqlRow, SqlValue, SyncSql } from "../src/store";

/** `SyncSql` over an in-process bun:sqlite database (same schema as DO SQLite). */
export class BunSql implements SyncSql {
  readonly db: Database;

  constructor(db: Database = new Database(":memory:")) {
    this.db = db;
  }

  exec(query: string, ...bindings: SqlValue[]): SqlCursor {
    const rows = this.db.query(query).all(...(bindings as unknown as never[])) as SqlRow[];
    const changes = (this.db as unknown as { changes?: number }).changes ?? 0;
    return { toArray: () => rows, rowsWritten: changes };
  }
}

export interface FakeContainerOptions {
  artifactBytes?: Uint8Array;
  goStartStatus?: number;
  goStopStatus?: number;
  goStopResult?: { state: string; size_bytes: number; sha256: string };
  metadataCounters?: Record<string, unknown>;
  otlpStatus?: number;
  otlpBody?: Uint8Array;
  otlpContentType?: string;
  readyError?: Error;
  /** Awaited by every /v1/* request; lets tests hold ingests open. */
  ingestGate?: () => Promise<void>;
  metadataDelayMs?: number;
}

export class FakeContainer implements ContainerControl {
  runningFlag = false;
  started = false;
  destroyed = false;
  startEnv: Record<string, string> | undefined;

  startStatus = 200;
  goStartStatus: number;
  goStopStatus: number;
  goStopResult: { state: string; size_bytes: number; sha256: string } | undefined;
  metadataCounters: Record<string, unknown> | undefined;

  otlpStatus: number;
  otlpBody: Uint8Array;
  otlpContentType: string;

  readyError: Error | undefined;
  ingestGate: (() => Promise<void>) | undefined;
  metadataDelayMs: number;

  readonly ingestBodies: Array<{ signal: string; bytes: number }> = [];
  artifactReads = 0;

  private artifactBytes: Uint8Array;
  private artifactSha = "";

  constructor(options: FakeContainerOptions = {}) {
    this.artifactBytes = options.artifactBytes ?? new Uint8Array([1, 2, 3, 4]);
    this.goStartStatus = options.goStartStatus ?? 200;
    this.goStopStatus = options.goStopStatus ?? 200;
    this.goStopResult = options.goStopResult;
    this.metadataCounters = options.metadataCounters;
    this.otlpStatus = options.otlpStatus ?? 200;
    this.otlpBody = options.otlpBody ?? new Uint8Array([0x0a, 0x00]);
    this.otlpContentType = options.otlpContentType ?? "application/x-protobuf";
    this.readyError = options.readyError;
    this.ingestGate = options.ingestGate;
    this.metadataDelayMs = options.metadataDelayMs ?? 0;
    void sha256Hex(this.artifactBytes).then((sha) => {
      this.artifactSha = sha;
    });
  }

  get running(): boolean {
    return this.runningFlag;
  }

  start(env: Record<string, string>): void {
    this.started = true;
    this.startEnv = env;
    this.runningFlag = true;
  }

  async ensureReady(): Promise<void> {
    if (this.readyError) throw this.readyError;
    this.runningFlag = true;
  }

  async request(path: string, init?: RequestInit): Promise<Response> {
    if (path === "/readyz") return new Response("ready\n", { status: 200 });

    if (path === "/internal/start") {
      return new Response(JSON.stringify({ state: "recording" }), { status: this.goStartStatus });
    }

    if (path === "/internal/stop") {
      if (!this.goStopResult) {
        const sha = this.artifactSha || (await sha256Hex(this.artifactBytes));
        this.goStopResult = {
          state: "frozen",
          size_bytes: this.artifactBytes.byteLength,
          sha256: sha,
        };
      }
      return new Response(JSON.stringify(this.goStopResult), { status: this.goStopStatus });
    }

    if (path === "/internal/artifact") {
      this.artifactReads += 1;
      const sha = this.artifactSha || (await sha256Hex(this.artifactBytes));
      return new Response(this.artifactBytes, {
        status: 200,
        headers: {
          "content-type": "application/octet-stream",
          "content-length": String(this.artifactBytes.byteLength),
          "x-burstel-sha256": sha,
        },
      });
    }

    if (path === "/internal/metadata") {
      if (this.metadataDelayMs > 0) await sleep(this.metadataDelayMs);
      return new Response(JSON.stringify({ counters: this.metadataCounters }), { status: 200 });
    }

    if (path.startsWith("/v1/")) {
      if (this.ingestGate) await this.ingestGate();
      const signal = path.slice("/v1/".length);
      const bytes = init?.body ? await readBody(init.body) : new Uint8Array();
      this.ingestBodies.push({ signal, bytes: bytes.byteLength });
      return new Response(this.otlpBody, {
        status: this.otlpStatus,
        headers: { "content-type": this.otlpContentType },
      });
    }

    return new Response("not found", { status: 404 });
  }

  async destroy(): Promise<void> {
    this.destroyed = true;
    this.runningFlag = false;
  }
}

export interface FakeArtifactsOptions {
  failPut?: Error;
}

export class FakeArtifacts implements ArtifactSink {
  readonly stored = new Map<string, Uint8Array>();
  readonly json = new Map<string, unknown>();
  putCalls = 0;
  failPut: Error | undefined;

  constructor(options: FakeArtifactsOptions = {}) {
    this.failPut = options.failPut;
  }

  async putArtifact(key: string, source: ArtifactSource): Promise<ArtifactResult> {
    this.putCalls += 1;
    if (this.failPut) throw this.failPut;
    if (!source.body) throw new Error("empty body");
    const bytes = new Uint8Array(await new Response(source.body).arrayBuffer());
    if (bytes.byteLength !== source.size) {
      throw new Error(`size mismatch: ${bytes.byteLength} != ${source.size}`);
    }
    this.stored.set(key, bytes);
    return { key, size: bytes.byteLength, sha256: source.sha256, multipart: false };
  }

  async putJSON(key: string, value: unknown): Promise<void> {
    if (this.failPut) throw this.failPut;
    this.json.set(key, value);
  }

  async getJSON<T>(key: string): Promise<T | null> {
    return (this.json.get(key) as T | undefined) ?? null;
  }
}

export interface TestHarness {
  core: ShotCore;
  sql: BunSql;
  container: FakeContainer;
  artifacts: FakeArtifacts;
}

export function makeCore(
  shotId: string,
  options: {
    container?: FakeContainer;
    artifacts?: FakeArtifacts;
    sql?: BunSql;
    now?: () => number;
    drainTimeoutMs?: number;
  } = {},
): TestHarness {
  const sql = options.sql ?? new BunSql();
  const container = options.container ?? new FakeContainer();
  const artifacts = options.artifacts ?? new FakeArtifacts();
  let tick = 0;
  const now = options.now ?? (() => 1_700_000_000_000 + tick++);
  const deps: ShotCoreDeps = {
    shotId,
    sql,
    container,
    artifacts,
    now,
    internalToken: "internal-secret",
    containerEnv: { BURSTEL_ADDR: ":8080", BURSTEL_DATA_DIR: "/data" },
    drainTimeoutMs: options.drainTimeoutMs ?? 2000,
  };
  const core = new ShotCore(deps);
  return { core, sql, container, artifacts };
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  const digest = await crypto.subtle.digest("SHA-256", copy.buffer);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function readBody(body: unknown): Promise<Uint8Array> {
  if (body instanceof Uint8Array) return body;
  if (typeof body === "string") return new TextEncoder().encode(body);
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  return new Uint8Array(await new Response(body as ReadableStream).arrayBuffer());
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
