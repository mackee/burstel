// Deterministic fakes for the pure ShotCore lifecycle tests.
//
// These let us exercise the real state machine, the real SQLite persistence
// (via bun:sqlite) and the real R2 streaming code paths without a Cloudflare
// account, Docker or a running container.

import { Database } from "bun:sqlite";

import type {
  ArtifactResult,
  ArtifactSink,
  ArtifactSource,
  R2BucketLike,
  R2ObjectBodyLike,
  R2ObjectLike,
  R2PutOptionsLike,
} from "../src/artifacts";
import { AuthCore } from "../src/auth-core";
import type { ContainerControl } from "../src/container";
import type { HeartbeatScheduler, ShotCoreDeps } from "../src/core";
import { ShotCore } from "../src/core";
import type { AuthNamespaceLike, DoStubLike, IngestNamespaceLike } from "../src/env";
import { IngestCore } from "../src/ingest-core";
import type { SqlCursor, SqlRow, SqlValue, SyncSql } from "../src/store";

/** `SyncSql` over an in-process bun:sqlite database (same schema as DO SQLite). */
export class BunSql implements SyncSql {
  readonly db: Database;

  constructor(db: Database = new Database(":memory:")) {
    this.db = db;
  }

  exec(query: string, ...bindings: SqlValue[]): SqlCursor {
    // bun:sqlite exposes the affected-row count only from `run()`, and `all()`
    // only returns rows. Route reads through `all()` and writes through `run()`
    // so `rowsWritten` matches the real Durable Object SQLite semantics.
    if (/^\s*(select|with|pragma|explain|values)\b/i.test(query)) {
      const rows = this.db.query(query).all(...(bindings as unknown as never[])) as SqlRow[];
      return { toArray: () => rows, rowsWritten: 0 };
    }
    const result = this.db.query(query).run(...(bindings as unknown as never[]));
    return { toArray: () => [], rowsWritten: result.changes ?? 0 };
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
  keepAliveError?: Error;
  /** Awaited by every /v1/* request; lets tests hold ingests open. */
  ingestGate?: () => Promise<void>;
  metadataDelayMs?: number;
  /** Canned analysis responses returned by the analysis endpoints. */
  queryResult?: Record<string, unknown>;
  analyzeResult?: Record<string, unknown>;
  compareResult?: Record<string, unknown>;
  schemaResult?: Record<string, unknown>;
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
  keepAliveError: Error | undefined;
  /** Every inactivity timeout applied, in order. */
  readonly keepAlives: number[] = [];
  ingestGate: (() => Promise<void>) | undefined;
  metadataDelayMs: number;

  queryResult: Record<string, unknown>;
  analyzeResult: Record<string, unknown>;
  compareResult: Record<string, unknown>;
  schemaResult: Record<string, unknown>;

  readonly ingestBodies: Array<{ signal: string; bytes: number }> = [];
  readonly restored = new Map<string, { bytes: Uint8Array; sha: string; size: number }>();
  /** Window analysis sessions and the segment bytes streamed into them. */
  readonly windowSessions = new Set<string>();
  readonly windowSegments: Array<{ session: string; key: string; bytes: number }> = [];
  /** Every proxied request, with its headers, for auth-leak assertions. */
  readonly requests: Array<{ path: string; method: string; headers: Record<string, string> }> = [];
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
    this.keepAliveError = options.keepAliveError;
    this.ingestGate = options.ingestGate;
    this.metadataDelayMs = options.metadataDelayMs ?? 0;
    this.queryResult = options.queryResult ?? {
      columns: [{ name: "n", type: "BIGINT" }],
      rows: [[1]],
      row_count: 1,
      truncated: false,
      limit: 1000,
      bytes: 8,
    };
    this.analyzeResult = options.analyzeResult ?? {
      shot_id: "shot",
      schema_version: "1",
      raw_counts: { spans: 1 },
      sections: [{ name: "overview", rows: [[1]], sample_size: 1, group_count: 1, observed: true }],
      observed: ["overview"],
    };
    this.compareResult = options.compareResult ?? {
      left: { counts: { spans: 2 } },
      right: { counts: { spans: 2 } },
      sample_sizes: { left: { spans: 2 }, right: { spans: 2 } },
      latency_deltas: [],
      error_deltas: [],
      disclosure: ["synthetic"],
    };
    this.schemaResult = options.schemaResult ?? {
      schema_version: "1",
      tables: [{ name: "spans" }],
      templates: [{ name: "overview" }],
      safety: { read_only: true, max_rows: 1000 },
    };
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

  async keepAlive(durationMs: number): Promise<void> {
    if (this.keepAliveError) throw this.keepAliveError;
    this.keepAlives.push(durationMs);
  }

  async request(path: string, init?: RequestInit): Promise<Response> {
    const headers: Record<string, string> = {};
    new Headers(init?.headers as Record<string, string> | undefined).forEach((value, key) => {
      headers[key.toLowerCase()] = value;
    });
    this.requests.push({ path, method: init?.method ?? "GET", headers });

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

    if (path === "/internal/restore") {
      const bytes = init?.body ? await readBody(init.body) : new Uint8Array();
      const headers = new Headers(init?.headers);
      const restoreId = headers.get("x-burstel-restore-id") ?? "";
      const declaredSize = headers.get("x-burstel-size");
      const declaredSha = (headers.get("x-burstel-sha256") ?? "").toLowerCase();
      const sha = await sha256Hex(bytes);
      if (declaredSize !== null && Number(declaredSize) !== bytes.byteLength) {
        return new Response(JSON.stringify({ error: "size mismatch", code: "size_mismatch" }), { status: 400 });
      }
      if (declaredSha && declaredSha !== sha) {
        return new Response(JSON.stringify({ error: "sha mismatch", code: "sha256_mismatch" }), { status: 400 });
      }
      this.restored.set(restoreId, { bytes, sha, size: bytes.byteLength });
      return new Response(
        JSON.stringify({ restore_id: restoreId, size_bytes: bytes.byteLength, sha256: sha, schema_version: "1" }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (path === "/internal/query") {
      const body = init?.body ? JSON.parse(await textBody(init.body)) : {};
      if (!this.restored.has(body.restore_id)) {
        return new Response(JSON.stringify({ error: "not restored", code: "not_found" }), { status: 404 });
      }
      return new Response(JSON.stringify(this.queryResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/analyze") {
      const body = init?.body ? JSON.parse(await textBody(init.body)) : {};
      if (!this.restored.has(body.restore_id)) {
        return new Response(JSON.stringify({ error: "not restored", code: "not_found" }), { status: 404 });
      }
      return new Response(JSON.stringify(this.analyzeResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/schema") {
      return new Response(JSON.stringify(this.schemaResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/compare") {
      const body = init?.body ? JSON.parse(await textBody(init.body)) : {};
      if (!this.restored.has(body.left_restore_id) || !this.restored.has(body.right_restore_id)) {
        return new Response(JSON.stringify({ error: "not restored", code: "not_found" }), { status: 404 });
      }
      return new Response(JSON.stringify(this.compareResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/release") {
      this.restored.clear();
      return new Response(JSON.stringify({ released: true }), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/window/open") {
      const body = init?.body ? JSON.parse(await textBody(init.body)) : {};
      this.windowSessions.add(String(body.session_id));
      return new Response(
        JSON.stringify({ session_id: body.session_id, window: body.window, selection: { selected: [], missing: [], expected: 0, justification: "fake" } }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (path === "/internal/window/segment") {
      const headers = new Headers(init?.headers as Record<string, string> | undefined);
      const session = headers.get("x-burstel-session-id") ?? "";
      const key = headers.get("x-burstel-segment-key") ?? "";
      const bytes = init?.body ? (await readBody(init.body)).byteLength : 0;
      this.windowSegments.push({ session, key, bytes });
      return new Response(JSON.stringify({ accepted: true, segment_key: key }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    if (path === "/internal/window/commit") {
      return new Response(
        JSON.stringify({
          expected_segments: this.windowSegments.length,
          available_segments: this.windowSegments.length,
          missing_segments: [],
          instances: [],
          complete: true,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (path === "/internal/window/query") {
      return new Response(JSON.stringify(this.queryResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/window/analyze") {
      return new Response(JSON.stringify(this.analyzeResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/window/compare") {
      return new Response(JSON.stringify(this.compareResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/window/schema") {
      return new Response(JSON.stringify(this.schemaResult), { status: 200, headers: { "content-type": "application/json" } });
    }

    if (path === "/internal/window/release") {
      const body = init?.body ? JSON.parse(await textBody(init.body)) : {};
      this.windowSessions.delete(String(body.session_id));
      return new Response(JSON.stringify({ released: true, session_id: body.session_id }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
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

  async getArtifact(key: string): Promise<{ body: ReadableStream<Uint8Array>; size: number; sha256?: string } | null> {
    const bytes = this.stored.get(key);
    if (!bytes) return null;
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return { body: new Response(copy).body as ReadableStream<Uint8Array>, size: copy.byteLength };
  }
}

/** In-memory R2 bucket for the durable raw-journal intake tests. */
export class FakeBucket implements R2BucketLike {
  readonly objects = new Map<
    string,
    { bytes: Uint8Array; contentType?: string; customMetadata?: Record<string, string>; sha256?: string }
  >();
  putCalls = 0;
  failPut: Error | undefined;

  async put(key: string, value: unknown, options?: R2PutOptionsLike): Promise<R2ObjectLike | null> {
    this.putCalls += 1;
    if (this.failPut) throw this.failPut;
    const bytes = await coerceBytes(value);
    this.objects.set(key, {
      bytes,
      contentType: options?.httpMetadata?.contentType,
      customMetadata: options?.customMetadata,
      sha256: options?.sha256,
    });
    return { key, size: bytes.byteLength, etag: `"${key}"` };
  }

  async get(key: string): Promise<R2ObjectBodyLike | null> {
    const object = this.objects.get(key);
    if (!object) return null;
    const copy = new Uint8Array(object.bytes.byteLength);
    copy.set(object.bytes);
    return {
      key,
      size: copy.byteLength,
      body: new Response(copy).body as ReadableStream<Uint8Array>,
      text: async () => new TextDecoder().decode(object.bytes),
      customMetadata: object.customMetadata,
    };
  }

  async createMultipartUpload(): Promise<never> {
    throw new Error("multipart upload is not implemented by FakeBucket");
  }
}

async function coerceBytes(value: unknown): Promise<Uint8Array> {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (typeof value === "string") return new TextEncoder().encode(value);
  if (value == null) return new Uint8Array();
  return new Uint8Array(await new Response(value as ReadableStream<Uint8Array>).arrayBuffer());
}

/** Records armed heartbeats instead of setting a DO alarm. */
export class FakeScheduler implements HeartbeatScheduler {
  readonly armed: number[] = [];
  failWith: Error | undefined;

  set(atMs: number): void {
    if (this.failWith) throw this.failWith;
    this.armed.push(atMs);
  }
}

export interface TestHarness {
  core: ShotCore;
  sql: BunSql;
  container: FakeContainer;
  artifacts: FakeArtifacts;
  scheduler: FakeScheduler;
}

export interface IngestHarness {
  core: IngestCore;
  sql: BunSql;
}

/** Builds an {@link IngestCore} over real in-process SQLite with a fake clock. */
export function makeIngest(
  options: {
    now?: () => number;
    generateToken?: () => string;
    generateInstanceId?: () => string;
    generateTokenId?: () => string;
    sql?: BunSql;
  } = {},
): IngestHarness {
  const sql = options.sql ?? new BunSql();
  const core = new IngestCore({
    sql,
    now: options.now,
    generateToken: options.generateToken,
    generateInstanceId: options.generateInstanceId,
    generateTokenId: options.generateTokenId,
  });
  return { core, sql };
}

/** Namespace stand-in for the singleton IngestDO binding. */
export class FakeIngestNamespace implements IngestNamespaceLike {
  constructor(readonly core: IngestCore) {}

  getByName(): DoStubLike {
    return { fetch: (request: Request) => this.core.handleInternal(request) };
  }
}

export interface AuthHarness {
  core: AuthCore;
  sql: BunSql;
}

/** Builds an {@link AuthCore} over real in-process SQLite with a fake clock. */
export function makeAuth(
  options: { now?: () => number; generateToken?: () => string; generateId?: () => string; sql?: BunSql } = {},
): AuthHarness {
  const sql = options.sql ?? new BunSql();
  const core = new AuthCore({ sql, now: options.now, generateToken: options.generateToken, generateId: options.generateId });
  return { core, sql };
}

/** Namespace stand-in for the singleton AuthDO binding. */
export class FakeAuthNamespace implements AuthNamespaceLike {
  constructor(readonly core: AuthCore) {}

  getByName(): DoStubLike {
    return { fetch: (request: Request) => this.core.handleInternal(request) };
  }
}

export function makeCore(
  shotId: string,
  options: {
    container?: FakeContainer;
    artifacts?: FakeArtifacts;
    sql?: BunSql;
    now?: () => number;
    drainTimeoutMs?: number;
    scheduler?: FakeScheduler;
    maxRecordingMs?: number;
    /** Override the resolved internal-token state (defaults to a valid one). */
    internals?: { token?: string; ready?: boolean; error?: "missing" | "not_distinct" };
  } = {},
): TestHarness {
  const sql = options.sql ?? new BunSql();
  const container = options.container ?? new FakeContainer();
  const artifacts = options.artifacts ?? new FakeArtifacts();
  const scheduler = options.scheduler ?? new FakeScheduler();
  let tick = 0;
  const now = options.now ?? (() => 1_700_000_000_000 + tick++);
  const internals = options.internals;
  const deps: ShotCoreDeps = {
    shotId,
    sql,
    container,
    artifacts,
    now,
    internalToken: internals ? internals.token : "internal-secret",
    internalTokenReady: internals?.ready ?? true,
    internalTokenError: internals?.error,
    containerEnv: { BURSTEL_ADDR: ":8080", BURSTEL_DATA_DIR: "/data" },
    drainTimeoutMs: options.drainTimeoutMs ?? 2000,
    scheduler,
    heartbeatIntervalMs: 60_000,
    maxRecordingMs: options.maxRecordingMs,
  };
  const core = new ShotCore(deps);
  return { core, sql, container, artifacts, scheduler };
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

async function textBody(body: unknown): Promise<string> {
  return new TextDecoder().decode(await readBody(body));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
