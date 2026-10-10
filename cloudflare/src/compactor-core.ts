// Compactor coordinator core (spec 018).
//
// This is the missing production link between the durable raw journal (spec 014)
// and the window-shot analysis (spec 016). It is deliberately free of
// Cloudflare-specific globals so the exact same orchestration runs in
// production (a `CompactorDO` with a real Container + R2 binding + IngestDO) and
// in bounded unit tests (a fake container + fake R2 + real SQLite IngestCore).
//
// One bounded tick:
//   1. acquire a fenced, single-active lease from the IngestDO (the durable
//      source of truth — never a local DuckDB manifest);
//   2. ask the IngestDO for the next bounded batch of *uncovered*, finalized
//      journal objects, grouped into receive-time windows;
//   3. stream each journal object's exact bytes into the short-lived Go
//      compactor Container, which reuses `internal/compaction` + the decoder and
//      returns one immutable DuckDB segment (deterministic key + checksum);
//   4. stream the segment bytes straight into R2 (R2 itself enforces the
//      advertised SHA-256), then HEAD-verify size/checksum;
//   5. publish the verified manifest + exact coverage set to the IngestDO, which
//      re-verifies the stored object before it advertises it;
//   6. optionally GC verified, unpinned journal objects; release the lease and
//      arm the next bounded tick.
//
// Cross-DO/R2 operations are not atomic: a crash after the R2 PUT but before the
// publish leaves the object orphaned. Because the segment key is deterministic
// over the sorted source keys, the next tick rebuilds the same key and R2's
// SHA-256 precondition makes the re-PUT idempotent, so retries can never
// double-count or delete acknowledged rows.

import type { R2BucketLike } from "./artifacts";
import type { ContainerControl } from "./container";
import { sha256HexBytes } from "./crypto";
import { DO_ORIGIN } from "./env";
import type { DoStubLike, IngestNamespaceLike } from "./env";
import { HttpError } from "./errors";
import { json } from "./http";
import { INGEST_DO_NAME } from "./ingest-core";
import type {
  CompactionBatch,
  CompactionLease,
  CompactionPlan,
  GCandidate,
  SegmentCoverageInput,
  SegmentPublishInput,
  SegmentPublishResult,
} from "./ingest-core";

/** The single compaction partition (one active compactor per installation). */
export const COMPACTION_PARTITION = "primary";
/** Name of the singleton compactor coordinator Durable Object. */
export const COMPACTOR_DO_NAME = "compactor-primary";

export interface CompactorConfig {
  windowMs: number;
  graceMs: number;
  retentionMs: number;
  gcEnabled: boolean;
  batchObjects: number;
  batchBytes: number;
  maxBatchesPerTick: number;
  tickBudgetMs: number;
  scheduleMs: number;
  leaseTtlMs: number;
  maxSegmentBytes: number;
  containerKeepAliveMs: number;
}

export function defaultCompactorConfig(): CompactorConfig {
  return {
    windowMs: 60_000,
    graceMs: 90_000,
    retentionMs: 24 * 60 * 60 * 1000,
    gcEnabled: false,
    batchObjects: 2048,
    batchBytes: 32 * 1024 * 1024,
    maxBatchesPerTick: 4,
    tickBudgetMs: 10 * 60 * 1000,
    scheduleMs: 120_000,
    leaseTtlMs: 15 * 60 * 1000,
    maxSegmentBytes: 256 * 1024 * 1024,
    containerKeepAliveMs: 10 * 60 * 1000,
  };
}

export interface CompactorDeps {
  /** Durable coordinator: plan, lease, publish, GC, status. */
  ingest: IngestNamespaceLike;
  /** Durable object bucket binding. Required to read the journal and store segments. */
  bucket?: R2BucketLike;
  /** The short-lived Go compactor Container. */
  container: ContainerControl;
  /** Extra environment forwarded to the container process. */
  containerEnv?: Record<string, string>;
  /** Bearer token for the container's /v1/* endpoints (defense in depth). */
  internalToken?: string;
  /**
   * Whether a usable, distinct internal token (or explicit local insecure dev)
   * is configured. When false the core fails closed and never boots or talks to
   * an unguarded container.
   */
  internalReady?: boolean;
  /** Injectable clock (epoch ms) for deterministic tests. */
  now?: () => number;
  /** Unique holder identity for lease fencing. */
  holder: string;
  config?: Partial<CompactorConfig>;
  scheduler?: { set(atMs: number): Promise<void> | void };
  log?: (message: string, fields?: Record<string, unknown>) => void;
}

export interface TickReport {
  ok: boolean;
  skipped?: string;
  batches: number;
  segments_published: number;
  bytes_published: number;
  journal_objects: number;
  gc_deleted: number;
  gc_protected: number;
  duration_ms: number;
  error?: string;
}

/** Built-segment response returned by the Go compactor service. */
interface BuildResponse {
  segment_key: string;
  size_bytes: number;
  sha256: string;
  first_received_at: number;
  last_received_at: number;
  request_count: number;
  record_count: number;
  span_count: number;
  log_count: number;
  metric_count: number;
  event_min_ns: number | null;
  event_max_ns: number | null;
  coverage: SegmentCoverageInput[];
  download_token: string;
}

export class CompactorCore {
  private readonly ingest: IngestNamespaceLike;
  private readonly bucket?: R2BucketLike;
  private readonly container: ContainerControl;
  private readonly containerEnv: Record<string, string>;
  private readonly internalToken?: string;
  private readonly internalReady: boolean;
  private readonly now: () => number;
  private readonly holder: string;
  private readonly config: CompactorConfig;
  private readonly scheduler?: { set(atMs: number): Promise<void> | void };
  private readonly log: (message: string, fields?: Record<string, unknown>) => void;

  private lastTickAt: number | null = null;
  private lastError: string | null = null;
  private schedulerActive = false;

  constructor(deps: CompactorDeps) {
    this.ingest = deps.ingest;
    this.bucket = deps.bucket;
    this.container = deps.container;
    this.containerEnv = deps.containerEnv ?? {};
    this.internalToken = deps.internalToken;
    this.internalReady = deps.internalReady ?? true;
    this.now = deps.now ?? (() => Date.now());
    this.holder = deps.holder;
    this.config = { ...defaultCompactorConfig(), ...(deps.config ?? {}) };
    this.scheduler = deps.scheduler;
    this.log = deps.log ?? (() => undefined);
  }

  // ------------------------------------------------------------------ tick

  /** Runs one bounded compaction tick and arms the next one. */
  async tick(): Promise<TickReport> {
    const start = Date.now();
    const report: TickReport = {
      ok: true,
      batches: 0,
      segments_published: 0,
      bytes_published: 0,
      journal_objects: 0,
      gc_deleted: 0,
      gc_protected: 0,
      duration_ms: 0,
    };
    this.lastTickAt = this.now();

    if (!this.bucket) {
      report.ok = false;
      report.skipped = "bucket_unavailable";
      report.error = "no object bucket binding is configured";
      await this.finish(report, start);
      return report;
    }
    if (!this.internalReady) {
      report.ok = false;
      report.skipped = "internal_token_unconfigured";
      report.error = "no distinct container internal token is configured";
      await this.finish(report, start);
      return report;
    }

    let lease: CompactionLease | null = null;
    try {
      lease = await this.acquireLease();
      if (!lease.ok) {
        // The finally block below finishes the tick (arms the next alarm).
        report.skipped = "lease_held";
        return report;
      }

      const plan = await this.plan();
      const deadline = start + this.config.tickBudgetMs;
      for (const batch of plan.batches) {
        if (Date.now() >= deadline) break;
        try {
          const published = await this.compactBatch(batch);
          report.batches += 1;
          report.segments_published += 1;
          report.bytes_published += published.size_bytes;
          report.journal_objects += batch.objects.length;
        } catch (error) {
          report.ok = false;
          report.error = errorMessage(error);
          this.lastError = report.error;
          this.log("burstel: compaction batch failed", { error: report.error });
          break;
        }
      }

      if (this.config.gcEnabled) {
        const gc = await this.runGC(false).catch((error) => {
          this.log("burstel: gc failed", { error: errorMessage(error) });
          return { deleted: 0, protected: 0 };
        });
        report.gc_deleted = gc.deleted;
        report.gc_protected = gc.protected;
      }
    } catch (error) {
      report.ok = false;
      report.error = errorMessage(error);
      this.lastError = report.error;
      this.log("burstel: compaction tick failed", { error: report.error });
    } finally {
      if (lease?.ok) {
        await this.releaseLease(lease).catch(() => undefined);
      }
      await this.finish(report, start);
    }
    return report;
  }

  /** Alias used by the DO alarm handler. */
  async alarm(): Promise<void> {
    await this.tick();
  }

  private async finish(report: TickReport, start: number): Promise<void> {
    report.duration_ms = Date.now() - start;
    await this.armNext();
    // Report the outcome (best effort; a stale holder is ignored by the DO).
    // Only meaningful when a lease was actually held; skipped ticks carry no
    // result.
    if (!report.skipped) {
      await this.ingestCall("/internal/compaction/result", "POST", {
        holder: this.holder,
        fencing: this.lastFencing,
        ok: report.ok,
        segments_published: report.segments_published,
        bytes_published: report.bytes_published,
        error: report.error ?? null,
      }).catch(() => undefined);
    }
  }

  private lastFencing = 0;

  private async acquireLease(): Promise<CompactionLease> {
    const lease = await this.ingestCall<CompactionLease>("/internal/compaction/lease", "POST", {
      partition: COMPACTION_PARTITION,
      holder: this.holder,
      ttl_ms: this.config.leaseTtlMs,
    });
    if (lease.ok) this.lastFencing = lease.fencing;
    return lease;
  }

  private async releaseLease(lease: CompactionLease): Promise<void> {
    await this.ingestCall("/internal/compaction/release", "POST", {
      partition: COMPACTION_PARTITION,
      holder: lease.holder,
      fencing: lease.fencing,
    });
  }

  private plan(): Promise<CompactionPlan> {
    return this.ingestCall<CompactionPlan>("/internal/compaction/plan", "POST", {
      grace_ms: this.config.graceMs,
      window_ms: this.config.windowMs,
      max_objects: this.config.batchObjects,
      max_bytes: this.config.batchBytes,
      max_batches: this.config.maxBatchesPerTick,
    });
  }

  private async armNext(): Promise<void> {
    if (!this.scheduler) {
      this.schedulerActive = false;
      return;
    }
    try {
      await this.scheduler.set(this.now() + this.config.scheduleMs);
      this.schedulerActive = true;
    } catch (error) {
      this.schedulerActive = false;
      this.log("burstel: compactor alarm could not be armed", { error: errorMessage(error) });
    }
  }

  // -------------------------------------------------------------- compact

  /** Streams one window into the container and publishes the verified segment. */
  private async compactBatch(batch: CompactionBatch): Promise<{ size_bytes: number }> {
    await this.ensureContainer();

    const objects: Array<Record<string, unknown>> = [];
    const expectedKeys = new Set<string>();
    for (const obj of batch.objects) {
      if (!obj.key.startsWith("journal/")) {
        throw new Error(`refusing to read ${obj.key}: outside the journal/ prefix`);
      }
      const stored = await this.bucket!.get(obj.key);
      if (!stored) throw new Error(`journal object ${obj.key} is missing from R2`);
      const bytes = new Uint8Array(await new Response(stored.body).arrayBuffer());
      if (obj.size > 0 && bytes.byteLength !== obj.size) {
        throw new Error(`journal object ${obj.key} size mismatch`);
      }
      const gotSha = await sha256HexBytes(bytes);
      if (obj.sha256 && gotSha !== obj.sha256.toLowerCase()) {
        throw new Error(`journal object ${obj.key} failed checksum verification`);
      }
      expectedKeys.add(obj.key);
      objects.push({
        key: obj.key,
        signal: obj.signal,
        received_at: obj.received_at,
        content_encoding: obj.content_encoding,
        content_type: obj.content_type,
        size: bytes.byteLength,
        sha256: gotSha,
        body_base64: bytesToBase64(bytes),
      });
    }

    const built = await this.buildSegment(batch, objects);
    await this.assertTrustedBuild(batch, built, expectedKeys);

    const objectKey = built.segment_key;
    const putResp = await this.bucket!.put(objectKey, await this.segmentStream(built.download_token), {
      sha256: built.sha256,
      httpMetadata: { contentType: "application/vnd.duckdb" },
      customMetadata: {
        sha256: built.sha256,
        instance_id: batch.instance_id,
        segment_key: objectKey,
        first_received_at: String(built.first_received_at),
        last_received_at: String(built.last_received_at),
        record_count: String(built.record_count),
      },
    });
    if (!putResp) throw new Error(`R2 PUT of segment ${objectKey} was rejected`);
    if (putResp.size !== built.size_bytes) {
      throw new Error(`stored segment ${objectKey} size ${putResp.size} != reported ${built.size_bytes}`);
    }
    if (this.bucket!.head) {
      const head = await this.bucket!.head(objectKey);
      if (!head || head.size !== built.size_bytes) {
        throw new Error(`stored segment ${objectKey} failed HEAD verification`);
      }
      const storedSha = (head.sha256 ?? head.customMetadata?.["sha256"] ?? "").toLowerCase();
      if (storedSha && storedSha !== built.sha256.toLowerCase()) {
        throw new Error(`stored segment ${objectKey} checksum mismatch`);
      }
    }

    const publish: SegmentPublishInput = {
      segment_key: built.segment_key,
      instance_id: batch.instance_id,
      object_key: objectKey,
      size_bytes: built.size_bytes,
      sha256: built.sha256,
      first_received_at: built.first_received_at,
      last_received_at: built.last_received_at,
      request_count: built.request_count,
      record_count: built.record_count,
      span_count: built.span_count,
      log_count: built.log_count,
      metric_count: built.metric_count,
      event_min_ns: built.event_min_ns,
      event_max_ns: built.event_max_ns,
      late: false,
      coverage: built.coverage,
    };
    const result = await this.ingestCall<SegmentPublishResult>("/internal/segment/publish", "POST", publish);
    await this.discardSegment(built.download_token);
    if (!result.committed && !result.duplicate) {
      throw new Error(`segment ${objectKey} was not published`);
    }
    return { size_bytes: built.size_bytes };
  }

  /** Rejects any build whose key/coverage the coordinator did not ask for. */
  private async assertTrustedBuild(
    batch: CompactionBatch,
    built: BuildResponse,
    expectedKeys: Set<string>,
  ): Promise<void> {
    if (!built.segment_key.startsWith("segments/")) {
      throw new Error("compactor returned a segment key outside the segments/ prefix");
    }
    if (built.size_bytes <= 0 || built.size_bytes > this.config.maxSegmentBytes) {
      throw new Error("compactor returned an out-of-bounds segment size");
    }
    if (!/^[0-9a-f]{64}$/.test(built.sha256)) {
      throw new Error("compactor returned an invalid segment checksum");
    }
    const requested = new Map(batch.objects.map((o) => [o.key, o]));
    if (built.coverage.length !== expectedKeys.size) {
      throw new Error("compactor coverage does not match the requested batch");
    }
    const seen = new Set<string>();
    for (const cov of built.coverage) {
      const obj = requested.get(cov.journal_key);
      if (!obj) throw new Error(`compactor claimed unrequested coverage ${cov.journal_key}`);
      if (seen.has(cov.journal_key)) throw new Error(`compactor returned duplicate coverage ${cov.journal_key}`);
      if (cov.signal !== obj.signal || cov.received_at !== obj.received_at) {
        throw new Error(`compactor coverage for ${cov.journal_key} disagrees with the ledger`);
      }
      seen.add(cov.journal_key);
    }
    const first = Math.min(...batch.objects.map((o) => o.received_at));
    const last = Math.max(...batch.objects.map((o) => o.received_at));
    const expected = await expectedSegmentKey(batch.instance_id, first, last, batch.objects.map((o) => o.key));
    if (built.segment_key !== expected) {
      throw new Error("compactor returned a non-deterministic segment key");
    }
  }

  private async ensureContainer(): Promise<void> {
    if (!this.container.running) {
      this.container.start(this.containerEnv);
      await this.container.ensureReady();
      await this.container.keepAlive(this.config.containerKeepAliveMs).catch(() => undefined);
    }
  }

  private async buildSegment(
    batch: CompactionBatch,
    objects: Array<Record<string, unknown>>,
  ): Promise<BuildResponse> {
    const resp = await this.container.request("/v1/segments", {
      method: "POST",
      headers: this.containerHeaders({ "content-type": "application/json" }),
      body: JSON.stringify({ instance_id: batch.instance_id, instance: batch.instance, objects }),
    });
    return (await parseJson<BuildResponse>(resp, "compactor build")) as BuildResponse;
  }

  private async segmentStream(token: string): Promise<ReadableStream<Uint8Array>> {
    const resp = await this.container.request(`/v1/segments/${encodeURIComponent(token)}`, {
      headers: this.containerHeaders(),
    });
    if (resp.status < 200 || resp.status >= 300 || !resp.body) {
      throw new HttpError(503, `compactor segment download failed with status ${resp.status}`, true, "segment_download");
    }
    return resp.body;
  }

  private async discardSegment(token: string): Promise<void> {
    await this.container
      .request(`/v1/segments/${encodeURIComponent(token)}`, { method: "DELETE", headers: this.containerHeaders() })
      .catch(() => undefined);
  }

  // -------------------------------------------------------------------- gc

  /** Lists and deletes verified, unpinned journal objects past retention. */
  async runGC(force: boolean): Promise<{ deleted: number; protected: number }> {
    if (!this.bucket || !this.bucket.delete) return { deleted: 0, protected: 0 };
    let deleted = 0;
    let protectedCount = 0;
    for (let page = 0; page < 20; page++) {
      const listed = await this.ingestCall<{ candidates: GCandidate[]; protected: number; considered: number }>(
        "/internal/compaction/gc/list",
        "POST",
        { retention_ms: this.config.retentionMs, force, limit: 500 },
      );
      protectedCount += listed.protected;
      if (listed.candidates.length === 0) break;
      const deletedKeys: string[] = [];
      for (const candidate of listed.candidates) {
        try {
          await this.bucket.delete(candidate.key);
          deletedKeys.push(candidate.key);
          deleted += 1;
        } catch (error) {
          this.log("burstel: gc delete failed", { key: candidate.key, error: errorMessage(error) });
        }
      }
      if (deletedKeys.length > 0) {
        await this.ingestCall("/internal/compaction/gc/delete", "POST", { keys: deletedKeys }).catch(() => undefined);
      }
      if (listed.considered < 500) break;
    }
    return { deleted, protected: protectedCount };
  }

  // --------------------------------------------------------------- status

  async status(): Promise<Record<string, unknown>> {
    let ingestStatus: Record<string, unknown> = {};
    try {
      ingestStatus = await this.ingestCall<Record<string, unknown>>("/internal/compaction/status", "GET");
    } catch {
      ingestStatus = {};
    }
    return {
      ...ingestStatus,
      holder: this.holder,
      scheduler_active: this.schedulerActive,
      last_tick_at: this.lastTickAt,
      last_error: this.lastError,
      config: {
        window_ms: this.config.windowMs,
        grace_ms: this.config.graceMs,
        retention_ms: this.config.retentionMs,
        gc_enabled: this.config.gcEnabled,
        schedule_ms: this.config.scheduleMs,
      },
    };
  }

  // ------------------------------------------------------------- internal

  async handleInternal(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path === "/healthz") return json(200, { status: "ok", service: "burstel-compactor" });
    if (path === "/internal/status") {
      if (request.method !== "GET") return json(405, { error: "method not allowed" });
      return json(200, await this.status());
    }
    if (path === "/internal/trigger") {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      const report = await this.tick();
      return json(report.ok ? 200 : 503, report);
    }
    if (path === "/internal/gc") {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      const result = await this.runGC(true);
      return json(200, result);
    }
    return json(404, { error: "not found" });
  }

  private containerHeaders(extra?: Record<string, string>): Record<string, string> {
    const headers: Record<string, string> = { ...extra };
    if (this.internalToken) headers["authorization"] = `Bearer ${this.internalToken}`;
    return headers;
  }

  private async ingestCall<T>(path: string, method: string, body?: unknown): Promise<T> {
    const init: RequestInit = { method };
    if (body !== undefined) {
      init.headers = { "content-type": "application/json" };
      init.body = JSON.stringify(body);
    }
    const stub: DoStubLike = this.ingest.getByName(INGEST_DO_NAME);
    const resp = await stub.fetch(new Request(`${DO_ORIGIN}${path}`, init));
    return (await parseJson<T>(resp, path)) as T;
  }
}

// ---------------------------------------------------------------- helpers

async function parseJson<T>(resp: Response, what: string): Promise<T> {
  if (resp.status >= 200 && resp.status < 300) {
    return (await resp.json()) as T;
  }
  let message = `${what} failed with status ${resp.status}`;
  let code = "error";
  let retryable = resp.status >= 500;
  try {
    const body = (await resp.json()) as { error?: string; code?: string; retryable?: boolean };
    if (body.error) message = body.error;
    if (body.code) code = body.code;
    if (body.retryable !== undefined) retryable = body.retryable === true;
  } catch {
    // keep defaults
  }
  throw new HttpError(resp.status, message, retryable, code);
}

/** Chunked base64 so a large batch never overflows the argument stack. */
export function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

/**
 * Deterministic segment key, byte-for-byte identical to the Go
 * `compaction.SegmentKey`. The coordinator uses it to reject a container that
 * returns a different key for the same source keys.
 */
export async function expectedSegmentKey(
  instanceId: string,
  firstMS: number,
  lastMS: number,
  sourceKeys: string[],
): Promise<string> {
  const sorted = [...sourceKeys].sort();
  const digest = await sha256HexBytes(new TextEncoder().encode(sorted.join("\n") + "\n"));
  return `segments/${instanceId}/${firstMS}-${lastMS}-${digest.slice(0, 16)}.duckdb`;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
