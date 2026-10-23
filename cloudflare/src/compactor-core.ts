// Compactor coordinator core (spec 018, retrofitted by spec 023 D3).
//
// This is the missing production link between the durable raw journal (spec
// 014) and the window-shot analysis (spec 016). It is deliberately free of
// Cloudflare-specific globals so the exact same orchestration runs in
// production (a `CompactorDO` with a real Container + R2 binding + ControlDO)
// and in bounded unit tests (a fake container + fake R2 + real SQLite core).
//
// One bounded tick:
//   1. acquire a fenced, single-active lease from the ControlDO;
//   2. discover finalized journal objects **by listing R2** (spec 023 D3.1):
//      `received_at < now - grace`, re-listing a lookback window of at least
//      2 x grace every run so a late object is picked up after a previous
//      run's cursor;
//   3. exclude objects already covered by a verified segment (key de-dup) and
//      objects whose `token_id` was revoked at/before `received_at` (D3.2);
//   4. group the rest into cross-instance time buckets (D3.3), stream each
//      object's exact bytes into the short-lived Go compactor Container, and
//      get one immutable DuckDB segment back;
//   5. stream the segment bytes into R2 (R2 enforces the advertised SHA-256),
//      HEAD-verify size/checksum, then publish the verified manifest + exact
//      coverage set to the ControlDO;
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
import {
  INGEST_DO_NAME,
  MULTI_INSTANCE_SCOPE,
} from "./ingest-core";
import type {
  CompactionBatch,
  CompactionLease,
  CompactionObject,
  ExcludedJournalInput,
  GCandidate,
  SegmentCoverageInput,
  SegmentPublishInput,
  SegmentPublishResult,
} from "./ingest-core";
import { listJournalObjects } from "./journal-listing";
import type { JournalObjectInfo } from "./journal-listing";

/** The single compaction partition (one active compactor per installation). */
export const COMPACTION_PARTITION = "primary";
/** Name of the singleton compactor coordinator Durable Object. */
export const COMPACTOR_DO_NAME = "compactor-primary";

export interface CompactorConfig {
  windowMs: number;
  graceMs: number;
  /**
   * Lookback re-list window (spec 023, D3.1). Must be at least 2 x grace; a
   * smaller configured value is raised to that floor.
   */
  lookbackMs: number;
  retentionMs: number;
  /**
   * Audit retention for revoked-token journal objects (spec 024, R5). They are
   * excluded from segments immediately but kept in place this long before GC.
   */
  revokedRetentionMs: number;
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
    lookbackMs: 180_000,
    retentionMs: 24 * 60 * 60 * 1000,
    revokedRetentionMs: 7 * 24 * 60 * 60 * 1000,
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
  /** Durable control plane: lease, publish, GC, discovery cursor, status. */
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
  discovered_objects: number;
  /** Objects excluded because their token was revoked before receipt (D3.2). */
  excluded_revoked: number;
  gc_deleted: number;
  gc_protected: number;
  /** Revoked/excluded objects deleted after the audit retention window (R5). */
  revoked_deleted: number;
  duration_ms: number;
  watermark: number;
  lookback_ms: number;
  lookback_truncated: boolean;
  /** Inclusive floor of the lookback range this tick scanned (R3). */
  lookback_floor_ms: number | null;
  /** Exclusive ceiling of that lookback range. */
  lookback_until_ms: number | null;
  /** False when discovery never ran, so status must not clear prior health. */
  lookback_known: boolean;
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

interface DiscoveryResult {
  batches: CompactionBatch[];
  /** Every finalized, eligible object before batch caps were applied. */
  objects: CompactionObject[];
  /** How many of `objects` were assigned to `batches`. */
  batched: number;
  excluded: number;
  watermark: number;
  floor: number | null;
  truncated: boolean;
  lookback_truncated: boolean;
  lookback_floor: number | null;
  lookback_until: number | null;
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
  private lastReport: TickReport | null = null;
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
    // A lookback shorter than two grace windows could finalize an object before
    // a late arrival in its time bucket is visible. Raise it, never lower it.
    this.config.lookbackMs = Math.max(this.config.lookbackMs, 2 * this.config.graceMs);
    this.scheduler = deps.scheduler;
    this.log = deps.log ?? (() => undefined);
  }

  // ------------------------------------------------------------------ tick

  /** Runs one bounded compaction tick and arms the next one. */
  async tick(): Promise<TickReport> {
    const start = Date.now();
    const watermark = this.now() - Math.max(0, this.config.graceMs);
    const report: TickReport = {
      ok: true,
      batches: 0,
      segments_published: 0,
      bytes_published: 0,
      journal_objects: 0,
      discovered_objects: 0,
      excluded_revoked: 0,
      gc_deleted: 0,
      gc_protected: 0,
      revoked_deleted: 0,
      duration_ms: 0,
      watermark,
      lookback_ms: this.config.lookbackMs,
      lookback_truncated: false,
      lookback_floor_ms: null,
      lookback_until_ms: null,
      lookback_known: false,
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
    let discovery: DiscoveryResult | null = null;
    let lastCompactedAt: number | null = null;
    try {
      lease = await this.acquireLease();
      if (!lease.ok) {
        // The finally block below finishes the tick (arms the next alarm).
        report.skipped = "lease_held";
        return report;
      }

      discovery = await this.discover(watermark);
      report.discovered_objects = discovery.objects.length;
      report.excluded_revoked = discovery.excluded;
      report.lookback_truncated = discovery.lookback_truncated;
      report.lookback_floor_ms = discovery.lookback_floor;
      report.lookback_until_ms = discovery.lookback_until;
      report.lookback_known = true;
      const deadline = start + this.config.tickBudgetMs;
      let failed = false;
      for (const batch of discovery.batches) {
        if (Date.now() >= deadline) {
          failed = true;
          break;
        }
        try {
          const published = await this.compactBatch(batch);
          report.batches += 1;
          report.segments_published += 1;
          report.bytes_published += published.size_bytes;
          report.journal_objects += batch.objects.length;
          for (const obj of batch.objects) {
            lastCompactedAt = Math.max(lastCompactedAt ?? obj.received_at, obj.received_at);
          }
        } catch (error) {
          report.ok = false;
          report.error = errorMessage(error);
          this.lastError = report.error;
          this.log("burstel: compaction batch failed", { error: report.error });
          failed = true;
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

      // Spec 024 R5: revoked-token objects are excluded from analysis at once
      // but retained for audit (default 7 days) before deletion - independent
      // of the destructive COMPACTION_GC opt-in.
      const revoked = await this.runRevokedRetention().catch((error) => {
        this.log("burstel: revoked retention failed", { error: errorMessage(error) });
        return { deleted: 0 };
      });
      report.revoked_deleted = revoked.deleted;

      // Advance the discovery cursor only after a fully successful tick, and
      // only past what was actually handled. A failed/truncated run leaves the
      // cursor in place so the same objects are re-listed next time.
      if (report.ok && !failed) {
        // Advance the cursor only past everything that was actually handed to
        // a batch. A time-bucket split can stop at maxBatches before the
        // listing is exhausted; advancing to the watermark there would skip
        // discovered-but-uncompacted objects forever.
        const incomplete = discovery.truncated || discovery.batched < discovery.objects.length;
        const nextFloor = incomplete
          ? lastCompactedAt !== null
            ? lastCompactedAt + 1
            : (discovery.objects[discovery.batched]?.received_at ?? fromFloor(discovery))
          : watermark;
        await this.setFloor(nextFloor).catch((error) => {
          this.log("burstel: discovery cursor update failed", { error: errorMessage(error) });
        });
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
    // Skipped ticks are reported too (spec 025 R4), so a lease-held tick leaves
    // `last_skipped_at` instead of silently leaving status stale. Lookback
    // health is only sent when discovery actually ran (spec 025 R3), otherwise
    // a failure or skip would erase the previous truncation warning.
    const lookback = report.lookback_known
      ? {
          truncated: report.lookback_truncated,
          floor_ms: report.lookback_floor_ms,
          until_ms: report.lookback_until_ms,
        }
      : null;
    await this.ingestCall("/internal/compaction/result", "POST", {
      holder: this.holder,
      fencing: this.lastFencing,
      ok: report.ok,
      skipped: report.skipped ?? null,
      segments_published: report.segments_published,
      bytes_published: report.bytes_published,
      excluded_revoked: report.excluded_revoked,
      error: report.error ?? null,
      lookback,
    }).catch(() => undefined);
    // Set here (not only after the try/finally) so an early lease-skip return
    // still updates `compaction_status.last_report` (spec 025, R4).
    this.lastReport = report;
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

  private async setFloor(floorMs: number): Promise<void> {
    await this.ingestCall("/internal/compaction/discovery", "POST", { floor_ms: floorMs });
  }

  private async discoveryFloor(): Promise<number | null> {
    const state = await this.ingestCall<{ floor_ms: number | null }>("/internal/compaction/discovery", "GET");
    return typeof state.floor_ms === "number" && Number.isFinite(state.floor_ms) ? state.floor_ms : null;
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

  // ---------------------------------------------------------- discovery (D3)

  /**
   * Lists and classifies the finalized journal:
   *   * `received_at < watermark` (grace);
   *   * **main range** `[cursor, watermark)` for never-seen objects;
   *   * **lookback range** `[now - lookback, cursor)` re-listed every run so a
   *     late object is picked up after a previous run's cursor;
   *   * covered and revoked objects are removed/deduplicated;
   *   * the remainder is grouped into cross-instance time buckets.
   *
   * The two ranges have separate scan budgets: an already-covered lookback
   * prefix can never starve the main range (or vice versa).
   */
  private async discover(watermark: number): Promise<DiscoveryResult> {
    const now = this.now();
    const floor = await this.discoveryFloor().catch(() => null);
    const mainFrom = floor === null ? 0 : floor;
    const lookbackFrom = Math.min(mainFrom, now - this.config.lookbackMs);
    const maxObjects = Math.max(1, this.config.batchObjects * this.config.maxBatchesPerTick);

    const main = await listJournalObjects(this.bucket!, {
      fromMs: mainFrom,
      toMs: watermark,
      maxObjects,
      maxScan: maxObjects * 4,
      pageSize: 1000,
    });
    const lookback =
      lookbackFrom < mainFrom
        ? await listJournalObjects(this.bucket!, {
            fromMs: lookbackFrom,
            toMs: mainFrom,
            maxObjects,
            maxScan: maxObjects * 4,
            pageSize: 1000,
          })
        : { objects: [], truncated: false, scanned: 0 };
    // Whether a lookback range actually exists for this tick. Its bounds are
    // persisted in compaction_status so a truncated scan is never invisible
    // (spec 025, R3).
    const hasLookback = lookbackFrom < mainFrom;
    const merged = new Map<string, JournalObjectInfo>();
    for (const info of [...main.objects, ...lookback.objects]) merged.set(info.key, info);

    let objects: CompactionObject[] = [...merged.values()].map(journalToCompactionObject);

    // De-duplicate against the verified manifest and previously excluded keys.
    const coverage = await this.ingestCall<{ covered: string[]; excluded: string[] }>(
      "/internal/compaction/coverage",
      "POST",
      { keys: objects.map((o) => o.key) },
    ).catch(() => ({ covered: [] as string[], excluded: [] as string[] }));
    const covered = new Set(coverage.covered);
    const alreadyExcluded = new Set(coverage.excluded);
    objects = objects.filter((o) => !covered.has(o.key) && !alreadyExcluded.has(o.key));

    // Revocation correctness (D3.2): exclude everything written at/after the
    // token's effective revocation instant, record it, and GC it immediately.
    const tokenIds = [...new Set(objects.map((o) => o.token_id ?? "").filter((id) => id !== ""))];
    let excluded = 0;
    if (tokenIds.length > 0) {
      const revocation = await this.ingestCall<{ revocations: Array<{ token_id: string; revoked_at: number | null }> }>(
        "/internal/instance/token/revocations",
        "POST",
        { token_ids: tokenIds },
      );
      const revokedAt = new Map<string, number | null>();
      for (const row of revocation.revocations) revokedAt.set(row.token_id, row.revoked_at);
      if (revokedAt.size > 0) {
        const toExclude: CompactionObject[] = [];
        objects = objects.filter((o) => {
          const tokenId = o.token_id ?? "";
          if (!tokenId || !revokedAt.has(tokenId)) return true;
          const revoked = revokedAt.get(tokenId) ?? null;
          // `null` (legacy) is treated as revoked from the beginning.
          if (revoked !== null && revoked > o.received_at) return true;
          toExclude.push(o);
          return false;
        });
        if (toExclude.length > 0) {
          excluded = toExclude.length;
          const entries: ExcludedJournalInput[] = toExclude.map((o) => ({
            key: o.key,
            token_id: o.token_id ?? "",
            instance_id: o.instance_id,
            received_at: o.received_at,
            size: o.size,
          }));
          await this.ingestCall("/internal/compaction/excluded", "POST", { entries }).catch((error) => {
            this.log("burstel: failed to record revoked exclusion", { error: errorMessage(error) });
          });
          // Revoked data must never be analysed. The durable exclusion row is
          // written above and the manifest lookup filters it forever; the R2
          // object itself is retained for the audit window (spec 024 R5) and
          // deleted later by runRevokedRetention().
        }
      }
    }

    const batches = this.groupBatches(objects);
    const batched = batches.reduce((n, batch) => n + batch.objects.length, 0);
    return {
      batches,
      objects,
      batched,
      excluded,
      watermark,
      floor,
      // Only the main range gates the cursor; the lookback is always re-listed.
      truncated: main.truncated,
      lookback_truncated: lookback.truncated,
      lookback_floor: hasLookback ? lookbackFrom : null,
      lookback_until: hasLookback ? mainFrom : null,
    };
  }

  /** Groups objects into cross-instance time buckets (spec 023, D3.3). */
  private groupBatches(objects: CompactionObject[]): CompactionBatch[] {
    const bucketMs = Math.max(1, this.config.windowMs);
    const maxObjects = Math.max(1, this.config.batchObjects);
    const maxBytes = Math.max(1, this.config.batchBytes);
    const batches: CompactionBatch[] = [];
    let current: CompactionBatch | null = null;
    let currentBytes = 0;
    let currentBucket = -1;

    for (const object of objects) {
      if (batches.length >= this.config.maxBatchesPerTick) break;
      const bucket = Math.floor(object.received_at / bucketMs) * bucketMs;
      const split =
        current === null ||
        bucket !== currentBucket ||
        current.objects.length >= maxObjects ||
        currentBytes + object.size > maxBytes;
      if (split) {
        if (current !== null) batches.push(current);
        if (batches.length >= this.config.maxBatchesPerTick) {
          current = null;
          break;
        }
        current = {
          scope_id: MULTI_INSTANCE_SCOPE,
          first_received_at: object.received_at,
          last_received_at: object.received_at,
          objects: [],
        };
        currentBucket = bucket;
        currentBytes = 0;
      }
      current!.objects.push(object);
      current!.first_received_at = Math.min(current!.first_received_at, object.received_at);
      current!.last_received_at = Math.max(current!.last_received_at, object.received_at);
      currentBytes += object.size;
    }
    if (current !== null && batches.length < this.config.maxBatchesPerTick) batches.push(current);
    return batches;
  }

  // -------------------------------------------------------------- compact

  /** Streams one cross-instance time bucket into the container and publishes it. */
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
        // Per-object trusted attribution: one segment may span instances.
        instance_id: obj.instance_id,
        instance: obj.instance,
        body_base64: bytesToBase64(bytes),
      });
    }

    const built = await this.buildSegment(batch, objects);
    await this.assertTrustedBuild(batch, built, expectedKeys);
    // Preserve each object's real instance in the coverage atoms so GC/pins
    // remain instance-scoped even though the segment scope is cross-instance.
    const objectByKey = new Map(batch.objects.map((o) => [o.key, o]));
    const coverage: SegmentCoverageInput[] = built.coverage.map((cov) => ({
      ...cov,
      instance_id: objectByKey.get(cov.journal_key)?.instance_id,
    }));

    const objectKey = built.segment_key;
    const putResp = await this.bucket!.put(objectKey, await this.segmentStream(built.download_token), {
      sha256: built.sha256,
      httpMetadata: { contentType: "application/vnd.duckdb" },
      customMetadata: {
        sha256: built.sha256,
        instance_id: batch.scope_id,
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
      instance_id: batch.scope_id,
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
      coverage,
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
        throw new Error(`compactor coverage for ${cov.journal_key} disagrees with the journal`);
      }
      seen.add(cov.journal_key);
    }
    const first = Math.min(...batch.objects.map((o) => o.received_at));
    const last = Math.max(...batch.objects.map((o) => o.received_at));
    const expected = await expectedSegmentKey(batch.scope_id, first, last, batch.objects.map((o) => o.key));
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
      body: JSON.stringify({ instance_id: batch.scope_id, instance: "", objects }),
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
        {
          retention_ms: this.config.retentionMs,
          revoked_retention_ms: this.config.revokedRetentionMs,
          force,
          limit: 500,
        },
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

  /**
   * Deletes revoked-token journal objects whose audit retention has elapsed
   * (spec 024 R5). Runs on every tick, independently of COMPACTION_GC, because
   * these objects are already excluded from every segment and analysis view.
   */
  async runRevokedRetention(): Promise<{ deleted: number }> {
    if (!this.bucket || !this.bucket.delete) return { deleted: 0 };
    let deleted = 0;
    for (let page = 0; page < 20; page++) {
      const listed = await this.ingestCall<{ candidates: GCandidate[]; protected: number; considered: number }>(
        "/internal/compaction/gc/list",
        "POST",
        {
          retention_ms: 0,
          revoked_retention_ms: this.config.revokedRetentionMs,
          revoked_only: true,
          force: false,
          limit: 500,
        },
      );
      if (listed.candidates.length === 0) break;
      const deletedKeys: string[] = [];
      for (const candidate of listed.candidates) {
        try {
          await this.bucket.delete(candidate.key);
          deletedKeys.push(candidate.key);
          deleted += 1;
        } catch (error) {
          this.log("burstel: revoked retention delete failed", {
            key: candidate.key,
            error: errorMessage(error),
          });
        }
      }
      if (deletedKeys.length > 0) {
        await this.ingestCall("/internal/compaction/gc/delete", "POST", { keys: deletedKeys }).catch(() => undefined);
      }
      if (listed.considered < 500) break;
    }
    return { deleted };
  }

  // --------------------------------------------------------------- status

  async status(): Promise<Record<string, unknown>> {
    let ingestStatus: Record<string, unknown> = {};
    let ingestStatusError = "";
    try {
      ingestStatus = await this.ingestCall<Record<string, unknown>>("/internal/compaction/status", "GET");
    } catch (error) {
      ingestStatus = {};
      ingestStatusError = errorMessage(error);
    }
    return {
      ...ingestStatus,
      holder: this.holder,
      scheduler_active: this.schedulerActive,
      last_tick_at: this.lastTickAt,
      last_error: this.lastError,
      ingest_status_error: ingestStatusError,
      last_report: this.lastReport,
      config: {
        window_ms: this.config.windowMs,
        grace_ms: this.config.graceMs,
        lookback_ms: this.config.lookbackMs,
        retention_ms: this.config.retentionMs,
        revoked_retention_ms: this.config.revokedRetentionMs,
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

function journalToCompactionObject(info: JournalObjectInfo): CompactionObject {
  return {
    key: info.key,
    instance_id: info.instance_id,
    instance: info.instance,
    signal: info.signal,
    size: info.size,
    sha256: info.sha256,
    content_type: info.content_type,
    content_encoding: info.content_encoding,
    received_at: info.received_at,
    token_id: info.token_id,
  };
}

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
  scopeId: string,
  firstMS: number,
  lastMS: number,
  sourceKeys: string[],
): Promise<string> {
  const sorted = [...sourceKeys].sort();
  const digest = await sha256HexBytes(new TextEncoder().encode(sorted.join("\n") + "\n"));
  return `segments/${scopeId}/${firstMS}-${lastMS}-${digest.slice(0, 16)}.duckdb`;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** First eligible-but-unbatched object's receive time, or the listing floor. */
function fromFloor(discovery: DiscoveryResult): number {
  return discovery.objects[discovery.batched]?.received_at ?? discovery.floor ?? 0;
}
