// Control-plane core (spec 014, reduced by spec 023 D4).
//
// This core is deliberately free of Cloudflare-specific globals so the exact
// same logic runs in production (over `ControlDO` SQLite storage) and in unit
// tests (over `bun:sqlite`). It owns:
//
//   * the named-instance registry and push-only instance credentials, delegated
//     to {@link InstanceCore} over the same SQLite storage (spec 014);
//   * the shot registry: which shots exist and their attribution-only `owner`,
//     so `GET /api/shots` can list shots. The former
//     `MAX_CONCURRENT_RECORDING` cap is gone (spec 014): shots may record
//     concurrently, bounded only by explicit backend capacity controls;
//   * the segment manifest, compaction lease/pins and discovery cursor.
//
// Spec 023 removed the per-request journal ledger from the ingest ACK path:
// the Worker authenticates from KV and writes one R2 object. The legacy ledger
// table/endpoints remain only for reconciliation tooling and older tests;
// compaction discovers work by listing R2.
//
// Design boundaries:
//   * An instance token authorizes *only* POST /ingest/v1/{traces,metrics,logs}
//     and resolves to an instance, never to a shot or a segment.
//   * Expiry is evaluated at the edge from `expires_at` and is exact; the
//     authoritative check here is the source of truth on a cache miss.
//   * A frozen shot's artifact outlives its (former) credentials by design.

import type { R2BucketLike } from "./artifacts";
import { DEFAULT_REVOKED_RETENTION_MS } from "./env";
import { badRequest } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { InstanceCore, newInstanceToken } from "./instance-core";
import type { InstanceCoreDeps } from "./instance-core";
import { listAllJournalObjects, listJournalObjects } from "./journal-listing";
import type { JournalObjectInfo } from "./journal-listing";
import {
  DEFAULT_WINDOW_BOUNDS,
  externalWindow,
  normalizeInstances,
  serverWindow,
  validateWindow,
  windowFromJSON,
  windowToJSON,
} from "./shot-window";
import type { ShotWindow, WindowBounds, WindowSegmentMeta, SegmentSelection } from "./shot-window";
import { selectSegments } from "./shot-window";
import { SIGNALS } from "./store";
import type { Signal, SqlRow, SqlValue, SyncSql } from "./store";

export { constantTimeEqual, sha256Hex } from "./crypto";

/** Name of the singleton control-plane Durable Object (spec 023, D4). */
export const CONTROL_DO_NAME = "ingest-primary";
/** Backwards-compatible alias used by existing callers/tests. */
export const INGEST_DO_NAME = CONTROL_DO_NAME;

/**
 * Segment scope for a cross-instance time bucket (spec 023, D3.3). It is the
 * synthetic `instance_id` of a multi-instance segment; individual rows keep
 * their real `instance_id` column.
 */
export { MULTI_INSTANCE_SCOPE } from "./segment-scope";

/**
 * How long a logical window shot protects its receive-time range from GC. The
 * pin is refreshed on start/stop and expires on its own, so a shot can never
 * protect data forever (spec 018).
 */
export const DEFAULT_SHOT_PIN_TTL_MS = 7 * 24 * 60 * 60 * 1000;

/** One registered shot, as persisted by the coordinator. */
export interface RegisteredShot {
  shot_id: string;
  owner: string;
  created_at: number;
  state: string;
  stopped_at: number | null;
  size_bytes: number | null;
  sha256: string | null;
}

/** One materialized shot part (`shots/<shot_id>/part-<i>.duckdb`). */
export interface MaterializationPart {
  index: number;
  object_key: string;
  size_bytes: number;
  sha256: string;
  object_count: number;
  request_count: number;
  record_count: number;
  span_count: number;
  log_count: number;
  metric_count: number;
  first_received_at: number;
  last_received_at: number;
}

/**
 * Durable lease/attempt state for one materialization part (spec 032).
 *
 * The alarm driver claims a part by writing `running` plus a `lease_expires_at`
 * timestamp. A crash or a cancelled alarm leaves the lease behind; the next
 * alarm retries it after expiry, bounded by `maxAttempts`.
 */
export interface MaterializationPartState {
  index: number;
  state: "pending" | "running" | "complete" | "failed";
  attempts: number;
  lease_owner?: string | null;
  lease_expires_at?: number | null;
  started_at?: number | null;
  finished_at?: number | null;
  error?: string | null;
}

/** Durable status of shot-time parallel materialization (spec 031/032). */
export interface MaterializationRecord {
  state: "pending" | "running" | "complete" | "failed";
  parts_done: number;
  parts_total: number;
  spans: number;
  started_at: number | null;
  finished_at: number | null;
  parts: MaterializationPart[];
  error?: string | null;
  /** Per-part leases/attempts; exposed so a lazy reader can detect a stale run. */
  part_states?: MaterializationPartState[];
  /** Run-level lease, refreshed by the driver heartbeat. */
  lease_expires_at?: number | null;
  heartbeat_at?: number | null;
  /** Number of alarm ticks spent on this run (diagnostics only). */
  attempts?: number;
}

/** One journal object included in a materialization plan. */
export interface MaterializationObject {
  key: string;
  signal: string;
  received_at: number;
  content_encoding: string;
  content_type: string;
  size: number;
  sha256: string;
  instance_id: string;
  instance: string;
}

/** The ControlDO's bounded enumeration of a shot's journal keys. */
export interface MaterializationPlan {
  shot_id: string;
  from_ms: number;
  to_ms: number;
  objects: MaterializationObject[];
  excluded: string[];
  total_bytes: number;
  truncated: boolean;
}

/** One registered logical-window shot (spec 016). */
export interface WindowShotRecord extends RegisteredShot {
  kind: "window" | "recording";
  name: string | null;
  window: ShotWindow | null;
  query_watermark_ms: number | null;
  coverage: WindowCoverage | null;
  materialization: MaterializationRecord | null;
}

/** Completeness of a window shot. Missing data is always disclosed. */
export interface WindowCoverage {
  expected_segments: number;
  available_segments: number;
  missing_segments: string[];
  instances: string[];
  late_arrivals?: number;
  log_observed_fallbacks?: number;
  /**
   * Un-compacted raw journal objects inside the window. A non-zero count makes
   * the window explicitly incomplete, never silently empty/complete (spec 018).
   */
  pending_journal?: number;
  /**
   * True when a truncated compaction lookback overlaps this window, so the
   * window can never be reported complete even when nothing is pending yet
   * (spec 025, R3).
   */
  lookback_truncated?: boolean;
  /** Floor (inclusive start) of the last truncated lookback scan. */
  lookback_floor_ms?: number | null;
  /**
   * Spec 032: a window is never complete before its materialization run has
   * completed. These fields make the reason visible to API/MCP callers.
   */
  materialization_state?: "pending" | "running" | "complete" | "failed" | null;
  materialization_parts_done?: number;
  materialization_parts_total?: number;
  materialization_error?: string | null;
  complete: boolean;
}

/** Input for an externally supplied, backdated complete window. */
export interface CreateWindowShotInput {
  shot_id: string;
  owner?: string;
  name?: string;
  started_at_ms: number;
  ended_at_ms: number;
  instances?: unknown;
  metadata?: Record<string, unknown>;
  coverage?: WindowCoverage;
}

/** Input for a server-clock window marker. */
export interface StartWindowShotInput {
  shot_id: string;
  owner?: string;
  name?: string;
  metadata?: Record<string, unknown>;
  instances?: unknown;
}

/** One acknowledged OTLP export in the durable raw-journal ledger. */
export interface JournalEntry {
  /** R2 object key holding the immutable raw OTLP bytes. */
  key: string;
  instance_id: string;
  instance: string;
  signal: string;
  size: number;
  /** Lowercase hex SHA-256 of the stored (possibly gzip) bytes. */
  sha256: string;
  content_type: string;
  content_encoding: string;
  /** Original `request received_at`, epoch ms. */
  received_at: number;
  /** Set by the coordinator when the row is committed. */
  committed_at?: number;
}

/** One journal object excluded from every segment. */
export interface ExcludedJournalInput {
  key: string;
  token_id: string;
  instance_id: string;
  received_at: number;
  size: number;
  /**
   * Why the object must never be compacted: a revoked token (spec 023 D3.2) or
   * a rejected late write whose R2 DELETE failed (spec 025, R1). Both follow the
   * same durable exclusion and revoked-retention rule.
   */
  reason?: "revoked" | "late_write";
}

/** Aggregated journal occupancy straight from the R2 listing (spec 023). */
export interface JournalListingStats {
  objects: number;
  bytes: number;
  pending: number;
  by_signal: Record<string, number>;
  pending_by_signal: Record<string, number>;
  oldest_received_at: number | null;
  truncated: boolean;
  scanned: number;
}

/** One journal object attributed to a segment by the compactor (spec 015). */
export interface SegmentCoverageInput {
  journal_key: string;
  signal: string;
  received_at: number;
  /** Real instance of the object; absent for a single-instance segment. */
  instance_id?: string;
}

/**
 * A publish request from the bounded compactor. The compactor has already
 * verified the R2 object's size and SHA-256; the coordinator only records the
 * durable manifest row and its coverage atoms.
 */
export interface SegmentPublishInput {
  segment_key: string;
  instance_id: string;
  object_key: string;
  size_bytes: number;
  sha256: string;
  first_received_at: number;
  last_received_at: number;
  request_count: number;
  record_count: number;
  span_count?: number;
  log_count?: number;
  metric_count?: number;
  event_min_ns?: number | null;
  event_max_ns?: number | null;
  late?: boolean;
  coverage: SegmentCoverageInput[];
}

/** One durable, advertised segment. */
export interface SegmentManifestRow {
  segment_key: string;
  instance_id: string;
  object_key: string;
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
  status: string;
  late: boolean;
  created_at: number;
  verified_at: number | null;
}

export interface SegmentPublishResult {
  committed: boolean;
  duplicate: boolean;
  covered_new: number;
  covered_skipped: number;
}

/** One uncovered raw journal object selected for compaction (spec 018). */
export interface CompactionObject {
  key: string;
  instance_id: string;
  instance: string;
  signal: string;
  size: number;
  sha256: string;
  content_type: string;
  content_encoding: string;
  received_at: number;
  /** Token that authorized the write; used for compaction-time revocation. */
  token_id?: string;
}

/** One bounded receive-time bucket ready to compact (spec 023, D3.3). */
export interface CompactionBatch {
  /**
   * Segment scope. `MULTI_INSTANCE_SCOPE` ("all") for the normal cross-instance
   * time bucket; a specific instance id only for the legacy ledger planner.
   */
  scope_id: string;
  first_received_at: number;
  last_received_at: number;
  objects: CompactionObject[];
}

/** The coordinator's durable answer to "what should be compacted next?". */
export interface CompactionPlan {
  batches: CompactionBatch[];
  pending: number;
  eligible: number;
  oldest_pending_age_ms: number | null;
  watermark: number;
}

export interface CompactionPlanOptions {
  grace_ms: number;
  window_ms: number;
  max_objects: number;
  max_bytes: number;
  max_batches: number;
  max_scan?: number;
}

/** A fenced, single-active compaction lease. */
export interface CompactionLease {
  ok: boolean;
  holder: string;
  fencing: number;
  expires_at: number;
  held_by?: string;
}

export interface CompactionPin {
  shot_id: string;
  instance_id: string | null;
  from_ms: number;
  to_ms: number;
  pinned_at: number;
  expires_at: number | null;
}

/** One GC candidate: a covered journal object eligible for deletion. */
export interface GCandidate {
  key: string;
  size: number;
  received_at: number;
  instance_id: string;
  segment_key: string;
}

export interface CompactionResultInput {
  holder: string;
  fencing: number;
  ok: boolean;
  segments_published?: number;
  bytes_published?: number;
  excluded_revoked?: number;
  error?: string | null;
  /** Set when the tick did no work for a non-error reason (spec 025, R4). */
  skipped?: string | null;
  /** Lookback health observed by this tick (spec 025, R3). */
  lookback?: {
    truncated: boolean;
    floor_ms: number | null;
    until_ms: number | null;
  } | null;
}

export interface IngestCoreDeps {
  sql: SyncSql;
  /** Injectable clock (epoch ms) for deterministic tests. */
  now?: () => number;
  /** Injectable CSPRNG token generator for deterministic tests. */
  generateToken?: () => string;
  /** Injectable instance-id generator for deterministic tests. */
  generateInstanceId?: () => string;
  /** Injectable token-id generator for deterministic tests. */
  generateTokenId?: () => string;
  /**
   * Durable object bucket (spec 018). When present, `publishSegment` re-verifies
   * the advertised R2 object's size and checksum itself before it advertises a
   * segment, so the coordinator can never silently believe an unverified
   * client-supplied hash. Optional only for pure unit tests.
   */
  bucket?: R2BucketLike;
  /**
   * Late-write budget used to extend a shot's materialization range
   * (`end + LATE_WRITE_LIMIT_MS`). Defaults to 10 000 ms (spec 031 D4).
   */
  lateWriteLimitMs?: number;
  /** Upper bound on journal objects in one materialization plan. */
  maxMaterializeObjects?: number;
}

const SCHEMA_STATEMENTS: string[] = [
  // The shot registry: attribution-only owner plus enough state for
  // `GET /api/shots` fallbacks. Recording is no longer capped by this table.
  `CREATE TABLE IF NOT EXISTS shot_registry (
     shot_id    TEXT PRIMARY KEY,
     owner      TEXT NOT NULL DEFAULT 'legacy',
     created_at INTEGER NOT NULL,
     state      TEXT NOT NULL DEFAULT 'recording',
     stopped_at INTEGER,
     size_bytes INTEGER,
     sha256     TEXT,
     kind             TEXT NOT NULL DEFAULT 'recording',
     name             TEXT,
     window_json      TEXT,
     query_watermark_ms INTEGER,
     coverage_json    TEXT,
     materialization_json TEXT
   )`,
  `CREATE INDEX IF NOT EXISTS shot_registry_created ON shot_registry (created_at)`,
  // Durable raw-journal index: one row per acknowledged OTLP export. The raw
  // bytes live in R2; this row is the durable ledger the ACK depends on.
  `CREATE TABLE IF NOT EXISTS journal_ledger (
     object_key       TEXT PRIMARY KEY,
     instance_id      TEXT NOT NULL,
     instance         TEXT NOT NULL,
     signal           TEXT NOT NULL,
     size             INTEGER NOT NULL,
     sha256           TEXT NOT NULL,
     content_type     TEXT NOT NULL DEFAULT '',
     content_encoding TEXT NOT NULL DEFAULT '',
     received_at      INTEGER NOT NULL,
     committed_at     INTEGER NOT NULL
   )`,
  `CREATE INDEX IF NOT EXISTS journal_ledger_instance ON journal_ledger (instance_id, received_at)`,
  // Safe structured diagnostics; never contains token material.
  `CREATE TABLE IF NOT EXISTS ingest_counters (
     name  TEXT PRIMARY KEY,
     value INTEGER NOT NULL DEFAULT 0
   )`,
  // Durable segment catalog (spec 015). A row is inserted only after the
  // compactor has verified the R2 segment object's size and SHA-256.
  `CREATE TABLE IF NOT EXISTS segment_manifest (
     segment_key       TEXT PRIMARY KEY,
     instance_id       TEXT NOT NULL,
     object_key        TEXT NOT NULL,
     size_bytes        INTEGER NOT NULL,
     sha256            TEXT NOT NULL,
     first_received_at INTEGER NOT NULL,
     last_received_at  INTEGER NOT NULL,
     request_count     INTEGER NOT NULL,
     record_count      INTEGER NOT NULL,
     span_count        INTEGER NOT NULL DEFAULT 0,
     log_count         INTEGER NOT NULL DEFAULT 0,
     metric_count      INTEGER NOT NULL DEFAULT 0,
     event_min_ns      INTEGER,
     event_max_ns      INTEGER,
     status            TEXT NOT NULL,
     late              INTEGER NOT NULL DEFAULT 0,
     created_at        INTEGER NOT NULL,
     verified_at       INTEGER
   )`,
  `CREATE INDEX IF NOT EXISTS segment_manifest_instance ON segment_manifest (instance_id, first_received_at)`,
  // Exactly-once coverage: each raw journal object is attributed to one segment.
  `CREATE TABLE IF NOT EXISTS segment_coverage (
     journal_key TEXT PRIMARY KEY,
     segment_key TEXT NOT NULL,
     instance_id TEXT NOT NULL,
     signal      TEXT NOT NULL,
     received_at INTEGER NOT NULL,
     covered_at  INTEGER NOT NULL,
     deleted_from_r2 INTEGER NOT NULL DEFAULT 0
   )`,
  `CREATE INDEX IF NOT EXISTS segment_coverage_segment ON segment_coverage (segment_key)`,
  // Spec 023: journal objects written with a token revoked before receipt are
  // excluded from every segment. Spec 025 R1 reuses the same durable row for a
  // late write whose R2 DELETE failed, so neither can ever be compacted.
  `CREATE TABLE IF NOT EXISTS excluded_journal (
     journal_key TEXT PRIMARY KEY,
     token_id    TEXT NOT NULL,
     instance_id TEXT NOT NULL,
     received_at INTEGER NOT NULL,
     size        INTEGER NOT NULL,
     excluded_at INTEGER NOT NULL,
     reason      TEXT NOT NULL DEFAULT 'revoked'
   )`,
  // Free-form compaction health that is not an integer counter (skip reasons).
  `CREATE TABLE IF NOT EXISTS compaction_meta_text (
     name  TEXT PRIMARY KEY,
     value TEXT NOT NULL
   )`,
  `CREATE INDEX IF NOT EXISTS excluded_journal_time ON excluded_journal (received_at)`,
  // Spec 018: single-active lease per compaction partition. At-least-once
  // scheduling is fenced by (holder, fencing): a stale holder can never publish
  // or delete after a newer holder has taken the lease.
  `CREATE TABLE IF NOT EXISTS compaction_lease (
     partition  TEXT PRIMARY KEY,
     holder     TEXT NOT NULL,
     fencing    INTEGER NOT NULL,
     expires_at INTEGER NOT NULL,
     updated_at INTEGER NOT NULL
   )`,
  // Spec 018: durable scheduler health (last success/failure, consecutive
  // failures). Counters that only ever increase live in `ingest_counters`.
  `CREATE TABLE IF NOT EXISTS compaction_meta (
     name  TEXT PRIMARY KEY,
     value INTEGER NOT NULL
   )`,
  // Spec 018: shot pins protect a receive-time range from GC. GC never deletes a
  // journal object whose segment range overlaps a pin.
  `CREATE TABLE IF NOT EXISTS segment_pins (
     shot_id     TEXT NOT NULL,
     instance_id TEXT,
     from_ms     INTEGER NOT NULL,
     to_ms       INTEGER NOT NULL,
     pinned_at   INTEGER NOT NULL,
     expires_at  INTEGER
   )`,
  `CREATE INDEX IF NOT EXISTS segment_pins_shot ON segment_pins (shot_id)`,
  // Spec 031: active materialization runs make continuous compaction yield.
  // Rows carry an expiry so a crashed materializer can never stall compaction.
  `CREATE TABLE IF NOT EXISTS materialization_runs (
     shot_id    TEXT PRIMARY KEY,
     started_at INTEGER NOT NULL,
     expires_at INTEGER NOT NULL
   )`,
];

export class IngestCore {
  private readonly sql: SyncSql;
  private readonly now: () => number;
  private readonly instances: InstanceCore;
  private readonly bucket?: R2BucketLike;
  private readonly lateWriteLimitMs: number;
  private readonly maxMaterializeObjects: number;
  private loaded = false;
  private loading?: Promise<void>;

  constructor(deps: IngestCoreDeps) {
    this.sql = deps.sql;
    this.now = deps.now ?? (() => Date.now());
    this.bucket = deps.bucket;
    this.lateWriteLimitMs = Math.max(0, deps.lateWriteLimitMs ?? 10_000);
    this.maxMaterializeObjects = Math.max(1, deps.maxMaterializeObjects ?? 200_000);
    this.instances = new InstanceCore({
      sql: deps.sql,
      now: deps.now,
      generateToken: deps.generateToken ?? newInstanceToken,
      generateInstanceId: deps.generateInstanceId,
      generateTokenId: deps.generateTokenId,
    });
  }

  async load(): Promise<void> {
    for (const stmt of SCHEMA_STATEMENTS) this.sql.exec(stmt);
    // Additive migrations for pre-existing shot_registry tables (spec 016).
    for (const stmt of [
      `ALTER TABLE shot_registry ADD COLUMN kind TEXT NOT NULL DEFAULT 'recording'`,
      `ALTER TABLE shot_registry ADD COLUMN name TEXT`,
      `ALTER TABLE shot_registry ADD COLUMN window_json TEXT`,
      `ALTER TABLE shot_registry ADD COLUMN query_watermark_ms INTEGER`,
      `ALTER TABLE shot_registry ADD COLUMN coverage_json TEXT`,
      `ALTER TABLE shot_registry ADD COLUMN materialization_json TEXT`,
      // Additive migration for a segment_pins table created before TTL support.
      `ALTER TABLE segment_pins ADD COLUMN expires_at INTEGER`,
      // Additive migration for coverage rows created before spec 023 GC markers.
      `ALTER TABLE segment_coverage ADD COLUMN deleted_from_r2 INTEGER NOT NULL DEFAULT 0`,
      // Additive migration for exclusions created before spec 025 R1 reasons.
      `ALTER TABLE excluded_journal ADD COLUMN reason TEXT NOT NULL DEFAULT 'revoked'`,
    ]) {
      try {
        this.sql.exec(stmt);
      } catch {
        // Column already exists.
      }
    }
    await this.instances.load();
    this.loaded = true;
  }

  async ensureLoaded(): Promise<void> {
    if (this.loaded) return;
    this.loading ??= this.load();
    await this.loading;
  }

  /** The named-instance registry and credential store over the same storage. */
  get instanceRegistry(): InstanceCore {
    return this.instances;
  }

  // ----------------------------------------------------------- registry

  /**
   * Registers a freshly created shot for listing and the owner filter. There is
   * no concurrency cap (spec 014): recording is bounded by the explicit backend
   * container capacity controls, not by an application-level rule.
   */
  async registerShot(shotId: string, owner: string): Promise<{ registered: boolean }> {
    await this.ensureLoaded();
    const cursor = this.sql.exec(
      `INSERT INTO shot_registry (shot_id, owner, created_at, state)
       VALUES (?, ?, ?, 'recording')
       ON CONFLICT (shot_id) DO UPDATE SET owner = excluded.owner, state = 'recording'`,
      shotId,
      owner,
      this.now(),
    );
    this.bump("registered");
    return { registered: (cursor.rowsWritten ?? 0) > 0 };
  }

  /**
   * Releases one or more registrations. Used to roll back a failed shot start
   * and to reconcile stale `recording` entries whose ShotDO is gone.
   */
  async releaseShots(shotIds: string[]): Promise<{ released: number }> {
    await this.ensureLoaded();
    let released = 0;
    for (const shotId of shotIds) {
      const cursor = this.sql.exec(`DELETE FROM shot_registry WHERE shot_id = ? AND state = 'recording'`, shotId);
      released += cursor.rowsWritten ?? 0;
    }
    if (released > 0) this.bump("released", released);
    return { released };
  }

  /** Records a shot's terminal/observed state so listing stays true. */
  async setShotState(
    shotId: string,
    state: string,
    extra: { stopped_at?: number; size_bytes?: number; sha256?: string } = {},
  ): Promise<{ updated: boolean }> {
    await this.ensureLoaded();
    const cursor = this.sql.exec(
      `UPDATE shot_registry
          SET state = ?,
              stopped_at = COALESCE(?, stopped_at),
              size_bytes = COALESCE(?, size_bytes),
              sha256 = COALESCE(?, sha256)
        WHERE shot_id = ?`,
      state,
      extra.stopped_at ?? null,
      extra.size_bytes ?? null,
      extra.sha256 ?? null,
      shotId,
    );
    return { updated: (cursor.rowsWritten ?? 0) > 0 };
  }

  /** Shot ids currently registered as recording (reconciliation fallback). */
  async listRecording(): Promise<{ shots: Array<{ shot_id: string }> }> {
    await this.ensureLoaded();
    const shots = this.sql
      .exec(`SELECT shot_id FROM shot_registry WHERE state = 'recording' ORDER BY created_at ASC`)
      .toArray()
      .map((row) => ({ shot_id: String(row.shot_id) }));
    return { shots };
  }

  /** Registered shots, newest first, optionally filtered by owner. */
  async listShots(opts: { owner?: string; limit: number; offset: number }): Promise<{
    shots: WindowShotRecord[];
    total: number;
  }> {
    await this.ensureLoaded();
    const where = opts.owner ? `WHERE owner = ?` : "";
    const args: SqlValue[] = opts.owner ? [opts.owner] : [];
    const totalRows = this.sql.exec(`SELECT COUNT(*) AS n FROM shot_registry ${where}`, ...args).toArray();
    const total = Number(totalRows[0]?.n ?? 0);
    const rows = this.sql
      .exec(
        `SELECT shot_id, owner, created_at, state, stopped_at, size_bytes, sha256,
                kind, name, window_json, query_watermark_ms, coverage_json, materialization_json
           FROM shot_registry ${where}
          ORDER BY created_at DESC, shot_id DESC
          LIMIT ? OFFSET ?`,
        ...args,
        opts.limit,
        opts.offset,
      )
      .toArray();
    return { shots: rows.map(registeredRow), total };
  }

  // -------------------------------------------------------- window shots

  /**
   * Creates an externally supplied, backdated complete window. The timestamps
   * are validated (ordered, bounded, retained) and retained as untrusted
   * client-clock values.
   */
  async createWindowShot(input: CreateWindowShotInput, bounds: WindowBounds = DEFAULT_WINDOW_BOUNDS): Promise<WindowShotRecord> {
    await this.ensureLoaded();
    const window = externalWindow(input.started_at_ms, input.ended_at_ms, input.instances, input.name);
    const valid = validateWindow(window, this.now(), bounds);
    if (!valid.ok) throw badRequest(`${valid.code}: ${valid.error}`);
    // Spec 032: a new window starts with a pending materialization run, so even
    // an externally supplied "complete" coverage cannot claim completeness
    // before the parts exist.
    const pending = pendingMaterialization();
    const coverage = normalizeCoverage(input.coverage, window.instances, pending);
    const state = coverage.complete ? "ready" : "incomplete";
    this.upsertWindowRow(input.shot_id, input.owner, window, state, coverage, input.metadata);
    // Protect the shot's receive-time range from GC (TTL-bounded).
    await this.addPin(input.shot_id, null, window.start_ms, window.end_ms, DEFAULT_SHOT_PIN_TTL_MS);
    this.ensureMaterializationPending(input.shot_id);
    const row = this.getShotRecord(input.shot_id);
    if (!row) throw new Error("failed to persist window shot");
    return row;
  }

  /** Opens a server-clock window marker. */
  async startWindowShot(input: StartWindowShotInput, nowMs?: number): Promise<WindowShotRecord> {
    await this.ensureLoaded();
    const start = nowMs ?? this.now();
    // The end is provisional until stop; keep a 1ms placeholder so the stored
    // window is always ordered. It is replaced on stop.
    const window = serverWindow(start, start + 1, input.instances, input.name);
    const coverage: WindowCoverage = {
      expected_segments: 0,
      available_segments: 0,
      missing_segments: [],
      instances: window.instances,
      complete: false,
    };
    this.upsertWindowRow(input.shot_id, input.owner, window, "open", coverage, input.metadata);
    await this.addPin(input.shot_id, null, window.start_ms, window.end_ms, DEFAULT_SHOT_PIN_TTL_MS);
    const row = this.getShotRecord(input.shot_id);
    if (!row) throw new Error("failed to persist window shot");
    return row;
  }

  /**
   * Stops and finalizes a server-clock window. `ended_at_ms` may be supplied to
   * backdate the end (still validated as an ordered, bounded instant). The stop
   * is idempotent. Coverage defaults to complete for a server window unless a
   * later analysis marks it incomplete.
   */
  async stopWindowShot(
    shotId: string,
    endedAtMs?: number,
    bounds: WindowBounds = DEFAULT_WINDOW_BOUNDS,
  ): Promise<WindowShotRecord> {
    await this.ensureLoaded();
    const row = this.getShotRecord(shotId);
    if (!row) throw badRequest("unknown window shot");
    if (!row.window) throw badRequest("shot is not a window shot");
    if (row.state === "ready" || row.state === "incomplete") return row;
    const now = this.now();
    const candidate = typeof endedAtMs === "number" && Number.isFinite(endedAtMs) ? Math.floor(endedAtMs) : now;
    // A window is always half-open and ordered; a stop in the same millisecond
    // as the start still produces a valid, non-empty interval.
    const end = Math.max(candidate, row.window.start_ms + 1);
    const window: ShotWindow = { ...row.window, end_ms: end };
    const valid = validateWindow(window, now, bounds);
    if (!valid.ok) throw badRequest(`${valid.code}: ${valid.error}`);
    const prior = row.coverage;
    const materialization = row.materialization ?? pendingMaterialization();
    const coverage: WindowCoverage =
      prior && prior.missing_segments.length > 0
        ? normalizeCoverage(prior, window.instances, materialization)
        : normalizeCoverage(
            {
              expected_segments: prior?.expected_segments ?? 0,
              available_segments: prior?.available_segments ?? 0,
              missing_segments: [],
              instances: window.instances,
              complete: true,
            },
            window.instances,
            materialization,
          );
    const state = coverage.complete ? "ready" : "incomplete";
    this.sql.exec(
      `UPDATE shot_registry SET state = ?, stopped_at = ?, window_json = ?, query_watermark_ms = ?, coverage_json = ?
        WHERE shot_id = ?`,
      state,
      end,
      windowToJSON(window),
      now,
      JSON.stringify(coverage),
      shotId,
    );
    // Refresh the GC pin over the finalized window range (TTL-bounded).
    await this.addPin(shotId, null, window.start_ms, window.end_ms, DEFAULT_SHOT_PIN_TTL_MS);
    this.ensureMaterializationPending(shotId);
    const updated = this.getShotRecord(shotId);
    if (!updated) throw new Error("failed to persist stopped window shot");
    return updated;
  }

  /** Records the coverage/completeness computed by an analysis pass. */
  async setWindowCoverage(shotId: string, coverage: WindowCoverage): Promise<WindowShotRecord> {
    await this.ensureLoaded();
    const row = this.getShotRecord(shotId);
    if (!row) throw badRequest("unknown window shot");
    // Spec 032: coverage.complete is false until materialization completes.
    // The materialization state is always disclosed alongside it.
    const normalized = normalizeCoverage(coverage, row.window?.instances ?? [], row.materialization);
    const state = normalized.complete ? "ready" : "incomplete";
    this.sql.exec(
      `UPDATE shot_registry SET coverage_json = ?, state = ? WHERE shot_id = ?`,
      JSON.stringify(normalized),
      state,
      shotId,
    );
    const updated = this.getShotRecord(shotId);
    if (!updated) throw new Error("failed to persist coverage");
    return updated;
  }

  /**
   * Enumerates the journal objects a shot needs, bounded to
   * `[start - 5s, end + LATE_WRITE_LIMIT_MS]` for every selected instance and
   * signal (spec 031 D3.1). Keys sort by `received_at`, so the JS coordinator
   * can split contiguous chunks by byte size without another listing.
   */
  async materializationPlan(shotId: string): Promise<MaterializationPlan> {
    await this.ensureLoaded();
    const row = this.getShotRecord(shotId);
    if (!row) throw badRequest("unknown window shot");
    if (!row.window) throw badRequest("shot is not a window shot");
    if (!this.bucket) throw badRequest("object bucket binding is unavailable");
    const window = row.window;
    const fromMs = Math.max(0, window.start_ms - 5_000);
    const toMs = window.end_ms + this.lateWriteLimitMs;

    let instanceIds: string[] = [];
    if (window.instances.length > 0) {
      const listed = await this.instances.list();
      const wanted = new Set(window.instances);
      instanceIds = listed.instances.filter((i) => wanted.has(i.name)).map((i) => i.instance_id);
    }
    const listed = await listJournalObjects(this.bucket, {
      instanceIds: instanceIds.length > 0 ? instanceIds : undefined,
      fromMs,
      toMs,
      maxObjects: this.maxMaterializeObjects,
      maxScan: this.maxMaterializeObjects * 4,
      pageSize: 1000,
    });
    const excludedRows = this.sql
      .exec(`SELECT journal_key FROM excluded_journal WHERE received_at >= ? AND received_at < ?`, fromMs, toMs)
      .toArray();
    const excluded = new Set(excludedRows.map((r) => String(r.journal_key)));
    const objects: MaterializationObject[] = [];
    let totalBytes = 0;
    for (const info of listed.objects) {
      if (excluded.has(info.key)) continue;
      totalBytes += info.size;
      objects.push({
        key: info.key,
        signal: info.signal,
        received_at: info.received_at,
        content_encoding: info.content_encoding,
        content_type: info.content_type,
        size: info.size,
        sha256: info.sha256,
        instance_id: info.instance_id,
        instance: info.instance,
      });
    }
    return {
      shot_id: shotId,
      from_ms: fromMs,
      to_ms: toMs,
      objects,
      excluded: [...excluded].sort(),
      total_bytes: totalBytes,
      truncated: listed.truncated,
    };
  }

  /** Initializes `materialization.state = pending` exactly once. */
  private ensureMaterializationPending(shotId: string): void {
    this.sql.exec(
      `UPDATE shot_registry SET materialization_json = ?
        WHERE shot_id = ? AND (materialization_json IS NULL OR materialization_json = '')`,
      JSON.stringify(pendingMaterialization()),
      shotId,
    );
  }

  /** Persists the current materialization status for a shot. */
  async setMaterialization(shotId: string, record: MaterializationRecord): Promise<WindowShotRecord> {
    await this.ensureLoaded();
    const row = this.getShotRecord(shotId);
    if (!row) throw badRequest("unknown window shot");
    if (!["pending", "running", "complete", "failed"].includes(record.state)) {
      throw badRequest("invalid materialization state");
    }
    const normalized: MaterializationRecord = {
      state: record.state,
      parts_done: numOr(record.parts_done, 0),
      parts_total: numOr(record.parts_total, 0),
      spans: numOr(record.spans, 0),
      started_at: typeof record.started_at === "number" ? record.started_at : null,
      finished_at: typeof record.finished_at === "number" ? record.finished_at : null,
      parts: Array.isArray(record.parts) ? record.parts : [],
      error: record.error ?? null,
      part_states: normalizePartStates(record.part_states),
      lease_expires_at:
        typeof record.lease_expires_at === "number" && Number.isFinite(record.lease_expires_at)
          ? record.lease_expires_at
          : null,
      heartbeat_at:
        typeof record.heartbeat_at === "number" && Number.isFinite(record.heartbeat_at)
          ? record.heartbeat_at
          : null,
      attempts: numOr(record.attempts, 0),
    };
    this.sql.exec(
      `UPDATE shot_registry SET materialization_json = ? WHERE shot_id = ?`,
      JSON.stringify(normalized),
      shotId,
    );
    // Spec 032: keep the derived coverage honest. A pending/running/failed run
    // can never leave a stale `complete: true` behind, and a successful run
    // lets coverage become complete when no other gap is known.
    const coverage = row.coverage ?? emptyCoverage(row.window?.instances ?? []);
    const nextCoverage = normalizeCoverage(
      { ...coverage, complete: true },
      row.window?.instances ?? [],
      normalized,
    );
    this.sql.exec(
      `UPDATE shot_registry SET coverage_json = ?, state = ? WHERE shot_id = ?`,
      JSON.stringify(nextCoverage),
      row.state === "open" ? "open" : nextCoverage.complete ? "ready" : "incomplete",
      shotId,
    );
    const updated = this.getShotRecord(shotId);
    if (!updated) throw new Error("failed to persist materialization");
    return updated;
  }

  /**
   * Marks a materialization as active so continuous compaction yields. The
   * lease is TTL-bounded: a crashed coordinator cannot stall compaction.
   */
  async beginMaterializationRun(shotId: string, ttlMs: number): Promise<{ active: number }> {
    await this.ensureLoaded();
    const now = this.now();
    const ttl = Math.max(1_000, ttlMs > 0 ? ttlMs : 5 * 60 * 1000);
    this.pruneMaterializationRuns(now);
    this.sql.exec(
      `INSERT INTO materialization_runs (shot_id, started_at, expires_at) VALUES (?, ?, ?)
       ON CONFLICT (shot_id) DO UPDATE SET started_at = excluded.started_at, expires_at = excluded.expires_at`,
      shotId,
      now,
      now + ttl,
    );
    return { active: this.activeMaterializationRuns(now) };
  }

  /** Releases one materialization run. */
  async endMaterializationRun(shotId: string): Promise<{ active: number }> {
    await this.ensureLoaded();
    const now = this.now();
    this.pruneMaterializationRuns(now);
    this.sql.exec(`DELETE FROM materialization_runs WHERE shot_id = ?`, shotId);
    return { active: this.activeMaterializationRuns(now) };
  }

  /** True while any unexpired materialization run is active. */
  async materializationYield(): Promise<{ yield: boolean; active: number }> {
    await this.ensureLoaded();
    const now = this.now();
    this.pruneMaterializationRuns(now);
    const active = this.activeMaterializationRuns(now);
    return { yield: active > 0, active };
  }

  private pruneMaterializationRuns(now: number): void {
    this.sql.exec(`DELETE FROM materialization_runs WHERE expires_at <= ?`, now);
  }

  private activeMaterializationRuns(now: number): number {
    const rows = this.sql
      .exec(`SELECT COUNT(*) AS n FROM materialization_runs WHERE expires_at > ?`, now)
      .toArray();
    return Number(rows[0]?.n ?? 0);
  }

  /** One registry row (window or legacy), or null. */
  getShotRecord(shotId: string): WindowShotRecord | null {
    const rows = this.sql
      .exec(
        `SELECT shot_id, owner, created_at, state, stopped_at, size_bytes, sha256,
                kind, name, window_json, query_watermark_ms, coverage_json, materialization_json
           FROM shot_registry WHERE shot_id = ?`,
        shotId,
      )
      .toArray();
    return rows.length === 0 ? null : registeredRow(rows[0]!);
  }

  private upsertWindowRow(
    shotId: string,
    owner: string | undefined,
    window: ShotWindow,
    state: string,
    coverage: WindowCoverage,
    metadata: Record<string, unknown> | undefined,
  ): void {
    this.sql.exec(
      `INSERT INTO shot_registry
         (shot_id, owner, created_at, state, kind, name, window_json, coverage_json, materialization_json)
       VALUES (?, ?, ?, ?, 'window', ?, ?, ?, ?)
       ON CONFLICT (shot_id) DO UPDATE SET
         owner = excluded.owner, state = excluded.state, kind = 'window',
         name = excluded.name, window_json = excluded.window_json,
         coverage_json = excluded.coverage_json,
         materialization_json = COALESCE(shot_registry.materialization_json, excluded.materialization_json)`,
      shotId,
      owner && owner.trim() !== "" ? owner.trim() : "legacy",
      this.now(),
      state,
      window.name ?? null,
      windowToJSON(window),
      JSON.stringify(coverage),
      JSON.stringify(pendingMaterialization()),
    );
    void metadata;
  }

  // ------------------------------------------------------------- journal

  /**
   * Durably indexes one acknowledged OTLP export.
   *
   * @deprecated Spec 023 removed the ledger from the ingest ACK path. This
   * remains only for legacy reconciliation tooling; new intake never calls it,
   * and compaction discovers work from the R2 listing.
   */
  async commitJournal(entry: JournalEntry): Promise<{ committed: boolean }> {
    await this.ensureLoaded();
    const cursor = this.sql.exec(
      `INSERT INTO journal_ledger
         (object_key, instance_id, instance, signal, size, sha256, content_type, content_encoding, received_at, committed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (object_key) DO NOTHING`,
      entry.key,
      entry.instance_id,
      entry.instance,
      entry.signal,
      entry.size,
      entry.sha256,
      entry.content_type ?? "",
      entry.content_encoding ?? "",
      entry.received_at,
      this.now(),
    );
    const committed = (cursor.rowsWritten ?? 0) > 0;
    this.bump(committed ? "journal_committed" : "journal_duplicate");
    this.bump(`journal_committed:${entry.signal}`);
    return { committed };
  }

  /**
   * Reconciles one or more candidate R2 objects into the ledger. Used after a
   * partial failure (raw bytes durable, ledger write lost) so an unindexed
   * object is recovered without re-writing the payload.
   */
  async reconcileJournal(entries: JournalEntry[]): Promise<{ reconciled: number; skipped: number }> {
    await this.ensureLoaded();
    let reconciled = 0;
    let skipped = 0;
    for (const entry of entries) {
      const result = await this.commitJournal(entry);
      if (result.committed) reconciled += 1;
      else skipped += 1;
    }
    if (reconciled > 0) this.bump("journal_reconciled", reconciled);
    return { reconciled, skipped };
  }

  /** Ledger rows, newest first, optionally scoped to one instance. */
  async listJournal(opts: { instance_id?: string; limit: number; offset: number }): Promise<{
    entries: JournalEntry[];
    total: number;
  }> {
    await this.ensureLoaded();
    const where = opts.instance_id ? `WHERE instance_id = ?` : "";
    const args: SqlValue[] = opts.instance_id ? [opts.instance_id] : [];
    const totalRows = this.sql.exec(`SELECT COUNT(*) AS n FROM journal_ledger ${where}`, ...args).toArray();
    const total = Number(totalRows[0]?.n ?? 0);
    const rows = this.sql
      .exec(
        `SELECT object_key, instance_id, instance, signal, size, sha256,
                content_type, content_encoding, received_at, committed_at
           FROM journal_ledger ${where}
          ORDER BY received_at DESC, object_key DESC
          LIMIT ? OFFSET ?`,
        ...args,
        opts.limit,
        opts.offset,
      )
      .toArray();
    return { entries: rows.map(journalRow), total };
  }

  // ------------------------------------------------------------- segments

  /**
   * Durably publishes a verified segment's manifest row and its coverage atoms.
   *
   * The caller (the bounded compactor) must have already re-verified the R2
   * object's size and SHA-256. Publication is idempotent on `segment_key`: a
   * repeated publish with the same checksum is a no-op duplicate and can never
   * create duplicate coverage, because `segment_coverage.journal_key` is a
   * primary key. A differing checksum for an existing key is a hard conflict.
   */
  async publishSegment(input: SegmentPublishInput): Promise<SegmentPublishResult> {
    await this.ensureLoaded();
    const validated = validateSegmentPublish(input);
    if (!validated.ok) throw badRequest(validated.error);

    // Spec 018 hardening: never advertise a segment before re-verifying the
    // stored R2 object ourselves. This closes the gap where a caller could
    // otherwise assert an unverified size/checksum. Only skipped by pure unit
    // tests that inject no bucket binding at all.
    if (this.bucket) {
      const verified = await verifyStoredSegment(this.bucket, validated.value);
      if (!verified.ok) throw new Error(verified.error);
    }

    const now = this.now();
    const seg = validated.value;

    // Spec 025 R2: the manifest row and every coverage atom are one atomic
    // unit. A crash or an injected failure between them rolls the whole
    // publication back, so no partially published segment is ever visible.
    // The 024 duplicate-publish healing below remains as defense in depth.
    const publish = (): {
      duplicate: boolean;
      coveredNew: number;
      coveredSkipped: number;
    } => {
      const existing = this.sql
        .exec(`SELECT sha256 FROM segment_manifest WHERE segment_key = ?`, seg.segment_key)
        .toArray();
      const duplicate = existing.length > 0;
      if (duplicate && String(existing[0]!.sha256) !== seg.sha256) {
        throw new Error("segment_key already exists with a different checksum");
      }
      if (!duplicate) {
        this.sql.exec(
          `INSERT INTO segment_manifest
             (segment_key, instance_id, object_key, size_bytes, sha256, first_received_at, last_received_at,
              request_count, record_count, span_count, log_count, metric_count, event_min_ns, event_max_ns,
              status, late, created_at, verified_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'verified', ?, ?, ?)`,
          seg.segment_key,
          seg.instance_id,
          seg.object_key,
          seg.size_bytes,
          seg.sha256,
          seg.first_received_at,
          seg.last_received_at,
          seg.request_count,
          seg.record_count,
          seg.span_count ?? 0,
          seg.log_count ?? 0,
          seg.metric_count ?? 0,
          seg.event_min_ns ?? null,
          seg.event_max_ns ?? null,
          seg.late ? 1 : 0,
          now,
          now,
        );
      }

      // Coverage insertion is idempotent per journal key and also heals a
      // crash after the manifest row was written but before every atom landed:
      // a duplicate publish re-checks all atoms instead of returning early
      // (spec 024 R7 review fix).
      let coveredNew = 0;
      let coveredSkipped = 0;
      for (const cov of seg.coverage) {
        const hit = this.sql
          .exec(`SELECT 1 AS one FROM segment_coverage WHERE journal_key = ?`, cov.journal_key)
          .toArray();
        if (hit.length > 0) {
          coveredSkipped += 1;
          continue;
        }
        this.sql.exec(
          `INSERT INTO segment_coverage (journal_key, segment_key, instance_id, signal, received_at, covered_at)
           VALUES (?,?,?,?,?,?)`,
          cov.journal_key,
          seg.segment_key,
          cov.instance_id && cov.instance_id !== "" ? cov.instance_id : seg.instance_id,
          cov.signal,
          cov.received_at,
          now,
        );
        coveredNew += 1;
      }
      return { duplicate, coveredNew, coveredSkipped };
    };

    // Use the storage transaction when the adapter provides one (ControlDO,
    // in-process SQLite). The fallback keeps minimal read-only doubles usable.
    const outcome = this.sql.transaction ? this.sql.transaction(publish) : publish();

    if (outcome.duplicate) {
      this.bump("segment_duplicate");
      return {
        committed: false,
        duplicate: true,
        covered_new: outcome.coveredNew,
        covered_skipped: outcome.coveredSkipped,
      };
    }
    this.bump("segment_published");
    if (outcome.coveredNew > 0) this.bump("segment_covered", outcome.coveredNew);
    if (seg.late) this.bump("segment_late");
    return {
      committed: true,
      duplicate: false,
      covered_new: outcome.coveredNew,
      covered_skipped: outcome.coveredSkipped,
    };
  }

  /** Advertised segments, oldest range first, optionally filtered by instance. */
  async listSegments(opts: { instance_id?: string; limit: number; offset: number }): Promise<{
    segments: SegmentManifestRow[];
    total: number;
  }> {
    await this.ensureLoaded();
    const where = opts.instance_id ? `WHERE instance_id = ?` : "";
    const args: SqlValue[] = opts.instance_id ? [opts.instance_id] : [];
    const totalRows = this.sql.exec(`SELECT COUNT(*) AS n FROM segment_manifest ${where}`, ...args).toArray();
    const total = Number(totalRows[0]?.n ?? 0);
    const rows = this.sql
      .exec(
        `SELECT segment_key, instance_id, object_key, size_bytes, sha256, first_received_at, last_received_at,
                request_count, record_count, span_count, log_count, metric_count, event_min_ns, event_max_ns,
                status, late, created_at, verified_at
           FROM segment_manifest ${where}
          ORDER BY first_received_at ASC, segment_key ASC
          LIMIT ? OFFSET ?`,
        ...args,
        opts.limit,
        opts.offset,
      )
      .toArray();
    return { segments: rows.map(segmentRow), total };
  }

  // ---------------------------------------------------------- compaction

  /**
   * Selects the next bounded batch of uncovered, finalized raw journal objects,
   * grouped by instance into receive-time windows. Only rows that are not yet
   * attributed to a segment and that are older than the grace watermark are
   * eligible, so a late arrival cannot be silently skipped.
   */
  async planCompaction(opts: CompactionPlanOptions): Promise<CompactionPlan> {
    await this.ensureLoaded();
    const now = this.now();
    const watermark = now - Math.max(0, opts.grace_ms);
    const maxObjects = Math.max(1, opts.max_objects);
    const maxBytes = Math.max(1, opts.max_bytes);
    const maxBatches = Math.max(1, opts.max_batches);
    const maxScan = Math.max(maxObjects, opts.max_scan ?? maxObjects * maxBatches * 4);

    const pendingRow = this.sql
      .exec(
        `SELECT COUNT(*) AS n FROM journal_ledger j
           LEFT JOIN segment_coverage c ON c.journal_key = j.object_key
          WHERE c.journal_key IS NULL`,
      )
      .toArray();
    const pending = Number(pendingRow[0]?.n ?? 0);
    const oldestRow = this.sql
      .exec(
        `SELECT MIN(j.received_at) AS m FROM journal_ledger j
           LEFT JOIN segment_coverage c ON c.journal_key = j.object_key
          WHERE c.journal_key IS NULL`,
      )
      .toArray();
    const oldest = oldestRow[0]?.m == null ? null : Number(oldestRow[0].m);

    const rows = this.sql
      .exec(
        `SELECT j.object_key, j.instance_id, j.instance, j.signal, j.size, j.sha256,
                j.content_type, j.content_encoding, j.received_at
           FROM journal_ledger j
           LEFT JOIN segment_coverage c ON c.journal_key = j.object_key
          WHERE c.journal_key IS NULL AND j.received_at <= ?
          ORDER BY j.instance_id ASC, j.received_at ASC, j.object_key ASC
          LIMIT ?`,
        watermark,
        maxScan,
      )
      .toArray();

    const batches: CompactionBatch[] = [];
    let cur: CompactionBatch | null = null;
    let curBytes = 0;
    for (const row of rows) {
      if (batches.length >= maxBatches) break;
      const obj: CompactionObject = {
        key: String(row.object_key),
        instance_id: String(row.instance_id),
        instance: String(row.instance ?? ""),
        signal: String(row.signal),
        size: Number(row.size),
        sha256: String(row.sha256),
        content_type: String(row.content_type ?? ""),
        content_encoding: String(row.content_encoding ?? ""),
        received_at: Number(row.received_at),
      };
      const split =
        cur === null ||
        cur.scope_id !== obj.instance_id ||
        obj.received_at - cur.first_received_at >= opts.window_ms ||
        cur.objects.length >= maxObjects ||
        curBytes + obj.size > maxBytes;
      if (split) {
        if (cur !== null) batches.push(cur);
        if (batches.length >= maxBatches) break;
        cur = {
          scope_id: obj.instance_id,
          first_received_at: obj.received_at,
          last_received_at: obj.received_at,
          objects: [],
        };
        curBytes = 0;
      }
      cur!.objects.push(obj);
      cur!.last_received_at = Math.max(cur!.last_received_at, obj.received_at);
      curBytes += obj.size;
    }
    if (cur !== null && batches.length < maxBatches) batches.push(cur);

    return {
      batches,
      pending,
      eligible: rows.length,
      oldest_pending_age_ms: oldest === null ? null : now - oldest,
      watermark,
    };
  }

  /**
   * Acquires or renews the single-active compaction lease for a partition. A
   * live lease held by a different holder is never stolen; an expired lease is
   * taken over with an incremented fencing token.
   */
  async acquireLease(partition: string, holder: string, ttlMs: number): Promise<CompactionLease> {
    await this.ensureLoaded();
    const now = this.now();
    const rows = this.sql
      .exec(`SELECT holder, fencing, expires_at FROM compaction_lease WHERE partition = ?`, partition)
      .toArray();
    if (rows.length > 0) {
      const existingHolder = String(rows[0]!.holder);
      const fencing = Number(rows[0]!.fencing);
      const expiresAt = Number(rows[0]!.expires_at);
      if (existingHolder !== holder && expiresAt > now) {
        return { ok: false, holder: existingHolder, fencing, expires_at: expiresAt, held_by: existingHolder };
      }
      const next = fencing + 1;
      this.sql.exec(
        `UPDATE compaction_lease SET holder = ?, fencing = ?, expires_at = ?, updated_at = ? WHERE partition = ?`,
        holder,
        next,
        now + ttlMs,
        now,
        partition,
      );
      return { ok: true, holder, fencing: next, expires_at: now + ttlMs };
    }
    this.sql.exec(
      `INSERT INTO compaction_lease (partition, holder, fencing, expires_at, updated_at) VALUES (?,?,?,?,?)`,
      partition,
      holder,
      1,
      now + ttlMs,
      now,
    );
    return { ok: true, holder, fencing: 1, expires_at: now + ttlMs };
  }

  /** Releases a lease only when the holder and fencing token still match. */
  async releaseLease(partition: string, holder: string, fencing: number): Promise<{ released: boolean }> {
    await this.ensureLoaded();
    const cursor = this.sql.exec(
      `DELETE FROM compaction_lease WHERE partition = ? AND holder = ? AND fencing = ?`,
      partition,
      holder,
      fencing,
    );
    return { released: (cursor.rowsWritten ?? 0) > 0 };
  }

  /** Records the outcome of a fenced compaction run (stale holders are ignored). */
  async recordCompaction(input: CompactionResultInput): Promise<{ ok: boolean; ignored: boolean }> {
    await this.ensureLoaded();
    const now = this.now();

    // Spec 025 R4: a tick skipped because another holder owns the lease is not
    // a failure and cannot carry a matching fence, but it must still leave a
    // visible trace so `compaction_status` is never silently stale.
    if (input.skipped) {
      this.setMeta("last_skipped_at", now);
      this.setMetaText("last_skipped_reason", input.skipped);
      return { ok: true, ignored: false };
    }

    const lease = this.sql
      .exec(`SELECT holder, fencing FROM compaction_lease WHERE partition = 'primary'`)
      .toArray();
    if (lease.length > 0) {
      const same =
        String(lease[0]!.holder) === input.holder && Number(lease[0]!.fencing) === input.fencing;
      if (!same) return { ok: false, ignored: true };
    }
    if (input.lookback) {
      this.setMeta("lookback_truncated", input.lookback.truncated ? 1 : 0);
      this.setMetaNullable("lookback_floor_ms", input.lookback.floor_ms);
      this.setMetaNullable("lookback_until_ms", input.lookback.until_ms);
      this.setMeta("lookback_updated_at", now);
    }
    if (input.ok) {
      this.setMeta("last_success_at", now);
      this.setMeta("consecutive_failures", 0);
      this.bump("compaction_success");
      if (input.segments_published) this.bump("segments_published", input.segments_published);
      if (input.bytes_published) this.bump("bytes_published", input.bytes_published);
      if (input.excluded_revoked) this.bump("excluded_revoked", input.excluded_revoked);
    } else {
      this.setMeta("last_failure_at", now);
      this.bump("consecutive_failures", 1);
      this.bump("compaction_failure");
    }
    return { ok: true, ignored: false };
  }

  /**
   * Counts raw journal objects that are still un-compacted inside a half-open
   * receive-time range (optionally scoped to instances).
   *
   * Spec 023: the count comes from the R2 listing of the window range, because
   * there is no per-request ledger. Covered objects are de-duplicated against
   * the manifest's coverage atoms. A ledger fallback is kept only for pure unit
   * tests that inject no bucket.
   */
  async pendingJournalInRange(
    instanceIds: string[],
    fromMS: number,
    toMS: number,
  ): Promise<{
    pending_journal: number;
    /** Last tick left the lookback scan truncated (spec 025, R3). */
    lookback_truncated: boolean;
    /** That truncation overlaps this receive-time range. */
    lookback_in_range: boolean;
    lookback_floor_ms: number | null;
    lookback_until_ms: number | null;
  }> {
    await this.ensureLoaded();
    const risk = this.lookbackRisk(fromMS, toMS);
    if (this.bucket?.list) {
      const listed = await listJournalObjects(this.bucket, {
        instanceIds,
        fromMs: fromMS,
        toMs: toMS,
        maxObjects: 20_000,
      });
      const covered = this.coveredKeySet(listed.objects.map((o) => o.key));
      const excluded = this.excludedKeySet(listed.objects.map((o) => o.key));
      const pending = listed.objects.filter((o) => !covered.has(o.key) && !excluded.has(o.key)).length;
      return { pending_journal: pending, ...risk };
    }
    let q = `SELECT COUNT(*) AS n FROM journal_ledger j
               LEFT JOIN segment_coverage c ON c.journal_key = j.object_key
              WHERE c.journal_key IS NULL AND j.received_at >= ? AND j.received_at < ?`;
    const args: SqlValue[] = [fromMS, toMS];
    if (instanceIds.length > 0) {
      q += ` AND j.instance_id IN (${instanceIds.map(() => "?").join(",")})`;
      for (const id of instanceIds) args.push(id);
    }
    const rows = this.sql.exec(q, ...args).toArray();
    return { pending_journal: Number(rows[0]?.n ?? 0), ...risk };
  }

  /**
   * Whether the last tick's truncated lookback range can still hold objects
   * inside `[fromMS, toMS)`. Without a recorded range we fail safe and treat
   * the whole range as at risk.
   */
  private lookbackRisk(
    fromMS: number,
    toMS: number,
  ): {
    lookback_truncated: boolean;
    lookback_in_range: boolean;
    lookback_floor_ms: number | null;
    lookback_until_ms: number | null;
  } {
    const truncated = this.metaValue("lookback_truncated") === 1;
    const floor = this.metaValue("lookback_floor_ms");
    const until = this.metaValue("lookback_until_ms");
    const inRange =
      truncated && (floor === null || until === null || (toMS > floor && fromMS < until));
    return {
      lookback_truncated: truncated,
      lookback_in_range: inRange,
      lookback_floor_ms: floor,
      lookback_until_ms: until,
    };
  }

  /**
   * Journal occupancy from the R2 listing (spec 023, D3.4). This is the
   * operational and benchmark-verification view: every acknowledged export is
   * exactly one object, so `objects` is the number of durable 200s.
   */
  async journalStats(maxObjects = 20_000): Promise<JournalListingStats> {
    await this.ensureLoaded();
    if (!this.bucket?.list) {
      const objects = Number(this.sql.exec(`SELECT COUNT(*) AS n FROM journal_ledger`).toArray()[0]?.n ?? 0);
      const pendingRow = this.sql
        .exec(
          `SELECT COUNT(*) AS n FROM journal_ledger j
             LEFT JOIN segment_coverage c ON c.journal_key = j.object_key
            WHERE c.journal_key IS NULL`,
        )
        .toArray();
      const bySignal: Record<string, number> = {};
      for (const row of this.sql
        .exec(`SELECT signal, COUNT(*) AS n FROM journal_ledger GROUP BY signal`)
        .toArray()) {
        bySignal[String(row.signal)] = Number(row.n ?? 0);
      }
      return {
        objects,
        bytes: Number(this.sql.exec(`SELECT COALESCE(SUM(size),0) AS n FROM journal_ledger`).toArray()[0]?.n ?? 0),
        pending: Number(pendingRow[0]?.n ?? 0),
        by_signal: bySignal,
        pending_by_signal: bySignal,
        oldest_received_at: null,
        truncated: false,
        scanned: objects,
      };
    }
    const listed = await listAllJournalObjects(this.bucket, Math.max(1, maxObjects));
    const keys = listed.objects.map((o) => o.key);
    const covered = this.coveredKeySet(keys);
    const excluded = this.excludedKeySet(keys);
    const bySignal: Record<string, number> = {};
    const pendingBySignal: Record<string, number> = {};
    let bytes = 0;
    let pending = 0;
    let oldest: number | null = null;
    for (const object of listed.objects) {
      bytes += object.size;
      bySignal[object.signal] = (bySignal[object.signal] ?? 0) + 1;
      if (oldest === null || object.received_at < oldest) oldest = object.received_at;
      if (!covered.has(object.key) && !excluded.has(object.key)) {
        pending += 1;
        pendingBySignal[object.signal] = (pendingBySignal[object.signal] ?? 0) + 1;
      }
    }
    return {
      objects: listed.objects.length,
      bytes,
      pending,
      by_signal: bySignal,
      pending_by_signal: pendingBySignal,
      oldest_received_at: oldest,
      truncated: listed.truncated,
      scanned: listed.scanned,
    };
  }

  /** Which of these journal keys are already attributed to a verified segment. */
  async coverageLookup(keys: string[]): Promise<{ covered: string[]; excluded: string[] }> {
    await this.ensureLoaded();
    const covered: string[] = [];
    const excluded: string[] = [];
    for (const chunk of chunkKeys(keys, 90)) {
      const placeholders = chunk.map(() => "?").join(",");
      for (const row of this.sql
        .exec(
          `SELECT c.journal_key FROM segment_coverage c
             JOIN segment_manifest s ON s.segment_key = c.segment_key
            WHERE s.status = 'verified' AND c.journal_key IN (${placeholders})`,
          ...chunk,
        )
        .toArray()) {
        covered.push(String(row.journal_key));
      }
      for (const row of this.sql
        .exec(`SELECT journal_key FROM excluded_journal WHERE journal_key IN (${placeholders})`, ...chunk)
        .toArray()) {
        excluded.push(String(row.journal_key));
      }
    }
    return { covered, excluded };
  }

  private coveredKeySet(keys: string[]): Set<string> {
    const set = new Set<string>();
    for (const chunk of chunkKeys(keys, 90)) {
      const placeholders = chunk.map(() => "?").join(",");
      for (const row of this.sql
        .exec(
          `SELECT c.journal_key FROM segment_coverage c
             JOIN segment_manifest s ON s.segment_key = c.segment_key
            WHERE s.status = 'verified' AND c.journal_key IN (${placeholders})`,
          ...chunk,
        )
        .toArray()) {
        set.add(String(row.journal_key));
      }
    }
    return set;
  }

  /** Journal keys durably excluded by revocation (never pending/compacted). */
  private excludedKeySet(keys: string[]): Set<string> {
    const set = new Set<string>();
    for (const chunk of chunkKeys(keys, 90)) {
      const placeholders = chunk.map(() => "?").join(",");
      for (const row of this.sql
        .exec(`SELECT journal_key FROM excluded_journal WHERE journal_key IN (${placeholders})`, ...chunk)
        .toArray()) {
        set.add(String(row.journal_key));
      }
    }
    return set;
  }

  // ------------------------------------------------- discovery floor (D3.1)

  /** Persisted discovery cursor; `null` means "scan from the beginning". */
  async discoveryFloor(): Promise<{ floor_ms: number | null }> {
    await this.ensureLoaded();
    return { floor_ms: this.metaValue("discovery_floor_ms") };
  }

  /** Advances the discovery cursor. Never moves it backwards. */
  async setDiscoveryFloor(floorMs: number): Promise<{ floor_ms: number }> {
    await this.ensureLoaded();
    const current = this.metaValue("discovery_floor_ms");
    const next = current === null ? Math.max(0, Math.floor(floorMs)) : Math.max(current, Math.floor(floorMs));
    this.setMeta("discovery_floor_ms", next);
    return { floor_ms: next };
  }

  // ------------------------------------------------------ revoked exclusion

  /**
   * Durably records journal objects excluded from every segment: revoked-token
   * writes (spec 023 D3.2) and late writes whose DELETE failed (spec 025 R1).
   */
  async recordExcludedRevoked(entries: ExcludedJournalInput[]): Promise<{ recorded: number }> {
    await this.ensureLoaded();
    const now = this.now();
    let recorded = 0;
    let revoked = 0;
    let late = 0;
    for (const entry of entries) {
      if (this.recordExcludedRow(entry, now)) {
        recorded += 1;
        if (entry.reason === "late_write") late += 1;
        else revoked += 1;
      }
    }
    if (revoked > 0) this.bump("revoked_excluded_recorded", revoked);
    if (late > 0) this.bump("late_write_rejected", late);
    return { recorded };
  }

  private recordExcludedRow(entry: ExcludedJournalInput, now: number): number {
    const cursor = this.sql.exec(
      `INSERT INTO excluded_journal (journal_key, token_id, instance_id, received_at, size, excluded_at, reason)
       VALUES (?,?,?,?,?,?,?)
       ON CONFLICT (journal_key) DO NOTHING`,
      entry.key,
      entry.token_id,
      entry.instance_id,
      entry.received_at,
      entry.size,
      now,
      entry.reason === "late_write" ? "late_write" : "revoked",
    );
    return cursor.rowsWritten ?? 0;
  }

  /** Excluded journal objects, newest first (audit/diagnostics). */
  async listExcluded(limit: number): Promise<{ entries: ExcludedJournalInput[]; total: number }> {
    await this.ensureLoaded();
    const rows = this.sql
      .exec(
        `SELECT journal_key, token_id, instance_id, received_at, size, reason FROM excluded_journal
          ORDER BY received_at DESC LIMIT ?`,
        Math.max(1, Math.min(limit, 1000)),
      )
      .toArray();
    const total = Number(this.sql.exec(`SELECT COUNT(*) AS n FROM excluded_journal`).toArray()[0]?.n ?? 0);
    return {
      entries: rows.map((row) => ({
        key: String(row.journal_key),
        token_id: String(row.token_id),
        instance_id: String(row.instance_id),
        received_at: Number(row.received_at),
        size: Number(row.size),
        reason: row.reason === "late_write" ? "late_write" : "revoked",
      })),
      total,
    };
  }

  /** Storage/retention/backlog health for the compaction subsystem. */
  async compactionStatus(): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    const now = this.now();
    const listed = await this.journalStats();
    const oldest = listed.oldest_received_at;
    const lease = this.sql
      .exec(`SELECT holder, fencing, expires_at FROM compaction_lease WHERE partition = 'primary'`)
      .toArray();
    return {
      unprocessed_journal: listed.pending,
      oldest_backlog_age_ms: oldest === null ? null : now - oldest,
      completed_segments: this.countSegments(),
      coverage_rows: this.countCoverage(),
      covered_bytes: Number(
        this.sql.exec(`SELECT COALESCE(SUM(size_bytes),0) AS n FROM segment_manifest`).toArray()[0]?.n ?? 0,
      ),
      journal_bytes: listed.bytes,
      journal_objects: listed.objects,
      journal_pending_by_signal: listed.pending_by_signal,
      journal_listing_truncated: listed.truncated,
      excluded_revoked: Number(
        this.sql
          .exec(`SELECT COUNT(*) AS n FROM excluded_journal WHERE reason != 'late_write'`)
          .toArray()[0]?.n ?? 0,
      ),
      excluded_late_write: Number(
        this.sql
          .exec(`SELECT COUNT(*) AS n FROM excluded_journal WHERE reason = 'late_write'`)
          .toArray()[0]?.n ?? 0,
      ),
      // Explicit alias for the spec 025 R1 marker name.
      late_write_rejected: Number(
        this.sql
          .exec(`SELECT COUNT(*) AS n FROM excluded_journal WHERE reason = 'late_write'`)
          .toArray()[0]?.n ?? 0,
      ),
      late_segments: Number(
        this.sql.exec(`SELECT COUNT(*) AS n FROM segment_manifest WHERE late = 1`).toArray()[0]?.n ?? 0,
      ),
      last_success_at: this.metaValue("last_success_at"),
      last_failure_at: this.metaValue("last_failure_at"),
      consecutive_failures: this.metaValue("consecutive_failures") ?? 0,
      // Spec 025 R4: a lease-skipped tick is visible instead of silently stale.
      last_skipped_at: this.metaValue("last_skipped_at"),
      last_skipped_reason: this.metaText("last_skipped_reason"),
      // Spec 025 R3: truncated lookback visibility.
      lookback_truncated: this.metaValue("lookback_truncated") === 1,
      lookback_floor_ms: this.metaValue("lookback_floor_ms"),
      lookback_until_ms: this.metaValue("lookback_until_ms"),
      lookback_updated_at: this.metaValue("lookback_updated_at"),
      discovery_floor_ms: this.metaValue("discovery_floor_ms"),
      lease:
        lease.length === 0
          ? null
          : {
              holder: String(lease[0]!.holder),
              fencing: Number(lease[0]!.fencing),
              expires_at: Number(lease[0]!.expires_at),
              expired: Number(lease[0]!.expires_at) <= now,
            },
      counters: this.counters(),
    };
  }

  /**
   * Lists journal objects eligible for GC.
   *
   * Two classes (spec 023):
   *   * revoked-excluded objects are always eligible (never pinned, no
   *     retention: revoked data must never be analysed);
   *   * covered objects are eligible once a verified segment covers them, the
   *     retention window has elapsed (unless forced) and their segment range
   *     does not overlap an active shot pin.
   */
  async listGCEligible(opts: {
    now: number;
    retention_ms: number;
    /** Audit retention for revoked/excluded objects (spec 024, R5). */
    revoked_retention_ms?: number;
    /** Return only revoked-excluded candidates (retention sweep). */
    revoked_only?: boolean;
    force: boolean;
    limit: number;
  }): Promise<{ candidates: GCandidate[]; protected: number; considered: number }> {
    await this.ensureLoaded();
    const limit = Math.max(1, Math.min(opts.limit, 5000));
    const excludedRetention =
      opts.revoked_retention_ms === undefined ? DEFAULT_REVOKED_RETENTION_MS : Math.max(0, opts.revoked_retention_ms);
    const candidates: GCandidate[] = [];
    let protectedCount = 0;

    // Revoked-excluded objects: eligible only after the audit retention window.
    const excludedRows = this.sql
      .exec(
        `SELECT journal_key, size, received_at, instance_id, excluded_at FROM excluded_journal
          ORDER BY ${opts.revoked_only ? "excluded_at ASC" : "received_at ASC"} LIMIT ?`,
        limit,
      )
      .toArray();
    let excludedConsidered = 0;
    for (const row of excludedRows) {
      excludedConsidered += 1;
      if (!opts.force && Number(row.excluded_at) > opts.now - excludedRetention) continue;
      candidates.push({
        key: String(row.journal_key),
        size: Number(row.size ?? 0),
        received_at: Number(row.received_at),
        instance_id: String(row.instance_id),
        segment_key: "",
      });
    }
    if (opts.revoked_only) {
      return { candidates, protected: 0, considered: excludedConsidered };
    }

    const remaining = Math.max(0, limit - candidates.length);
    const rows = remaining === 0
      ? []
      : this.sql
          .exec(
            `SELECT c.journal_key, j.size, c.received_at, c.instance_id, c.segment_key,
                    s.first_received_at, s.last_received_at
               FROM segment_coverage c
               JOIN segment_manifest s ON s.segment_key = c.segment_key
               LEFT JOIN journal_ledger j ON j.object_key = c.journal_key
              WHERE s.status = 'verified' AND c.deleted_from_r2 = 0
              ORDER BY c.received_at ASC
              LIMIT ?`,
            remaining,
          )
          .toArray();
    const pins = this.sql
      .exec(`SELECT shot_id, instance_id, from_ms, to_ms, expires_at FROM segment_pins`)
      .toArray();
    const cutoff = opts.now - opts.retention_ms;
    for (const row of rows) {
      const segFirst = Number(row.first_received_at);
      const segLast = Number(row.last_received_at);
      const instanceId = String(row.instance_id);
      if (isPinned(pins, instanceId, segFirst, segLast, opts.now)) {
        protectedCount += 1;
        continue;
      }
      const receivedAt = Number(row.received_at);
      if (!opts.force && (opts.retention_ms <= 0 || receivedAt > cutoff)) continue;
      let size = Number(row.size ?? 0);
      if (!Number.isFinite(size) || size <= 0) {
        const head = await this.bucket?.head?.(String(row.journal_key));
        size = head?.size ?? 0;
      }
      candidates.push({
        key: String(row.journal_key),
        size,
        received_at: receivedAt,
        instance_id: instanceId,
        segment_key: String(row.segment_key),
      });
    }
    return { candidates, protected: protectedCount, considered: excludedConsidered + rows.length };
  }

  /**
   * Marks journal objects as deleted from R2. Only objects already attributed
   * to a *verified* segment (or explicitly excluded for revocation) may be
   * marked; the coverage row is retained so compaction never resurrects them.
   */
  async deleteJournalKeys(keys: string[]): Promise<{ deleted: number }> {
    await this.ensureLoaded();
    let deleted = 0;
    for (const key of keys) {
      const covered = this.sql
        .exec(
          `SELECT 1 AS one FROM segment_coverage c
             JOIN segment_manifest s ON s.segment_key = c.segment_key
            WHERE c.journal_key = ? AND s.status = 'verified'`,
          key,
        )
        .toArray();
      const excluded = this.sql
        .exec(`SELECT 1 AS one FROM excluded_journal WHERE journal_key = ?`, key)
        .toArray();
      if (covered.length === 0 && excluded.length === 0) continue;
      this.sql.exec(`UPDATE segment_coverage SET deleted_from_r2 = 1 WHERE journal_key = ?`, key);
      this.sql.exec(`DELETE FROM excluded_journal WHERE journal_key = ?`, key);
      this.sql.exec(`DELETE FROM journal_ledger WHERE object_key = ?`, key);
      deleted += 1;
    }
    if (deleted > 0) this.bump("gc_deleted", deleted);
    return { deleted };
  }

  /**
   * Protects a receive-time range for a shot from GC. A shot holds at most one
   * pin (the range is replaced on update) and the pin may carry a TTL so it can
   * never accumulate forever.
   */
  async addPin(
    shotId: string,
    instanceId: string | null,
    fromMS: number,
    toMS: number,
    ttlMs?: number,
  ): Promise<{ pinned: boolean }> {
    await this.ensureLoaded();
    const now = this.now();
    const expiresAt = typeof ttlMs === "number" && ttlMs > 0 ? now + ttlMs : null;
    this.sql.exec(`DELETE FROM segment_pins WHERE shot_id = ?`, shotId);
    this.sql.exec(
      `INSERT INTO segment_pins (shot_id, instance_id, from_ms, to_ms, pinned_at, expires_at) VALUES (?,?,?,?,?,?)`,
      shotId,
      instanceId,
      fromMS,
      toMS,
      now,
      expiresAt,
    );
    this.bump("pins_added");
    return { pinned: true };
  }

  /** Releases every pin held by a shot (idempotent). */
  async removePins(shotId: string): Promise<{ removed: number }> {
    await this.ensureLoaded();
    const cursor = this.sql.exec(`DELETE FROM segment_pins WHERE shot_id = ?`, shotId);
    const removed = cursor.rowsWritten ?? 0;
    if (removed > 0) this.bump("pins_removed", removed);
    return { removed };
  }

  async listPins(): Promise<{ pins: CompactionPin[] }> {
    await this.ensureLoaded();
    const pins = this.sql
      .exec(
        `SELECT shot_id, instance_id, from_ms, to_ms, pinned_at, expires_at FROM segment_pins ORDER BY pinned_at ASC`,
      )
      .toArray()
      .map((row) => ({
        shot_id: String(row.shot_id),
        instance_id: row.instance_id == null ? null : String(row.instance_id),
        from_ms: Number(row.from_ms),
        to_ms: Number(row.to_ms),
        pinned_at: Number(row.pinned_at),
        expires_at: row.expires_at == null ? null : Number(row.expires_at),
      }));
    return { pins };
  }

  /** Storage/health summary for the compaction subsystem. */
  async segmentStatus(): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    return {
      segments: this.countSegments(),
      coverage: this.countCoverage(),
      unverified: Number(
        this.sql.exec(`SELECT COUNT(*) AS n FROM segment_manifest WHERE status != 'verified'`).toArray()[0]?.n ?? 0,
      ),
      late: Number(
        this.sql.exec(`SELECT COUNT(*) AS n FROM segment_manifest WHERE late = 1`).toArray()[0]?.n ?? 0,
      ),
      counters: this.counters(),
    };
  }

  async status(): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    const instances = await this.instances.list();
    return {
      recording: this.countRecording(),
      shots: this.countShots(),
      journal: this.countJournal(),
      segments: this.countSegments(),
      coverage: this.countCoverage(),
      instances: instances.instances.length,
      counters: this.counters(),
    };
  }

  // ------------------------------------------------------------- internal

  async handleInternal(request: Request): Promise<Response> {
    await this.ensureLoaded();
    try {
      const path = new URL(request.url).pathname;
      if (path.startsWith("/internal/instance/")) {
        return await this.instances.handleInternal(request);
      }
      if (path === "/internal/journal/commit") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const entry = await readJson<Partial<JournalEntry>>(request);
        const validated = validateJournalEntry(entry);
        if (!validated.ok) return json(400, { error: validated.error, code: "bad_request" });
        return json(200, await this.commitJournal(validated.entry));
      }
      if (path === "/internal/journal/reconcile") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ entries?: Partial<JournalEntry>[] }>(request);
        const entries: JournalEntry[] = [];
        for (const raw of body.entries ?? []) {
          const validated = validateJournalEntry(raw);
          if (!validated.ok) return json(400, { error: validated.error, code: "bad_request" });
          entries.push(validated.entry);
        }
        return json(200, await this.reconcileJournal(entries));
      }
      if (path === "/internal/journal/list") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ instance_id?: string; limit?: number; offset?: number }>(request);
        const limit = typeof body.limit === "number" && body.limit > 0 ? Math.min(body.limit, 200) : 50;
        const offset = typeof body.offset === "number" && body.offset > 0 ? body.offset : 0;
        return json(200, await this.listJournal({ instance_id: body.instance_id, limit, offset }));
      }
      if (path === "/internal/segment/publish") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<SegmentPublishInput>(request);
        return json(201, await this.publishSegment(body));
      }
      if (path === "/internal/segment/list") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ instance_id?: string; limit?: number; offset?: number }>(request);
        const limit = typeof body.limit === "number" && body.limit > 0 ? Math.min(body.limit, 200) : 50;
        const offset = typeof body.offset === "number" && body.offset > 0 ? body.offset : 0;
        return json(200, await this.listSegments({ instance_id: body.instance_id, limit, offset }));
      }
      if (path === "/internal/segment/status") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.segmentStatus());
      }
      if (path === "/internal/compaction/plan") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<Partial<CompactionPlanOptions>>(request);
        return json(200, await this.planCompaction(normalizePlanOptions(body)));
      }
      if (path === "/internal/compaction/lease") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ partition?: string; holder?: string; ttl_ms?: number }>(request);
        if (typeof body.holder !== "string" || body.holder === "") {
          return json(400, { error: "holder is required", code: "bad_request" });
        }
        const partition = typeof body.partition === "string" && body.partition !== "" ? body.partition : "primary";
        const ttl = typeof body.ttl_ms === "number" && body.ttl_ms > 0 ? Math.min(body.ttl_ms, 600_000) : 60_000;
        return json(200, await this.acquireLease(partition, body.holder, ttl));
      }
      if (path === "/internal/compaction/release") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ partition?: string; holder?: string; fencing?: number }>(request);
        if (typeof body.holder !== "string" || typeof body.fencing !== "number") {
          return json(400, { error: "holder and fencing are required", code: "bad_request" });
        }
        const partition = typeof body.partition === "string" && body.partition !== "" ? body.partition : "primary";
        return json(200, await this.releaseLease(partition, body.holder, body.fencing));
      }
      if (path === "/internal/compaction/result") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<Partial<CompactionResultInput>>(request);
        if (typeof body.holder !== "string" || typeof body.fencing !== "number") {
          return json(400, { error: "holder and fencing are required", code: "bad_request" });
        }
        return json(
          200,
          await this.recordCompaction({
            holder: body.holder,
            fencing: body.fencing,
            ok: body.ok === true,
            segments_published: numberOrUndefined(body.segments_published),
            bytes_published: numberOrUndefined(body.bytes_published),
            excluded_revoked: numberOrUndefined(body.excluded_revoked),
            error: typeof body.error === "string" ? body.error : null,
            skipped: typeof body.skipped === "string" && body.skipped !== "" ? body.skipped : null,
            lookback: normalizeLookback(body.lookback),
          }),
        );
      }
      if (path === "/internal/compaction/status") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.compactionStatus());
      }
      if (path === "/internal/compaction/pending") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ instance_ids?: string[]; from_ms?: number; to_ms?: number }>(request);
        const instanceIds = Array.isArray(body.instance_ids)
          ? body.instance_ids.filter((v): v is string => typeof v === "string")
          : [];
        const fromMS = typeof body.from_ms === "number" && Number.isFinite(body.from_ms) ? body.from_ms : 0;
        const toMS = typeof body.to_ms === "number" && Number.isFinite(body.to_ms) ? body.to_ms : Number.MAX_SAFE_INTEGER;
        return json(200, await this.pendingJournalInRange(instanceIds, fromMS, toMS));
      }
      if (path === "/internal/compaction/journal") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.journalStats());
      }
      if (path === "/internal/compaction/coverage") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ keys?: string[] }>(request);
        const keys = Array.isArray(body.keys) ? body.keys.filter((k): k is string => typeof k === "string") : [];
        return json(200, await this.coverageLookup(keys));
      }
      if (path === "/internal/compaction/excluded") {
        if (request.method === "POST") {
          const body = await readJson<{ entries?: ExcludedJournalInput[] }>(request);
          const entries: ExcludedJournalInput[] = [];
          for (const raw of body.entries ?? []) {
            if (
              !raw ||
              typeof raw.key !== "string" ||
              raw.key === "" ||
              typeof raw.token_id !== "string" ||
              typeof raw.instance_id !== "string" ||
              typeof raw.received_at !== "number" ||
              typeof raw.size !== "number"
            ) {
              return json(400, { error: "invalid excluded entry", code: "bad_request" });
            }
            entries.push({
              key: raw.key,
              token_id: raw.token_id,
              instance_id: raw.instance_id,
              received_at: raw.received_at,
              size: raw.size,
              reason: raw.reason === "late_write" ? "late_write" : "revoked",
            });
          }
          return json(200, await this.recordExcludedRevoked(entries));
        }
        if (request.method === "GET") {
          return json(200, await this.listExcluded(500));
        }
        return json(405, { error: "method not allowed" });
      }
      if (path === "/internal/compaction/discovery") {
        if (request.method === "GET") return json(200, await this.discoveryFloor());
        if (request.method === "POST") {
          const body = await readJson<{ floor_ms?: number }>(request);
          if (typeof body.floor_ms !== "number" || !Number.isFinite(body.floor_ms)) {
            return json(400, { error: "floor_ms is required", code: "bad_request" });
          }
          return json(200, await this.setDiscoveryFloor(body.floor_ms));
        }
        return json(405, { error: "method not allowed" });
      }
      if (path === "/internal/compaction/gc/list") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{
          retention_ms?: number;
          revoked_retention_ms?: number;
          revoked_only?: boolean;
          force?: boolean;
          limit?: number;
        }>(request);
        const retention = typeof body.retention_ms === "number" ? body.retention_ms : 24 * 60 * 60 * 1000;
        const limit = typeof body.limit === "number" && body.limit > 0 ? body.limit : 500;
        return json(
          200,
          await this.listGCEligible({
            now: this.now(),
            retention_ms: retention,
            revoked_retention_ms:
              typeof body.revoked_retention_ms === "number" ? body.revoked_retention_ms : undefined,
            revoked_only: body.revoked_only === true,
            force: body.force === true,
            limit,
          }),
        );
      }
      if (path === "/internal/compaction/gc/delete") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ keys?: string[] }>(request);
        const keys = Array.isArray(body.keys) ? body.keys.filter((k): k is string => typeof k === "string") : [];
        return json(200, await this.deleteJournalKeys(keys));
      }
      if (path === "/internal/compaction/pin") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{
          shot_id?: string;
          instance_id?: string | null;
          from_ms?: number;
          to_ms?: number;
          ttl_ms?: number;
        }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "" || typeof body.from_ms !== "number" || typeof body.to_ms !== "number") {
          return json(400, { error: "shot_id, from_ms and to_ms are required", code: "bad_request" });
        }
        const instanceId = typeof body.instance_id === "string" && body.instance_id !== "" ? body.instance_id : null;
        const ttl = typeof body.ttl_ms === "number" && body.ttl_ms > 0 ? body.ttl_ms : undefined;
        return json(200, await this.addPin(body.shot_id, instanceId, body.from_ms, body.to_ms, ttl));
      }
      if (path === "/internal/compaction/unpin") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.removePins(body.shot_id));
      }
      if (path === "/internal/compaction/pins") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.listPins());
      }
      if (path === "/internal/shot/register") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; owner?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        const owner = typeof body.owner === "string" && body.owner !== "" ? body.owner : "legacy";
        return json(200, await this.registerShot(body.shot_id, owner));
      }
      if (path === "/internal/shot/release") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; shot_ids?: string[] }>(request);
        const ids = body.shot_ids ?? (body.shot_id ? [body.shot_id] : []);
        if (ids.length === 0) return json(400, { error: "shot_id is required", code: "bad_request" });
        return json(200, await this.releaseShots(ids));
      }
      if (path === "/internal/shot/state") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{
          shot_id?: string;
          state?: string;
          stopped_at?: number;
          size_bytes?: number;
          sha256?: string;
        }>(request);
        if (typeof body.shot_id !== "string" || typeof body.state !== "string" || body.state === "") {
          return json(400, { error: "shot_id and state are required", code: "bad_request" });
        }
        return json(
          200,
          await this.setShotState(body.shot_id, body.state, {
            stopped_at: body.stopped_at,
            size_bytes: body.size_bytes,
            sha256: body.sha256,
          }),
        );
      }
      if (path === "/internal/shot/window/create") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<CreateWindowShotInput>(request);
        return json(201, await this.createWindowShot(body));
      }
      if (path === "/internal/shot/window/start") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<StartWindowShotInput>(request);
        return json(201, await this.startWindowShot(body));
      }
      if (path === "/internal/shot/window/stop") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; ended_at_ms?: number }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.stopWindowShot(body.shot_id, body.ended_at_ms));
      }
      if (path === "/internal/shot/window/coverage") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; coverage?: WindowCoverage }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.setWindowCoverage(body.shot_id, body.coverage ?? ({} as WindowCoverage)));
      }
      if (path === "/internal/shot/materialize-plan") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.materializationPlan(body.shot_id));
      }
      if (path === "/internal/shot/materialization") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; materialization?: MaterializationRecord }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "" || !body.materialization) {
          return json(400, { error: "shot_id and materialization are required", code: "bad_request" });
        }
        return json(200, await this.setMaterialization(body.shot_id, body.materialization));
      }
      if (path === "/internal/materialization/begin") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; ttl_ms?: number }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.beginMaterializationRun(body.shot_id, body.ttl_ms ?? 0));
      }
      if (path === "/internal/materialization/end") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.endMaterializationRun(body.shot_id));
      }
      if (path === "/internal/compaction/yield") {
        if (request.method !== "GET" && request.method !== "POST") {
          return json(405, { error: "method not allowed" });
        }
        return json(200, await this.materializationYield());
      }
      if (path === "/internal/shot/get") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        const row = this.getShotRecord(body.shot_id);
        if (!row) return json(404, { error: "unknown shot", code: "not_found" });
        return json(200, row);
      }
      if (path === "/internal/shot/select-segments") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ window?: ShotWindow; instance_id?: string; limit?: number }>(request);
        if (!body.window) return json(400, { error: "window is required", code: "bad_request" });
        const catalog = await this.listSegments({ instance_id: body.instance_id, limit: 1000, offset: 0 });
        const metas: WindowSegmentMeta[] = catalog.segments.map((s) => ({
          segment_key: s.segment_key,
          instance_id: s.instance_id,
          instance: "",
          first_received_at: s.first_received_at,
          last_received_at: s.last_received_at,
          event_min_ns: s.event_min_ns,
          event_max_ns: s.event_max_ns,
          status: s.status,
        }));
        const selection: SegmentSelection = selectSegments(body.window, metas);
        return json(200, selection);
      }
      if (path === "/internal/shots/recording") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.listRecording());
      }
      if (path === "/internal/shots/list") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ owner?: string; limit?: number; offset?: number }>(request);
        const limit = typeof body.limit === "number" && body.limit > 0 ? body.limit : 50;
        const offset = typeof body.offset === "number" && body.offset > 0 ? body.offset : 0;
        return json(200, await this.listShots({ owner: body.owner, limit, offset }));
      }
      if (path === "/internal/status") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.status());
      }
      return json(404, { error: "not found" });
    } catch (error) {
      return errorResponse(error);
    }
  }

  // ---------------------------------------------------------------- helpers

  private countRecording(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM shot_registry WHERE state = 'recording'`).toArray();
    return Number(rows[0]?.n ?? 0);
  }

  private countShots(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM shot_registry`).toArray();
    return Number(rows[0]?.n ?? 0);
  }

  private countJournal(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM journal_ledger`).toArray();
    return Number(rows[0]?.n ?? 0);
  }

  private countSegments(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM segment_manifest`).toArray();
    return Number(rows[0]?.n ?? 0);
  }

  private countCoverage(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM segment_coverage`).toArray();
    return Number(rows[0]?.n ?? 0);
  }

  private setMeta(name: string, value: number): void {
    this.sql.exec(
      `INSERT INTO compaction_meta (name, value) VALUES (?, ?)
       ON CONFLICT (name) DO UPDATE SET value = excluded.value`,
      name,
      value,
    );
  }

  private metaValue(name: string): number | null {
    const rows = this.sql.exec(`SELECT value FROM compaction_meta WHERE name = ?`, name).toArray();
    return rows.length === 0 ? null : Number(rows[0]!.value);
  }

  private setMetaNullable(name: string, value: number | null): void {
    if (value === null) {
      this.sql.exec(`DELETE FROM compaction_meta WHERE name = ?`, name);
      return;
    }
    this.setMeta(name, value);
  }

  private setMetaText(name: string, value: string): void {
    this.sql.exec(
      `INSERT INTO compaction_meta_text (name, value) VALUES (?, ?)
       ON CONFLICT (name) DO UPDATE SET value = excluded.value`,
      name,
      value,
    );
  }

  private metaText(name: string): string | null {
    const rows = this.sql.exec(`SELECT value FROM compaction_meta_text WHERE name = ?`, name).toArray();
    return rows.length === 0 ? null : String(rows[0]!.value);
  }

  private bump(name: string, delta = 1): void {
    this.sql.exec(
      `INSERT INTO ingest_counters (name, value) VALUES (?, ?)
       ON CONFLICT (name) DO UPDATE SET value = value + excluded.value`,
      name,
      delta as SqlValue,
    );
  }

  private counters(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const row of this.sql.exec(`SELECT name, value FROM ingest_counters`).toArray()) {
      out[String(row.name)] = Number(row.value ?? 0);
    }
    return out;
  }
}

function registeredRow(row: SqlRow): WindowShotRecord {
  const window = windowFromJSON(row.window_json == null ? null : String(row.window_json));
  let coverage: WindowCoverage | null = null;
  if (row.coverage_json != null && String(row.coverage_json) !== "") {
    try {
      coverage = JSON.parse(String(row.coverage_json)) as WindowCoverage;
    } catch {
      coverage = null;
    }
  }
  let materialization: MaterializationRecord | null = null;
  if (row.materialization_json != null && String(row.materialization_json) !== "") {
    try {
      materialization = JSON.parse(String(row.materialization_json)) as MaterializationRecord;
    } catch {
      materialization = null;
    }
  }
  return {
    shot_id: String(row.shot_id),
    owner: row.owner == null ? "legacy" : String(row.owner),
    created_at: Number(row.created_at),
    state: String(row.state ?? "recording"),
    stopped_at: row.stopped_at == null ? null : Number(row.stopped_at),
    size_bytes: row.size_bytes == null ? null : Number(row.size_bytes),
    sha256: row.sha256 == null ? null : String(row.sha256),
    kind: row.kind === "window" || window !== null ? "window" : "recording",
    name: row.name == null ? null : String(row.name),
    window,
    query_watermark_ms: row.query_watermark_ms == null ? null : Number(row.query_watermark_ms),
    coverage,
    materialization,
  };
}

/**
 * Normalizes a coverage summary; a missing list is never silently complete.
 *
 * Spec 032 adds the materialization gate: when a materialization record is
 * supplied (or the input already carries a state), `complete` is forced false
 * until the run is `complete`, and the state/parts/error are exposed.
 */
export function normalizeCoverage(
  input: WindowCoverage | undefined,
  instances: string[],
  materialization?: MaterializationRecord | null,
): WindowCoverage {
  const missing = Array.isArray(input?.missing_segments) ? input!.missing_segments.map(String) : [];
  const pending = numOr(input?.pending_journal, 0);
  // Spec 025 R3: a window that overlaps a truncated lookback is never complete,
  // even when nothing is pending yet, because undiscovered objects may remain.
  const lookbackTruncated = input?.lookback_truncated === true;
  const floor =
    typeof input?.lookback_floor_ms === "number" && Number.isFinite(input.lookback_floor_ms)
      ? Math.floor(input.lookback_floor_ms)
      : null;
  const materializationState =
    materialization !== undefined
      ? (materialization?.state ?? null)
      : (input?.materialization_state ?? null);
  const materializationComplete = materializationState === null || materializationState === "complete";
  const coverage: WindowCoverage = {
    expected_segments: numOr(input?.expected_segments, 0),
    available_segments: numOr(input?.available_segments, 0),
    missing_segments: missing,
    instances: normalizeInstances(instances.length > 0 ? instances : input?.instances),
    late_arrivals: numOr(input?.late_arrivals, 0),
    log_observed_fallbacks: numOr(input?.log_observed_fallbacks, 0),
    pending_journal: pending,
    lookback_truncated: lookbackTruncated,
    lookback_floor_ms: floor,
    materialization_state: materializationState,
    materialization_parts_done: numOr(
      materialization !== undefined ? materialization?.parts_done : input?.materialization_parts_done,
      0,
    ),
    materialization_parts_total: numOr(
      materialization !== undefined ? materialization?.parts_total : input?.materialization_parts_total,
      0,
    ),
    materialization_error:
      materialization !== undefined ? (materialization?.error ?? null) : (input?.materialization_error ?? null),
    complete:
      input?.complete === true &&
      missing.length === 0 &&
      pending === 0 &&
      !lookbackTruncated &&
      materializationComplete,
  };
  return coverage;
}

/** Initial `pending` materialization record (spec 032). */
function pendingMaterialization(): MaterializationRecord {
  return {
    state: "pending",
    parts_done: 0,
    parts_total: 0,
    spans: 0,
    started_at: null,
    finished_at: null,
    parts: [],
    error: null,
    part_states: [],
    lease_expires_at: null,
    heartbeat_at: null,
    attempts: 0,
  };
}

/** Empty, explicitly incomplete coverage for a window with no analysis yet. */
function emptyCoverage(instances: string[]): WindowCoverage {
  return {
    expected_segments: 0,
    available_segments: 0,
    missing_segments: [],
    instances,
    complete: false,
  };
}

function normalizePartStates(raw: unknown): MaterializationPartState[] {
  if (!Array.isArray(raw)) return [];
  const out: MaterializationPartState[] = [];
  for (const item of raw) {
    if (!item || typeof item !== "object") continue;
    const value = item as Record<string, unknown>;
    const state = String(value.state ?? "pending");
    if (!["pending", "running", "complete", "failed"].includes(state)) continue;
    out.push({
      index: numOr(value.index, out.length),
      state: state as MaterializationPartState["state"],
      attempts: numOr(value.attempts, 0),
      lease_owner: typeof value.lease_owner === "string" ? value.lease_owner : null,
      lease_expires_at:
        typeof value.lease_expires_at === "number" && Number.isFinite(value.lease_expires_at)
          ? value.lease_expires_at
          : null,
      started_at:
        typeof value.started_at === "number" && Number.isFinite(value.started_at) ? value.started_at : null,
      finished_at:
        typeof value.finished_at === "number" && Number.isFinite(value.finished_at) ? value.finished_at : null,
      error: typeof value.error === "string" ? value.error : null,
    });
  }
  return out.sort((a, b) => a.index - b.index);
}

/** Validates the lookback health carried by a compaction result. */
function normalizeLookback(
  input: CompactionResultInput["lookback"] | undefined,
): CompactionResultInput["lookback"] {
  if (!input || typeof input !== "object") return null;
  const floor =
    typeof input.floor_ms === "number" && Number.isFinite(input.floor_ms) ? Math.floor(input.floor_ms) : null;
  const until =
    typeof input.until_ms === "number" && Number.isFinite(input.until_ms) ? Math.floor(input.until_ms) : null;
  return { truncated: input.truncated === true, floor_ms: floor, until_ms: until };
}

function numOr(v: unknown, fallback: number): number {
  return typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : fallback;
}

function numberOrUndefined(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

/** Splits key arrays so a SQL `IN (...)` never exceeds the variable limit. */
function chunkKeys(keys: string[], size: number): string[][] {
  const chunks: string[][] = [];
  for (let i = 0; i < keys.length; i += size) chunks.push(keys.slice(i, i + size));
  return chunks;
}

/** Bounds a compaction plan request so the coordinator can never over-ask. */
function normalizePlanOptions(raw: Partial<CompactionPlanOptions> | undefined): CompactionPlanOptions {
  const bounded = (v: unknown, def: number, max: number): number =>
    typeof v === "number" && Number.isFinite(v) && v > 0 ? Math.min(v, max) : def;
  const grace =
    typeof raw?.grace_ms === "number" && Number.isFinite(raw.grace_ms) && raw.grace_ms >= 0
      ? Math.min(raw.grace_ms, 24 * 60 * 60 * 1000)
      : 90_000;
  return {
    grace_ms: grace,
    window_ms: bounded(raw?.window_ms, 60_000, 24 * 60 * 60 * 1000),
    max_objects: bounded(raw?.max_objects, 2048, 100_000),
    max_bytes: bounded(raw?.max_bytes, 32 * 1024 * 1024, 512 * 1024 * 1024),
    max_batches: bounded(raw?.max_batches, 4, 1000),
    max_scan: bounded(raw?.max_scan, 20_000, 1_000_000),
  };
}

function journalRow(row: SqlRow): JournalEntry {
  return {
    key: String(row.object_key),
    instance_id: String(row.instance_id),
    instance: String(row.instance),
    signal: String(row.signal),
    size: Number(row.size),
    sha256: String(row.sha256),
    content_type: String(row.content_type ?? ""),
    content_encoding: String(row.content_encoding ?? ""),
    received_at: Number(row.received_at),
    committed_at: Number(row.committed_at),
  };
}

type JournalValidation = { ok: true; entry: JournalEntry } | { ok: false; error: string };

export function validateJournalEntry(raw: Partial<JournalEntry> | undefined): JournalValidation {
  if (!raw || typeof raw.key !== "string" || raw.key === "") return { ok: false, error: "key is required" };
  if (typeof raw.instance_id !== "string" || raw.instance_id === "") {
    return { ok: false, error: "instance_id is required" };
  }
  if (typeof raw.signal !== "string" || !SIGNALS.includes(raw.signal as Signal)) {
    return { ok: false, error: "signal is required" };
  }
  if (typeof raw.size !== "number" || !Number.isFinite(raw.size) || raw.size < 0) {
    return { ok: false, error: "size must be a non-negative number" };
  }
  if (typeof raw.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(raw.sha256)) {
    return { ok: false, error: "sha256 must be lowercase hex" };
  }
  if (typeof raw.received_at !== "number" || !Number.isFinite(raw.received_at)) {
    return { ok: false, error: "received_at is required" };
  }
  return {
    ok: true,
    entry: {
      key: raw.key,
      instance_id: raw.instance_id,
      instance: typeof raw.instance === "string" ? raw.instance : "",
      signal: raw.signal,
      size: raw.size,
      sha256: raw.sha256,
      content_type: typeof raw.content_type === "string" ? raw.content_type : "",
      content_encoding: typeof raw.content_encoding === "string" ? raw.content_encoding : "",
      received_at: raw.received_at,
    },
  };
}

type SegmentValidation = { ok: true; value: SegmentPublishInput } | { ok: false; error: string };

const SEGMENT_KEY_PATTERN = /^[A-Za-z0-9._/-]{1,512}$/;
const MAX_SEGMENT_COVERAGE = 10000;

/** Validates a compactor publish request. Only verified segments may be advertised. */
export function validateSegmentPublish(raw: Partial<SegmentPublishInput> | undefined): SegmentValidation {
  if (!raw || typeof raw.segment_key !== "string" || !SEGMENT_KEY_PATTERN.test(raw.segment_key)) {
    return { ok: false, error: "segment_key is required and must be path-safe" };
  }
  if (typeof raw.instance_id !== "string" || raw.instance_id === "") {
    return { ok: false, error: "instance_id is required" };
  }
  if (typeof raw.object_key !== "string" || raw.object_key === "") {
    return { ok: false, error: "object_key is required" };
  }
  if (typeof raw.size_bytes !== "number" || !Number.isFinite(raw.size_bytes) || raw.size_bytes <= 0) {
    return { ok: false, error: "size_bytes must be a positive number" };
  }
  if (typeof raw.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(raw.sha256)) {
    return { ok: false, error: "sha256 must be lowercase hex" };
  }
  for (const field of ["first_received_at", "last_received_at", "request_count", "record_count"] as const) {
    const v = raw[field];
    if (typeof v !== "number" || !Number.isFinite(v) || v < 0) {
      return { ok: false, error: `${field} must be a non-negative number` };
    }
  }
  if ((raw.last_received_at as number) < (raw.first_received_at as number)) {
    return { ok: false, error: "last_received_at must not precede first_received_at" };
  }
  if (!Array.isArray(raw.coverage)) {
    return { ok: false, error: "coverage is required" };
  }
  if (raw.coverage.length > MAX_SEGMENT_COVERAGE) {
    return { ok: false, error: `coverage exceeds ${MAX_SEGMENT_COVERAGE} entries` };
  }
  const coverage: SegmentCoverageInput[] = [];
  for (const entry of raw.coverage) {
    if (!entry || typeof entry.journal_key !== "string" || entry.journal_key === "") {
      return { ok: false, error: "coverage.journal_key is required" };
    }
    if (typeof entry.signal !== "string" || !SIGNALS.includes(entry.signal as Signal)) {
      return { ok: false, error: "coverage.signal is required" };
    }
    if (typeof entry.received_at !== "number" || !Number.isFinite(entry.received_at)) {
      return { ok: false, error: "coverage.received_at is required" };
    }
    coverage.push({
      journal_key: entry.journal_key,
      signal: entry.signal,
      received_at: entry.received_at,
      instance_id:
        typeof entry.instance_id === "string" && entry.instance_id !== "" ? entry.instance_id : undefined,
    });
  }
  const num = (v: unknown): number => (typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : 0);
  const nullable = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
  return {
    ok: true,
    value: {
      segment_key: raw.segment_key,
      instance_id: raw.instance_id,
      object_key: raw.object_key,
      size_bytes: raw.size_bytes,
      sha256: raw.sha256,
      first_received_at: raw.first_received_at as number,
      last_received_at: raw.last_received_at as number,
      request_count: raw.request_count as number,
      record_count: raw.record_count as number,
      span_count: num(raw.span_count),
      log_count: num(raw.log_count),
      metric_count: num(raw.metric_count),
      event_min_ns: nullable(raw.event_min_ns),
      event_max_ns: nullable(raw.event_max_ns),
      late: raw.late === true,
      coverage,
    },
  };
}

/**
 * Re-verifies a segment object directly against R2 before it is advertised.
 * Requires a size match and a stored checksum match; a missing object, a size
 * mismatch or a checksum mismatch is a hard rejection (spec 018).
 */
async function verifyStoredSegment(
  bucket: R2BucketLike,
  seg: SegmentPublishInput,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!bucket.head) return { ok: true };
  const head = await bucket.head(seg.object_key);
  if (!head) return { ok: false, error: `segment object ${seg.object_key} is not present in R2` };
  if (head.size !== seg.size_bytes) {
    return { ok: false, error: `stored segment ${seg.object_key} size mismatch` };
  }
  const stored = (head.sha256 ?? head.customMetadata?.["sha256"] ?? "").toLowerCase();
  if (!stored) {
    return { ok: false, error: `stored segment ${seg.object_key} has no verifiable checksum` };
  }
  if (stored !== seg.sha256.toLowerCase()) {
    return { ok: false, error: `stored segment ${seg.object_key} checksum mismatch` };
  }
  return { ok: true };
}

/** True when a live pin's receive-time range overlaps a segment's range. */
function isPinned(pins: SqlRow[], instanceId: string, segFirst: number, segLast: number, now: number): boolean {
  for (const pin of pins) {
    const expiresAt = pin.expires_at == null ? null : Number(pin.expires_at);
    if (expiresAt !== null && expiresAt <= now) continue;
    const pinInstance = pin.instance_id == null ? null : String(pin.instance_id);
    if (pinInstance !== null && pinInstance !== instanceId) continue;
    if (Number(pin.to_ms) < segFirst || Number(pin.from_ms) > segLast) continue;
    return true;
  }
  return false;
}

function segmentRow(row: SqlRow): SegmentManifestRow {
  return {
    segment_key: String(row.segment_key),
    instance_id: String(row.instance_id),
    object_key: String(row.object_key),
    size_bytes: Number(row.size_bytes),
    sha256: String(row.sha256),
    first_received_at: Number(row.first_received_at),
    last_received_at: Number(row.last_received_at),
    request_count: Number(row.request_count),
    record_count: Number(row.record_count),
    span_count: Number(row.span_count ?? 0),
    log_count: Number(row.log_count ?? 0),
    metric_count: Number(row.metric_count ?? 0),
    event_min_ns: row.event_min_ns == null ? null : Number(row.event_min_ns),
    event_max_ns: row.event_max_ns == null ? null : Number(row.event_max_ns),
    status: String(row.status),
    late: Number(row.late ?? 0) === 1,
    created_at: Number(row.created_at),
    verified_at: row.verified_at == null ? null : Number(row.verified_at),
  };
}

// Re-export the instance registry primitives so callers can build a standalone
// registry over the same storage (used by tests).
export {
  INSTANCE_TOKEN_TTL_SECONDS,
  INSTANCE_TOKEN_TTL_MS,
  InstanceCore,
  isValidInstanceName,
  MAX_INSTANCE_ROTATION_OVERLAP_SECONDS,
  newInstanceId,
  newInstanceToken,
  normalizeInstanceName,
} from "./instance-core";
export type {
  InstanceAdmitResult,
  InstanceMetadata,
  InstanceTokenIssue,
  InstanceTokenMetadata,
} from "./instance-core";
export type { InstanceCoreDeps };
