// Ingestion coordinator core (spec 014).
//
// This core is deliberately free of Cloudflare-specific globals so the exact
// same logic runs in production (over `IngestDO` SQLite storage) and in unit
// tests (over `bun:sqlite`). It owns:
//
//   * the named-instance registry and push-only instance credentials, delegated
//     to {@link InstanceCore} over the same SQLite storage (spec 014);
//   * the durable raw-journal ledger: one row per *acknowledged* OTLP export,
//     written after the raw bytes are durable in R2 and before the OTLP 200;
//   * the shot registry: which shots exist and their attribution-only `owner`,
//     so `GET /api/shots` can list shots. The former
//     `MAX_CONCURRENT_RECORDING` cap is gone (spec 014): shots may record
//     concurrently, bounded only by explicit backend capacity controls.
//
// Design boundaries:
//   * An instance token authorizes *only* POST /ingest/v1/{traces,metrics,logs}
//     and resolves to an instance, never to a shot or a segment.
//   * Expiry is evaluated per arriving request against an injectable clock and
//     is never extended by shot creation, stop, queries or telemetry.
//   * A frozen shot's artifact outlives its (former) credentials by design.

import { badRequest } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { InstanceCore, newInstanceToken } from "./instance-core";
import type { InstanceCoreDeps } from "./instance-core";
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

/** Name of the singleton coordinator Durable Object. */
export const INGEST_DO_NAME = "ingest-primary";

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

/** One registered logical-window shot (spec 016). */
export interface WindowShotRecord extends RegisteredShot {
  kind: "window" | "recording";
  name: string | null;
  window: ShotWindow | null;
  query_watermark_ms: number | null;
  coverage: WindowCoverage | null;
}

/** Completeness of a window shot. Missing data is always disclosed. */
export interface WindowCoverage {
  expected_segments: number;
  available_segments: number;
  missing_segments: string[];
  instances: string[];
  late_arrivals?: number;
  log_observed_fallbacks?: number;
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

/** One journal object attributed to a segment by the compactor (spec 015). */
export interface SegmentCoverageInput {
  journal_key: string;
  signal: string;
  received_at: number;
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
     coverage_json    TEXT
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
     covered_at  INTEGER NOT NULL
   )`,
  `CREATE INDEX IF NOT EXISTS segment_coverage_segment ON segment_coverage (segment_key)`,
];

export class IngestCore {
  private readonly sql: SyncSql;
  private readonly now: () => number;
  private readonly instances: InstanceCore;
  private loaded = false;
  private loading?: Promise<void>;

  constructor(deps: IngestCoreDeps) {
    this.sql = deps.sql;
    this.now = deps.now ?? (() => Date.now());
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
                kind, name, window_json, query_watermark_ms, coverage_json
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
    const coverage = normalizeCoverage(input.coverage, window.instances);
    const state = coverage.complete ? "ready" : "incomplete";
    this.upsertWindowRow(input.shot_id, input.owner, window, state, coverage, input.metadata);
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
    const coverage: WindowCoverage =
      prior && prior.missing_segments.length > 0
        ? prior
        : {
            expected_segments: prior?.expected_segments ?? 0,
            available_segments: prior?.available_segments ?? 0,
            missing_segments: [],
            instances: window.instances,
            complete: true,
          };
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
    const updated = this.getShotRecord(shotId);
    if (!updated) throw new Error("failed to persist stopped window shot");
    return updated;
  }

  /** Records the coverage/completeness computed by an analysis pass. */
  async setWindowCoverage(shotId: string, coverage: WindowCoverage): Promise<WindowShotRecord> {
    await this.ensureLoaded();
    const row = this.getShotRecord(shotId);
    if (!row) throw badRequest("unknown window shot");
    const normalized = normalizeCoverage(coverage, row.window?.instances ?? []);
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

  /** One registry row (window or legacy), or null. */
  getShotRecord(shotId: string): WindowShotRecord | null {
    const rows = this.sql
      .exec(
        `SELECT shot_id, owner, created_at, state, stopped_at, size_bytes, sha256,
                kind, name, window_json, query_watermark_ms, coverage_json
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
         (shot_id, owner, created_at, state, kind, name, window_json, coverage_json)
       VALUES (?, ?, ?, ?, 'window', ?, ?, ?)
       ON CONFLICT (shot_id) DO UPDATE SET
         owner = excluded.owner, state = excluded.state, kind = 'window',
         name = excluded.name, window_json = excluded.window_json, coverage_json = excluded.coverage_json`,
      shotId,
      owner && owner.trim() !== "" ? owner.trim() : "legacy",
      this.now(),
      state,
      window.name ?? null,
      windowToJSON(window),
      JSON.stringify(coverage),
    );
    void metadata;
  }

  // ------------------------------------------------------------- journal

  /**
   * Durably indexes one acknowledged OTLP export. Idempotent on the object key
   * so a reconciliation replay cannot double-count. Must succeed before the
   * Worker answers the exporter with OTLP 200.
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

    const existing = this.sql
      .exec(`SELECT sha256 FROM segment_manifest WHERE segment_key = ?`, validated.value.segment_key)
      .toArray();
    if (existing.length > 0) {
      if (String(existing[0]!.sha256) !== validated.value.sha256) {
        return Promise.reject(new Error("segment_key already exists with a different checksum"));
      }
      this.bump("segment_duplicate");
      return { committed: false, duplicate: true, covered_new: 0, covered_skipped: 0 };
    }

    const now = this.now();
    const seg = validated.value;
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
        seg.instance_id,
        cov.signal,
        cov.received_at,
        now,
      );
      coveredNew += 1;
    }
    this.bump("segment_published");
    if (coveredNew > 0) this.bump("segment_covered", coveredNew);
    if (seg.late) this.bump("segment_late");
    return { committed: true, duplicate: false, covered_new: coveredNew, covered_skipped: coveredSkipped };
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
  };
}

/** Normalizes a coverage summary; a missing list is never silently complete. */
export function normalizeCoverage(input: WindowCoverage | undefined, instances: string[]): WindowCoverage {
  const missing = Array.isArray(input?.missing_segments) ? input!.missing_segments.map(String) : [];
  const coverage: WindowCoverage = {
    expected_segments: numOr(input?.expected_segments, 0),
    available_segments: numOr(input?.available_segments, 0),
    missing_segments: missing,
    instances: normalizeInstances(instances.length > 0 ? instances : input?.instances),
    late_arrivals: numOr(input?.late_arrivals, 0),
    log_observed_fallbacks: numOr(input?.log_observed_fallbacks, 0),
    complete: input?.complete === true && missing.length === 0,
  };
  return coverage;
}

function numOr(v: unknown, fallback: number): number {
  return typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : fallback;
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
    coverage.push({ journal_key: entry.journal_key, signal: entry.signal, received_at: entry.received_at });
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
