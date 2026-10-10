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

import { errorResponse, json, readJson } from "./http";
import { InstanceCore, newInstanceToken } from "./instance-core";
import type { InstanceCoreDeps } from "./instance-core";
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
     sha256     TEXT
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
    shots: RegisteredShot[];
    total: number;
  }> {
    await this.ensureLoaded();
    const where = opts.owner ? `WHERE owner = ?` : "";
    const args: SqlValue[] = opts.owner ? [opts.owner] : [];
    const totalRows = this.sql.exec(`SELECT COUNT(*) AS n FROM shot_registry ${where}`, ...args).toArray();
    const total = Number(totalRows[0]?.n ?? 0);
    const rows = this.sql
      .exec(
        `SELECT shot_id, owner, created_at, state, stopped_at, size_bytes, sha256
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

  async status(): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    return {
      recording: this.countRecording(),
      shots: this.countShots(),
      journal: this.countJournal(),
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

function registeredRow(row: SqlRow): RegisteredShot {
  return {
    shot_id: String(row.shot_id),
    owner: row.owner == null ? "legacy" : String(row.owner),
    created_at: Number(row.created_at),
    state: String(row.state ?? "recording"),
    stopped_at: row.stopped_at == null ? null : Number(row.stopped_at),
    size_bytes: row.size_bytes == null ? null : Number(row.size_bytes),
    sha256: row.sha256 == null ? null : String(row.sha256),
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
