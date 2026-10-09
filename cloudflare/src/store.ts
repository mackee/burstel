// Durable per-shot state store.
//
// The store is deliberately written against a tiny structural `SyncSql`
// interface so the exact same persistence logic runs on:
//   * the production Durable Object SQLite storage (`ctx.storage.sql`), and
//   * an in-process SQLite database (`bun:sqlite`) inside unit tests.
//
// This keeps the lifecycle rules testable without a Cloudflare account while
// still being genuinely SQLite-backed in production.

export type Signal = "traces" | "metrics" | "logs";

export const SIGNALS: readonly Signal[] = ["traces", "metrics", "logs"];

export type SqlValue = string | number | boolean | null | ArrayBuffer | Uint8Array;

export interface SqlRow {
  [column: string]: SqlValue;
}

export interface SqlCursor {
  toArray(): SqlRow[];
  readonly rowsWritten?: number;
}

/** Synchronous SQL executor shared by DO SQLite and bun:sqlite adapters. */
export interface SyncSql {
  exec(query: string, ...bindings: SqlValue[]): SqlCursor;
}

export type ShotState = "created" | "recording" | "stopping" | "frozen" | "failed";

export interface FreezeMetadata {
  /**
   * Version of the *freeze* record (distinct from the DuckDB schema_version).
   * Bump when the meaning of the stored metadata changes; readers must ignore
   * unknown fields so new keys can be added without a migration.
   */
  freeze_version: number;
  artifact_key: string;
  artifact_size: number;
  artifact_sha256: string;
  metadata_key: string;
  container_image?: string;
  uploaded_at: string;
  [key: string]: unknown;
}

export interface StateRow {
  shot_id: string;
  state: ShotState;
  metadata_json: string;
  freeze_json: string;
  created_at: number;
  started_at: number | null;
  stopping_at: number | null;
  stopped_at: number | null;
  size_bytes: number | null;
  sha256: string | null;
  last_error: string | null;
}

export interface Counters {
  batches: Record<Signal, number>;
  records: Record<Signal, number>;
  spans: number;
  logRecords: number;
  metricPoints: number;
}

export function emptyCounters(): Counters {
  return {
    batches: { traces: 0, metrics: 0, logs: 0 },
    records: { traces: 0, metrics: 0, logs: 0 },
    spans: 0,
    logRecords: 0,
    metricPoints: 0,
  };
}

const SCHEMA_STATEMENTS: string[] = [
  `CREATE TABLE IF NOT EXISTS shot_state (
     shot_id       TEXT PRIMARY KEY,
     state         TEXT NOT NULL,
     metadata_json TEXT NOT NULL DEFAULT '{}',
     freeze_json   TEXT NOT NULL DEFAULT '{}',
     created_at    INTEGER NOT NULL,
     started_at    INTEGER,
     stopping_at   INTEGER,
     stopped_at    INTEGER,
     size_bytes    INTEGER,
     sha256        TEXT,
     last_error    TEXT
   )`,
  `CREATE TABLE IF NOT EXISTS shot_counters (
     shot_id TEXT NOT NULL,
     signal  TEXT NOT NULL,
     batches INTEGER NOT NULL DEFAULT 0,
     records INTEGER NOT NULL DEFAULT 0,
     PRIMARY KEY (shot_id, signal)
   )`,
  `CREATE TABLE IF NOT EXISTS shot_totals (
     shot_id       TEXT PRIMARY KEY,
     spans         INTEGER NOT NULL DEFAULT 0,
     log_records   INTEGER NOT NULL DEFAULT 0,
     metric_points INTEGER NOT NULL DEFAULT 0
   )`,
];

const STATE_COLUMNS = new Set<string>([
  "state",
  "metadata_json",
  "freeze_json",
  "started_at",
  "stopping_at",
  "stopped_at",
  "size_bytes",
  "sha256",
  "last_error",
]);

/** Typed helpers over a {@link SyncSql} connection. */
export class ShotStore {
  constructor(private readonly sql: SyncSql) {}

  migrate(): void {
    for (const stmt of SCHEMA_STATEMENTS) {
      this.sql.exec(stmt);
    }
  }

  get(id: string): StateRow | undefined {
    const rows = this.sql
      .exec(
        `SELECT shot_id, state, metadata_json, freeze_json, created_at, started_at,
                stopping_at, stopped_at, size_bytes, sha256, last_error
           FROM shot_state WHERE shot_id = ?`,
        id,
      )
      .toArray();
    if (rows.length === 0) return undefined;
    return rowToState(rows[0]!);
  }

  /** Inserts a `created` row. Returns true when a new row was inserted. */
  create(id: string, metadataJSON: string, now: number): boolean {
    const cursor = this.sql.exec(
      `INSERT INTO shot_state (shot_id, state, metadata_json, created_at)
       VALUES (?, 'created', ?, ?)
       ON CONFLICT (shot_id) DO NOTHING`,
      id,
      metadataJSON,
      now,
    );
    return (cursor.rowsWritten ?? 0) > 0;
  }

  update(id: string, patch: Partial<Pick<StateRow, "state" | "metadata_json" | "freeze_json" | "started_at" | "stopping_at" | "stopped_at" | "size_bytes" | "sha256" | "last_error">>): void {
    const entries = Object.entries(patch).filter(([k]) => STATE_COLUMNS.has(k));
    if (entries.length === 0) return;
    const assignments = entries.map(([k]) => `${k} = ?`).join(", ");
    const values = entries.map(([, v]) => v as SqlValue);
    this.sql.exec(`UPDATE shot_state SET ${assignments} WHERE shot_id = ?`, ...values, id);
  }

  addCounters(
    id: string,
    signal: Signal,
    records: number,
    deltas: { spans: number; logRecords: number; metricPoints: number },
  ): void {
    this.sql.exec(
      `INSERT INTO shot_counters (shot_id, signal, batches, records)
       VALUES (?, ?, 1, ?)
       ON CONFLICT (shot_id, signal)
       DO UPDATE SET batches = batches + 1, records = records + excluded.records`,
      id,
      signal,
      records,
    );
    this.sql.exec(
      `INSERT INTO shot_totals (shot_id, spans, log_records, metric_points)
       VALUES (?, ?, ?, ?)
       ON CONFLICT (shot_id)
       DO UPDATE SET
         spans = spans + excluded.spans,
         log_records = log_records + excluded.log_records,
         metric_points = metric_points + excluded.metric_points`,
      id,
      deltas.spans,
      deltas.logRecords,
      deltas.metricPoints,
    );
  }

  counters(id: string): Counters {
    const out = emptyCounters();
    for (const row of this.sql
      .exec(`SELECT signal, batches, records FROM shot_counters WHERE shot_id = ?`, id)
      .toArray()) {
      const signal = String(row.signal) as Signal;
      if (SIGNALS.includes(signal)) {
        out.batches[signal] = Number(row.batches ?? 0);
        out.records[signal] = Number(row.records ?? 0);
      }
    }
    const totals = this.sql
      .exec(`SELECT spans, log_records, metric_points FROM shot_totals WHERE shot_id = ?`, id)
      .toArray();
    if (totals.length > 0) {
      out.spans = Number(totals[0]!.spans ?? 0);
      out.logRecords = Number(totals[0]!.log_records ?? 0);
      out.metricPoints = Number(totals[0]!.metric_points ?? 0);
    }
    return out;
  }
}

function rowToState(row: SqlRow): StateRow {
  return {
    shot_id: String(row.shot_id),
    state: String(row.state) as ShotState,
    metadata_json: String(row.metadata_json ?? "{}"),
    freeze_json: String(row.freeze_json ?? "{}"),
    created_at: Number(row.created_at),
    started_at: nullableNumber(row.started_at),
    stopping_at: nullableNumber(row.stopping_at),
    stopped_at: nullableNumber(row.stopped_at),
    size_bytes: nullableNumber(row.size_bytes),
    sha256: row.sha256 == null ? null : String(row.sha256),
    last_error: row.last_error == null ? null : String(row.last_error),
  };
}

function nullableNumber(v: SqlValue | undefined): number | null {
  return v == null ? null : Number(v);
}
