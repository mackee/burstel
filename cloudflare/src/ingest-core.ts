// Per-shot push-only OTLP ingest credentials + shot registry (spec 013, Part A).
//
// This core is deliberately free of Cloudflare-specific globals so the exact
// same logic runs in production (over `IngestDO` SQLite storage) and in unit
// tests (over `bun:sqlite`). It owns:
//
//   * the per-shot push-only ingest tokens: each is bound to exactly one shot
//     id and persisted only as a one-way SHA-256 digest with its exact 24h
//     (`86_400s`) issue/expiry timestamps. Several tokens may be valid at once
//     and route independently (token -> shot);
//   * the shot registry: which shots exist, their attribution-only `owner` and
//     the set that is currently recording, so `POST /api/shots` can enforce the
//     `MAX_CONCURRENT_RECORDING` cap and `GET /api/shots` can list shots.
//
// Design boundaries:
//   * A push token authorizes *only* POST /ingest/v1/{traces,metrics,logs}. It
//     is never compared against, or treated as, `BURSTEL_API_TOKEN`.
//   * Expiry is evaluated per arriving request against an injectable clock and
//     is never extended by shot creation, stop, queries or telemetry.
//   * A token is revoked when its shot freezes, with a short grace so batches
//     already admitted still drain (they reach a non-recording shot, which the
//     stable `/ingest` forwarder consciously discards). Expiry never calls
//     `shot_stop`, deletes a shot or expires one; frozen artifacts outlive the
//     token by design.

import { capacityExhausted } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { SIGNALS } from "./store";
import type { Signal, SqlRow, SqlValue, SyncSql } from "./store";

/** Exact token lifetime mandated by the spec. */
export const INGEST_TOKEN_TTL_SECONDS = 86_400;
export const INGEST_TOKEN_TTL_MS = INGEST_TOKEN_TTL_SECONDS * 1000;

/**
 * Grace after a shot freezes during which its ingest token is still admitted:
 * late, previously admitted batches reach a non-recording shot (and are
 * discarded) instead of being answered with a hard 401.
 */
export const INGEST_TOKEN_GRACE_MS = 60_000;

/** Name of the singleton coordinator Durable Object. */
export const INGEST_DO_NAME = "ingest-primary";

export type IngestRejectReason = "missing" | "invalid" | "expired" | "revoked";

export interface ShotIngestTokenIssue {
  /** Raw secret. Returned exactly once, at issuance. */
  token: string;
  shot_id: string;
  issued_at: string;
  expires_at: string;
  ttl_seconds: number;
}

export interface IngestAdmitSuccess {
  ok: true;
  /** The shot the presented token is bound to. */
  shot_id: string;
  expires_at: number;
}

export interface IngestAdmitFailure {
  ok: false;
  reason: IngestRejectReason;
}

export type IngestAdmitResult = IngestAdmitSuccess | IngestAdmitFailure;

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

export interface IngestCoreDeps {
  sql: SyncSql;
  /** Injectable clock (epoch ms) for deterministic tests. */
  now?: () => number;
  /** Injectable CSPRNG token generator for deterministic tests. */
  generateToken?: () => string;
}

const SCHEMA_STATEMENTS: string[] = [
  // Per-shot push tokens, bound to exactly one shot id. Only the digest is
  // persisted; `grace_until` lets already-admitted batches drain after freeze.
  `CREATE TABLE IF NOT EXISTS shot_ingest_token (
     digest      TEXT PRIMARY KEY,
     shot_id     TEXT NOT NULL,
     issued_at   INTEGER NOT NULL,
     expires_at  INTEGER NOT NULL,
     revoked     INTEGER NOT NULL DEFAULT 0,
     grace_until INTEGER
   )`,
  `CREATE INDEX IF NOT EXISTS shot_ingest_token_shot ON shot_ingest_token (shot_id)`,
  // The shot registry: attribution-only owner plus enough state for the
  // concurrency cap and `GET /api/shots` fallbacks.
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
  // Safe structured diagnostics; never contains token material.
  `CREATE TABLE IF NOT EXISTS ingest_counters (
     name  TEXT PRIMARY KEY,
     value INTEGER NOT NULL DEFAULT 0
   )`,
];

/** 256 bits of CSPRNG entropy, URL-safe. Prefix aids operator identification. */
export function newIngestToken(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  const encoded = btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `blt_${encoded}`;
}

/** Lowercase hex SHA-256 digest of a UTF-8 string. */
export async function sha256Hex(text: string): Promise<string> {
  const data = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Constant-time-ish string comparison folded over both lengths. */
export function constantTimeEqual(a: string, b: string): boolean {
  const len = Math.max(a.length, b.length);
  let diff = a.length ^ b.length;
  for (let i = 0; i < len; i++) {
    diff |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  }
  return diff === 0;
}

interface TokenRow {
  digest: string;
  shot_id: string;
  expires_at: number;
  revoked: number;
  grace_until: number | null;
}

export class IngestCore {
  private readonly sql: SyncSql;
  private readonly now: () => number;
  private readonly generateToken: () => string;
  private loaded = false;
  private loading?: Promise<void>;

  constructor(deps: IngestCoreDeps) {
    this.sql = deps.sql;
    this.now = deps.now ?? (() => Date.now());
    this.generateToken = deps.generateToken ?? newIngestToken;
  }

  async load(): Promise<void> {
    for (const stmt of SCHEMA_STATEMENTS) this.sql.exec(stmt);
    this.loaded = true;
  }

  async ensureLoaded(): Promise<void> {
    if (this.loaded) return;
    this.loading ??= this.load();
    await this.loading;
  }

  // ----------------------------------------------------------- tokens

  /**
   * Mints a fresh push token bound to `shotId`, persisted only as a digest.
   * There is no silent renewal: every issuance is an explicit request and the
   * token is disclosed exactly once.
   */
  async issueShotToken(shotId: string): Promise<ShotIngestTokenIssue> {
    await this.ensureLoaded();
    const token = this.generateToken();
    const digest = await sha256Hex(token);
    const issuedAt = this.now();
    const expiresAt = issuedAt + INGEST_TOKEN_TTL_MS;
    this.sql.exec(
      `INSERT INTO shot_ingest_token (digest, shot_id, issued_at, expires_at, revoked, grace_until)
       VALUES (?, ?, ?, ?, 0, NULL)`,
      digest,
      shotId,
      issuedAt,
      expiresAt,
    );
    this.bump("issued");
    return {
      token,
      shot_id: shotId,
      issued_at: new Date(issuedAt).toISOString(),
      expires_at: new Date(expiresAt).toISOString(),
      ttl_seconds: INGEST_TOKEN_TTL_SECONDS,
    };
  }

  /**
   * Revokes every active token for a shot (at freeze), leaving a short grace so
   * already-admitted batches drain into a non-recording shot.
   */
  async revokeShotTokens(shotId: string): Promise<{ revoked: number }> {
    await this.ensureLoaded();
    const grace = this.now() + INGEST_TOKEN_GRACE_MS;
    const cursor = this.sql.exec(
      `UPDATE shot_ingest_token SET revoked = 1, grace_until = ?
       WHERE shot_id = ? AND revoked = 0`,
      grace,
      shotId,
    );
    const revoked = cursor.rowsWritten ?? 0;
    if (revoked > 0) this.bump("revoked_count", revoked);
    return { revoked };
  }

  /**
   * Verifies a push token for one signal and resolves the shot it is bound to.
   *
   * Acceptance is exactly `now < expires_at`; at `expires_at` and later the
   * request is rejected regardless of recording state. Revocation fails closed
   * after the freeze grace. Comparison against stored digests is constant-time
   * and never reveals token material.
   */
  async admit(token: string | null, signal: Signal): Promise<IngestAdmitResult> {
    await this.ensureLoaded();
    this.bump("requests");
    if (!token) {
      this.bump("missing");
      return { ok: false, reason: "missing" };
    }
    const digest = await sha256Hex(token);
    let match: TokenRow | undefined;
    for (const row of this.tokenRows()) {
      if (constantTimeEqual(digest, row.digest)) match = row;
    }
    if (!match) {
      this.bump("invalid");
      return { ok: false, reason: "invalid" };
    }
    const now = this.now();
    const inGrace = match.grace_until !== null && now < match.grace_until;
    if (match.revoked === 1 && !inGrace) {
      this.bump("revoked");
      return { ok: false, reason: "revoked" };
    }
    if (now >= match.expires_at) {
      this.bump("expired");
      return { ok: false, reason: "expired" };
    }
    this.bump("accepted");
    this.bump(`accepted:${signal}`);
    return { ok: true, shot_id: match.shot_id, expires_at: match.expires_at };
  }

  // ----------------------------------------------------------- registry

  /**
   * Atomically reserves a recording slot for a freshly created shot, enforcing
   * the concurrency cap. Throws `503 capacity_exhausted` (retryable) without
   * writing any state when the cap is reached.
   */
  async reserveShot(shotId: string, owner: string, max: number): Promise<{ active: number }> {
    await this.ensureLoaded();
    const active = this.countRecording();
    if (active >= max) {
      this.bump("capacity_rejected");
      throw capacityExhausted(max);
    }
    this.sql.exec(
      `INSERT INTO shot_registry (shot_id, owner, created_at, state)
       VALUES (?, ?, ?, 'recording')
       ON CONFLICT (shot_id) DO UPDATE SET owner = excluded.owner, state = 'recording'`,
      shotId,
      owner,
      this.now(),
    );
    this.bump("reserved");
    return { active: active + 1 };
  }

  /**
   * Releases one or more reservations. Used to roll back a failed shot start
   * (so the cap never leaks) and to reconcile stale `recording` entries whose
   * ShotDO is gone.
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

  /** Records a shot's terminal/observed state so listing and the cap stay true. */
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

  /** Shot ids currently registered as recording, for cap reconciliation. */
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

  async status(): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    return {
      tokens: this.tokenCount(),
      recording: this.countRecording(),
      shots: this.countShots(),
      counters: this.counters(),
    };
  }

  // ------------------------------------------------------------- internal

  async handleInternal(request: Request): Promise<Response> {
    await this.ensureLoaded();
    try {
      const path = new URL(request.url).pathname;
      if (path === "/internal/admit") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ token?: string | null; signal?: string }>(request);
        if (typeof body.signal !== "string" || !SIGNALS.includes(body.signal as Signal)) {
          return json(400, { error: "signal is required", code: "bad_request" });
        }
        return json(200, await this.admit(body.token ?? null, body.signal as Signal));
      }
      if (path === "/internal/shot-token/issue") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.issueShotToken(body.shot_id));
      }
      if (path === "/internal/shot-token/revoke") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.revokeShotTokens(body.shot_id));
      }
      if (path === "/internal/shot/reserve") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; owner?: string; max?: number }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        const max = typeof body.max === "number" && body.max > 0 ? body.max : Number.MAX_SAFE_INTEGER;
        const owner = typeof body.owner === "string" && body.owner !== "" ? body.owner : "legacy";
        return json(200, await this.reserveShot(body.shot_id, owner, max));
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

  private tokenRows(): TokenRow[] {
    return this.sql
      .exec(
        `SELECT digest, shot_id, expires_at, revoked, grace_until
           FROM shot_ingest_token`,
      )
      .toArray()
      .map((row) => ({
        digest: String(row.digest),
        shot_id: String(row.shot_id),
        expires_at: Number(row.expires_at),
        revoked: Number(row.revoked ?? 0),
        grace_until: row.grace_until == null ? null : Number(row.grace_until),
      }));
  }

  private countRecording(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM shot_registry WHERE state = 'recording'`).toArray();
    return Number(rows[0]?.n ?? 0);
  }

  private countShots(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM shot_registry`).toArray();
    return Number(rows[0]?.n ?? 0);
  }

  private tokenCount(): number {
    const rows = this.sql.exec(`SELECT COUNT(*) AS n FROM shot_ingest_token WHERE revoked = 0`).toArray();
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
