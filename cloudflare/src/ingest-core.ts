// Push-only OTLP ingest token + active-shot coordinator.
//
// This core is deliberately free of Cloudflare-specific globals so the exact
// same logic runs in production (over `IngestDO` SQLite storage) and in unit
// tests (over `bun:sqlite`). It owns two things:
//
//   * one active, admin-issued push-only token, persisted only as a one-way
//     SHA-256 digest with its exact 24h (`86_400s`) issue/expiry timestamps;
//   * the installation's active shot mapping so the stable `/ingest` endpoint
//     always routes to the shot that `shot_start` most recently activated.
//
// Design boundaries (see spec 006):
//   * The push token authorizes *only* POST /ingest/v1/{traces,metrics,logs}.
//     It is never compared against, or treated as, `BURSTEL_API_TOKEN`.
//   * Expiry is evaluated per arriving request against an injectable clock and
//     is never extended by shot creation, stop, queries or telemetry.
//   * Expiry never calls `shot_stop`, deletes a shot or expires one; frozen
//     artifacts outlive the token by design.

import { conflict, HttpError } from "./errors";
import { errorResponse, json, readJson } from "./http";
import { SIGNALS } from "./store";
import type { Signal, SqlRow, SqlValue, SyncSql } from "./store";

/** Exact token lifetime mandated by the spec. */
export const INGEST_TOKEN_TTL_SECONDS = 86_400;
export const INGEST_TOKEN_TTL_MS = INGEST_TOKEN_TTL_SECONDS * 1000;

/** Name of the singleton coordinator Durable Object. */
export const INGEST_DO_NAME = "ingest-primary";

export type IngestRejectReason = "missing" | "invalid" | "expired" | "revoked";

export interface IngestTokenIssue {
  /** Raw secret. Returned exactly once, at issuance. */
  token: string;
  issued_at: string;
  expires_at: string;
  ttl_seconds: number;
}

export interface IngestTokenStatus {
  configured: boolean;
  active: boolean;
  revoked: boolean;
  issued_at?: string;
  expires_at?: string;
  ttl_seconds: number;
  remaining_seconds?: number;
  expired?: boolean;
  /** Set when the remaining lifetime is under one hour, for proactive rotation. */
  warning?: string;
  active_shot_id: string | null;
  active_state: string;
  counters: Record<string, number>;
}

export interface IngestAdmitSuccess {
  ok: true;
  active_shot_id: string | null;
  expires_at: number;
}

export interface IngestAdmitFailure {
  ok: false;
  reason: IngestRejectReason;
}

export type IngestAdmitResult = IngestAdmitSuccess | IngestAdmitFailure;

export interface IngestCoreDeps {
  sql: SyncSql;
  /** Injectable clock (epoch ms) for deterministic tests. */
  now?: () => number;
  /** Injectable CSPRNG token generator for deterministic tests. */
  generateToken?: () => string;
}

const ACTIVE_STATE_IDLE = "idle";
const ACTIVE_STATE_RECORDING = "recording";

const SCHEMA_STATEMENTS: string[] = [
  // Exactly one active token per installation, enforced by `CHECK (id = 1)`.
  `CREATE TABLE IF NOT EXISTS ingest_token (
     id         INTEGER PRIMARY KEY CHECK (id = 1),
     digest     TEXT NOT NULL,
     issued_at  INTEGER NOT NULL,
     expires_at INTEGER NOT NULL,
     revoked    INTEGER NOT NULL DEFAULT 0
   )`,
  // Exactly one active-shot mapping per installation.
  `CREATE TABLE IF NOT EXISTS ingest_active (
     id      INTEGER PRIMARY KEY CHECK (id = 1),
     shot_id TEXT,
     state   TEXT NOT NULL DEFAULT 'idle'
   )`,
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
  issued_at: number;
  expires_at: number;
  revoked: number;
}

interface ActiveRow {
  shot_id: string | null;
  state: string;
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
    this.sql.exec(
      `INSERT INTO ingest_active (id, shot_id, state) VALUES (1, NULL, 'idle')
       ON CONFLICT (id) DO NOTHING`,
    );
    this.loaded = true;
  }

  async ensureLoaded(): Promise<void> {
    if (this.loaded) return;
    this.loading ??= this.load();
    await this.loading;
  }

  // ---------------------------------------------------------------- token

  /**
   * Issues a fresh push token, atomically replacing any prior one.
   *
   * Rotation is refused while a shot is actively recording so an in-progress
   * benchmark cannot be abruptly cut off (spec 006). There is no silent
   * renewal: every issuance is an explicit admin request.
   */
  async issueToken(): Promise<IngestTokenIssue> {
    await this.ensureLoaded();
    const active = this.activeRow();
    if (active.state === ACTIVE_STATE_RECORDING && active.shot_id) {
      throw conflict("cannot rotate the ingest token while a shot is recording");
    }
    const token = this.generateToken();
    const digest = await sha256Hex(token);
    const issuedAt = this.now();
    const expiresAt = issuedAt + INGEST_TOKEN_TTL_MS;
    this.sql.exec(
      `INSERT INTO ingest_token (id, digest, issued_at, expires_at, revoked)
       VALUES (1, ?, ?, ?, 0)
       ON CONFLICT (id) DO UPDATE SET
         digest = excluded.digest,
         issued_at = excluded.issued_at,
         expires_at = excluded.expires_at,
         revoked = 0`,
      digest,
      issuedAt,
      expiresAt,
    );
    this.bump("issued");
    return {
      token,
      issued_at: new Date(issuedAt).toISOString(),
      expires_at: new Date(expiresAt).toISOString(),
      ttl_seconds: INGEST_TOKEN_TTL_SECONDS,
    };
  }

  /** Management status. Never contains the raw token or its digest. */
  async status(): Promise<IngestTokenStatus> {
    await this.ensureLoaded();
    const row = this.tokenRow();
    const active = this.activeRow();
    const now = this.now();
    if (!row) {
      return {
        configured: false,
        active: false,
        revoked: false,
        ttl_seconds: INGEST_TOKEN_TTL_SECONDS,
        active_shot_id: active.shot_id,
        active_state: active.state,
        counters: this.counters(),
      };
    }
    const expiresAt = row.expires_at;
    const expired = now >= expiresAt;
    const remainingSeconds = Math.floor((expiresAt - now) / 1000);
    const status: IngestTokenStatus = {
      configured: true,
      active: !row.revoked && !expired,
      revoked: row.revoked === 1,
      issued_at: new Date(row.issued_at).toISOString(),
      expires_at: new Date(expiresAt).toISOString(),
      ttl_seconds: INGEST_TOKEN_TTL_SECONDS,
      remaining_seconds: Math.max(0, remainingSeconds),
      expired,
      active_shot_id: active.shot_id,
      active_state: active.state,
      counters: this.counters(),
    };
    if (!row.revoked && !expired && remainingSeconds < 3600) {
      status.warning = "ingest token expires in less than one hour; issue a replacement";
    }
    return status;
  }

  async revoke(): Promise<{ revoked: boolean; configured: boolean }> {
    await this.ensureLoaded();
    const row = this.tokenRow();
    if (!row) return { revoked: false, configured: false };
    this.sql.exec(`UPDATE ingest_token SET revoked = 1 WHERE id = 1`);
    this.bump("revoked_count");
    return { revoked: true, configured: true };
  }

  /**
   * Verifies a push token for one signal and resolves the active shot.
   *
   * Acceptance is exactly `now < expires_at`; at `expires_at` and later the
   * request is rejected regardless of recording state. Revocation and mismatch
   * never reveal token material.
   */
  async admit(token: string | null, signal: Signal): Promise<IngestAdmitResult> {
    await this.ensureLoaded();
    this.bump("requests");
    if (!token) {
      this.bump("missing");
      return { ok: false, reason: "missing" };
    }
    const row = this.tokenRow();
    if (!row) {
      this.bump("invalid");
      return { ok: false, reason: "invalid" };
    }
    const digest = await sha256Hex(token);
    if (!constantTimeEqual(digest, row.digest)) {
      this.bump("invalid");
      return { ok: false, reason: "invalid" };
    }
    if (row.revoked === 1) {
      this.bump("revoked");
      return { ok: false, reason: "revoked" };
    }
    if (this.now() >= row.expires_at) {
      this.bump("expired");
      return { ok: false, reason: "expired" };
    }
    this.bump("accepted");
    this.bump(`accepted:${signal}`);
    const active = this.activeRow();
    if (!active.shot_id || active.state !== ACTIVE_STATE_RECORDING) {
      this.bump("discarded_no_active");
      return { ok: true, active_shot_id: null, expires_at: row.expires_at };
    }
    return { ok: true, active_shot_id: active.shot_id, expires_at: row.expires_at };
  }

  // ------------------------------------------------------------ active shot

  /**
   * Marks a shot as the active ingest target. Refuses to silently displace a
   * shot that is already recording.
   */
  async activate(shotId: string): Promise<{ active_shot_id: string }> {
    await this.ensureLoaded();
    const active = this.activeRow();
    if (active.state === ACTIVE_STATE_RECORDING && active.shot_id && active.shot_id !== shotId) {
      this.bump("conflicts");
      throw new HttpError(
        409,
        "another shot is already recording; stop it before starting a new one",
        false,
        "active_shot_recording",
      );
    }
    this.sql.exec(
      `INSERT INTO ingest_active (id, shot_id, state) VALUES (1, ?, 'recording')
       ON CONFLICT (id) DO UPDATE SET shot_id = excluded.shot_id, state = 'recording'`,
      shotId,
    );
    this.bump("activated");
    return { active_shot_id: shotId };
  }

  /**
   * Detaches the active shot once it stops. `late` counts a request that raced
   * the stop so it is never attributed to the next shot.
   */
  async detach(shotId: string, late = false): Promise<{ detached: boolean }> {
    await this.ensureLoaded();
    const active = this.activeRow();
    if (active.shot_id !== shotId) return { detached: false };
    if (late) this.bump("discarded_late");
    this.sql.exec(`UPDATE ingest_active SET shot_id = NULL, state = 'idle' WHERE id = 1`);
    this.bump("detached");
    return { detached: true };
  }

  async active(): Promise<{ shot_id: string | null; state: string }> {
    await this.ensureLoaded();
    return this.activeRow();
  }

  // --------------------------------------------------------------- internal

  async handleInternal(request: Request): Promise<Response> {
    await this.ensureLoaded();
    try {
      const path = new URL(request.url).pathname;
      if (path === "/internal/token/issue") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        return json(200, await this.issueToken());
      }
      if (path === "/internal/token/status") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.status());
      }
      if (path === "/internal/token/revoke") {
        if (request.method !== "DELETE") return json(405, { error: "method not allowed" });
        return json(200, await this.revoke());
      }
      if (path === "/internal/admit") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ token?: string | null; signal?: string }>(request);
        if (typeof body.signal !== "string" || !SIGNALS.includes(body.signal as Signal)) {
          return json(400, { error: "signal is required", code: "bad_request" });
        }
        return json(200, await this.admit(body.token ?? null, body.signal as Signal));
      }
      if (path === "/internal/activate") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.activate(body.shot_id));
      }
      if (path === "/internal/detach") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ shot_id?: string; late?: boolean }>(request);
        if (typeof body.shot_id !== "string" || body.shot_id === "") {
          return json(400, { error: "shot_id is required", code: "bad_request" });
        }
        return json(200, await this.detach(body.shot_id, body.late === true));
      }
      if (path === "/internal/active") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.active());
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

  private tokenRow(): TokenRow | undefined {
    const rows = this.sql
      .exec(`SELECT digest, issued_at, expires_at, revoked FROM ingest_token WHERE id = 1`)
      .toArray();
    if (rows.length === 0) return undefined;
    const row = rows[0]!;
    return {
      digest: String(row.digest),
      issued_at: Number(row.issued_at),
      expires_at: Number(row.expires_at),
      revoked: Number(row.revoked ?? 0),
    };
  }

  private activeRow(): ActiveRow {
    const rows = this.sql.exec(`SELECT shot_id, state FROM ingest_active WHERE id = 1`).toArray();
    if (rows.length === 0) return { shot_id: null, state: ACTIVE_STATE_IDLE };
    const row: SqlRow = rows[0]!;
    return {
      shot_id: row.shot_id == null ? null : String(row.shot_id),
      state: row.state == null ? ACTIVE_STATE_IDLE : String(row.state),
    };
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
