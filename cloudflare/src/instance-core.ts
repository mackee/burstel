// Named-instance registry and push-only instance credentials (spec 014).
//
// This core is deliberately free of Cloudflare-specific globals so the exact
// same logic runs in production (over `ControlDO` SQLite storage) and in unit
// tests (over `bun:sqlite`). It owns:
//
//   * the named logical *instance* registry — an explicit, normalized name maps
//     to a server-assigned immutable, opaque `instance_id` used for R2 keys and
//     trusted attribution;
//   * the push-only *instance tokens*: 256-bit bearer secrets valid for exactly
//     24h from issue, persisted only as a one-way SHA-256 digest, individually
//     revocable and rotatable with a bounded overlap.
//
// Design boundaries:
//   * Token -> instance. A token never selects a shot; ingestion is not tied to
//     any shot lifecycle.
//   * A push-only instance token authorizes *only* POST
//     /ingest/v1/{traces,metrics,logs}. It is never compared against, or treated
//     as, `BURSTEL_API_TOKEN`, and it can never call `/api/*` or `/mcp`.
//   * The 24h lifetime applies to the token, never to a shot, segment or
//     artifact.
//   * The admin/member credential is resolved elsewhere (AuthDO) and is never
//     accepted for OTLP ingest.

import { constantTimeEqual, randomBase64Url, randomHexId, sha256Hex } from "./crypto";
import { badRequest, notFound } from "./errors";
import { errorResponse, json, readJson } from "./http";
import type { SqlRow, SyncSql } from "./store";

/** Exact instance-token lifetime mandated by the spec. */
export const INSTANCE_TOKEN_TTL_SECONDS = 86_400;
export const INSTANCE_TOKEN_TTL_MS = INSTANCE_TOKEN_TTL_SECONDS * 1000;

/**
 * Upper bound on the overlap during rotation. Rotation revokes the previous
 * active token(s) with a grace so an already-configured exporter can finish a
 * batch and pick up the replacement; the overlap is bounded so a forgotten
 * rotation cannot leave old credentials valid indefinitely.
 */
export const MAX_INSTANCE_ROTATION_OVERLAP_SECONDS = 3_600;

/** Prefix of an instance token, for operator identification. */
export const INSTANCE_TOKEN_PREFIX = "bit_";

export type InstanceAdmitReason = "missing" | "invalid" | "expired" | "revoked";

export interface InstanceMetadata {
  instance_id: string;
  name: string;
  created_at: string;
  updated_at: string;
  tokens: number;
  active_tokens: number;
}

export interface InstanceTokenIssue {
  /** Raw secret. Returned exactly once, at issuance. */
  token: string;
  id: string;
  instance_id: string;
  instance: string;
  issued_at: string;
  expires_at: string;
  ttl_seconds: number;
}

export interface InstanceTokenMetadata {
  id: string;
  instance_id: string;
  instance: string;
  issued_at: string;
  expires_at: string;
  revoked: boolean;
  /** Set only on a rotated (grace) token; ISO-8601. */
  grace_until: string | null;
}

export interface InstanceAdmitSuccess {
  ok: true;
  instance_id: string;
  instance_name: string;
  /** Stable token id, recorded with each journal object for D3 exclusion. */
  token_id: string;
  expires_at: number;
}

export interface InstanceAdmitFailure {
  ok: false;
  reason: InstanceAdmitReason;
}

export type InstanceAdmitResult = InstanceAdmitSuccess | InstanceAdmitFailure;

export interface InstanceCoreDeps {
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
  `CREATE TABLE IF NOT EXISTS instance (
     instance_id TEXT PRIMARY KEY,
     name        TEXT NOT NULL UNIQUE,
     created_at  INTEGER NOT NULL,
     updated_at  INTEGER NOT NULL
   )`,
  `CREATE TABLE IF NOT EXISTS instance_token (
     digest       TEXT PRIMARY KEY,
     token_id     TEXT NOT NULL UNIQUE,
     instance_id  TEXT NOT NULL,
     issued_at    INTEGER NOT NULL,
     expires_at   INTEGER NOT NULL,
     revoked      INTEGER NOT NULL DEFAULT 0,
     grace_until  INTEGER,
     revoked_at   INTEGER
   )`,
  `CREATE INDEX IF NOT EXISTS instance_token_instance ON instance_token (instance_id)`,
];

/**
 * Normalizes an instance name to its canonical wire/storage form. Names are
 * lowercased and trimmed so lookups are case-insensitive and stable.
 */
export function normalizeInstanceName(raw: string): string {
  return raw.trim().toLowerCase();
}

/**
 * A valid instance name is a DNS-label-like token: lowercase alphanumerics and
 * hyphens, not starting/ending with a hyphen, 1-63 chars. This prevents path
 * tricks (`/`, `.`, `..`) and keeps names safe as path components and keys.
 */
export function isValidInstanceName(name: string): boolean {
  return /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(name);
}

/** 256 bits of CSPRNG entropy, URL-safe, prefixed. */
export function newInstanceToken(): string {
  return `${INSTANCE_TOKEN_PREFIX}${randomBase64Url(32)}`;
}

/** Opaque, unguessable instance id (128 bits, hex). */
export function newInstanceId(): string {
  return randomHexId(16);
}

interface InstanceRow {
  instance_id: string;
  name: string;
  created_at: number;
  updated_at: number;
}

interface InstanceTokenRow {
  digest: string;
  token_id: string;
  instance_id: string;
  issued_at: number;
  expires_at: number;
  revoked: number;
  grace_until: number | null;
  revoked_at: number | null;
}

/** One token digest, exposed only on internal control-plane routes. */
export interface InstanceTokenDigest {
  digest: string;
  token_id: string;
  instance_id: string;
  instance: string;
  issued_at: number;
  expires_at: number;
  revoked: boolean;
  grace_until: number | null;
  revoked_at: number | null;
}

/** Effective revocation instant: a grace token is revoked when it stops working. */
export interface InstanceTokenRevocation {
  token_id: string;
  revoked_at: number | null;
  expires_at: number;
}

export class InstanceCore {
  private readonly sql: SyncSql;
  private readonly now: () => number;
  private readonly generateToken: () => string;
  private readonly generateInstanceId: () => string;
  private readonly generateTokenId: () => string;
  private loaded = false;
  private loading?: Promise<void>;

  constructor(deps: InstanceCoreDeps) {
    this.sql = deps.sql;
    this.now = deps.now ?? (() => Date.now());
    this.generateToken = deps.generateToken ?? newInstanceToken;
    this.generateInstanceId = deps.generateInstanceId ?? newInstanceId;
    this.generateTokenId = deps.generateTokenId ?? (() => randomHexId(16));
  }

  async load(): Promise<void> {
    for (const stmt of SCHEMA_STATEMENTS) this.sql.exec(stmt);
    // Additive migration for an `instance_token` table created before spec 023.
    for (const stmt of [
      `ALTER TABLE instance_token ADD COLUMN revoked_at INTEGER`,
      // Legacy revoked rows have no recorded instant. Excluding from their issue
      // time is conservative (over-exclusion is safe for analysis).
      `UPDATE instance_token SET revoked_at = issued_at WHERE revoked = 1 AND revoked_at IS NULL`,
    ]) {
      try {
        this.sql.exec(stmt);
      } catch {
        // Column already exists.
      }
    }
    this.loaded = true;
  }

  async ensureLoaded(): Promise<void> {
    if (this.loaded) return;
    this.loading ??= this.load();
    await this.loading;
  }

  // ---------------------------------------------------------- instances

  /**
   * Creates or returns the named instance. The `instance_id` is assigned once
   * and never changes, so R2 keys and trusted attribution stay stable across
   * renames of the exporter configuration.
   */
  async upsert(name: string): Promise<InstanceMetadata> {
    await this.ensureLoaded();
    const normalized = normalizeInstanceName(name);
    requireName(normalized);
    const existing = this.instanceByName(normalized);
    if (existing) return this.metadata(existing);
    const instanceId = this.generateInstanceId();
    const at = this.now();
    this.sql.exec(
      `INSERT INTO instance (instance_id, name, created_at, updated_at) VALUES (?, ?, ?, ?)
       ON CONFLICT (name) DO NOTHING`,
      instanceId,
      normalized,
      at,
      at,
    );
    // A concurrent insert may have won; read back the canonical row.
    const row = this.instanceByName(normalized);
    if (!row) throw new Error("failed to persist instance");
    return this.metadata(row);
  }

  /** Instance metadata, newest first. Never includes secrets. */
  async list(): Promise<{ instances: InstanceMetadata[] }> {
    await this.ensureLoaded();
    const instances = this.sql
      .exec(`SELECT instance_id, name, created_at, updated_at FROM instance ORDER BY created_at DESC, name ASC`)
      .toArray()
      .map((row) => this.metadata(instanceRow(row)));
    return { instances };
  }

  async get(name: string): Promise<InstanceMetadata | null> {
    await this.ensureLoaded();
    const normalized = normalizeInstanceName(name);
    if (!isValidInstanceName(normalized)) return null;
    const row = this.instanceByName(normalized);
    return row ? this.metadata(row) : null;
  }

  // ------------------------------------------------------------- tokens

  /**
   * Issues a fresh 24h push-only instance token, persisted only as a digest.
   * When `rotate` is set, every currently-active token for *this instance only*
   * is revoked with a bounded grace so an exporter can switch over; other
   * instances are never touched.
   */
  async issue(
    name: string,
    opts: { rotate?: boolean; overlapSeconds?: number } = {},
  ): Promise<InstanceTokenIssue> {
    await this.ensureLoaded();
    const normalized = normalizeInstanceName(name);
    requireName(normalized);
    const instance = this.instanceByName(normalized);
    if (!instance) throw notFound(`unknown instance ${normalized}`);

    if (opts.rotate) {
      this.revokeActiveTokens(instance.instance_id, overlapMs(opts.overlapSeconds));
    }

    const token = this.generateToken();
    const digest = await sha256Hex(token);
    const id = this.generateTokenId();
    const issuedAt = this.now();
    const expiresAt = issuedAt + INSTANCE_TOKEN_TTL_MS;
    this.sql.exec(
      `INSERT INTO instance_token (digest, token_id, instance_id, issued_at, expires_at, revoked, grace_until, revoked_at)
       VALUES (?, ?, ?, ?, ?, 0, NULL, NULL)`,
      digest,
      id,
      instance.instance_id,
      issuedAt,
      expiresAt,
    );
    this.touch(instance.instance_id, issuedAt);
    return {
      token,
      id,
      instance_id: instance.instance_id,
      instance: instance.name,
      issued_at: new Date(issuedAt).toISOString(),
      expires_at: new Date(expiresAt).toISOString(),
      ttl_seconds: INSTANCE_TOKEN_TTL_SECONDS,
    };
  }

  /** Token metadata for one instance, newest first. Never includes secrets. */
  async listTokens(name: string): Promise<{ tokens: InstanceTokenMetadata[] }> {
    await this.ensureLoaded();
    const normalized = normalizeInstanceName(name);
    requireName(normalized);
    const instance = this.instanceByName(normalized);
    if (!instance) throw notFound(`unknown instance ${normalized}`);
    const tokens = this.sql
      .exec(
        `SELECT token_id, instance_id, issued_at, expires_at, revoked, grace_until
           FROM instance_token
          WHERE instance_id = ?
          ORDER BY issued_at DESC, token_id DESC`,
        instance.instance_id,
      )
      .toArray()
      .map((row) => tokenMetadataRow(row, instance.name));
    return { tokens };
  }

  /**
   * Revokes one token immediately (idempotent). Scoped to the named instance so
   * a caller cannot revoke another instance's token by guessing an id.
   */
  async revoke(name: string, id: string): Promise<{ revoked: boolean; id: string }> {
    await this.ensureLoaded();
    const normalized = normalizeInstanceName(name);
    requireName(normalized);
    const instance = this.instanceByName(normalized);
    if (!instance) throw notFound(`unknown instance ${normalized}`);
    if (typeof id !== "string" || id.trim() === "") return { revoked: false, id };
    const cursor = this.sql.exec(
      `UPDATE instance_token SET revoked = 1, grace_until = NULL, revoked_at = ?
        WHERE token_id = ? AND instance_id = ? AND revoked = 0`,
      this.now(),
      id,
      instance.instance_id,
    );
    return { revoked: (cursor.rowsWritten ?? 0) > 0, id };
  }

  /**
   * Verifies a push token and resolves the instance it belongs to.
   *
   * Acceptance is exactly `now < expires_at`; at `expires_at` and later the
   * request is rejected. Revocation fails closed immediately, except during a
   * bounded rotation grace. Digest comparison is constant-time and never reveals
   * token material.
   */
  async admit(token: string | null): Promise<InstanceAdmitResult> {
    await this.ensureLoaded();
    if (!token) return { ok: false, reason: "missing" };
    const digest = await sha256Hex(token);
    let match: InstanceTokenRow | undefined;
    for (const row of this.tokenRows()) {
      if (constantTimeEqual(digest, row.digest)) match = row;
    }
    if (!match) return { ok: false, reason: "invalid" };
    const now = this.now();
    const inGrace = match.grace_until !== null && now < match.grace_until;
    if (match.revoked === 1 && !inGrace) return { ok: false, reason: "revoked" };
    if (now >= match.expires_at) return { ok: false, reason: "expired" };
    const instance = this.instanceById(match.instance_id);
    if (!instance) return { ok: false, reason: "invalid" };
    return {
      ok: true,
      instance_id: instance.instance_id,
      instance_name: instance.name,
      token_id: match.token_id,
      expires_at: match.expires_at,
    };
  }

  /**
   * One-shot backfill source (spec 023, D1.4): every live (non-revoked,
   * unexpired) token digest with the exact grant the Worker caches. Internal
   * only: a raw digest is a credential-adjacent value and never leaves the
   * trusted control plane.
   */
  async listLiveTokenDigests(): Promise<{ tokens: InstanceTokenDigest[] }> {
    await this.ensureLoaded();
    const now = this.now();
    const tokens: InstanceTokenDigest[] = [];
    for (const row of this.tokenRows()) {
      if (row.revoked === 1) continue;
      if (now >= row.expires_at) continue;
      const instance = this.instanceById(row.instance_id);
      tokens.push({
        digest: row.digest,
        token_id: row.token_id,
        instance_id: row.instance_id,
        instance: instance?.name ?? "",
        issued_at: row.issued_at,
        expires_at: row.expires_at,
        revoked: false,
        grace_until: row.grace_until,
        revoked_at: row.revoked_at,
      });
    }
    return { tokens };
  }

  /** All token digests for one instance; used to invalidate KV on rotate/revoke. */
  async listTokenDigests(name: string): Promise<{ tokens: InstanceTokenDigest[] }> {
    await this.ensureLoaded();
    const normalized = normalizeInstanceName(name);
    requireName(normalized);
    const instance = this.instanceByName(normalized);
    if (!instance) throw notFound(`unknown instance ${normalized}`);
    const tokens = this.tokenRows()
      .filter((row) => row.instance_id === instance.instance_id)
      .map((row) => ({
        digest: row.digest,
        token_id: row.token_id,
        instance_id: row.instance_id,
        instance: instance.name,
        issued_at: row.issued_at,
        expires_at: row.expires_at,
        revoked: row.revoked === 1,
        grace_until: row.grace_until,
        revoked_at: row.revoked_at,
      }));
    return { tokens };
  }

  /**
   * Effective revocation instants for the compactor (spec 023, D3.2).
   *
   * A rotated token is still accepted until `grace_until`; its effective
   * revocation instant is therefore `max(revoked_at, grace_until)`. An object
   * with `received_at >= effective_revoked_at` is excluded from segments.
   */
  async tokenRevocations(tokenIds: string[]): Promise<{ revocations: InstanceTokenRevocation[] }> {
    await this.ensureLoaded();
    if (tokenIds.length === 0) return { revocations: [] };
    const wanted = new Set(tokenIds.filter((id) => id !== ""));
    const revocations: InstanceTokenRevocation[] = [];
    for (const row of this.tokenRows()) {
      if (!wanted.has(row.token_id) || row.revoked !== 1) continue;
      const grace = row.grace_until ?? 0;
      const revokedAt = Math.max(row.revoked_at ?? 0, grace);
      revocations.push({
        token_id: row.token_id,
        revoked_at: revokedAt > 0 ? revokedAt : null,
        expires_at: row.expires_at,
      });
    }
    return { revocations };
  }

  // ----------------------------------------------------------- internal

  async handleInternal(request: Request): Promise<Response> {
    await this.ensureLoaded();
    try {
      const path = new URL(request.url).pathname;
      if (path === "/internal/instance/upsert") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ name?: string }>(request);
        if (typeof body.name !== "string" || body.name.trim() === "") {
          return json(400, { error: "name is required", code: "bad_request" });
        }
        return json(201, await this.upsert(body.name));
      }
      if (path === "/internal/instance/list") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.list());
      }
      if (path === "/internal/instance/token/issue") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ name?: string; rotate?: boolean; overlap_seconds?: number }>(request);
        if (typeof body.name !== "string" || body.name.trim() === "") {
          return json(400, { error: "name is required", code: "bad_request" });
        }
        return json(
          201,
          await this.issue(body.name, {
            rotate: body.rotate === true,
            overlapSeconds: typeof body.overlap_seconds === "number" ? body.overlap_seconds : undefined,
          }),
        );
      }
      if (path === "/internal/instance/token/list") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ name?: string }>(request);
        if (typeof body.name !== "string" || body.name.trim() === "") {
          return json(400, { error: "name is required", code: "bad_request" });
        }
        return json(200, await this.listTokens(body.name));
      }
      if (path === "/internal/instance/token/revoke") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ name?: string; id?: string }>(request);
        if (typeof body.name !== "string" || body.name.trim() === "") {
          return json(400, { error: "name is required", code: "bad_request" });
        }
        if (typeof body.id !== "string" || body.id.trim() === "") {
          return json(400, { error: "id is required", code: "bad_request" });
        }
        return json(200, await this.revoke(body.name, body.id));
      }
      if (path === "/internal/instance/token/digests") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ name?: string }>(request);
        if (typeof body.name !== "string" || body.name.trim() === "") {
          return json(400, { error: "name is required", code: "bad_request" });
        }
        return json(200, await this.listTokenDigests(body.name));
      }
      if (path === "/internal/instance/token/live") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.listLiveTokenDigests());
      }
      if (path === "/internal/instance/token/revocations") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ token_ids?: string[] }>(request);
        const ids = Array.isArray(body.token_ids)
          ? body.token_ids.filter((id): id is string => typeof id === "string")
          : [];
        return json(200, await this.tokenRevocations(ids));
      }
      if (path === "/internal/instance/admit") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ token?: string | null }>(request);
        return json(200, await this.admit(typeof body.token === "string" ? body.token : null));
      }
      return json(404, { error: "not found" });
    } catch (error) {
      return errorResponse(error);
    }
  }

  // ------------------------------------------------------------ helpers

  private revokeActiveTokens(instanceId: string, grace: number): number {
    const now = this.now();
    const cursor = this.sql.exec(
      `UPDATE instance_token SET revoked = 1, grace_until = ?, revoked_at = ?
        WHERE instance_id = ? AND revoked = 0`,
      now + grace,
      now + grace,
      instanceId,
    );
    return cursor.rowsWritten ?? 0;
  }

  private instanceByName(name: string): InstanceRow | undefined {
    const rows = this.sql
      .exec(`SELECT instance_id, name, created_at, updated_at FROM instance WHERE name = ?`, name)
      .toArray();
    return rows.length === 0 ? undefined : instanceRow(rows[0]!);
  }

  private instanceById(id: string): InstanceRow | undefined {
    const rows = this.sql
      .exec(`SELECT instance_id, name, created_at, updated_at FROM instance WHERE instance_id = ?`, id)
      .toArray();
    return rows.length === 0 ? undefined : instanceRow(rows[0]!);
  }

  private tokenRows(): InstanceTokenRow[] {
    return this.sql
      .exec(
        `SELECT digest, token_id, instance_id, issued_at, expires_at, revoked, grace_until, revoked_at
           FROM instance_token`,
      )
      .toArray()
      .map((row) => ({
        digest: String(row.digest),
        token_id: String(row.token_id),
        instance_id: String(row.instance_id),
        issued_at: Number(row.issued_at),
        expires_at: Number(row.expires_at),
        revoked: Number(row.revoked ?? 0),
        grace_until: row.grace_until == null ? null : Number(row.grace_until),
        revoked_at: row.revoked_at == null ? null : Number(row.revoked_at),
      }));
  }

  private touch(instanceId: string, at: number): void {
    this.sql.exec(`UPDATE instance SET updated_at = ? WHERE instance_id = ?`, at, instanceId);
  }

  private metadata(row: InstanceRow): InstanceMetadata {
    const counts = this.sql
      .exec(
        `SELECT COUNT(*) AS total, SUM(CASE WHEN revoked = 0 THEN 1 ELSE 0 END) AS active
           FROM instance_token WHERE instance_id = ?`,
        row.instance_id,
      )
      .toArray();
    return {
      instance_id: row.instance_id,
      name: row.name,
      created_at: new Date(row.created_at).toISOString(),
      updated_at: new Date(row.updated_at).toISOString(),
      tokens: Number(counts[0]?.total ?? 0),
      active_tokens: Number(counts[0]?.active ?? 0),
    };
  }
}

// -------------------------------------------------------------- helpers

function instanceRow(row: SqlRow): InstanceRow {
  return {
    instance_id: String(row.instance_id),
    name: String(row.name),
    created_at: Number(row.created_at),
    updated_at: Number(row.updated_at),
  };
}

function tokenMetadataRow(row: SqlRow, instanceName: string): InstanceTokenMetadata {
  return {
    id: String(row.token_id),
    instance_id: String(row.instance_id),
    instance: instanceName,
    issued_at: new Date(Number(row.issued_at)).toISOString(),
    expires_at: new Date(Number(row.expires_at)).toISOString(),
    revoked: Number(row.revoked ?? 0) === 1,
    grace_until: row.grace_until == null ? null : new Date(Number(row.grace_until)).toISOString(),
  };
}

function overlapMs(seconds: number | undefined): number {
  const bounded =
    typeof seconds === "number" && Number.isFinite(seconds) && seconds > 0
      ? Math.min(Math.floor(seconds), MAX_INSTANCE_ROTATION_OVERLAP_SECONDS)
      : 60;
  return bounded * 1000;
}

function requireName(name: string): void {
  if (!isValidInstanceName(name)) throw badRequest("invalid instance name");
}

// Re-exported so the Worker can share the exact comparison primitive.
export { constantTimeEqual } from "./crypto";
