// Shared multi-user token registry (spec 013, Part B).
//
// This core is deliberately free of Cloudflare-specific globals so the exact
// same logic runs in production (over the `AuthDO` SQLite storage) and in unit
// tests (over `bun:sqlite`). It owns the **per-person** bearer-token registry:
//
//   * every registered token carries a scope (`admin` | `member`) and an
//     attribution-only `owner` label;
//   * only a one-way SHA-256 **digest** is persisted — a raw token exists only
//     in the issuing response, exactly once;
//   * `admin` may manage the registry; `member` has the full *operational*
//     capability set on every shot but may not touch the registry.
//
// Design boundaries:
//   * Unknown/tampered tokens fail closed (`invalid`); revoked tokens fail
//     immediately; an optional `expires_at` is enforced per request against an
//     injectable clock.
//   * The env `BURSTEL_API_TOKEN` remains the legacy admin credential and is
//     resolved in the Worker (never stored here). This registry only ever holds
//     digests of operator-minted tokens.
//   * `owner` is attribution/audit only. It is never consulted for
//     authorization, so a `member` token may operate on any shot.

import { constantTimeEqual, sha256Hex } from "./ingest-core";
import { errorResponse, json, readJson } from "./http";
import type { SqlRow, SqlValue, SyncSql } from "./store";

/** Name of the singleton token-registry Durable Object. */
export const AUTH_DO_NAME = "auth-primary";

export type Scope = "admin" | "member";

export const SCOPES: readonly Scope[] = ["admin", "member"];

export function isScope(value: unknown): value is Scope {
  return value === "admin" || value === "member";
}

/** A resolved, authenticated caller. */
export interface AuthPrincipal {
  /** Registry row id, or a synthetic value for the env/dev admin. */
  id: string;
  scope: Scope;
  /** Attribution-only principal label; never an authorization boundary. */
  owner: string;
}

export type AuthFailureReason = "missing" | "invalid" | "expired" | "revoked";

export type AuthResult = { ok: true; principal: AuthPrincipal } | { ok: false; reason: AuthFailureReason };

export interface AuthTokenIssue {
  /** Raw secret. Returned exactly once, at issuance. */
  token: string;
  id: string;
  scope: Scope;
  owner: string;
  created_at: string;
  expires_at: string | null;
}

/** Registry metadata. Deliberately has no value/digest field. */
export interface AuthTokenMetadata {
  id: string;
  scope: Scope;
  owner: string;
  created_at: string;
  expires_at: string | null;
  revoked: boolean;
}

export interface MintInput {
  scope: Scope;
  owner?: string;
  /** Absolute expiry, epoch ms. */
  expiresAt?: number | null;
  /** Relative expiry in seconds. Ignored when `expiresAt` is set. */
  ttlSeconds?: number;
}

export interface AuthCoreDeps {
  sql: SyncSql;
  /** Injectable clock (epoch ms) for deterministic tests. */
  now?: () => number;
  /** Injectable CSPRNG token generator for deterministic tests. */
  generateToken?: () => string;
  /** Injectable id generator for deterministic tests. */
  generateId?: () => string;
}

const SCHEMA_STATEMENTS: string[] = [
  // Per-person bearer tokens. Only the digest is persisted; `scope`/`owner` are
  // what the Worker resolves to a principal.
  `CREATE TABLE IF NOT EXISTS auth_token (
     id         TEXT PRIMARY KEY,
     digest     TEXT NOT NULL UNIQUE,
     scope      TEXT NOT NULL,
     owner      TEXT NOT NULL,
     created_at INTEGER NOT NULL,
     expires_at INTEGER,
     revoked    INTEGER NOT NULL DEFAULT 0
   )`,
  `CREATE INDEX IF NOT EXISTS auth_token_digest ON auth_token (digest)`,
];

/** 256 bits of CSPRNG entropy, URL-safe, prefixed for operator identification. */
export function newAuthToken(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  const encoded = btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `bta_${encoded}`;
}

/** URL-safe opaque token id (128 bits, lowercase hex). */
export function newTokenId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

interface AuthTokenRow {
  id: string;
  digest: string;
  scope: string;
  owner: string;
  created_at: number;
  expires_at: number | null;
  revoked: number;
}

export class AuthCore {
  private readonly sql: SyncSql;
  private readonly now: () => number;
  private readonly generateToken: () => string;
  private readonly generateId: () => string;
  private loaded = false;
  private loading?: Promise<void>;

  constructor(deps: AuthCoreDeps) {
    this.sql = deps.sql;
    this.now = deps.now ?? (() => Date.now());
    this.generateToken = deps.generateToken ?? newAuthToken;
    this.generateId = deps.generateId ?? newTokenId;
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

  // ----------------------------------------------------------- registry

  /**
   * Mints a fresh per-person token and persists only its digest. The raw secret
   * is disclosed exactly once, in this return value.
   */
  async mint(input: MintInput): Promise<AuthTokenIssue> {
    await this.ensureLoaded();
    const scope = input.scope;
    const owner = (input.owner ?? "").trim() || (scope === "admin" ? "admin" : "member");
    const createdAt = this.now();
    let expiresAt: number | null = null;
    if (typeof input.expiresAt === "number" && Number.isFinite(input.expiresAt)) {
      expiresAt = input.expiresAt;
    } else if (typeof input.ttlSeconds === "number" && Number.isFinite(input.ttlSeconds) && input.ttlSeconds > 0) {
      expiresAt = createdAt + Math.floor(input.ttlSeconds) * 1000;
    }
    const token = this.generateToken();
    const digest = await sha256Hex(token);
    const id = this.generateId();
    this.sql.exec(
      `INSERT INTO auth_token (id, digest, scope, owner, created_at, expires_at, revoked)
       VALUES (?, ?, ?, ?, ?, ?, 0)`,
      id,
      digest,
      scope,
      owner,
      createdAt,
      expiresAt as SqlValue,
    );
    return {
      token,
      id,
      scope,
      owner,
      created_at: new Date(createdAt).toISOString(),
      expires_at: expiresAt === null ? null : new Date(expiresAt).toISOString(),
    };
  }

  /** Registry metadata, newest first. Never includes a value or digest. */
  async list(): Promise<{ tokens: AuthTokenMetadata[] }> {
    await this.ensureLoaded();
    const tokens = this.sql
      .exec(
        `SELECT id, scope, owner, created_at, expires_at, revoked
           FROM auth_token
          ORDER BY created_at DESC, id DESC`,
      )
      .toArray()
      .map(metadataRow);
    return { tokens };
  }

  /** Revokes a token immediately (idempotent). */
  async revoke(id: string): Promise<{ revoked: boolean; id: string }> {
    await this.ensureLoaded();
    const cursor = this.sql.exec(`UPDATE auth_token SET revoked = 1 WHERE id = ?`, id);
    return { revoked: (cursor.rowsWritten ?? 0) > 0, id };
  }

  /**
   * Resolves a presented bearer token to a principal.
   *
   * Comparison against stored digests is constant-time and folded over every
   * row so a missing digest is not distinguishable by an early return. Revoked
   * and expired tokens fail immediately; an unknown scope fails closed.
   */
  async authorize(token: string): Promise<AuthResult> {
    await this.ensureLoaded();
    if (token.length === 0) return { ok: false, reason: "missing" };
    const digest = await sha256Hex(token);
    let match: AuthTokenRow | undefined;
    for (const row of this.tokenRows()) {
      if (constantTimeEqual(digest, row.digest)) match = row;
    }
    if (!match) return { ok: false, reason: "invalid" };
    if (!isScope(match.scope)) return { ok: false, reason: "invalid" };
    if (match.revoked === 1) return { ok: false, reason: "revoked" };
    if (match.expires_at !== null && this.now() >= match.expires_at) {
      return { ok: false, reason: "expired" };
    }
    return { ok: true, principal: { id: match.id, scope: match.scope, owner: match.owner } };
  }

  async status(): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    const rows = this.sql
      .exec(`SELECT COUNT(*) AS total, SUM(CASE WHEN revoked = 0 THEN 1 ELSE 0 END) AS active FROM auth_token`)
      .toArray();
    return { tokens: Number(rows[0]?.total ?? 0), active: Number(rows[0]?.active ?? 0) };
  }

  // ------------------------------------------------------------- internal

  async handleInternal(request: Request): Promise<Response> {
    await this.ensureLoaded();
    try {
      const path = new URL(request.url).pathname;
      if (path === "/internal/authorize") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ token?: string | null }>(request);
        if (typeof body.token !== "string" || body.token === "") {
          return json(200, { ok: false, reason: "missing" });
        }
        return json(200, await this.authorize(body.token));
      }
      if (path === "/internal/mint") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{
          scope?: string;
          owner?: string;
          expires_at?: string;
          ttl_seconds?: number;
        }>(request);
        if (!isScope(body.scope)) {
          return json(400, { error: "scope must be admin or member", code: "bad_request" });
        }
        let expiresAt: number | undefined;
        if (typeof body.expires_at === "string" && body.expires_at !== "") {
          const parsed = Date.parse(body.expires_at);
          if (!Number.isFinite(parsed)) {
            return json(400, { error: "expires_at must be an ISO-8601 timestamp", code: "bad_request" });
          }
          expiresAt = parsed;
        }
        return json(
          200,
          await this.mint({
            scope: body.scope,
            owner: typeof body.owner === "string" ? body.owner : undefined,
            expiresAt,
            ttlSeconds: typeof body.ttl_seconds === "number" ? body.ttl_seconds : undefined,
          }),
        );
      }
      if (path === "/internal/list") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.list());
      }
      if (path === "/internal/revoke") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ id?: string }>(request);
        if (typeof body.id !== "string" || body.id === "") {
          return json(400, { error: "id is required", code: "bad_request" });
        }
        return json(200, await this.revoke(body.id));
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

  private tokenRows(): AuthTokenRow[] {
    return this.sql
      .exec(
        `SELECT id, digest, scope, owner, created_at, expires_at, revoked
           FROM auth_token`,
      )
      .toArray()
      .map((row) => ({
        id: String(row.id),
        digest: String(row.digest),
        scope: String(row.scope),
        owner: String(row.owner),
        created_at: Number(row.created_at),
        expires_at: row.expires_at == null ? null : Number(row.expires_at),
        revoked: Number(row.revoked ?? 0),
      }));
  }
}

function metadataRow(row: SqlRow): AuthTokenMetadata {
  return {
    id: String(row.id),
    scope: isScope(row.scope) ? row.scope : "member",
    owner: String(row.owner ?? ""),
    created_at: new Date(Number(row.created_at)).toISOString(),
    expires_at: row.expires_at == null ? null : new Date(Number(row.expires_at)).toISOString(),
    revoked: Number(row.revoked ?? 0) === 1,
  };
}
