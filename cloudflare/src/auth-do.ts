// The singleton, SQLite-backed multi-user token registry Durable Object.
//
// One `AuthDO` per installation owns the per-person bearer-token digests and
// their scopes/owners (spec 013, Part B). It is *not* a container: it is a
// lightweight coordinator so the Worker can resolve any request's bearer token
// to a principal (scope + attribution-only owner) before doing work. The env
// `BURSTEL_API_TOKEN` admin credential is handled in the Worker and never
// stored here.

import { DurableObject } from "cloudflare:workers";

import { AuthCore } from "./auth-core";
import type { SqlCursor, SqlValue, SyncSql } from "./store";
import type { Env } from "./shot-do";

export class AuthDO extends DurableObject<Env> {
  private readonly core: AuthCore;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const sql = createSqlAdapter(ctx.storage.sql);
    this.core = new AuthCore({ sql, now: () => Date.now() });
    ctx.blockConcurrencyWhile(async () => {
      await this.core.load();
    });
  }

  override async fetch(request: Request): Promise<Response> {
    return this.core.handleInternal(request);
  }
}

/** Adapts the synchronous DO SQLite API to the store's `SyncSql` interface. */
function createSqlAdapter(sql: SqlStorage): SyncSql {
  return {
    exec(query: string, ...bindings: SqlValue[]): SqlCursor {
      const cursor = sql.exec(query, ...(bindings as unknown[]));
      return cursor as unknown as SqlCursor;
    },
  };
}
