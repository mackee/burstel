// The singleton, SQLite-backed ingest coordinator Durable Object.
//
// One `IngestDO` per installation owns the push-only token digest and the
// active-shot mapping. It is *not* a container: it is a lightweight coordinator
// so the stable `/ingest` endpoint never needs shot-specific exporter config.
// The real telemetry recorder still runs in a per-shot `ShotDO`/Container; this
// DO only decides whether a request is admitted and which shot it belongs to.

import { DurableObject } from "cloudflare:workers";

import { IngestCore } from "./ingest-core";
import type { SqlCursor, SqlValue, SyncSql } from "./store";
import type { Env } from "./shot-do";

export class IngestDO extends DurableObject<Env> {
  private readonly core: IngestCore;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const sql = createSqlAdapter(ctx.storage.sql);
    this.core = new IngestCore({ sql, now: () => Date.now() });
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
