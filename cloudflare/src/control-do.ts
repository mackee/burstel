// The singleton, SQLite-backed control-plane Durable Object (spec 023, D4).
//
// Formerly `IngestDO`. It owns the named-instance registry, the authoritative
// instance tokens, the shot registry, the segment manifest, the compaction
// lease/pins and the discovery cursor. It is *not* on the ingest hot path: the
// Worker authenticates from KV and writes exactly one R2 object per accepted
// export.
//
// A `ControlDO` is not a container. The real telemetry recorder still runs in a
// per-shot `ShotDO`/Container.

import { DurableObject } from "cloudflare:workers";

import type { R2BucketLike } from "./artifacts";
import { IngestCore } from "./ingest-core";
import type { SqlCursor, SqlValue, SyncSql } from "./store";
import type { Env } from "./shot-do";

export class ControlDO extends DurableObject<Env> {
  private readonly core: IngestCore;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const sql = createSqlAdapter(ctx.storage.sql);
    this.core = new IngestCore({
      sql,
      now: () => Date.now(),
      // Spec 023 D3.4: pending/backlog counts come from the R2 listing.
      bucket: env.BUCKET as unknown as R2BucketLike,
    });
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
