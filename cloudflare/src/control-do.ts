// The singleton, SQLite-backed control-plane Durable Object (spec 023, D4).
//
// The exported storage class deliberately keeps the pre-023 name `IngestDO`
// (spec 024, R1). Renaming a Durable Object class requires a `renamed_classes`
// migration; declaring one is not possible in this repo's `exports`-style
// Wrangler config, and deploying the new name without it would orphan the
// authoritative state (instances, tokens, shots, segment manifest, compaction
// lease/pins and cursor). The code-level alias `ControlDO` remains for readers
// and tests, but the binding/export name that maps to existing storage is
// `IngestDO`.
//
// It owns the named-instance registry, the authoritative instance tokens, the
// shot registry, the segment manifest, the compaction lease/pins and the
// discovery cursor. It is *not* on the ingest hot path: the Worker
// authenticates from KV and writes exactly one R2 object per accepted export.
//
// A control DO is not a container. The real telemetry recorder still runs in a
// per-shot `ShotDO`/Container.

import { DurableObject } from "cloudflare:workers";

import type { R2BucketLike } from "./artifacts";
import { IngestCore } from "./ingest-core";
import type { SqlCursor, SqlValue, SyncSql } from "./store";
import type { Env } from "./shot-do";

/**
 * Deploy-safe storage class name (spec 024, R1).
 *
 * Keep this name in `wrangler.jsonc` (`INGEST_DO.class_name` and `exports`) so
 * an existing `IngestDO` installation reuses its SQLite storage. Rename only
 * together with a `renamed_classes` migration and a staging upgrade test.
 */
export class IngestDO extends DurableObject<Env> {
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

/** Code-level alias for the control-plane role; the exported class stays `IngestDO`. */
export { IngestDO as ControlDO };

/** Adapts the synchronous DO SQLite API to the store's `SyncSql` interface. */
function createSqlAdapter(sql: SqlStorage): SyncSql {
  return {
    exec(query: string, ...bindings: SqlValue[]): SqlCursor {
      const cursor = sql.exec(query, ...(bindings as unknown[]));
      return cursor as unknown as SqlCursor;
    },
  };
}
