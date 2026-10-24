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
import { lateWriteLimitMs } from "./env";
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
    const sql = createSqlAdapter(ctx.storage);
    const maxMaterializeObjects = Number.parseInt((env as { MATERIALIZE_MAX_OBJECTS?: string }).MATERIALIZE_MAX_OBJECTS ?? "", 10);
    this.core = new IngestCore({
      sql,
      now: () => Date.now(),
      // Spec 023 D3.4: pending/backlog counts come from the R2 listing.
      bucket: env.BUCKET as unknown as R2BucketLike,
      // Spec 031 D4: materialization starts at end + late-write limit.
      lateWriteLimitMs: lateWriteLimitMs(env),
      maxMaterializeObjects: Number.isFinite(maxMaterializeObjects) && maxMaterializeObjects > 0 ? maxMaterializeObjects : undefined,
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

/**
 * Adapts the synchronous DO SQLite API to the store's `SyncSql` interface.
 *
 * `transactionSync` is the storage-level atomic primitive (spec 025, R2): the
 * manifest row and every coverage atom of one segment commit or roll back
 * together, so a crash can never advertise a partially published segment.
 */
function createSqlAdapter(storage: DurableObjectStorage): SyncSql {
  return {
    exec(query: string, ...bindings: SqlValue[]): SqlCursor {
      const cursor = storage.sql.exec(query, ...(bindings as unknown[]));
      return cursor as unknown as SqlCursor;
    },
    transaction<T>(fn: () => T): T {
      return storage.transactionSync(fn);
    },
  };
}
