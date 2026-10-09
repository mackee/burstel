// The per-shot Durable Object.
//
// One named ShotDO owns one Container instance (via the Durable Object
// Container API `this.ctx.container`) and one SQLite-backed lifecycle record.
// All lifecycle logic lives in ShotCore; this file is only the production
// wiring of Cloudflare bindings into that core.

import { DurableObject } from "cloudflare:workers";

import { createR2Sink } from "./artifacts";
import type { R2BucketLike } from "./artifacts";
import { createContainerControl } from "./container";
import type { ContainerLike } from "./container";
import { ShotCore } from "./core";
import type { SqlCursor, SqlValue, SyncSql } from "./store";

export interface Env extends Cloudflare.Env {
  /** Bearer token for the public control/ingest APIs. */
  BURSTEL_API_TOKEN?: string;
  /** Bearer token for the container's /internal/* endpoints (defense in depth). */
  BURSTEL_INTERNAL_TOKEN?: string;
  /** Public origin used to advertise per-shot OTLP endpoints. */
  PUBLIC_BASE_URL?: string;
  /** Container listen port; defaults to 8080. */
  CONTAINER_PORT?: string;
  /** R2 key prefix; defaults to `shots`. */
  ARTIFACT_PREFIX?: string;
  /**
   * Trusted local-development escape hatch. When set to `1`/`true`, admin
   * endpoints accept requests even when `BURSTEL_API_TOKEN` is unset. Never set
   * this in production.
   */
  BURSTEL_DEV_INSECURE_AUTH?: string;
}

const DEFAULT_CONTAINER_PORT = 8080;

export class ShotDO extends DurableObject<Env> {
  private readonly core: ShotCore;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    const port = containerPort(env);
    const shotId = ctx.id.name ?? ctx.id.toString();
    const sql = createSqlAdapter(ctx.storage.sql);
    const container = createContainerControl(requireContainer(ctx), { port });
    const artifacts = createR2Sink(env.BUCKET as unknown as R2BucketLike);

    this.core = new ShotCore({
      shotId,
      sql,
      container,
      artifacts,
      internalToken: env.BURSTEL_INTERNAL_TOKEN ?? env.BURSTEL_API_TOKEN,
      containerEnv: {
        BURSTEL_ADDR: `:${port}`,
        BURSTEL_DATA_DIR: "/data",
      },
      artifactPrefix: env.ARTIFACT_PREFIX,
    });

    // Restore persisted state before the first request is served.
    ctx.blockConcurrencyWhile(async () => {
      await this.core.load();
    });
  }

  override async fetch(request: Request): Promise<Response> {
    return this.core.handleInternal(request);
  }
}

function containerPort(env: Env): number {
  const raw = env.CONTAINER_PORT;
  const port = raw ? Number.parseInt(raw, 10) : DEFAULT_CONTAINER_PORT;
  return Number.isFinite(port) && port > 0 ? port : DEFAULT_CONTAINER_PORT;
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

function requireContainer(ctx: DurableObjectState): ContainerLike {
  const container = ctx.container;
  if (!container) {
    return {
      running: false,
      start() {
        throw new Error("no container is configured for this Durable Object");
      },
      async destroy() {},
      getTcpPort() {
        throw new Error("no container is configured for this Durable Object");
      },
    };
  }
  return container as unknown as ContainerLike;
}
