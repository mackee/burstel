// The per-shot Durable Object.
//
// One named ShotDO owns one Container instance (via the Durable Object
// Container API `this.ctx.container`) and one SQLite-backed lifecycle record.
// All lifecycle logic lives in ShotCore; this file is only the production
// wiring of Cloudflare bindings into that core.

import { DurableObject } from "cloudflare:workers";

import { createR2Sink } from "./artifacts";
import type { R2BucketLike } from "./artifacts";
import { resolveInternalToken } from "./auth";
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
  /** Maximum number of shots that may record concurrently; default 10. */
  MAX_CONCURRENT_RECORDING?: string;
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

    // The container's internal bearer token must be distinct from the admin
    // token and is never derived from it. In production a missing/equal token
    // fails closed; only the explicit local-development opt-in runs without it.
    const internal = resolveInternalToken(env);
    const containerEnv: Record<string, string> = {
      BURSTEL_ADDR: `:${port}`,
      BURSTEL_DATA_DIR: "/data",
    };
    if (internal.ok && internal.insecure) {
      // Forward the explicit opt-in so the recorder's `/internal/*` routes are
      // intentionally ungated too, without ever sending the admin token.
      containerEnv["BURSTEL_DEV_INSECURE_AUTH"] = "1";
    }

    this.core = new ShotCore({
      shotId,
      sql,
      container,
      artifacts,
      internalToken: internal.ok && internal.token.length > 0 ? internal.token : undefined,
      internalTokenReady: internal.ok,
      internalTokenError: internal.ok ? undefined : internal.reason,
      containerEnv,
      artifactPrefix: env.ARTIFACT_PREFIX,
      // The recording heartbeat runs on this DO's alarm.
      scheduler: { set: (atMs) => ctx.storage.setAlarm(atMs) },
    });

    // Restore persisted state before the first request is served. For a
    // recording shot this also re-applies the container inactivity timeout
    // (lost on every DO restart) and re-arms the heartbeat.
    ctx.blockConcurrencyWhile(async () => {
      await this.core.load();
    });
  }

  override async fetch(request: Request): Promise<Response> {
    return this.core.handleInternal(request);
  }

  override async alarm(): Promise<void> {
    await this.core.heartbeat();
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
