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
import { containerEgressFor, createContainerControl } from "./container";
import type { ContainerLike } from "./container";
import { ShotCore } from "./core";
import { json } from "./http";
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
    const container = createContainerControl(requireContainer(ctx), {
      port,
      enableInternet: containerEgressFor("ShotDO"),
    });
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
    const path = new URL(request.url).pathname;
    if (path === "/internal/prewarm") {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      // Spec 033: shot_start only records the request and arms the alarm. The
      // alarm starts the analysis container, so a cancelled HTTP request can
      // never leave the prewarm half-done.
      await this.ctx.storage.put("analysis_prewarm_at", Date.now());
      await this.ctx.storage.setAlarm(Date.now());
      return json(202, { shot_id: this.ctx.id.name ?? this.ctx.id.toString(), prewarm: "scheduled" });
    }
    return this.core.handleInternal(request);
  }

  override async alarm(): Promise<void> {
    const prewarmAt = await this.ctx.storage.get<number>("analysis_prewarm_at");
    if (typeof prewarmAt === "number") {
      await this.ctx.storage.delete("analysis_prewarm_at");
      await this.core.prewarmAnalysis().catch((error) => {
        console.warn("analysis.container.prewarm.failure", {
          shot_id: this.ctx.id.name ?? this.ctx.id.toString(),
          error: error instanceof Error ? error.message : String(error),
        });
      });
    }
    await this.core.heartbeat();
  }
}

export function containerPort(env: { CONTAINER_PORT?: string }): number {
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

export function requireContainer(ctx: DurableObjectState): ContainerLike {
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
