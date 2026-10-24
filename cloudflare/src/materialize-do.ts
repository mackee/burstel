// Durable materialization coordinator (spec 032).
//
// One `MaterializeDO` is addressed by shot id (`MATERIALIZE_DO.getByName(shot)`).
// It owns the durable run state, the alarm schedule, part leases/retries and the
// terminal `complete`/`failed` result. A Worker request only records a run and
// polls the ControlDO record; cancelling that request cannot lose the run.
//
// The real algorithm lives in MaterializeDriver (pure, unit-tested). This file
// is only the production wiring of DO storage/alarms and the ControlDO /
// CompactorDO bindings.

import { DurableObject } from "cloudflare:workers";

import { DO_ORIGIN } from "./env";
import type { CompactorNamespaceLike, IngestNamespaceLike } from "./env";
import { HttpError } from "./errors";
import { json, readJson } from "./http";
import type { WindowShotRecord } from "./ingest-core";
import { MaterializeDriver } from "./materialize-driver";
import type { MaterializePrewarmState, MaterializeRunState } from "./materialize-driver";

export interface MaterializeEnv {
  INGEST_DO?: unknown;
  COMPACTOR_DO?: unknown;
  MATERIALIZE_PARALLELISM?: string;
  MATERIALIZE_MAX_INSTANCES?: string;
  MATERIALIZE_MAX_ATTEMPTS?: string;
  MATERIALIZE_LEASE_TTL_MS?: string;
  MATERIALIZE_RETRY_MS?: string;
  MATERIALIZE_HEARTBEAT_MS?: string;
  MATERIALIZE_WAIT_MS?: string;
  /** Spec 033: pool keepalive and no-capacity backoff. */
  MATERIALIZE_KEEPALIVE_MS?: string;
  MATERIALIZE_CAPACITY_BACKOFF_MS?: string;
  MATERIALIZE_PARTS_PER_MEMBER?: string;
  LATE_WRITE_LIMIT_MS?: string;
  COMPACTION_CONTAINER_TIMEOUT_MS?: string;
  [key: string]: unknown;
}

export class MaterializeDO extends DurableObject<MaterializeEnv> {
  private readonly driver: MaterializeDriver;

  constructor(ctx: DurableObjectState, env: MaterializeEnv) {
    super(ctx, env);
    const shotId = ctx.id.name ?? ctx.id.toString();
    this.driver = new MaterializeDriver({
      shotId,
      ingest: env.INGEST_DO as unknown as IngestNamespaceLike,
      compactor: env.COMPACTOR_DO as unknown as CompactorNamespaceLike | undefined,
      store: {
        load: async () => (await ctx.storage.get<MaterializeRunState>("run")) ?? null,
        save: async (run) => {
          await ctx.storage.put("run", run);
        },
      },
      // Spec 033: shot_start records a prewarm request; only the alarm starts
      // the pool containers, so a cancelled request cannot lose the work.
      prewarmStore: {
        load: async () => (await ctx.storage.get<MaterializePrewarmState>("prewarm")) ?? null,
        save: async (state) => {
          if (state === null) await ctx.storage.delete("prewarm");
          else await ctx.storage.put("prewarm", state);
        },
      },
      scheduler: { set: (atMs) => ctx.storage.setAlarm(atMs) },
      now: () => Date.now(),
      config: {
        parallelism: int(env.MATERIALIZE_PARALLELISM, 3),
        maxInstances: int(env.MATERIALIZE_MAX_INSTANCES, 4),
        maxAttempts: int(env.MATERIALIZE_MAX_ATTEMPTS, 3),
        leaseTtlMs: int(env.MATERIALIZE_LEASE_TTL_MS, 120_000),
        retryDelayMs: int(env.MATERIALIZE_RETRY_MS, 5_000),
        heartbeatMs: int(env.MATERIALIZE_HEARTBEAT_MS, 30_000),
        lateWriteLimitMs: int(env.LATE_WRITE_LIMIT_MS, 10_000),
        keepAliveMs: int(env.MATERIALIZE_KEEPALIVE_MS, 10 * 60 * 1000),
        capacityBackoffMs: int(env.MATERIALIZE_CAPACITY_BACKOFF_MS, 10_000),
        partsPerMember: int(env.MATERIALIZE_PARTS_PER_MEMBER, 2),
      },
      log: (message, fields) => console.log(message, fields ?? {}),
    });
  }

  override async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path === "/internal/status") {
      if (request.method !== "GET") return json(405, { error: "method not allowed" });
      const run = await this.driver.status();
      return json(200, { shot_id: this.driver.shotId, run });
    }
    if (path === "/internal/prewarm") {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      // Only arms the alarm; the alarm runs `MaterializeDriver.prewarm`.
      await this.driver.schedulePrewarm();
      return json(202, { shot_id: this.driver.shotId, prewarm: "scheduled" });
    }
    if (
      path === "/internal/materialize" ||
      path === "/internal/start" ||
      path === "/internal/restart"
    ) {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      let body: { shot_id?: unknown; force?: unknown; restart?: unknown };
      try {
        body = await readJson(request);
      } catch (error) {
        return json(400, { error: error instanceof Error ? error.message : String(error), code: "bad_request" });
      }
      const shotId = typeof body.shot_id === "string" && body.shot_id !== "" ? body.shot_id : this.driver.shotId;
      if (shotId !== this.driver.shotId) {
        return json(400, { error: "shot_id does not match this coordinator", code: "bad_request" });
      }
      const record = await this.loadShot(shotId);
      if (!record) return json(404, { error: "unknown shot", code: "not_found" });
      try {
        const run = await this.driver.schedule(record, {
          force: body.force === true,
          restart: path === "/internal/restart" || body.restart === true,
        });
        return json(202, {
          shot_id: run.shot_id,
          run_id: run.run_id,
          state: run.state,
          scheduled_at: new Date(run.scheduled_at).toISOString(),
          parts_total: run.parts_total,
          parts_done: run.results.length,
        });
      } catch (error) {
        if (error instanceof HttpError) {
          return json(error.status, { error: error.message, code: error.code, retryable: error.retryable });
        }
        throw error;
      }
    }
    return json(404, { error: "not found" });
  }

  override async alarm(): Promise<void> {
    await this.driver.alarm();
  }

  /** Reads the authoritative shot record from the ControlDO. */
  private async loadShot(shotId: string): Promise<WindowShotRecord | null> {
    const ingest = (this.env as unknown as { INGEST_DO: IngestNamespaceLike }).INGEST_DO;
    const resp = await ingest.getByName("ingest-primary").fetch(
      new Request(`${DO_ORIGIN}/internal/shot/get`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ shot_id: shotId }),
      }),
    );
    if (resp.status === 404) return null;
    if (resp.status < 200 || resp.status >= 300) {
      throw new Error(`shot lookup failed with status ${resp.status}`);
    }
    return (await resp.json()) as WindowShotRecord;
  }
}

function int(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw.trim() === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}
