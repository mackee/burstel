// The singleton compactor coordinator Durable Object (spec 018).
//
// One named CompactorDO owns one short-lived Container instance (the
// `burstel-compact` service) and drives it over the DO Container API. All
// orchestration lives in CompactorCore; this file is only the production wiring
// of Cloudflare bindings (the R2 bucket, the IngestDO and the alarm scheduler)
// into that core.
//
// This DO is deliberately independent of `ShotDO`: it never touches shot
// storage, the legacy recording image, or the frozen `shots/*` prefix. It only
// reads `journal/*`, writes `segments/*` and records verified manifest rows in
// the IngestDO.

import { DurableObject } from "cloudflare:workers";

import type { R2BucketLike } from "./artifacts";
import { resolveInternalToken } from "./auth";
import { CompactorCore } from "./compactor-core";
import type { CompactorConfig } from "./compactor-core";
import { createContainerControl } from "./container";
import { containerPort, requireContainer } from "./shot-do";

export interface CompactorEnv extends Cloudflare.Env {
  BURSTEL_API_TOKEN?: string;
  BURSTEL_INTERNAL_TOKEN?: string;
  BURSTEL_DEV_INSECURE_AUTH?: string;
  CONTAINER_PORT?: string;
}

export class CompactorDO extends DurableObject<CompactorEnv> {
  private readonly core: CompactorCore;

  constructor(ctx: DurableObjectState, env: CompactorEnv) {
    super(ctx, env);

    const port = containerPort(env);
    const container = createContainerControl(requireContainer(ctx), { port });
    const internal = resolveInternalToken(env);
    const containerEnv: Record<string, string> = {
      BURSTEL_ADDR: `:${port}`,
      BURSTEL_COMPACT_WORK_DIR: "/work",
    };
    if (internal.ok && internal.insecure) {
      containerEnv["BURSTEL_DEV_INSECURE_AUTH"] = "1";
    }

    this.core = new CompactorCore({
      ingest: env.INGEST_DO,
      bucket: env.BUCKET as unknown as R2BucketLike,
      container,
      containerEnv,
      internalToken: internal.ok && internal.token.length > 0 ? internal.token : undefined,
      internalReady: internal.ok,
      holder: ctx.id.name ?? ctx.id.toString(),
      config: compactorConfigFromEnv(env),
      // The next bounded tick runs on this DO's alarm, which the platform
      // retries at-least-once; the fenced lease makes that safe.
      scheduler: { set: (atMs) => ctx.storage.setAlarm(atMs) },
      log: (message, fields) => console.log(message, fields ?? {}),
    });
  }

  override async fetch(request: Request): Promise<Response> {
    return this.core.handleInternal(request);
  }

  override async alarm(): Promise<void> {
    await this.core.alarm();
  }
}

function compactorConfigFromEnv(env: CompactorEnv): Partial<CompactorConfig> {
  const num = (raw: string | undefined, def: number): number => {
    if (raw === undefined || raw.trim() === "") return def;
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : def;
  };
  const bool = (raw: string | undefined): boolean => {
    const value = (raw ?? "").trim().toLowerCase();
    return value === "1" || value === "true" || value === "yes" || value === "on";
  };
  return {
    windowMs: num(env.COMPACTION_WINDOW_MS, 60_000),
    graceMs: num(env.COMPACTION_GRACE_MS, 90_000),
    retentionMs: num(env.COMPACTION_RETENTION_MS, 24 * 60 * 60 * 1000),
    gcEnabled: bool(env.COMPACTION_GC),
    batchObjects: num(env.COMPACTION_BATCH_OBJECTS, 2048),
    batchBytes: num(env.COMPACTION_BATCH_BYTES, 32 * 1024 * 1024),
    maxBatchesPerTick: num(env.COMPACTION_MAX_BATCHES, 4),
    tickBudgetMs: num(env.COMPACTION_TICK_BUDGET_MS, 10 * 60 * 1000),
    scheduleMs: num(env.COMPACTION_SCHEDULE_MS, 120_000),
    leaseTtlMs: num(env.COMPACTION_LEASE_TTL_MS, 15 * 60 * 1000),
  };
}
