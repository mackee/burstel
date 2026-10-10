// Worker entrypoint for burstel.
//
// The Worker receives public HTTP traffic, authenticates it, and routes each
// shot to its own named ShotDO. A singleton IngestDO coordinates per-shot
// push-only ingest tokens (token -> shot), the shot registry and the
// concurrent-recording cap. The ShotDO owns the Container and R2 freeze logic;
// see src/app.ts, src/ingest-do.ts and src/shot-do.ts.
//
// Spec 018 adds the singleton CompactorDO: a short-lived Container coordinator
// that turns the durable raw journal into verified R2 segments and publishes
// them to the IngestDO, driven by a Cron `scheduled` trigger and its own DO
// alarm. It never touches ShotDO storage or the legacy recording image.

import { handleRequest } from "./app";
import { COMPACTOR_DO_NAME } from "./compactor-core";
import type { Env } from "./shot-do";

export { ShotDO } from "./shot-do";
export { IngestDO } from "./ingest-do";
export { AuthDO } from "./auth-do";
export { CompactorDO } from "./compactor-do";
export type { Env } from "./shot-do";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleRequest(request, env);
  },

  /**
   * Cron backstop for the compaction coordinator. The DO's own alarm is the
   * primary scheduler; this simply kicks a bounded tick so a lost alarm cannot
   * stall compaction indefinitely. The fenced lease makes overlapping triggers
   * safe (at most one active compaction per partition).
   */
  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    const compactor = env.COMPACTOR_DO;
    if (!compactor) return;
    const stub = compactor.getByName(COMPACTOR_DO_NAME);
    ctx.waitUntil(
      stub
        .fetch(new Request("https://compactor.internal/internal/trigger", { method: "POST" }))
        .then(() => undefined)
        .catch(() => undefined),
    );
  },
} satisfies ExportedHandler<Env>;
