// Worker entrypoint for burstel.
//
// The Worker receives public HTTP traffic, authenticates it, and routes each
// shot to its own named ShotDO. A singleton IngestDO coordinates the push-only
// `/ingest` token and the active shot. The ShotDO owns the Container and R2
// freeze logic; see src/app.ts, src/ingest-do.ts and src/shot-do.ts.

import { handleRequest } from "./app";
import type { Env } from "./shot-do";

export { ShotDO } from "./shot-do";
export { IngestDO } from "./ingest-do";
export type { Env } from "./shot-do";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleRequest(request, env);
  },
} satisfies ExportedHandler<Env>;
