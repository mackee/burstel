// Remote MCP endpoint (Streamable HTTP, JSON responses).
//
// The official `@modelcontextprotocol/sdk` server is built per request because
// the Worker is stateless: there is no place to keep an in-memory session
// between isolates. Tools are thin wrappers over the same per-shot ShotDO
// boundary the REST API uses, so MCP callers are authenticated and scoped the
// same way.
//
// Spec 013, Part B: `/mcp` accepts the env admin credential and any registered
// per-person token. `admin` and `member` both get the full *operational* tool
// set on every shot; only `admin` may call the registry tools
// (`token_mint`/`token_list`/`token_revoke`), and `member` is denied before any
// DO work.

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { z } from "zod";

import type { AuthPrincipal } from "./auth-core";
import { resolvePrincipal, isRegistryAdmin } from "./authz";
import type { AppEnv } from "./env";
import { forbidden, badRequest, unauthorized } from "./errors";
import { errorMessage } from "./errors";
import { errorResponse } from "./http";
import { newShotId } from "./ids";
import { createTools } from "./tools";

export const MCP_SERVER_NAME = "burstel";
export const MCP_SERVER_VERSION = "0.5.0";

interface ToolOutput {
  [key: string]: unknown;
  content: Array<{ type: "text"; text: string }>;
  structuredContent?: Record<string, unknown>;
  isError?: boolean;
}

function ok(data: object): ToolOutput {
  return {
    content: [{ type: "text", text: JSON.stringify(data) }],
    structuredContent: data as Record<string, unknown>,
  };
}

function fail(error: unknown): ToolOutput {
  return {
    content: [{ type: "text", text: errorMessage(error) }],
    isError: true,
  };
}

/** A stable, machine-readable 403 tool error for a denied registry call. */
function failForbidden(): ToolOutput {
  return { content: [{ type: "text", text: `403 ${forbidden().message}` }], isError: true };
}

function buildServer(env: AppEnv, origin: string, principal: AuthPrincipal): McpServer {
  const server = new McpServer({ name: MCP_SERVER_NAME, version: MCP_SERVER_VERSION });
  const tools = createTools({
    namespace: env.SHOT_DO,
    ingest: env.INGEST_DO,
    auth: env.AUTH_DO,
    origin,
    publicBaseUrl: env.PUBLIC_BASE_URL,
    bucket: env.BUCKET,
  });
  const isAdmin = isRegistryAdmin(principal);

  server.registerTool(
    "shots_list",
    {
      title: "List shots",
      description:
        "List recordings (newest first) with state, attribution-only owner, timestamps, size/SHA-256 and counters. Optional owner filter and limit/offset pagination.",
      inputSchema: {
        owner: z.string().optional().describe("Attribution-only filter; never an authorization boundary"),
        limit: z.number().int().positive().max(200).optional(),
        offset: z.number().int().nonnegative().optional(),
      },
    },
    async ({ owner, limit, offset }) => {
      try {
        return ok(await tools.list({ owner, limit, offset }));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_start",
    {
      title: "Start a shot",
      description:
        "Create a new recording shot and return its id plus the stable OTLP/HTTP ingest URL (legacy mode). With mode=window, open a server-clock logical time window [start,end) marker instead; ingestion stays continuous and no per-shot token is issued. metadata may carry the commit SHA, benchmark/scenario and a note; owner is an optional attribution label.",
      inputSchema: {
        metadata: z.record(z.string(), z.any()).optional(),
        owner: z.string().optional().describe("Attribution-only owner label; never an authorization boundary"),
        mode: z.enum(["recording", "window"]).optional().describe("recording (default) or window"),
        name: z.string().optional().describe("Window marker label (mode=window)"),
        instances: z.array(z.string()).optional().describe("Instance subset (mode=window); default all"),
      },
    },
    async ({ metadata, owner, mode, name, instances }) => {
      try {
        return ok(await tools.start((metadata ?? {}) as Record<string, unknown>, owner, { mode, name, instances }));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_create",
    {
      title: "Create an externally marked window",
      description:
        "Create a backdated, externally supplied complete window shot with half-open [started_at, ended_at) UTC instants. Timestamps are validated (ordered, bounded, retained) and retained as untrusted client-clock values. Returns duration and coverage.",
      inputSchema: {
        name: z.string().optional(),
        started_at: z.string().describe("ISO-8601 inclusive start (UTC)"),
        ended_at: z.string().describe("ISO-8601 exclusive end (UTC)"),
        instances: z.array(z.string()).optional(),
        metadata: z.record(z.string(), z.any()).optional(),
      },
    },
    async ({ name, started_at, ended_at, instances, metadata }) => {
      try {
        const started = Date.parse(started_at);
        const ended = Date.parse(ended_at);
        if (!Number.isFinite(started) || !Number.isFinite(ended)) {
          throw badRequest("started_at and ended_at must be ISO-8601 timestamps");
        }
        const owner = undefined;
        const shotId = newShotId();
        const record = await tools.createWindow({
          shot_id: shotId,
          owner,
          name,
          started_at_ms: started,
          ended_at_ms: ended,
          instances,
          metadata: (metadata ?? {}) as Record<string, unknown>,
        });
        return ok(record);
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "instance_create",
    {
      title: "Create or get a named instance",
      description:
        "Create/upsert a named logical instance (e.g. web-1, db-1) and return its immutable instance_id. admin|member.",
      inputSchema: { name: z.string().describe("Instance name: lowercase alphanumerics and hyphens") },
    },
    async ({ name }) => {
      try {
        return ok(await tools.createInstance(name));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "instance_list",
    {
      title: "List instances",
      description: "List named instances with metadata and token counts (never secrets). admin|member.",
      inputSchema: {},
    },
    async () => {
      try {
        return ok(await tools.listInstances());
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "instances_list",
    {
      title: "List instances (spec 016 alias)",
      description: "List named instances with metadata and token counts (never secrets). admin|member.",
      inputSchema: {},
    },
    async () => {
      try {
        return ok(await tools.listInstances());
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "instance_token_mint",
    {
      title: "Issue or rotate an instance token",
      description:
        "Issue a 256-bit push-only OTLP instance token valid for exactly 24h (disclosed once). With rotate, the previous token for this instance is revoked after a bounded overlap; other instances are untouched. admin|member.",
      inputSchema: {
        name: z.string().describe("Instance name"),
        rotate: z.boolean().optional().describe("Revoke the previous active token(s) with a bounded overlap"),
        overlap_seconds: z.number().int().positive().optional().describe("Rotation overlap seconds (bounded)"),
      },
    },
    async ({ name, rotate, overlap_seconds }) => {
      try {
        return ok(await tools.issueInstanceToken(name, { rotate, overlap_seconds }));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "instance_token_issue",
    {
      title: "Issue or rotate an instance token",
      description:
        "Issue a 256-bit push-only OTLP instance token valid for exactly 24h (disclosed once). With rotate, the previous token for this instance is revoked after a bounded overlap; other instances are untouched. admin|member.",
      inputSchema: {
        name: z.string().describe("Instance name"),
        rotate: z.boolean().optional().describe("Revoke the previous active token(s) with a bounded overlap"),
        overlap_seconds: z.number().int().positive().optional().describe("Rotation overlap seconds (bounded)"),
      },
    },
    async ({ name, rotate, overlap_seconds }) => {
      try {
        return ok(await tools.issueInstanceToken(name, { rotate, overlap_seconds }));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "instance_token_list",
    {
      title: "List instance token metadata",
      description: "List an instance's token metadata (id, timestamps, revoked). Values are never returned. admin|member.",
      inputSchema: { name: z.string() },
    },
    async ({ name }) => {
      try {
        return ok(await tools.listInstanceTokens(name));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "instance_token_revoke",
    {
      title: "Revoke an instance token",
      description: "Revoke one instance token immediately without rotating the others. admin|member.",
      inputSchema: { name: z.string(), id: z.string().describe("Token id from instance_token_list") },
    },
    async ({ name, id }) => {
      try {
        return ok(await tools.revokeInstanceToken(name, id));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_stop",
    {
      title: "Stop and finalize a shot",
      description:
        "Stop a shot. A recording shot is frozen to R2 (state, size, SHA-256). A logical window marker is finalized with half-open [start,end) semantics; ended_at may backdate the end.",
      inputSchema: {
        shot_id: z.string().describe("Shot id returned by shot_start or shot_create"),
        ended_at: z.string().optional().describe("ISO-8601 exclusive end for a window marker"),
      },
    },
    async ({ shot_id, ended_at }) => {
      try {
        const endedMs = ended_at ? Date.parse(ended_at) : undefined;
        if (ended_at && !Number.isFinite(endedMs)) throw badRequest("ended_at must be an ISO-8601 timestamp");
        return ok(await tools.stop(shot_id, endedMs));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_get",
    {
      title: "Get shot status",
      description:
        "Return one shot. A recording shot returns lifecycle state, metadata, counters and freeze info; a logical window shot returns its half-open window, provenance, coverage and query watermark.",
      inputSchema: { shot_id: z.string() },
    },
    async ({ shot_id }) => {
      try {
        return ok(await tools.get(shot_id));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_query",
    {
      title: "Query a frozen shot",
      description:
        "Run one constrained read-only SELECT against a frozen shot snapshot. Only the tables schema_meta, otlp_batches, spans, log_records and metric_points are queryable; a hard row limit, timeout and result cap apply.",
      inputSchema: {
        shot_id: z.string(),
        sql: z.string().describe("A single SELECT or WITH ... SELECT statement"),
        max_rows: z.number().int().positive().max(1000).optional(),
      },
    },
    async ({ shot_id, sql, max_rows }) => {
      try {
        return ok(await tools.query(shot_id, sql, max_rows));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_analyze",
    {
      title: "Analyze a frozen shot",
      description:
        "Run SQL-backed analytical summaries: overview, per-endpoint latency p50/p95/p99, slow spans, errors, DB operations, log correlation and metrics.",
      inputSchema: {
        shot_id: z.string(),
        sections: z.array(z.string()).optional(),
        top_n: z.number().int().positive().max(1000).optional(),
      },
    },
    async ({ shot_id, sections, top_n }) => {
      try {
        return ok(await tools.analyze(shot_id, sections, top_n));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_schema",
    {
      title: "Describe the queryable schema",
      description:
        "Return the documented tables, columns and analysis templates, plus the enforced query-safety policy.",
      inputSchema: { shot_id: z.string() },
    },
    async ({ shot_id }) => {
      try {
        return ok(await tools.schema(shot_id));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_compare",
    {
      title: "Compare two frozen shots",
      description:
        "Compare two frozen shots with SQL-derived latency, error and volume deltas, including sample sizes and explicit disclosure of missing data.",
      inputSchema: {
        left_shot_id: z.string(),
        right_shot_id: z.string(),
        top_n: z.number().int().positive().max(1000).optional(),
      },
    },
    async ({ left_shot_id, right_shot_id, top_n }) => {
      try {
        return ok(await tools.compare(left_shot_id, right_shot_id, top_n));
      } catch (error) {
        return fail(error);
      }
    },
  );

  // ------------------------------------------------------- registry tools
  // Admin only. A member token is denied here before any DO work.

  server.registerTool(
    "token_mint",
    {
      title: "Mint a per-person token",
      description:
        "Admin only. Mint a scoped per-person token (scope admin|member, attribution-only owner); the value is disclosed exactly once.",
      inputSchema: {
        scope: z.enum(["admin", "member"]),
        owner: z.string().optional().describe("Attribution/audit label"),
        expires_at: z.string().optional().describe("Absolute ISO-8601 expiry"),
        ttl_seconds: z.number().int().positive().optional().describe("Relative expiry in seconds"),
      },
    },
    async ({ scope, owner, expires_at, ttl_seconds }) => {
      if (!isAdmin) return failForbidden();
      try {
        return ok(await tools.mintToken({ scope, owner, expires_at, ttl_seconds }));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "token_list",
    {
      title: "List registry tokens",
      description: "Admin only. List token metadata (id, scope, owner, timestamps, revoked). Values are never returned.",
      inputSchema: {},
    },
    async () => {
      if (!isAdmin) return failForbidden();
      try {
        return ok(await tools.listTokens());
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "token_revoke",
    {
      title: "Revoke a token",
      description: "Admin only. Revoke a registered token immediately so that person is cut off without rotating everyone.",
      inputSchema: { id: z.string().describe("Token id from token_list") },
    },
    async ({ id }) => {
      if (!isAdmin) return failForbidden();
      try {
        return ok(await tools.revokeToken(id));
      } catch (error) {
        return fail(error);
      }
    },
  );

  return server;
}

/** Handles a single MCP HTTP request. Resolves the principal when not supplied. */
export async function handleMcp(
  request: Request,
  env: AppEnv,
  principal?: AuthPrincipal,
): Promise<Response> {
  let caller = principal;
  if (!caller) {
    const auth = await resolvePrincipal(request, env);
    if (!auth.ok) return errorResponse(unauthorized());
    caller = auth.principal;
  }
  const origin = new URL(request.url).origin;
  const server = buildServer(env, origin, caller);
  const transport = new WebStandardStreamableHTTPServerTransport({ enableJsonResponse: true });
  await server.connect(transport);
  return transport.handleRequest(request);
}
