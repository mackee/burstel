// Remote MCP endpoint (Streamable HTTP, JSON responses).
//
// The official `@modelcontextprotocol/sdk` server is built per request because
// the Worker is stateless: there is no place to keep an in-memory session
// between isolates. Tools are thin wrappers over the same per-shot ShotDO
// boundary the REST API uses, so MCP callers are authenticated and scoped the
// same way.

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { z } from "zod";

import { authenticate } from "./auth";
import type { AppEnv } from "./env";
import { unauthorized } from "./errors";
import { errorMessage } from "./errors";
import { errorResponse } from "./http";
import { createTools } from "./tools";

export const MCP_SERVER_NAME = "burstel";
export const MCP_SERVER_VERSION = "0.3.0";

interface ToolOutput {
  [key: string]: unknown;
  content: Array<{ type: "text"; text: string }>;
  structuredContent?: Record<string, unknown>;
  isError?: boolean;
}

function ok(data: Record<string, unknown>): ToolOutput {
  return {
    content: [{ type: "text", text: JSON.stringify(data) }],
    structuredContent: data,
  };
}

function fail(error: unknown): ToolOutput {
  return {
    content: [{ type: "text", text: errorMessage(error) }],
    isError: true,
  };
}

function buildServer(env: AppEnv, origin: string): McpServer {
  const server = new McpServer({ name: MCP_SERVER_NAME, version: MCP_SERVER_VERSION });
  const tools = createTools({ namespace: env.SHOT_DO, origin, publicBaseUrl: env.PUBLIC_BASE_URL });

  server.registerTool(
    "shot_start",
    {
      title: "Start a shot",
      description:
        "Create a new recording shot and return its id plus OTLP/HTTP ingest endpoints. metadata may carry the commit SHA, benchmark/scenario and a note.",
      inputSchema: { metadata: z.record(z.string(), z.any()).optional() },
    },
    async ({ metadata }) => {
      try {
        return ok(await tools.start((metadata ?? {}) as Record<string, unknown>));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_stop",
    {
      title: "Stop and freeze a shot",
      description:
        "Stop a recording shot, freeze its DuckDB artifact to R2 and return the state, size and SHA-256.",
      inputSchema: { shot_id: z.string().describe("Shot id returned by shot_start") },
    },
    async ({ shot_id }) => {
      try {
        return ok(await tools.stop(shot_id));
      } catch (error) {
        return fail(error);
      }
    },
  );

  server.registerTool(
    "shot_get",
    {
      title: "Get shot status",
      description: "Return lifecycle state, metadata, counters and freeze/artifact info for one shot.",
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

  return server;
}

/** Handles a single MCP HTTP request after authentication. */
export async function handleMcp(request: Request, env: AppEnv): Promise<Response> {
  if (!authenticate(request, env.BURSTEL_API_TOKEN)) {
    return errorResponse(unauthorized());
  }
  const origin = new URL(request.url).origin;
  const server = buildServer(env, origin);
  const transport = new WebStandardStreamableHTTPServerTransport({ enableJsonResponse: true });
  await server.connect(transport);
  return transport.handleRequest(request);
}
