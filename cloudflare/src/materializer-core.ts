// Materializer container controller (spec 031 D3).
//
// One CompactorDO name per materialization chunk owns one short-lived Container
// instance. The Go service downloads journal objects and uploads part files
// directly through the bucket-scoped R2 credential; this core only proxies the
// bounded /v1/materialize request and never relays object bytes.
//
// Distinct DO names (`materializer-<shot>-<part>`) are what give a shot its
// parallel materializers; the singleton `compactor-primary` name keeps the
// continuous compaction path separate.

import type { ContainerControl } from "./container";
import { HttpError } from "./errors";
import { json, readJson } from "./http";

export interface MaterializerConfig {
  containerTimeoutMs: number;
  containerKeepAliveMs: number;
}

export interface MaterializerDeps {
  container: ContainerControl;
  containerEnv?: Record<string, string>;
  internalToken?: string;
  internalReady?: boolean;
  holder: string;
  config?: Partial<MaterializerConfig>;
  log?: (message: string, fields?: Record<string, unknown>) => void;
}

/** One part request forwarded verbatim to the Go `/v1/materialize` endpoint. */
export interface MaterializePartRequest {
  shot_id: string;
  part_index: number;
  part_total: number;
  objects: Array<Record<string, unknown>>;
  excluded?: string[];
}

export class MaterializerCore {
  private readonly container: ContainerControl;
  private readonly containerEnv: Record<string, string>;
  private readonly internalToken?: string;
  private readonly internalReady: boolean;
  private readonly holder: string;
  private readonly config: MaterializerConfig;
  private readonly log: (message: string, fields?: Record<string, unknown>) => void;

  constructor(deps: MaterializerDeps) {
    this.container = deps.container;
    this.containerEnv = deps.containerEnv ?? {};
    this.internalToken = deps.internalToken;
    this.internalReady = deps.internalReady ?? true;
    this.holder = deps.holder;
    this.config = {
      containerTimeoutMs: deps.config?.containerTimeoutMs ?? 60_000,
      containerKeepAliveMs: deps.config?.containerKeepAliveMs ?? 10 * 60 * 1000,
    };
    this.log = deps.log ?? (() => undefined);
  }

  async handleInternal(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path === "/internal/status") {
      if (request.method !== "GET") return json(405, { error: "method not allowed" });
      return json(200, {
        holder: this.holder,
        running: this.container.running,
        internal_ready: this.internalReady,
      });
    }
    if (path === "/internal/materialize") {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      if (!this.internalReady) {
        return json(503, {
          error: "materializer has no distinct internal token configured",
          code: "internal_token_unconfigured",
          retryable: true,
        });
      }
      const body = await readJson<MaterializePartRequest>(request);
      if (typeof body.shot_id !== "string" || !body.shot_id) {
        return json(400, { error: "shot_id is required", code: "bad_request" });
      }
      await this.ensureContainer();
      const resp = await this.containerRequest("/v1/materialize", {
        method: "POST",
        headers: this.containerHeaders({ "content-type": "application/json" }),
        body: JSON.stringify(body),
      });
      return resp;
    }
    return json(404, { error: "not found" });
  }

  private async ensureContainer(): Promise<void> {
    if (!this.container.running) {
      this.container.start(this.containerEnv);
      await this.container.ensureReady();
      await this.container.keepAlive(this.config.containerKeepAliveMs).catch(() => undefined);
    }
  }

  private containerHeaders(extra?: Record<string, string>): Record<string, string> {
    const headers: Record<string, string> = { ...extra };
    if (this.internalToken) headers["authorization"] = `Bearer ${this.internalToken}`;
    return headers;
  }

  private async containerRequest(path: string, init: RequestInit = {}): Promise<Response> {
    const timeoutMs = Math.max(1, this.config.containerTimeoutMs);
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(
          new HttpError(
            504,
            `materializer container request ${path} timed out after ${timeoutMs}ms`,
            true,
            "container_timeout",
          ),
        );
      }, timeoutMs);
    });
    try {
      return await Promise.race([
        this.container.request(path, { ...init, signal: controller.signal }),
        deadline,
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }
}
