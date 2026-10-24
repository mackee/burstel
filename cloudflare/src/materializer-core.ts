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
  /** Base request timeout for one part (spec 034, MATERIALIZE_PART_TIMEOUT_MS). */
  containerTimeoutMs: number;
  /** Extra timeout per MiB of part input (spec 034 timeout scaling). */
  timeoutPerMiBMs: number;
  /** Hard ceiling for a scaled part timeout. */
  maxTimeoutMs: number;
  /** Spec 033: keep a finished pool member warm for the next shot. */
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

export const MATERIALIZE_PART_TIMEOUT_MS = 120_000;
export const MATERIALIZE_PART_TIMEOUT_PER_MIB_MS = 500;
export const MATERIALIZE_PART_MAX_TIMEOUT_MS = 600_000;

/**
 * Part request timeout: a fixed base plus a per-MiB allowance, capped. A part is
 * sized by rows (spec 034 R4) up to a 1 GiB ceiling, so a fixed 60 s timeout can
 * never cover the largest legal part.
 */
export function materializerPartTimeoutMs(
  bytes: number,
  baseMs: number = MATERIALIZE_PART_TIMEOUT_MS,
  perMiBMs: number = MATERIALIZE_PART_TIMEOUT_PER_MIB_MS,
  maxMs: number = MATERIALIZE_PART_MAX_TIMEOUT_MS,
): number {
  const base = Math.max(1, Math.floor(Number.isFinite(baseMs) ? baseMs : MATERIALIZE_PART_TIMEOUT_MS));
  const perMiB = Math.max(0, Math.floor(Number.isFinite(perMiBMs) ? perMiBMs : 0));
  const cap = Math.max(base, Math.floor(Number.isFinite(maxMs) ? maxMs : MATERIALIZE_PART_MAX_TIMEOUT_MS));
  const mib = Math.max(0, Math.floor(Number.isFinite(bytes) ? bytes : 0)) / (1024 * 1024);
  return Math.min(cap, base + Math.ceil(mib) * perMiB);
}

function partBytes(objects: Array<Record<string, unknown>> | undefined): number {
  if (!Array.isArray(objects)) return 0;
  let total = 0;
  for (const object of objects) {
    const size = object?.["size"];
    if (typeof size === "number" && Number.isFinite(size) && size > 0) total += size;
  }
  return total;
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
      containerTimeoutMs: deps.config?.containerTimeoutMs ?? MATERIALIZE_PART_TIMEOUT_MS,
      timeoutPerMiBMs: deps.config?.timeoutPerMiBMs ?? MATERIALIZE_PART_TIMEOUT_PER_MIB_MS,
      maxTimeoutMs: deps.config?.maxTimeoutMs ?? MATERIALIZE_PART_MAX_TIMEOUT_MS,
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
        part_timeout_ms: this.config.containerTimeoutMs,
        part_timeout_per_mib_ms: this.config.timeoutPerMiBMs,
        part_max_timeout_ms: this.config.maxTimeoutMs,
      });
    }
    if (path === "/internal/prewarm") {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      // Started by the MaterializeDO alarm, never by the shot_start request.
      await this.ensureContainer();
      return json(200, { holder: this.holder, running: this.container.running, prewarmed: true });
    }
    if (path === "/internal/keepalive") {
      if (request.method !== "POST") return json(405, { error: "method not allowed" });
      if (this.container.running) {
        await this.container.keepAlive(this.config.containerKeepAliveMs).catch(() => undefined);
      }
      return json(200, { holder: this.holder, running: this.container.running, keepalive_ms: this.config.containerKeepAliveMs });
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
      const bytes = partBytes(body.objects);
      const timeoutMs = materializerPartTimeoutMs(
        bytes,
        this.config.containerTimeoutMs,
        this.config.timeoutPerMiBMs,
        this.config.maxTimeoutMs,
      );
      this.log("materialize.part.timeout", {
        shot_id: body.shot_id,
        part_index: body.part_index,
        pool_member: this.holder,
        objects: Array.isArray(body.objects) ? body.objects.length : 0,
        input_bytes: bytes,
        timeout_ms: timeoutMs,
      });
      const resp = await this.containerRequest(
        "/v1/materialize",
        {
          method: "POST",
          headers: this.containerHeaders({ "content-type": "application/json" }),
          body: JSON.stringify(body),
        },
        timeoutMs,
      );
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

  private async containerRequest(path: string, init: RequestInit = {}, timeoutOverrideMs?: number): Promise<Response> {
    const timeoutMs = Math.max(1, Math.floor(timeoutOverrideMs ?? this.config.containerTimeoutMs));
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
