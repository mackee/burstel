// Shot lifecycle core: the race-safe state machine that a ShotDO delegates to.
//
// It is deliberately free of Cloudflare-specific globals so the exact same
// logic runs in production (over DO SQLite storage + a real container + R2) and
// in unit tests (over bun:sqlite + fake container + fake R2).
//
// State machine:
//
//   created --start--> recording --stop--> stopping --verified R2--> frozen
//                                  \-- any error --> failed (retryable; the
//                                                   container is NOT destroyed
//                                                   so the only copy of the
//                                                   ephemeral DuckDB survives)

import type { ArtifactSink } from "./artifacts";
import type { ContainerControl } from "./container";
import { emptyCounters, ShotStore, SIGNALS } from "./store";
import type { Counters, FreezeMetadata, ShotState, Signal, StateRow, SyncSql } from "./store";
import {
  conflict,
  errorMessage,
  HttpError,
  internalTokenUnconfigured,
  notFound,
  notRecording,
  shotStopping,
  unavailable,
} from "./errors";
import { errorResponse, json, readJson } from "./http";

export const ARTIFACT_NAME = "telemetry.duckdb";
export const METADATA_NAME = "metadata.json";

export interface ShotInfo {
  shot_id: string;
  state: ShotState;
  created_at: string;
  started_at?: string;
  stopped_at?: string;
  metadata: Record<string, unknown>;
  counters: Counters;
  size_bytes?: number;
  sha256?: string;
  artifact?: string;
  freeze?: FreezeMetadata;
  otlp_path: string;
  last_error?: string;
}

export interface ShotResult {
  shot_id: string;
  state: ShotState;
  created_at: string;
  started_at?: string;
  stopped_at?: string;
  size_bytes?: number;
  sha256?: string;
  artifact?: string;
  freeze?: FreezeMetadata;
  counters: Counters;
}

export interface ShotCoreDeps {
  shotId: string;
  sql: SyncSql;
  container: ContainerControl;
  artifacts: ArtifactSink;
  /** Injectable clock (epoch ms) for deterministic tests. */
  now?: () => number;
  /** Bearer token for the container's `/internal/*` endpoints. */
  internalToken?: string;
  /**
   * Whether a usable, distinct internal token (or explicit local insecure dev)
   * is configured. When false the core fails closed on every operation that
   * would boot or talk to the container.
   */
  internalTokenReady?: boolean;
  /** Why the internal token is unusable, for a stable failure code. */
  internalTokenError?: "missing" | "not_distinct";
  /** Extra environment for the container process. */
  containerEnv?: Record<string, string>;
  /** R2 key prefix; defaults to `shots`. */
  artifactPrefix?: string;
  drainTimeoutMs?: number;
  /** Hard row cap forwarded to the analysis container; defaults to 1000. */
  analysisMaxRows?: number;
  /** Default top-N for analysis sections; defaults to 20. */
  analysisTopN?: number;
}

const DEFAULT_DRAIN_TIMEOUT_MS = 60_000;

export class ShotCore {
  readonly shotId: string;
  private readonly store: ShotStore;
  private readonly container: ContainerControl;
  private readonly artifacts: ArtifactSink;
  private readonly now: () => number;
  private readonly internalToken?: string;
  private readonly internalTokenReady: boolean;
  private readonly internalTokenError: "missing" | "not_distinct";
  private readonly extraEnv: Record<string, string>;
  private readonly prefix: string;
  private readonly drainTimeoutMs: number;
  private readonly analysisMaxRows: number;
  private readonly analysisTopN: number;

  private counters: Counters = emptyCounters();
  private inFlight = 0;
  private stopRequested = false;
  private loaded = false;
  private loading?: Promise<void>;
  /** restore id -> verified sha256 already loaded in the analysis container. */
  private readonly analysisRestored = new Map<string, string>();
  /** Serialises lifecycle transitions (create/stop). */
  private transition: Promise<unknown> = Promise.resolve();

  constructor(deps: ShotCoreDeps) {
    this.shotId = deps.shotId;
    this.store = new ShotStore(deps.sql);
    this.container = deps.container;
    this.artifacts = deps.artifacts;
    this.now = deps.now ?? (() => Date.now());
    this.internalToken = deps.internalToken;
    this.internalTokenReady = deps.internalTokenReady ?? true;
    this.internalTokenError = deps.internalTokenError ?? "missing";
    this.extraEnv = deps.containerEnv ?? {};
    this.prefix = deps.artifactPrefix ?? "shots";
    this.drainTimeoutMs = deps.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
    this.analysisMaxRows = deps.analysisMaxRows ?? 1000;
    this.analysisTopN = deps.analysisTopN ?? 20;
  }

  /** Creates the schema and restores persisted state. */
  async load(): Promise<void> {
    this.store.migrate();
    this.counters = this.store.counters(this.shotId);
    this.loaded = true;
  }

  async ensureLoaded(): Promise<void> {
    if (this.loaded) return;
    this.loading ??= this.load();
    await this.loading;
  }

  // ---------------------------------------------------------------- create

  async create(metadata: Record<string, unknown> = {}): Promise<ShotInfo> {
    await this.ensureLoaded();
    this.requireInternalAuth();
    return this.exclusive(async () => {
      const metadataJSON = stableStringify(metadata);
      let row = this.store.get(this.shotId);

      if (row) {
        if (row.state === "frozen" || row.state === "recording") {
          if (row.metadata_json === metadataJSON) {
            return this.infoFromRow(row);
          }
          throw conflict(`shot ${this.shotId} already exists in state ${row.state}`);
        }
        if (row.state === "stopping") {
          throw conflict(`shot ${this.shotId} is stopping`);
        }
        if (row.state === "failed" && !this.retryable(row, "start")) {
          throw conflict(`shot ${this.shotId} is failed: ${row.last_error ?? "unknown error"}`);
        }
        if (row.metadata_json !== metadataJSON) {
          throw conflict("metadata does not match the existing shot");
        }
      } else {
        this.store.create(this.shotId, metadataJSON, this.now());
        row = this.store.get(this.shotId);
        if (!row) throw new Error("failed to persist new shot");
      }
      if (row.state === "created" && row.metadata_json !== metadataJSON) {
        this.store.update(this.shotId, { metadata_json: metadataJSON });
      }

      await this.startLocked(metadata);
      return this.statusUnlocked();
    });
  }

  private async startLocked(metadata: Record<string, unknown>): Promise<void> {
    try {
      this.container.start(this.containerEnvironment());
      await this.container.ensureReady();
    } catch (error) {
      this.markFailed("start", error);
      throw new HttpError(503, `container start failed: ${errorMessage(error)}`, true, "container_start");
    }

    try {
      const resp = await this.container.request("/internal/start", {
        method: "POST",
        headers: this.internalHeaders({ "content-type": "application/json" }),
        body: JSON.stringify({ shot_id: this.shotId, metadata }),
      });
      if (resp.status !== 200) {
        const detail = await resp.text().catch(() => "");
        throw new Error(`recorder start returned ${resp.status}${detail ? `: ${detail}` : ""}`);
      }
      await resp.body?.cancel().catch(() => undefined);
    } catch (error) {
      this.markFailed("start", error);
      throw new HttpError(503, `recorder start failed: ${errorMessage(error)}`, true, "recorder_start");
    }

    this.store.update(this.shotId, {
      state: "recording",
      started_at: this.now(),
      last_error: null,
      freeze_json: "{}",
    });
  }

  // ---------------------------------------------------------------- status

  async status(): Promise<ShotInfo> {
    await this.ensureLoaded();
    const row = this.store.get(this.shotId);
    if (!row) throw notFound(`unknown shot ${this.shotId}`);
    if (row.state === "recording" && this.container.running) {
      await this.refreshCountersFromGo().catch(() => undefined);
      const refreshed = this.store.get(this.shotId);
      if (refreshed) return this.infoFromRow(refreshed);
    }
    return this.infoFromRow(row);
  }

  private async statusUnlocked(): Promise<ShotInfo> {
    const row = this.store.get(this.shotId);
    if (!row) throw notFound(`unknown shot ${this.shotId}`);
    return this.infoFromRow(row);
  }

  // ---------------------------------------------------------------- ingest

  async ingest(signal: Signal, request: Request): Promise<Response> {
    await this.ensureLoaded();
    this.requireInternalAuth();
    // Both rejections below happen before anything is forwarded to the
    // recorder, so they are *provably* uncommitted. They carry a machine
    // readable code and a disposition marker so the stable `/ingest` forwarder
    // can answer with a valid OTLP ExportResponse and discard the payload.
    if (this.stopRequested) throw shotStopping();
    const row = this.store.get(this.shotId);
    if (!row || row.state !== "recording") {
      throw notRecording();
    }
    if (!this.container.running) {
      // The container may still hold in-flight/committed data or be mid-crash;
      // the outcome is not knowable here, so this must stay a retryable 503.
      throw unavailable("container is not running");
    }

    this.inFlight += 1;
    try {
      const headers: Record<string, string> = {};
      const contentType = request.headers.get("content-type");
      if (contentType) headers["content-type"] = contentType;
      const contentEncoding = request.headers.get("content-encoding");
      if (contentEncoding) headers["content-encoding"] = contentEncoding;

      const init = {
        method: "POST",
        headers,
        body: request.body ?? undefined,
        duplex: "half",
        signal: request.signal,
      } as RequestInit;

      const resp = await this.container.request(`/v1/${signal}`, init);
      if (resp.status === 200) {
        // We count accepted batches durably; record/span totals are refreshed
        // from the recorder's own metadata on demand.
        this.store.addCounters(this.shotId, signal, 0, { spans: 0, logRecords: 0, metricPoints: 0 });
        this.counters.batches[signal] += 1;
      }
      return resp;
    } finally {
      this.inFlight -= 1;
    }
  }

  // ------------------------------------------------------------------ stop

  async stop(): Promise<ShotResult> {
    // Stop admitting OTLP traffic synchronously, before any await, so a request
    // that races the stop is rejected rather than silently dropped.
    this.stopRequested = true;
    await this.ensureLoaded();
    return this.exclusive(async () => {
      const row = this.store.get(this.shotId);
      if (!row) throw notFound(`unknown shot ${this.shotId}`);
      if (row.state === "frozen") return this.resultFromRow(row, this.counters);
      if (row.state === "created") throw conflict("shot has not started recording");
      if (row.state === "failed" && !this.retryable(row, "stop")) {
        throw conflict(`shot is failed: ${row.last_error ?? "unknown error"}`);
      }
      return this.doStop();
    });
  }

  private async doStop(): Promise<ShotResult> {
    this.requireInternalAuth();
    const artifactKey = `${this.prefix}/${this.shotId}/${ARTIFACT_NAME}`;
    const metadataKey = `${this.prefix}/${this.shotId}/${METADATA_NAME}`;
    try {
      this.store.update(this.shotId, { state: "stopping", stopping_at: this.now() });

      await this.drain();
      await this.refreshCountersFromGo().catch(() => undefined);

      const stopResp = await this.container.request("/internal/stop", {
        method: "POST",
        headers: this.internalHeaders({ "content-type": "application/json" }),
        body: JSON.stringify({ shot_id: this.shotId }),
      });
      if (stopResp.status !== 200) {
        const detail = await stopResp.text().catch(() => "");
        throw new Error(`recorder stop returned ${stopResp.status}${detail ? `: ${detail}` : ""}`);
      }
      const goResult = (await stopResp.json()) as { state?: string; size_bytes?: number; sha256?: string };
      if (goResult.state !== "frozen" || !goResult.sha256 || !goResult.size_bytes) {
        throw new Error("recorder did not report a frozen artifact");
      }
      const expectedSize = Number(goResult.size_bytes);
      const expectedSha = String(goResult.sha256).toLowerCase();

      const artifactResp = await this.container.request("/internal/artifact", {
        headers: this.internalHeaders(),
      });
      if (artifactResp.status !== 200 || !artifactResp.body) {
        throw new Error(`artifact fetch returned ${artifactResp.status}`);
      }
      const declaredLength = artifactResp.headers.get("content-length");
      if (declaredLength !== null && Number(declaredLength) !== expectedSize) {
        throw new Error(`artifact Content-Length ${declaredLength} != reported ${expectedSize}`);
      }
      const declaredSha = artifactResp.headers.get("x-burstel-sha256");
      if (declaredSha && declaredSha.toLowerCase() !== expectedSha) {
        throw new Error("artifact header sha256 does not match the stop result");
      }

      const stored = await this.artifacts.putArtifact(artifactKey, {
        body: artifactResp.body,
        size: expectedSize,
        sha256: expectedSha,
        contentType: "application/octet-stream",
      });

      const freeze: FreezeMetadata = {
        freeze_version: 1,
        artifact_key: stored.key,
        artifact_size: stored.size,
        artifact_sha256: stored.sha256,
        metadata_key: metadataKey,
        uploaded_at: new Date(this.now()).toISOString(),
      };
      const currentRow = this.store.get(this.shotId);
      if (currentRow) {
        await this.artifacts.putJSON(metadataKey, { ...this.infoFromRow(currentRow), freeze });
      }

      this.store.update(this.shotId, {
        state: "frozen",
        stopped_at: this.now(),
        size_bytes: stored.size,
        sha256: stored.sha256,
        freeze_json: JSON.stringify(freeze),
        last_error: null,
      });
      const frozenRow = this.store.get(this.shotId);
      if (!frozenRow) throw new Error("failed to persist frozen state");

      // Only now that the artifact and metadata are durable in R2 may the
      // ephemeral container (and its DuckDB) be destroyed.
      await this.container.destroy().catch(() => undefined);
      return this.resultFromRow(frozenRow, this.counters);
    } catch (error) {
      // Do not destroy the container: its disk still holds the only copy of
      // the data. Record a retryable failure so a later stop can try again.
      this.stopRequested = false;
      this.store.update(this.shotId, {
        state: "failed",
        last_error: errorMessage(error),
        freeze_json: JSON.stringify({ phase: "stop", retryable: true }),
      });
      throw new HttpError(503, `freeze failed: ${errorMessage(error)}`, true, "freeze_failed");
    }
  }

  private async drain(): Promise<void> {
    const deadline = Date.now() + this.drainTimeoutMs;
    while (this.inFlight > 0) {
      if (Date.now() > deadline) {
        throw new Error(`timed out draining ${this.inFlight} in-flight ingest(s)`);
      }
      await sleep(5);
    }
  }

  // ------------------------------------------------------------- analysis

  /**
   * Restores the frozen R2 snapshot into a fresh read-only analysis container
   * and runs one constrained query. The original recording container is gone;
   * the only copy of the data lives in R2.
   */
  async query(sql: string, maxRows?: number): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    const freeze = this.requireFrozen();
    await this.ensureAnalysis(freeze);
    return this.analysisJson("/internal/query", {
      restore_id: this.shotId,
      sql,
      max_rows: maxRows ?? this.analysisMaxRows,
    });
  }

  /** Runs the SQL-backed analytical sections for this frozen shot. */
  async analyze(req: { sections?: string[]; topN?: number } = {}): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    const freeze = this.requireFrozen();
    await this.ensureAnalysis(freeze);
    return this.analysisJson("/internal/analyze", {
      restore_id: this.shotId,
      shot_id: this.shotId,
      sections: req.sections,
      top_n: req.topN ?? this.analysisTopN,
    });
  }

  /** Returns the documented queryable schema and safety policy. */
  async schema(): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    const freeze = this.requireFrozen();
    await this.ensureAnalysis(freeze);
    const resp = await this.container.request("/internal/schema", { headers: this.internalHeaders() });
    return this.parseAnalysisJson(resp, "/internal/schema");
  }

  /**
   * Compares this frozen shot with another frozen shot. Both artifacts are
   * streamed from R2 into the same analysis container and compared with SQL
   * aggregates.
   */
  async compare(
    other: { shot_id: string; freeze: FreezeMetadata },
    topN?: number,
  ): Promise<Record<string, unknown>> {
    await this.ensureLoaded();
    const freeze = this.requireFrozen();
    await this.ensureAnalysis(freeze);
    const otherRestoreId = `other-${other.shot_id}`;
    await this.ensureRestored(otherRestoreId, other.freeze);
    return this.analysisJson("/internal/compare", {
      left_restore_id: this.shotId,
      right_restore_id: otherRestoreId,
      top_n: topN ?? this.analysisTopN,
    });
  }

  /** Tears down the analysis container and forgets restored snapshots. */
  async releaseAnalysis(): Promise<void> {
    this.analysisRestored.clear();
    if (this.container.running) {
      await this.container.destroy().catch(() => undefined);
    }
  }

  private requireFrozen(): FreezeMetadata {
    const row = this.store.get(this.shotId);
    if (!row) throw notFound(`unknown shot ${this.shotId}`);
    if (row.state !== "frozen") {
      throw new HttpError(409, "shot is not frozen", false, "not_frozen");
    }
    const freeze = parseFreeze(row) as FreezeMetadata | undefined;
    if (!freeze || !freeze.artifact_key || !freeze.artifact_sha256) {
      throw new HttpError(409, "shot has no frozen artifact metadata", false, "not_frozen");
    }
    return freeze;
  }

  private async ensureAnalysis(selfFreeze: FreezeMetadata): Promise<void> {
    this.requireInternalAuth();
    if (!this.container.running) {
      this.container.start(this.analysisEnvironment());
      await this.container.ensureReady();
      this.analysisRestored.clear();
    }
    await this.ensureRestored(this.shotId, selfFreeze);
  }

  private async ensureRestored(restoreId: string, freeze: FreezeMetadata): Promise<void> {
    if (this.analysisRestored.get(restoreId) === freeze.artifact_sha256) return;
    const artifact = await this.artifacts.getArtifact(freeze.artifact_key);
    if (!artifact) throw notFound(`frozen artifact ${freeze.artifact_key} is missing`);
    const resp = await this.container.request("/internal/restore", {
      method: "POST",
      headers: this.internalHeaders({
        "content-type": "application/octet-stream",
        "x-burstel-restore-id": restoreId,
        "x-burstel-size": String(freeze.artifact_size),
        "x-burstel-sha256": freeze.artifact_sha256,
      }),
      body: artifact.body,
      duplex: "half",
    } as RequestInit);
    if (resp.status !== 200) {
      const detail = await resp.text().catch(() => "");
      throw new HttpError(
        503,
        `analysis restore failed: ${resp.status}${detail ? `: ${detail}` : ""}`,
        true,
        "restore_failed",
      );
    }
    await resp.body?.cancel().catch(() => undefined);
    this.analysisRestored.set(restoreId, freeze.artifact_sha256);
  }

  private async analysisJson(path: string, body: Record<string, unknown>): Promise<Record<string, unknown>> {
    const resp = await this.container.request(path, {
      method: "POST",
      headers: this.internalHeaders({ "content-type": "application/json" }),
      body: JSON.stringify(body),
    });
    return this.parseAnalysisJson(resp, path);
  }

  private async parseAnalysisJson(resp: Response, path: string): Promise<Record<string, unknown>> {
    const text = await resp.text();
    if (resp.status < 200 || resp.status >= 300) {
      let message = `analysis ${path} failed with status ${resp.status}`;
      let code = "analysis_error";
      try {
        const parsed = JSON.parse(text) as { error?: string; code?: string };
        if (parsed.error) message = parsed.error;
        if (parsed.code) code = parsed.code;
      } catch {
        // keep defaults
      }
      const status = resp.status === 400 || resp.status === 404 ? resp.status : 503;
      throw new HttpError(status, message, resp.status >= 500, code);
    }
    try {
      return JSON.parse(text) as Record<string, unknown>;
    } catch {
      throw new HttpError(503, "invalid analysis response", true, "analysis_error");
    }
  }

  private analysisEnvironment(): Record<string, string> {
    const env = this.containerEnvironment();
    env["BURSTEL_ANALYSIS"] = "1";
    return env;
  }

  // ------------------------------------------------------------- internal

  async handleInternal(request: Request): Promise<Response> {
    await this.ensureLoaded();
    try {
      const path = new URL(request.url).pathname;
      if (path === "/internal/create") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ metadata?: Record<string, unknown> }>(request);
        return json(201, await this.create(body.metadata ?? {}));
      }
      if (path === "/internal/status") {
        return json(200, await this.status());
      }
      if (path === "/internal/stop") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        return json(200, await this.stop());
      }
      if (path === "/internal/analysis/query") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ sql?: string; max_rows?: number }>(request);
        if (typeof body.sql !== "string" || body.sql.trim() === "") {
          return json(400, { error: "sql is required", code: "bad_request" });
        }
        return json(200, await this.query(body.sql, body.max_rows));
      }
      if (path === "/internal/analysis/analyze") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{ sections?: string[]; top_n?: number }>(request);
        return json(200, await this.analyze({ sections: body.sections, topN: body.top_n }));
      }
      if (path === "/internal/analysis/schema") {
        if (request.method !== "GET") return json(405, { error: "method not allowed" });
        return json(200, await this.schema());
      }
      if (path === "/internal/analysis/compare") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        const body = await readJson<{
          other?: { shot_id?: string; freeze?: FreezeMetadata };
          top_n?: number;
        }>(request);
        if (!body.other?.shot_id || !body.other.freeze) {
          return json(400, { error: "other shot with freeze metadata is required", code: "bad_request" });
        }
        return json(200, await this.compare({ shot_id: body.other.shot_id, freeze: body.other.freeze }, body.top_n));
      }
      if (path === "/internal/analysis/release") {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        await this.releaseAnalysis();
        return json(200, { released: true });
      }
      const ingest = /^\/internal\/ingest\/(traces|metrics|logs)$/.exec(path);
      if (ingest) {
        if (request.method !== "POST") return json(405, { error: "method not allowed" });
        return await this.ingest(ingest[1] as Signal, request);
      }
      return json(404, { error: "not found" });
    } catch (error) {
      return errorResponse(error);
    }
  }

  // -------------------------------------------------------------- helpers

  private async refreshCountersFromGo(): Promise<void> {
    const resp = await this.container.request("/internal/metadata", {
      headers: this.internalHeaders(),
    });
    if (resp.status !== 200) return;
    const meta = (await resp.json()) as {
      counters?: {
        batches?: Record<string, number>;
        records?: Record<string, number>;
        spans?: number;
        log_records?: number;
        metric_points?: number;
      };
    };
    const c = meta.counters;
    if (!c) return;
    const next = emptyCounters();
    for (const signal of SIGNALS) {
      next.batches[signal] = Number(c.batches?.[signal] ?? 0);
      next.records[signal] = Number(c.records?.[signal] ?? 0);
    }
    next.spans = Number(c.spans ?? 0);
    next.logRecords = Number(c.log_records ?? 0);
    next.metricPoints = Number(c.metric_points ?? 0);
    this.counters = next;
  }

  /**
   * Fails closed when the container would be reachable without a distinct
   * internal bearer token. The admin token is never an acceptable substitute.
   */
  private requireInternalAuth(): void {
    if (this.internalTokenReady) return;
    throw internalTokenUnconfigured(this.internalTokenError);
  }

  private internalHeaders(extra?: Record<string, string>): Record<string, string> {
    const headers: Record<string, string> = { ...extra };
    if (this.internalToken) headers["authorization"] = `Bearer ${this.internalToken}`;
    return headers;
  }

  private containerEnvironment(): Record<string, string> {
    const env: Record<string, string> = { ...this.extraEnv };
    if (this.internalToken) env["BURSTEL_INTERNAL_TOKEN"] = this.internalToken;
    return env;
  }

  private markFailed(phase: "start" | "stop", error: unknown): void {
    this.store.update(this.shotId, {
      state: "failed",
      last_error: errorMessage(error),
      freeze_json: JSON.stringify({ phase, retryable: true }),
    });
  }

  private retryable(row: StateRow, phase: "start" | "stop"): boolean {
    const freeze = parseFreeze(row);
    return freeze !== undefined && freeze["phase"] === phase && freeze["retryable"] === true;
  }

  private exclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.transition.then(fn, fn);
    this.transition = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private infoFromRow(row: StateRow): ShotInfo {
    return {
      shot_id: row.shot_id,
      state: row.state,
      created_at: iso(row.created_at),
      started_at: optionalIso(row.started_at),
      stopped_at: optionalIso(row.stopped_at),
      metadata: parseJSON(row.metadata_json),
      counters: cloneCounters(this.counters),
      size_bytes: row.size_bytes ?? undefined,
      sha256: row.sha256 ?? undefined,
      artifact: row.state === "frozen" ? ARTIFACT_NAME : undefined,
      freeze: parseFreeze(row) as FreezeMetadata | undefined,
      otlp_path: `/shots/${row.shot_id}`,
      last_error: row.last_error ?? undefined,
    };
  }

  private resultFromRow(row: StateRow, counters: Counters): ShotResult {
    return {
      shot_id: row.shot_id,
      state: row.state,
      created_at: iso(row.created_at),
      started_at: optionalIso(row.started_at),
      stopped_at: optionalIso(row.stopped_at),
      size_bytes: row.size_bytes ?? undefined,
      sha256: row.sha256 ?? undefined,
      artifact: row.state === "frozen" ? ARTIFACT_NAME : undefined,
      freeze: parseFreeze(row) as FreezeMetadata | undefined,
      counters: cloneCounters(counters),
    };
  }
}

function parseJSON(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

function parseFreeze(row: StateRow): Record<string, unknown> | undefined {
  if (!row.freeze_json || row.freeze_json === "{}") return undefined;
  const parsed = parseJSON(row.freeze_json);
  return Object.keys(parsed).length > 0 ? parsed : undefined;
}

function cloneCounters(counters: Counters): Counters {
  return {
    batches: { traces: counters.batches.traces, metrics: counters.batches.metrics, logs: counters.batches.logs },
    records: { traces: counters.records.traces, metrics: counters.records.metrics, logs: counters.records.logs },
    spans: counters.spans,
    logRecords: counters.logRecords,
    metricPoints: counters.metricPoints,
  };
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

function optionalIso(ms: number | null): string | undefined {
  return ms === null ? undefined : new Date(ms).toISOString();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Deterministic JSON: object keys are sorted recursively. */
export function stableStringify(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = sortKeys((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}
