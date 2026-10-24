// Durable, alarm-driven materialization coordinator (spec 032).
//
// The worker never owns a materialization attempt: `shot_stop` asks this
// driver (running inside a Durable Object keyed by shot id) to record a run and
// arm an alarm at `end + LATE_WRITE_LIMIT_MS`. Each alarm plans the journal,
// fans out bounded part builds to materializer Containers, records part leases
// and retries expired leases with a bounded attempt count. The run always ends
// `complete` or `failed` (with a reason); a cancelled Worker request cannot
// leave it stuck in `running`.
//
// The driver is deliberately free of the `cloudflare:workers` runtime so the
// exact alarm algorithm is unit-testable with a fake store, scheduler and
// container.

import { DO_ORIGIN } from "./env";
import type { CompactorNamespaceLike, IngestNamespaceLike } from "./env";
import type {
  MaterializationPart,
  MaterializationPartState,
  MaterializationPlan,
  MaterializationRecord,
  WindowShotRecord,
} from "./ingest-core";
import { chunkMaterializationPlan, materializeChunk, materializerPoolName } from "./materialize-part";

export type MaterializeRunStatus = "scheduled" | "running" | "complete" | "failed";

/** One part snapshot persisted in the coordinator DO. */
export interface MaterializePartRun extends MaterializationPartState {
  index: number;
  /** Spec 033: fixed pool member (`materializer-<n>`) currently leased. */
  pool_member?: number | null;
}

/** Durable prewarm request (spec 033): an alarm owns the actual container start. */
export interface MaterializePrewarmState {
  at: number;
  done: boolean;
}

export interface MaterializePrewarmStore {
  load(): Promise<MaterializePrewarmState | null>;
  save(state: MaterializePrewarmState | null): Promise<void>;
}

/** Durable run state persisted by the coordinator DO. */
export interface MaterializeRunState {
  run_id: string;
  shot_id: string;
  state: MaterializeRunStatus;
  scheduled_at: number;
  started_at: number | null;
  finished_at: number | null;
  force: boolean;
  parts_total: number;
  parts: MaterializePartRun[];
  results: MaterializationPart[];
  spans: number;
  error: string | null;
  attempts: number;
  plan_failures: number;
  lease_expires_at: number | null;
  heartbeat_at: number | null;
  /** False while a terminal mirror has not yet reached the ControlDO. */
  mirrored: boolean;
}

export interface MaterializeStore {
  load(): Promise<MaterializeRunState | null>;
  save(run: MaterializeRunState): Promise<void>;
}

export interface MaterializeScheduler {
  set(atMs: number): void | Promise<void>;
}

export interface MaterializeDriverConfig {
  parallelism: number;
  maxInstances: number;
  maxAttempts: number;
  leaseTtlMs: number;
  retryDelayMs: number;
  heartbeatMs: number;
  lateWriteLimitMs: number;
  /** Spec 033: keep a finished pool member warm for the next shot. */
  keepAliveMs: number;
  /** Spec 033: backoff after "no container instance", without burning an attempt. */
  capacityBackoffMs: number;
  /**
   * Spec 033: split the plan into more parts than pool members so completed
   * members can immediately take a queued part. Clamped to 1..8 and the Go
   * service's 64-part ceiling.
   */
  partsPerMember: number;
  /** Spec 034: estimated spans per part ceiling (default 500k). */
  partTargetSpans: number;
  /** Spec 034: measured spans per KiB of stored journal bytes (default 40). */
  partSpansPerKiB: number;
}

export interface MaterializeDriverDeps {
  shotId: string;
  ingest: IngestNamespaceLike;
  compactor?: CompactorNamespaceLike;
  store: MaterializeStore;
  scheduler: MaterializeScheduler;
  /** Spec 033: durable prewarm flag handled by this DO's alarm. */
  prewarmStore?: MaterializePrewarmStore;
  config?: Partial<MaterializeDriverConfig>;
  now?: () => number;
  log?: (message: string, fields?: Record<string, unknown>) => void;
  /** Test seam for the container call. */
  execute?: (
    shotId: string,
    chunk: ReturnType<typeof chunkMaterializationPlan>[number],
    partTotal: number,
    excluded: string[],
    poolMember: number,
  ) => Promise<MaterializationPart>;
  /** Test seam: keep a pool member warm after a finished part. */
  keepAlive?: (poolMember: number) => Promise<void>;
  /** Test seam: start one pool member's container during prewarm. */
  prewarm?: (poolMember: number) => Promise<void>;
}

const DEFAULT_CONFIG: MaterializeDriverConfig = {
  // Spec 033: fixed pool sized one below CompactorDO max_instances (4).
  parallelism: 3,
  maxInstances: 4,
  maxAttempts: 3,
  // Spec 034: must exceed the largest part timeout (120 s base + scaling)
  // with room for container start, otherwise an expired lease retries a part
  // that is still building.
  leaseTtlMs: 300_000,
  retryDelayMs: 5_000,
  heartbeatMs: 30_000,
  lateWriteLimitMs: 10_000,
  keepAliveMs: 10 * 60 * 1000,
  capacityBackoffMs: 10_000,
  partsPerMember: 2,
  partTargetSpans: 500_000,
  partSpansPerKiB: 40,
};

interface PartOutcome {
  part: MaterializePartRun;
  result?: MaterializationPart;
  error?: string;
  /** True when the platform had no free container instance. */
  capacity?: boolean;
  durationMs: number;
  bytes: number;
}

export class MaterializeDriver {
  readonly shotId: string;
  private readonly ingest: IngestNamespaceLike;
  private readonly compactor?: CompactorNamespaceLike;
  private readonly store: MaterializeStore;
  private readonly scheduler: MaterializeScheduler;
  private readonly prewarmStore?: MaterializePrewarmStore;
  private readonly config: MaterializeDriverConfig;
  private readonly now: () => number;
  private readonly log: (message: string, fields?: Record<string, unknown>) => void;
  private readonly execute:
    | ((
        shotId: string,
        chunk: ReturnType<typeof chunkMaterializationPlan>[number],
        partTotal: number,
        excluded: string[],
        poolMember: number,
      ) => Promise<MaterializationPart>)
    | undefined;
  private readonly keepAlive: (poolMember: number) => Promise<void>;
  private readonly prewarm: (poolMember: number) => Promise<void>;

  constructor(deps: MaterializeDriverDeps) {
    this.shotId = deps.shotId;
    this.ingest = deps.ingest;
    this.compactor = deps.compactor;
    this.store = deps.store;
    this.scheduler = deps.scheduler;
    this.prewarmStore = deps.prewarmStore;
    this.now = deps.now ?? (() => Date.now());
    this.log = deps.log ?? ((message, fields) => console.log(message, fields ?? {}));
    this.execute = deps.execute;
    this.keepAlive = deps.keepAlive ?? ((member) => this.poolCall(member, "/internal/keepalive", "POST"));
    this.prewarm = deps.prewarm ?? ((member) => this.poolCall(member, "/internal/prewarm", "POST"));
    const config = { ...DEFAULT_CONFIG, ...deps.config };
    this.config = {
      parallelism: clampInt(config.parallelism, 1, 8, DEFAULT_CONFIG.parallelism),
      maxInstances: clampInt(config.maxInstances, 1, 8, DEFAULT_CONFIG.maxInstances),
      maxAttempts: clampInt(config.maxAttempts, 1, 20, DEFAULT_CONFIG.maxAttempts),
      leaseTtlMs: clampInt(config.leaseTtlMs, 1_000, 3_600_000, DEFAULT_CONFIG.leaseTtlMs),
      retryDelayMs: clampInt(config.retryDelayMs, 0, 3_600_000, DEFAULT_CONFIG.retryDelayMs),
      heartbeatMs: clampInt(config.heartbeatMs, 0, 3_600_000, DEFAULT_CONFIG.heartbeatMs),
      lateWriteLimitMs: clampInt(config.lateWriteLimitMs, 0, 3_600_000, DEFAULT_CONFIG.lateWriteLimitMs),
      keepAliveMs: clampInt(config.keepAliveMs, 1_000, 3_600_000, DEFAULT_CONFIG.keepAliveMs),
      capacityBackoffMs: clampInt(config.capacityBackoffMs, 1_000, 3_600_000, DEFAULT_CONFIG.capacityBackoffMs),
      partsPerMember: clampInt(config.partsPerMember, 1, 8, DEFAULT_CONFIG.partsPerMember),
      partTargetSpans: clampInt(config.partTargetSpans, 1_000, 10_000_000, DEFAULT_CONFIG.partTargetSpans),
      partSpansPerKiB: clampInt(config.partSpansPerKiB, 1, 10_000, DEFAULT_CONFIG.partSpansPerKiB),
    };
  }

  /** Persists a prewarm flag; the next alarm (not the request) starts the pool. */
  async schedulePrewarm(): Promise<void> {
    if (!this.prewarmStore) return;
    const at = this.now();
    await this.prewarmStore.save({ at, done: false });
    await this.scheduler.set(at);
    this.log("materialize.pool.prewarm.scheduled", { shot_id: this.shotId, at });
  }

  /** Runs a due prewarm request. Kept separate so the alarm stays observable. */
  private async runPrewarm(): Promise<void> {
    if (!this.prewarmStore) return;
    const state = await this.prewarmStore.load();
    if (!state || state.done) return;
    const now = this.now();
    if (now < state.at) {
      await this.scheduler.set(state.at);
      return;
    }
    const started = now;
    try {
      for (let member = 0; member < this.parallelism(); member++) {
        await this.prewarm(member);
      }
      state.done = true;
      await this.prewarmStore.save(state);
      this.log("materialize.pool.prewarmed", {
        shot_id: this.shotId,
        members: this.parallelism(),
        duration_ms: this.now() - started,
      });
    } catch (error) {
      state.at = this.now() + this.config.capacityBackoffMs;
      await this.prewarmStore.save(state);
      await this.scheduler.set(state.at);
      this.log("materialize.pool.prewarm.retry", {
        shot_id: this.shotId,
        at: new Date(state.at).toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /** Sends one pool control request (`keepalive`/`prewarm`) to a member. */
  private async poolCall(member: number, path: string, method: string): Promise<void> {
    if (!this.compactor) return;
    const resp = await this.compactor.getByName(materializerPoolName(member)).fetch(
      new Request(`${DO_ORIGIN}${path}`, { method }),
    );
    if (resp.status < 200 || resp.status >= 300) {
      const text = await resp.text().catch(() => "");
      throw new Error(`materializer pool ${member} ${path} failed with status ${resp.status}${text ? `: ${text}` : ""}`);
    }
    await resp.body?.cancel().catch(() => undefined);
  }

  /** Effective parallelism: configured fan-out capped by container instances. */
  private parallelism(): number {
    return Math.max(1, Math.min(this.config.parallelism, this.config.maxInstances));
  }

  async status(): Promise<MaterializeRunState | null> {
    return this.store.load();
  }

  /**
   * Records a run and arms the first alarm.
   *
   * - New run, or `force`: schedule at `end + LATE_WRITE_LIMIT_MS` and reset
   *   every part (idempotent part keys make a re-run safe).
   * - Active run: leave a fresh lease alone, but re-arm a watchdog alarm.
   * - Stale run (`restart`, or a run/alarm older than its lease): keep finished
   *   parts and retry unfinished ones.
   * - Terminal run without `force`: return it unchanged; a `failed` run is
   *   retried only by the explicit `shot materialize` action.
   */
  async schedule(
    record: WindowShotRecord,
    options: { force?: boolean; restart?: boolean } = {},
  ): Promise<MaterializeRunState> {
    if (!record.window) throw new Error(`shot ${record.shot_id} is not a window shot`);
    const now = this.now();
    const existing = await this.store.load();
    const force = options.force === true;

    if (existing && existing.shot_id === record.shot_id && !force) {
      if (existing.state === "complete" || existing.state === "failed") {
        await this.persist(existing);
        return existing;
      }
      if (existing.state === "scheduled" && !options.restart && !this.stale(existing, now)) {
        await this.scheduler.set(existing.scheduled_at);
        return existing;
      }
      if (existing.state === "running" && !options.restart && !this.stale(existing, now)) {
        // The active alarm owns the run; a watchdog alarm revives it if the
        // alarm is lost before its lease is refreshed.
        await this.scheduler.set(Math.min(existing.lease_expires_at ?? now + this.config.leaseTtlMs, now + this.config.leaseTtlMs));
        return existing;
      }
      // Stale or explicit restart: retain completed parts, release every
      // in-flight lease, and let the next alarm retry.
      let changed = false;
      for (const part of existing.parts) {
        if (part.state === "running") {
          part.state = "pending";
          part.lease_owner = null;
          part.lease_expires_at = null;
          changed = true;
        }
      }
      existing.state = "scheduled";
      existing.scheduled_at = Math.max(now, this.windowStartAt(record));
      existing.finished_at = null;
      existing.error = null;
      existing.mirrored = false;
      if (changed) this.log("materialize.run.restart", { shot_id: this.shotId, scheduled_at: existing.scheduled_at });
      await this.persist(existing);
      await this.scheduler.set(existing.scheduled_at);
      return existing;
    }

    const run = newRun(record.shot_id, this.windowStartAt(record), force);
    if (existing && existing.shot_id === record.shot_id && force) {
      run.scheduled_at = Math.max(now, this.windowStartAt(record));
    }
    this.log("materialize.run.scheduled", {
      shot_id: run.shot_id,
      run_id: run.run_id,
      at: new Date(run.scheduled_at).toISOString(),
      end_ms: record.window.end_ms,
      late_write_limit_ms: this.config.lateWriteLimitMs,
      force,
    });
    await this.persist(run);
    await this.scheduler.set(run.scheduled_at);
    return run;
  }

  /** One alarm tick: plan, fan out, record leases and retry or terminate. */
  async alarm(): Promise<void> {
    // Spec 033 prewarm: the alarm (not the shot_start request) starts the pool.
    await this.runPrewarm();
    const run = await this.store.load();
    if (!run) return;
    const now = this.now();

    if (run.state === "complete" || run.state === "failed") {
      // A terminal mirror may have failed; a small retry alarm makes delivery
      // eventual without ever resurrecting the run itself.
      if (!run.mirrored) {
        await this.persist(run);
        if (!run.mirrored) await this.scheduler.set(now + Math.max(1_000, this.config.retryDelayMs));
        else await this.endYield();
      }
      return;
    }

    if (run.state === "scheduled" && now < run.scheduled_at) {
      await this.scheduler.set(run.scheduled_at);
      return;
    }

    run.state = "running";
    run.started_at ??= now;
    run.attempts += 1;
    run.lease_expires_at = now + this.config.leaseTtlMs;
    run.heartbeat_at = now;
    await this.beginYield();
    await this.persist(run);
    this.log("materialize.run.start", { shot_id: run.shot_id, run_id: run.run_id, attempt: run.attempts });

    const stopHeartbeat = this.startHeartbeat(run);
    let terminal: "complete" | "failed" | null = null;
    let capacityBackoff = false;
    try {
      const plan = await this.plan();
      // More parts than pool members keeps a freed member busy immediately
      // (spec 033), while spec 034 sizes every part by its *estimated span
      // count* so a ~1.3M-span part can never be planned again.
      const minParts = Math.min(64, this.parallelism() * this.config.partsPerMember);
      const chunks = chunkMaterializationPlan(plan.objects, minParts, {
        targetSpans: this.config.partTargetSpans,
        spansPerKiB: this.config.partSpansPerKiB,
      });
      const estimatedSpans = chunks.reduce((n, chunk) => n + chunk.spans, 0);
      this.log("materialize.plan", {
        shot_id: run.shot_id,
        objects: plan.objects.length,
        bytes: plan.total_bytes,
        parts: chunks.length,
        min_parts: minParts,
        estimated_spans: Math.round(estimatedSpans),
        max_part_spans: Math.round(chunks.reduce((n, chunk) => Math.max(n, chunk.spans), 0)),
        part_target_spans: this.config.partTargetSpans,
        spans_per_kib: this.config.partSpansPerKiB,
        pool: this.parallelism(),
        truncated: plan.truncated,
      });

      if (chunks.length === 0) {
        run.parts = [];
        run.parts_total = 0;
        run.results = [];
        terminal = "complete";
      } else {
        if (run.parts.length !== chunks.length) {
          const progressed = run.parts.some((part) => part.state === "complete" || part.attempts > 0);
          if (progressed && run.parts.length > 0) {
            terminal = "failed";
            run.error = `materialization plan changed from ${run.parts.length} to ${chunks.length} parts while a run was in progress`;
          } else {
            run.parts = chunks.map((chunk) => pendingPart(chunk.index));
          }
        }
        run.parts_total = chunks.length;
      }

      if (!terminal) {
        terminal = this.releaseExpiredLeases(run, now);
      }

      // Spec 033: fixed pool assignment with queueing. Only free pool members
      // receive a part; the rest stay `pending` for a later iteration/alarm.
      while (!terminal) {
        const busy = new Set<number>();
        for (const part of run.parts) {
          if (part.state === "running" && typeof part.pool_member === "number") busy.add(part.pool_member);
        }
        const free: number[] = [];
        for (let member = 0; member < this.parallelism(); member++) {
          if (!busy.has(member)) free.push(member);
        }
        const claimable = run.parts.filter((part) => part.state === "pending").slice(0, free.length);
        if (claimable.length === 0) break;
        for (let i = 0; i < claimable.length; i++) {
          const part = claimable[i]!;
          if (part.attempts >= this.config.maxAttempts) {
            part.state = "failed";
            part.error ??= `part gave up after ${part.attempts} attempts`;
            run.error = `part ${part.index} failed after ${part.attempts} attempts: ${part.error}`;
            terminal = "failed";
            break;
          }
          part.state = "running";
          part.attempts += 1;
          part.pool_member = free[i]!;
          part.lease_owner = run.run_id;
          part.lease_expires_at = this.now() + this.config.leaseTtlMs;
          part.started_at ??= this.now();
        }
        if (terminal) break;

        await this.persist(run);
        const outcomes = await Promise.all(
          claimable.map((part) =>
            this.runPart(run, part, chunks[part.index], chunks.length, plan.excluded, part.pool_member ?? 0),
          ),
        );
        let retry = false;
        for (const outcome of outcomes) {
          const part = outcome.part;
          if (outcome.capacity) {
            // No platform capacity is a transient queue condition: refund the
            // attempt so a busy installation cannot exhaust max_attempts.
            part.attempts = Math.max(0, part.attempts - 1);
            part.state = "pending";
            part.lease_owner = null;
            part.lease_expires_at = null;
            part.pool_member = null;
            part.error = outcome.error ?? "no container instance available";
            capacityBackoff = true;
            this.log("materialize.part.capacity_retry", {
              shot_id: run.shot_id,
              part: part.index,
              attempt: part.attempts,
              error: part.error,
            });
            continue;
          }
          part.pool_member = null;
          if (outcome.result) {
            part.state = "complete";
            part.finished_at = this.now();
            part.lease_owner = null;
            part.lease_expires_at = null;
            part.error = null;
            run.results.push(outcome.result);
            run.spans += outcome.result.span_count;
          } else {
            part.lease_owner = null;
            part.lease_expires_at = null;
            part.error = outcome.error ?? "unknown materializer failure";
            if (part.attempts >= this.config.maxAttempts) {
              part.state = "failed";
              terminal = "failed";
              run.error = `part ${part.index} failed after ${part.attempts} attempts: ${part.error}`;
            } else {
              part.state = "pending";
              retry = true;
              this.log("materialize.part.retry", {
                shot_id: run.shot_id,
                part: part.index,
                attempt: part.attempts,
                error: part.error,
              });
            }
          }
        }
        run.results.sort((a, b) => a.index - b.index);
        if (terminal) break;
        await this.persist(run);
        if (retry || capacityBackoff) break;
      }

      if (!terminal && run.parts.length > 0 && run.parts.every((part) => part.state === "complete")) {
        terminal = "complete";
      }
    } catch (error) {
      run.plan_failures += 1;
      const message = error instanceof Error ? error.message : String(error);
      this.log("materialize.plan.failure", { shot_id: run.shot_id, attempt: run.plan_failures, error: message });
      if (run.plan_failures >= this.config.maxAttempts) {
        terminal = "failed";
        run.error = `materialization planning failed after ${run.plan_failures} attempts: ${message}`;
      }
    } finally {
      stopHeartbeat();
    }

    const finishedAt = this.now();
    if (terminal === "complete") {
      run.state = "complete";
      run.finished_at = finishedAt;
      run.error = null;
      run.lease_expires_at = null;
      run.heartbeat_at = finishedAt;
      run.mirrored = false;
      this.log("materialize.run.complete", {
        shot_id: run.shot_id,
        run_id: run.run_id,
        parts: run.results.length,
        spans: run.spans,
        duration_ms: run.started_at === null ? null : finishedAt - run.started_at,
      });
    } else if (terminal === "failed") {
      run.state = "failed";
      run.finished_at = finishedAt;
      run.lease_expires_at = null;
      run.heartbeat_at = finishedAt;
      run.mirrored = false;
      this.log("materialize.run.failed", {
        shot_id: run.shot_id,
        run_id: run.run_id,
        error: run.error,
        parts_done: run.results.length,
        parts_total: run.parts_total,
      });
    } else {
      const delay = capacityBackoff
        ? this.config.capacityBackoffMs
        : this.config.retryDelayMs;
      run.state = "scheduled";
      run.scheduled_at = finishedAt + delay;
      run.lease_expires_at = finishedAt + this.config.leaseTtlMs;
      run.heartbeat_at = finishedAt;
      run.mirrored = false;
      this.log("materialize.run.retry", {
        shot_id: run.shot_id,
        run_id: run.run_id,
        at: new Date(run.scheduled_at).toISOString(),
        parts_done: run.results.length,
        parts_total: run.parts_total,
        capacity_backoff: capacityBackoff,
      });
    }

    await this.persist(run);
    if (run.state === "complete" || run.state === "failed") {
      await this.endYield();
      if (!run.mirrored) await this.scheduler.set(finishedAt + Math.max(1_000, this.config.retryDelayMs));
    } else {
      await this.scheduler.set(run.scheduled_at);
    }
  }

  private stale(run: MaterializeRunState, now: number): boolean {
    if (run.state === "scheduled") return run.scheduled_at <= now;
    if (run.state !== "running") return false;
    if (typeof run.lease_expires_at !== "number" || !Number.isFinite(run.lease_expires_at)) return true;
    return run.lease_expires_at <= now;
  }

  private releaseExpiredLeases(run: MaterializeRunState, now: number): "failed" | null {
    for (const part of run.parts) {
      if (part.state !== "running") continue;
      if (typeof part.lease_expires_at === "number" && part.lease_expires_at > now) continue;
      if (part.attempts >= this.config.maxAttempts) {
        part.state = "failed";
        part.error ??= "part lease expired after the last allowed attempt";
        run.error = `part ${part.index} failed after ${part.attempts} attempts: ${part.error}`;
        return "failed";
      }
      part.state = "pending";
      part.lease_owner = null;
      part.lease_expires_at = null;
      this.log("materialize.part.retry", {
        shot_id: run.shot_id,
        part: part.index,
        attempt: part.attempts,
        reason: "lease_expired",
      });
    }
    return null;
  }

  private async runPart(
    run: MaterializeRunState,
    part: MaterializePartRun,
    chunk: ReturnType<typeof chunkMaterializationPlan>[number] | undefined,
    partTotal: number,
    excluded: string[],
    poolMember: number,
  ): Promise<PartOutcome> {
    const started = this.now();
    if (!chunk) {
      const error = `materialization part ${part.index} has no planned objects`;
      this.log("materialize.part.failure", { shot_id: run.shot_id, part: part.index, error });
      return { part, error, durationMs: 0, bytes: 0 };
    }
    this.log("materialize.part.start", {
      shot_id: run.shot_id,
      part: part.index,
      attempt: part.attempts,
      pool_member: poolMember,
      objects: chunk.objects.length,
      bytes: chunk.bytes,
    });
    try {
      const result = this.execute
        ? await this.execute(run.shot_id, chunk, partTotal, excluded, poolMember)
        : await this.defaultExecute(run.shot_id, chunk, partTotal, excluded, poolMember);
      const durationMs = this.now() - started;
      this.log("materialize.part.finish", {
        shot_id: run.shot_id,
        part: part.index,
        pool_member: poolMember,
        duration_ms: durationMs,
        spans: result.span_count,
        bytes: result.size_bytes,
      });
      await this.releasePoolMember(poolMember);
      return { part, result, durationMs, bytes: chunk.bytes };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const durationMs = this.now() - started;
      const capacity = isNoCapacityError(message);
      this.log(capacity ? "materialize.part.capacity" : "materialize.part.failure", {
        shot_id: run.shot_id,
        part: part.index,
        attempt: part.attempts,
        pool_member: poolMember,
        duration_ms: durationMs,
        error: message,
      });
      if (capacity) {
        // The container was never started, so there is nothing to keep warm.
        return { part, error: message, capacity: true, durationMs, bytes: chunk.bytes };
      }
      await this.releasePoolMember(poolMember);
      return { part, error: message, durationMs, bytes: chunk.bytes };
    }
  }

  /** Keeps a finished member's container warm for the next shot. */
  private async releasePoolMember(poolMember: number): Promise<void> {
    await this.keepAlive(poolMember).catch((error) => {
      this.log("materialize.pool.keepalive.failure", {
        shot_id: this.shotId,
        pool_member: poolMember,
        keepalive_ms: this.config.keepAliveMs,
        error: error instanceof Error ? error.message : String(error),
      });
    });
  }

  private async defaultExecute(
    shotId: string,
    chunk: ReturnType<typeof chunkMaterializationPlan>[number],
    partTotal: number,
    excluded: string[],
    poolMember: number,
  ): Promise<MaterializationPart> {
    if (!this.compactor) throw new Error("materializer container binding is not configured");
    return materializeChunk(this.compactor, shotId, chunk, partTotal, excluded, poolMember);
  }

  private startHeartbeat(run: MaterializeRunState): () => void {
    if (this.config.heartbeatMs <= 0) return () => undefined;
    const timer = setInterval(() => {
      void this.heartbeat(run).catch(() => undefined);
    }, this.config.heartbeatMs);
    return () => clearInterval(timer);
  }

  private async heartbeat(run: MaterializeRunState): Promise<void> {
    if (run.state !== "running") return;
    const now = this.now();
    run.heartbeat_at = now;
    run.lease_expires_at = now + this.config.leaseTtlMs;
    for (const part of run.parts) {
      if (part.state === "running") part.lease_expires_at = now + this.config.leaseTtlMs;
    }
    await this.persist(run);
  }

  private async plan(): Promise<MaterializationPlan> {
    return this.ingestCall<MaterializationPlan>("/internal/shot/materialize-plan", "POST", {
      shot_id: this.shotId,
    });
  }

  private async beginYield(): Promise<void> {
    await this.ingestCall("/internal/materialization/begin", "POST", {
      shot_id: this.shotId,
      ttl_ms: Math.max(60_000, this.config.leaseTtlMs + 120_000),
    }).catch(() => undefined);
  }

  private async endYield(): Promise<void> {
    await this.ingestCall("/internal/materialization/end", "POST", {
      shot_id: this.shotId,
    }).catch(() => undefined);
  }

  private async persist(run: MaterializeRunState): Promise<void> {
    await this.store.save(run);
    await this.mirror(run);
    await this.store.save(run);
  }

  /** Mirrors the run into the ControlDO shot record (the read/coverage surface). */
  private async mirror(run: MaterializeRunState): Promise<void> {
    const record: MaterializationRecord = {
      state: run.state === "scheduled" ? "pending" : run.state,
      parts_done: run.results.length,
      parts_total: run.parts_total,
      spans: run.spans,
      started_at: run.started_at,
      finished_at: run.finished_at,
      parts: [...run.results].sort((a, b) => a.index - b.index),
      error: run.error,
      part_states: run.parts,
      lease_expires_at: run.lease_expires_at,
      heartbeat_at: run.heartbeat_at,
      attempts: run.attempts,
    };
    try {
      await this.ingestCall("/internal/shot/materialization", "POST", {
        shot_id: run.shot_id,
        materialization: record,
      });
      run.mirrored = true;
    } catch (error) {
      run.mirrored = false;
      this.log("materialize.mirror.failure", {
        shot_id: run.shot_id,
        state: run.state,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  private async ingestCall<T = unknown>(path: string, method: string, body?: unknown): Promise<T> {
    const init: RequestInit = { method };
    if (body !== undefined) {
      init.headers = { "content-type": "application/json" };
      init.body = JSON.stringify(body);
    }
    const resp = await this.ingest.getByName("ingest-primary").fetch(new Request(`${DO_ORIGIN}${path}`, init));
    const text = await resp.text();
    if (resp.status < 200 || resp.status >= 300) {
      let message = `${path} failed with status ${resp.status}`;
      try {
        const parsed = JSON.parse(text) as { error?: string };
        if (parsed.error) message = parsed.error;
      } catch {
        // keep default
      }
      throw new Error(message);
    }
    return (text.trim() === "" ? {} : JSON.parse(text)) as T;
  }

  private windowStartAt(record: WindowShotRecord): number {
    return Math.max(0, record.window?.end_ms ?? this.now()) + this.config.lateWriteLimitMs;
  }
}

function newRun(shotId: string, scheduledAt: number, force: boolean): MaterializeRunState {
  return {
    run_id: randomRunId(),
    shot_id: shotId,
    state: "scheduled",
    scheduled_at: scheduledAt,
    started_at: null,
    finished_at: null,
    force,
    parts_total: 0,
    parts: [],
    results: [],
    spans: 0,
    error: null,
    attempts: 0,
    plan_failures: 0,
    lease_expires_at: null,
    heartbeat_at: null,
    mirrored: false,
  };
}

function pendingPart(index: number): MaterializePartRun {
  return {
    index,
    state: "pending",
    attempts: 0,
    lease_owner: null,
    lease_expires_at: null,
    pool_member: null,
    started_at: null,
    finished_at: null,
    error: null,
  };
}

/**
 * The platform's capacity error ("There is no container instance that can be
 * provided to this Durable Object"). It is transient and must not count as a
 * part attempt (spec 033).
 */
export function isNoCapacityError(message: string): boolean {
  const text = message.toLowerCase();
  return text.includes("no container instance") || text.includes("no available container");
}

function randomRunId(): string {
  return `mat-${Date.now()}-${Math.floor(Math.random() * 1e12).toString(36)}`;
}

function clampInt(value: number, min: number, max: number, fallback: number): number {
  if (!Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

