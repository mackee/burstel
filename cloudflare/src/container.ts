// Thin, testable wrapper around the Durable Object Container API
// (`this.ctx.container`). Everything here is structural so the production
// `ctx.container` satisfies it and tests can supply a fake.

export interface ContainerStartOptions {
  enableInternet?: boolean;
  env?: Record<string, string>;
  instance?: string;
}

export interface ContainerPortLike {
  fetch(input: string, init?: RequestInit): Promise<Response>;
}

/** Structural subset of the platform `Container` type. */
export interface ContainerLike {
  readonly running: boolean;
  start(options?: ContainerStartOptions): void;
  destroy(error?: unknown): Promise<void>;
  setInactivityTimeout?(durationMs: number | bigint): Promise<void>;
  getTcpPort(port: number): ContainerPortLike;
}

/** Per-shot controller used by {@link ShotCore}. */
export interface ContainerControl {
  readonly running: boolean;
  /**
   * Boots the container if needed and forwards its runtime environment.
   * Spec 035: `options.internet` enables container egress for this start only
   * (the analysis path when a bucket-scoped R2 credential is configured).
   */
  start(env: Record<string, string>, options?: { internet?: boolean }): void;
  /** Resolves once the recorder answers `GET /readyz` with 200. */
  ensureReady(): Promise<void>;
  /** Proxies an HTTP request into the container via its TCP port. */
  request(path: string, init?: RequestInit): Promise<Response>;
  /**
   * Sets how long the container keeps running after the Durable Object goes
   * inactive. The platform forgets it on every DO restart, so callers re-apply
   * it. Rejects when the platform refuses the value.
   */
  keepAlive(durationMs: number): Promise<void>;
  /** Stops the container process. */
  destroy(): Promise<void>;
}

/** Durable Object container classes that can own a process. */
export type ContainerClass = "ShotDO" | "CompactorDO";

/**
 * Spec 033/035 egress policy: the CompactorDO (continuous compaction plus the
 * fixed materializer pool, which performs direct bucket-scoped R2 S3 GET/PUT)
 * always gets container internet access. The ShotDO recorder never does; its
 * analysis path opts in per start when a bucket-scoped R2 credential is
 * configured (direct part download, spec 035).
 */
export function containerEgressFor(containerClass: ContainerClass): boolean {
  return containerClass === "CompactorDO";
}

export interface ContainerControlOptions {
  port: number;
  readyTimeoutMs?: number;
  pollIntervalMs?: number;
  /**
   * Spec 033: allow the container process to reach the public internet. Only
   * the CompactorDO materializer (direct R2 S3 GET/PUT) opts in; the ShotDO
   * recorder/analysis container stays isolated.
   */
  enableInternet?: boolean;
}

export function createContainerControl(
  container: ContainerLike,
  options: ContainerControlOptions,
): ContainerControl {
  const readyTimeoutMs = options.readyTimeoutMs ?? 30_000;
  const pollIntervalMs = options.pollIntervalMs ?? 200;

  const request = (path: string, init?: RequestInit): Promise<Response> =>
    container.getTcpPort(options.port).fetch(`http://container${path}`, init);

  return {
    get running() {
      return container.running;
    },

    start(env, startOptions) {
      if (container.running) return;
      const internet = startOptions?.internet ?? options.enableInternet === true;
      container.start({ enableInternet: internet, env });
    },

    async keepAlive(durationMs) {
      if (!container.setInactivityTimeout) {
        throw new Error("container inactivity timeout is not supported by this runtime");
      }
      await container.setInactivityTimeout(durationMs);
    },

    async ensureReady() {
      const deadline = Date.now() + readyTimeoutMs;
      let lastError: unknown;
      while (Date.now() < deadline) {
        try {
          const resp = await request("/readyz");
          if (resp.status === 200) {
            // Drain the tiny body so the connection is released.
            await resp.body?.cancel().catch(() => undefined);
            return;
          }
        } catch (error) {
          lastError = error;
        }
        await sleep(pollIntervalMs);
      }
      throw new Error(
        `container did not become ready within ${readyTimeoutMs}ms` +
          (lastError ? `: ${String(lastError)}` : ""),
      );
    },

    request,

    async destroy() {
      await container.destroy("burstel: shot frozen");
    },
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
