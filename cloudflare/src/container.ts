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
  /** Boots the container if needed and forwards its runtime environment. */
  start(env: Record<string, string>): void;
  /** Resolves once the recorder answers `GET /readyz` with 200. */
  ensureReady(): Promise<void>;
  /** Proxies an HTTP request into the container via its TCP port. */
  request(path: string, init?: RequestInit): Promise<Response>;
  /** Stops the container process. */
  destroy(): Promise<void>;
}

export interface ContainerControlOptions {
  port: number;
  readyTimeoutMs?: number;
  pollIntervalMs?: number;
  /** Inactivity timeout; kept long so an active shot's ephemeral disk survives. */
  inactivityTimeoutMs?: number;
}

const SIX_HOURS_MS = 6 * 60 * 60 * 1000;

export function createContainerControl(
  container: ContainerLike,
  options: ContainerControlOptions,
): ContainerControl {
  const readyTimeoutMs = options.readyTimeoutMs ?? 30_000;
  const pollIntervalMs = options.pollIntervalMs ?? 200;
  const inactivityTimeoutMs = options.inactivityTimeoutMs ?? SIX_HOURS_MS;

  const request = (path: string, init?: RequestInit): Promise<Response> =>
    container.getTcpPort(options.port).fetch(`http://container${path}`, init);

  return {
    get running() {
      return container.running;
    },

    start(env) {
      if (container.running) return;
      container.start({ enableInternet: false, env });
      // Best effort: keep the container (and its ephemeral DuckDB) alive while
      // the shot is recording. Old platform builds may not expose the method.
      if (container.setInactivityTimeout) {
        void container.setInactivityTimeout(inactivityTimeoutMs).catch(() => undefined);
      }
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
