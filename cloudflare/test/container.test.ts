import { describe, expect, test } from "bun:test";

import { containerEgressFor, createContainerControl } from "../src/container";
import type { ContainerLike, ContainerPortLike } from "../src/container";

class FakePort implements ContainerPortLike {
  requests: Array<{ url: string; method: string }> = [];
  readyAfter = 0;
  private calls = 0;

  async fetch(input: string, init?: RequestInit): Promise<Response> {
    this.calls += 1;
    this.requests.push({ url: input, method: init?.method ?? "GET" });
    if (input.endsWith("/readyz")) {
      return this.calls > this.readyAfter ? new Response("ready", { status: 200 }) : new Response("no", { status: 503 });
    }
    return new Response("ok", { status: 200 });
  }
}

class FakeContainer implements ContainerLike {
  running = false;
  startCalls = 0;
  destroyCalls = 0;
  inactivityTimeout: number | undefined;
  startOptions: { enableInternet?: boolean; env?: Record<string, string> } | undefined;
  readonly port = new FakePort();

  start(options?: { enableInternet?: boolean; env?: Record<string, string> }): void {
    this.startCalls += 1;
    this.startOptions = options;
    this.running = true;
  }
  async destroy(): Promise<void> {
    this.destroyCalls += 1;
    this.running = false;
  }
  async setInactivityTimeout(ms: number | bigint): Promise<void> {
    this.inactivityTimeout = Number(ms);
  }
  getTcpPort(): ContainerPortLike {
    return this.port;
  }
}

describe("container control", () => {
  test("starts once with env and leaves the inactivity timeout to keepAlive", () => {
    const container = new FakeContainer();
    const control = createContainerControl(container, { port: 8080 });
    control.start({ BURSTEL_INTERNAL_TOKEN: "t" });
    control.start({ BURSTEL_INTERNAL_TOKEN: "t" });

    expect(container.startCalls).toBe(1);
    expect(container.inactivityTimeout).toBeUndefined();
    expect(container.startOptions?.enableInternet).toBe(false);
  });

  test("the egress flag is per container class", () => {
    expect(containerEgressFor("CompactorDO")).toBe(true);
    expect(containerEgressFor("ShotDO")).toBe(false);
  });

  test("only an explicitly enabled control starts a container with egress", () => {
    const container = new FakeContainer();
    const control = createContainerControl(container, { port: 8080, enableInternet: true });
    control.start({ BURSTEL_INTERNAL_TOKEN: "t" });
    expect(container.startOptions?.enableInternet).toBe(true);
    expect(container.startOptions?.env).toEqual({ BURSTEL_INTERNAL_TOKEN: "t" });
  });

  test("keepAlive awaits the platform and propagates its rejection", async () => {
    const container = new FakeContainer();
    const control = createContainerControl(container, { port: 8080 });
    await control.keepAlive(6 * 60 * 60 * 1000);
    expect(container.inactivityTimeout).toBe(6 * 60 * 60 * 1000);

    container.setInactivityTimeout = async () => {
      throw new Error("timeout too long");
    };
    await expect(control.keepAlive(1)).rejects.toThrow(/timeout too long/);
  });

  test("keepAlive fails loudly when the runtime lacks the method", async () => {
    const container = new FakeContainer() as ContainerLike & { setInactivityTimeout?: unknown };
    delete (container as { setInactivityTimeout?: unknown }).setInactivityTimeout;
    Object.defineProperty(container, "setInactivityTimeout", { value: undefined });
    const control = createContainerControl(container as ContainerLike, { port: 8080 });
    await expect(control.keepAlive(1)).rejects.toThrow(/not supported/);
  });

  test("ensureReady polls until the recorder answers 200", async () => {
    const container = new FakeContainer();
    container.port.readyAfter = 2;
    const control = createContainerControl(container, { port: 8080, pollIntervalMs: 1 });
    await control.ensureReady();
    expect(container.port.requests.filter((r) => r.url.endsWith("/readyz")).length).toBeGreaterThanOrEqual(3);
  });

  test("ensureReady times out when the recorder never becomes ready", async () => {
    const container = new FakeContainer();
    container.port.readyAfter = Number.MAX_SAFE_INTEGER;
    const control = createContainerControl(container, { port: 8080, pollIntervalMs: 1, readyTimeoutMs: 20 });
    await expect(control.ensureReady()).rejects.toThrow(/did not become ready/);
  });
});
