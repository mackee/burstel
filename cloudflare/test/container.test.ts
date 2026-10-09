import { describe, expect, test } from "bun:test";

import { createContainerControl } from "../src/container";
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
  readonly port = new FakePort();

  start(): void {
    this.startCalls += 1;
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
  test("starts once with env and keeps a long inactivity timeout", () => {
    const container = new FakeContainer();
    const control = createContainerControl(container, { port: 8080 });
    control.start({ BURSTEL_INTERNAL_TOKEN: "t" });
    control.start({ BURSTEL_INTERNAL_TOKEN: "t" });

    expect(container.startCalls).toBe(1);
    expect(container.inactivityTimeout).toBe(6 * 60 * 60 * 1000);
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
