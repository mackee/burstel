import { describe, expect, test } from "bun:test";

import {
  DEFAULT_WINDOW_BOUNDS,
  containsNS,
  externalWindow,
  selectSegments,
  serverWindow,
  validateWindow,
  windowsOverlap,
} from "../src/shot-window";
import type { WindowSegmentMeta } from "../src/shot-window";
import { makeIngest } from "./fakes";

const NOW = Date.UTC(2026, 0, 1, 12, 0, 0);

describe("shot window semantics", () => {
  test("half-open edges: start included, end excluded", () => {
    const w = serverWindow(NOW - 60_000, NOW, []);
    expect(containsNS(w, w.start_ms * 1e6)).toBe(true);
    expect(containsNS(w, w.end_ms * 1e6)).toBe(false);
  });

  test("adjacent half-open windows do not overlap; neighbours do", () => {
    const a = serverWindow(NOW, NOW + 60_000, []);
    const b = serverWindow(NOW + 60_000, NOW + 120_000, []);
    const c = serverWindow(NOW + 30_000, NOW + 90_000, []);
    expect(windowsOverlap(a, b)).toBe(false);
    expect(windowsOverlap(a, c)).toBe(true);
  });

  test("validates ordering, duration, retention and clock skew", () => {
    expect(validateWindow(serverWindow(NOW - 1000, NOW, []), NOW).ok).toBe(true);
    expect(validateWindow(externalWindow(NOW, NOW, []), NOW)).toMatchObject({ ok: false, code: "window_not_ordered" });
    expect(validateWindow(externalWindow(NOW, NOW - 1, []), NOW).ok).toBe(false);
    expect(
      validateWindow(externalWindow(NOW - DEFAULT_WINDOW_BOUNDS.max_duration_ms - 1, NOW, []), NOW),
    ).toMatchObject({ ok: false, code: "window_too_long" });
    const oldStart = NOW - 8 * 24 * 3600_000;
    expect(validateWindow(externalWindow(oldStart, oldStart + 60_000, []), NOW)).toMatchObject({
      ok: false,
      code: "window_too_old",
    });
    expect(validateWindow(externalWindow(NOW, NOW + 3600_000, []), NOW)).toMatchObject({
      ok: false,
      code: "window_in_future",
    });
  });

  test("external windows are always untrusted client clock", () => {
    const w = externalWindow(NOW - 1000, NOW, []);
    expect(w.provenance).toBe("external");
    expect(w.clock_trust).toBe("untrusted_client");
    // The trust field cannot be forged.
    expect(validateWindow({ ...w, clock_trust: "server" } as never, NOW)).toMatchObject({ ok: false, code: "invalid_trust" });
  });

  test("selects candidates by receive lag or event overlap and reports missing", () => {
    const w = serverWindow(NOW, NOW + 60_000, ["web-1", "db-1"]);
    const evMin = w.start_ms * 1e6 + 1000;
    const evMax = evMin + 1000;
    const segments: WindowSegmentMeta[] = [
      { segment_key: "a", instance_id: "i-web", instance: "web-1", first_received_at: NOW, last_received_at: NOW + 60_000, event_min_ns: evMin, event_max_ns: evMax, status: "verified" },
      { segment_key: "late", instance_id: "i-web", instance: "web-1", first_received_at: NOW + 10 * 60_000, last_received_at: NOW + 11 * 60_000, event_min_ns: evMin, event_max_ns: evMax, status: "verified" },
      { segment_key: "lagged", instance_id: "i-db", instance: "db-1", first_received_at: NOW + 90_000, last_received_at: NOW + 90_000, event_min_ns: null, event_max_ns: null, status: "verified" },
      { segment_key: "old", instance_id: "i-db", instance: "db-1", first_received_at: NOW - 10 * 60_000, last_received_at: NOW - 9 * 60_000, event_min_ns: null, event_max_ns: null, status: "verified" },
      { segment_key: "other", instance_id: "i-cache", instance: "cache-1", first_received_at: NOW, last_received_at: NOW + 60_000, event_min_ns: null, event_max_ns: null, status: "verified" },
      { segment_key: "missing", instance_id: "i-web", instance: "web-1", first_received_at: NOW, last_received_at: NOW + 60_000, event_min_ns: null, event_max_ns: null, status: "missing" },
    ];
    const sel = selectSegments(w, segments);
    const keys = new Set(sel.selected.map((s) => s.segment_key));
    expect(keys.has("a")).toBe(true);
    expect(keys.has("late")).toBe(true);
    expect(keys.has("lagged")).toBe(true);
    expect(keys.has("old")).toBe(false);
    expect(keys.has("other")).toBe(false);
    expect(sel.missing).toEqual(["missing"]);
    expect(sel.expected).toBe(4);
  });
});

describe("window shot registry", () => {
  test("creates an externally marked complete window", async () => {
    const now = 1_700_000_000_000;
    const { core } = makeIngest({ now: () => now });
    const record = await core.createWindowShot({
      shot_id: "shot-win-1",
      name: "bench",
      started_at_ms: now - 120_000,
      ended_at_ms: now - 60_000,
      instances: ["web-1", "web-1", "DB-1"],
    });
    expect(record.kind).toBe("window");
    expect(record.state).toBe("incomplete"); // default coverage is not complete
    expect(record.window?.provenance).toBe("external");
    expect(record.window?.clock_trust).toBe("untrusted_client");
    expect(record.window?.instances).toEqual(["db-1", "web-1"]);
    expect(record.window?.end_ms).toBe(now - 60_000);
  });

  test("rejects unordered or oversized external windows", async () => {
    const now = 1_700_000_000_000;
    const { core } = makeIngest({ now: () => now });
    await expect(
      core.createWindowShot({ shot_id: "shot-bad", started_at_ms: now, ended_at_ms: now - 1 }),
    ).rejects.toThrow(/window_not_ordered/);
    await expect(
      core.createWindowShot({ shot_id: "shot-long", started_at_ms: now - 2 * 3600_000, ended_at_ms: now }),
    ).rejects.toThrow(/window_too_long/);
  });

  test("server window start/stop is idempotent and finalizes", async () => {
    let now = 1_700_000_000_000;
    const { core } = makeIngest({ now: () => now });
    const started = await core.startWindowShot({ shot_id: "shot-open", name: "run", instances: ["web-1"] });
    expect(started.state).toBe("open");
    now += 90_000;
    const stopped = await core.stopWindowShot("shot-open");
    expect(stopped.state).toBe("ready");
    expect(stopped.window?.end_ms).toBe(now);
    const again = await core.stopWindowShot("shot-open");
    expect(again.state).toBe("ready");
  });

  test("lists window shots with their window metadata", async () => {
    const now = 1_700_000_000_000;
    const { core } = makeIngest({ now: () => now });
    await core.createWindowShot({ shot_id: "shot-list", started_at_ms: now - 60_000, ended_at_ms: now, instances: ["web-1"] });
    const page = await core.listShots({ limit: 10, offset: 0 });
    expect(page.total).toBe(1);
    expect(page.shots[0]!.kind).toBe("window");
    expect(page.shots[0]!.window?.start_ms).toBe(now - 60_000);
  });
});
