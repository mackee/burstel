// Logical time-window shot metadata (spec 016).
//
// A window shot is *metadata only*: it has no per-shot ingest token, no
// recording container and no R2 artifact. Its timestamps are half-open
// [start,end) UTC instants and carry explicit provenance. Analysis runs against
// the continuous instance streams through segment-scoped logical views.
//
// This module is pure (no Cloudflare globals) so the exact same validation and
// candidate-segment selection run in production and in unit tests.

/** Who assigned a window's timestamps. */
export type Provenance = "server" | "external";

/** Derived trust class; never caller-controlled. */
export type ClockTrust = "server" | "untrusted_client";

/** A half-open [start,end) UTC window. */
export interface ShotWindow {
  /** Inclusive lower bound, epoch ms. */
  start_ms: number;
  /** Exclusive upper bound, epoch ms. */
  end_ms: number;
  provenance: Provenance;
  clock_trust: ClockTrust;
  /** Instance names; empty means every appropriate registered instance. */
  instances: string[];
  name?: string;
}

/** Bounds for externally supplied windows. */
export interface WindowBounds {
  max_duration_ms: number;
  max_past_ms: number;
  max_future_ms: number;
}

export const DEFAULT_WINDOW_BOUNDS: WindowBounds = {
  max_duration_ms: 60 * 60 * 1000,
  max_past_ms: 7 * 24 * 60 * 60 * 1000,
  max_future_ms: 5 * 60 * 1000,
};

export interface WindowValidationError {
  ok: false;
  code: string;
  error: string;
}
export type WindowValidation = { ok: true } | WindowValidationError;

/** Normalizes an instance list to sorted unique lowercase names. */
export function normalizeInstances(input: unknown): string[] {
  if (!Array.isArray(input)) return [];
  const seen = new Set<string>();
  for (const raw of input) {
    if (typeof raw !== "string") continue;
    const v = raw.trim().toLowerCase();
    if (v === "" || !isValidInstanceLabel(v)) continue;
    seen.add(v);
  }
  return [...seen].sort();
}

const INSTANCE_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

/** Matches the named-instance wire form from spec 014. */
export function isValidInstanceLabel(name: string): boolean {
  return INSTANCE_LABEL.test(name);
}

/** Builds a server-clock window. */
export function serverWindow(startMs: number, endMs: number, instances: unknown, name?: string): ShotWindow {
  return {
    start_ms: Math.floor(startMs),
    end_ms: Math.floor(endMs),
    provenance: "server",
    clock_trust: "server",
    instances: normalizeInstances(instances),
    name: typeof name === "string" ? name.trim() : undefined,
  };
}

/** Builds an untrusted, externally supplied window (must still be validated). */
export function externalWindow(startMs: number, endMs: number, instances: unknown, name?: string): ShotWindow {
  return {
    start_ms: Math.floor(startMs),
    end_ms: Math.floor(endMs),
    provenance: "external",
    clock_trust: "untrusted_client",
    instances: normalizeInstances(instances),
    name: typeof name === "string" ? name.trim() : undefined,
  };
}

/**
 * Enforces ordering, magnitude, retention and clock-skew bounds. Externally
 * supplied timestamps are additionally retained as untrusted client-clock
 * values; a caller cannot reset `clock_trust` to make them trusted.
 */
export function validateWindow(
  window: ShotWindow,
  nowMs: number,
  bounds: WindowBounds = DEFAULT_WINDOW_BOUNDS,
): WindowValidation {
  if (window.provenance !== "server" && window.provenance !== "external") {
    return { ok: false, code: "invalid_provenance", error: "provenance must be server or external" };
  }
  const expectedTrust = window.provenance === "server" ? "server" : "untrusted_client";
  if (window.clock_trust !== expectedTrust) {
    return { ok: false, code: "invalid_trust", error: "clock trust does not match provenance" };
  }
  if (!Number.isFinite(window.start_ms) || !Number.isFinite(window.end_ms)) {
    return { ok: false, code: "window_bounds_required", error: "start and end are required" };
  }
  if (!(window.end_ms > window.start_ms)) {
    return { ok: false, code: "window_not_ordered", error: "end must be strictly after start (half-open [start,end))" };
  }
  if (window.end_ms - window.start_ms > bounds.max_duration_ms) {
    return { ok: false, code: "window_too_long", error: `window exceeds the ${bounds.max_duration_ms}ms limit` };
  }
  if (window.start_ms < nowMs - bounds.max_past_ms) {
    return { ok: false, code: "window_too_old", error: "window start is outside the retained range" };
  }
  if (window.end_ms > nowMs + bounds.max_future_ms) {
    return { ok: false, code: "window_in_future", error: "window end is beyond the allowed clock skew" };
  }
  for (const inst of window.instances) {
    if (!isValidInstanceLabel(inst)) {
      return { ok: false, code: "invalid_instance", error: `instance name ${inst} is not valid` };
    }
  }
  return { ok: true };
}

/** True when `ns` is inside the half-open window. */
export function containsNS(window: ShotWindow, ns: number): boolean {
  return ns >= window.start_ms * 1e6 && ns < window.end_ms * 1e6;
}

/** True when two half-open windows share an instant. */
export function windowsOverlap(a: ShotWindow, b: ShotWindow): boolean {
  return a.start_ms < b.end_ms && b.start_ms < a.end_ms;
}

/** A segment's selection-relevant metadata (mirrors spec 015 manifest). */
export interface WindowSegmentMeta {
  segment_key: string;
  instance_id: string;
  instance: string;
  first_received_at: number;
  last_received_at: number;
  event_min_ns: number | null;
  event_max_ns: number | null;
  status: string;
}

export interface SegmentSelection {
  selected: WindowSegmentMeta[];
  missing: string[];
  expected: number;
  justification: string;
}

export const DEFAULT_INGEST_LAG_MS = 2 * 60 * 1000;

/**
 * Selects candidate segments for a window. A segment is a candidate when its
 * receive-time range overlaps the window widened by the ingest lag, or when its
 * event-time min/max overlaps the half-open window (catching a late export whose
 * receive time is far after the window). Unverified segments are reported as
 * missing and still counted in `expected`.
 */
export function selectSegments(
  window: ShotWindow,
  segments: WindowSegmentMeta[],
  ingestLagMs: number = DEFAULT_INGEST_LAG_MS,
): SegmentSelection {
  const lag = ingestLagMs > 0 ? ingestLagMs : DEFAULT_INGEST_LAG_MS;
  const lo = window.start_ms - lag;
  const hi = window.end_ms + lag;
  const want = new Set(window.instances);
  const selected: WindowSegmentMeta[] = [];
  const missing: string[] = [];
  for (const seg of segments) {
    if (want.size > 0 && !want.has(seg.instance.toLowerCase()) && !want.has(seg.instance_id.toLowerCase())) {
      continue;
    }
    const receiveOverlap = seg.last_received_at >= lo && seg.first_received_at < hi;
    const eventOverlap =
      seg.event_min_ns !== null &&
      seg.event_max_ns !== null &&
      seg.event_max_ns >= window.start_ms * 1e6 &&
      seg.event_min_ns < window.end_ms * 1e6;
    if (!receiveOverlap && !eventOverlap) continue;
    if (seg.status !== "" && seg.status !== "verified") {
      missing.push(seg.segment_key);
      continue;
    }
    selected.push(seg);
  }
  selected.sort((a, b) =>
    a.first_received_at !== b.first_received_at
      ? a.first_received_at - b.first_received_at
      : a.segment_key.localeCompare(b.segment_key),
  );
  missing.sort();
  return {
    selected,
    missing,
    expected: selected.length + missing.length,
    justification: `candidates selected by receive-time overlap with [${lo},${hi}]ms (ingest lag ${lag}ms) OR event-time overlap with the half-open window`,
  };
}

/** Serializes a window for durable storage. */
export function windowToJSON(window: ShotWindow): string {
  return JSON.stringify({
    start_ms: window.start_ms,
    end_ms: window.end_ms,
    provenance: window.provenance,
    clock_trust: window.clock_trust,
    instances: window.instances,
    name: window.name ?? null,
  });
}

/** Parses a stored window. */
export function windowFromJSON(raw: string | null | undefined): ShotWindow | null {
  if (!raw) return null;
  try {
    const p = JSON.parse(raw) as Record<string, unknown>;
    if (typeof p.start_ms !== "number" || typeof p.end_ms !== "number") return null;
    const provenance: Provenance = p.provenance === "external" ? "external" : "server";
    return {
      start_ms: p.start_ms,
      end_ms: p.end_ms,
      provenance,
      clock_trust: provenance === "server" ? "server" : "untrusted_client",
      instances: normalizeInstances(p.instances),
      name: typeof p.name === "string" ? p.name : undefined,
    };
  } catch {
    return null;
  }
}
