// Ingest counters (spec 023, D2.3).
//
// The ledger is gone from the hot path, so operational counters are emitted as
// Workers Analytics Engine data points instead of durable SQLite rows. One
// point is written per ingest request:
//
//   index  = instance_id (or "anonymous")
//   blobs  = [status, reason, signal, instance_id, instance, token_id]
//   doubles= [accepted?, bytes, received_at, duration_ms]
//
// `last_received_at` per instance is `max(doubles[2])` in a query; `accepted`
// is 1 for a durable 200 and 0 for any rejection, with `reason` naming the
// rejection class (`invalid_token`, `payload_too_large`, ...).
//
// A missing binding (local tests) is a no-op and an Analytics Engine failure is
// swallowed: telemetry accounting must never fail an accepted ingest.

import type { AnalyticsEngineLike, AppEnv } from "./env";

export interface IngestPoint {
  status: "accepted" | "rejected";
  /** Rejection class (stable, low-cardinality) or "" when accepted. */
  reason?: string;
  signal: string;
  instance_id?: string;
  instance?: string;
  token_id?: string;
  bytes: number;
  received_at: number;
  duration_ms: number;
}

export function recordIngestPoint(env: Pick<AppEnv, "INGEST_ANALYTICS">, point: IngestPoint): void {
  const dataset = env.INGEST_ANALYTICS;
  if (!dataset) return;
  try {
    dataset.writeDataPoint({
      indexes: [point.instance_id && point.instance_id !== "" ? point.instance_id : "anonymous"],
      blobs: [
        point.status,
        point.reason ?? "",
        point.signal,
        point.instance_id ?? "",
        point.instance ?? "",
        point.token_id ?? "",
      ],
      doubles: [point.status === "accepted" ? 1 : 0, Math.max(0, point.bytes), point.received_at, Math.max(0, point.duration_ms)],
    });
  } catch {
    // Accounting is best effort; the durable object is the source of truth.
  }
}

/** Test/diagnostic helper: a dataset that records the last data points. */
export class RecordingAnalytics implements AnalyticsEngineLike {
  readonly points: Array<{ indexes?: string[]; doubles?: number[]; blobs?: string[] }> = [];

  writeDataPoint(point: { indexes?: string[]; doubles?: number[]; blobs?: string[] }): void {
    this.points.push({ indexes: point.indexes, doubles: point.doubles, blobs: point.blobs });
  }
}
