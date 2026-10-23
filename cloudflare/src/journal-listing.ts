// Listing-based journal discovery (spec 023, D3).
//
// There is no per-request ledger any more. The raw journal is an ordered set of
// R2 keys:
//
//   journal/{instance_id}/{signal}/{received_at_ms}-{rand}.bin
//
// `received_at_ms` is a fixed-width 13-digit epoch-ms value, so keys under one
// `journal/{instance}/{signal}/` prefix sort by receive time. Discovery seeks
// with `startAfter` instead of scanning the whole bucket, and stops as soon as
// the next key is at/after the exclusive upper bound.
//
// The same primitive backs compaction discovery, the window "pending journal"
// disclosure and the operational journal stats, so all three agree.

import type { R2BucketLike, R2ObjectLike } from "./artifacts";

/** One finalized raw export as recorded by the Worker. */
export interface JournalObjectInfo {
  key: string;
  instance_id: string;
  instance: string;
  signal: string;
  received_at: number;
  token_id: string;
  sha256: string;
  content_encoding: string;
  content_type: string;
  size: number;
}

export interface ListJournalOptions {
  /** Restrict to these instance ids. Empty/undefined means every instance. */
  instanceIds?: string[];
  /** Inclusive lower bound on `received_at`. */
  fromMs?: number;
  /** Exclusive upper bound on `received_at`. */
  toMs?: number;
  /** Maximum objects to return (bounded). */
  maxObjects: number;
  /** Maximum raw listing entries examined before giving up (default 4x max). */
  maxScan?: number;
  /** Per-page listing limit. */
  pageSize?: number;
}

export interface ListJournalResult {
  objects: JournalObjectInfo[];
  /** True when the scan stopped at a bound before reaching `toMs`. */
  truncated: boolean;
  /** Number of raw R2 entries examined. */
  scanned: number;
}

/**
 * Lists journal objects in receive-time order across instances.
 *
 * The `truncated` flag lets the compactor advance its discovery floor only as
 * far as it actually scanned, so a bounded tick can never skip a backlog.
 */
export async function listJournalObjects(
  bucket: R2BucketLike,
  options: ListJournalOptions,
): Promise<ListJournalResult> {
  const maxObjects = Math.max(1, Math.floor(options.maxObjects));
  const maxScan = Math.max(maxObjects, Math.floor(options.maxScan ?? maxObjects * 4));
  const pageSize = Math.max(1, Math.min(Math.floor(options.pageSize ?? 1000), 1000));
  const fromMs = options.fromMs ?? 0;
  const toMs = options.toMs ?? Number.MAX_SAFE_INTEGER;
  const objects: JournalObjectInfo[] = [];
  let truncated = false;
  let scanned = 0;

  if (!bucket.list) return { objects, truncated: false, scanned: 0 };

  const instancePrefixes = await discoverInstancePrefixes(bucket, options.instanceIds);
  outer: for (const instancePrefix of instancePrefixes) {
    const signalPrefixes = await discoverSignalPrefixes(bucket, instancePrefix);
    // Each signal prefix is independently time-ordered. Hitting the exclusive
    // upper bound in one prefix must not skip older objects in another signal
    // (or another instance) prefix: only that prefix is exhausted.
    signalLoop: for (const signalPrefix of signalPrefixes) {
      let cursor: string | undefined;
      let startAfter = `${signalPrefix}${timeCursor(fromMs)}`;
      for (;;) {
        if (scanned >= maxScan || objects.length >= maxObjects) {
          truncated = true;
          break outer;
        }
        const page = await bucket.list({
          prefix: signalPrefix,
          startAfter,
          cursor,
          limit: pageSize,
          include: ["customMetadata"],
        });
        let reachedUpperBound = false;
        for (const obj of page.objects) {
          scanned += 1;
          const info = toJournalInfo(obj, signalPrefix);
          if (!info) continue;
          if (info.received_at >= toMs) {
            // Keys are time-ordered inside a prefix; nothing later in *this*
            // prefix can qualify, but other prefixes still might.
            reachedUpperBound = true;
            break;
          }
          if (info.received_at < fromMs) continue;
          objects.push(info);
          if (objects.length >= maxObjects || scanned >= maxScan) {
            truncated = true;
            break outer;
          }
        }
        if (reachedUpperBound) continue signalLoop;
        if (!page.truncated || !page.cursor) break;
        cursor = page.cursor;
        startAfter = "";
      }
    }
  }

  objects.sort((a, b) => a.received_at - b.received_at || a.key.localeCompare(b.key));
  return { objects, truncated, scanned };
}

/** Every journal object currently stored, bounded for a status call. */
export async function listAllJournalObjects(
  bucket: R2BucketLike,
  maxObjects: number,
): Promise<ListJournalResult> {
  return listJournalObjects(bucket, { maxObjects, maxScan: maxObjects * 2 });
}

async function discoverInstancePrefixes(
  bucket: R2BucketLike,
  instanceIds: string[] | undefined,
): Promise<string[]> {
  if (instanceIds && instanceIds.length > 0) {
    return instanceIds.filter((id) => id !== "").map((id) => `journal/${id}/`);
  }
  const prefix = "journal/";
  const page = await bucket.list!({ prefix, delimiter: "/", limit: 1000 });
  const prefixes = (page.delimitedPrefixes ?? []).filter((p) => p.startsWith(prefix) && p !== prefix);
  if (prefixes.length > 0) return prefixes;
  // Backend without delimited prefixes: parse keys from a bounded page.
  const found = new Set<string>();
  for (const obj of page.objects) {
    const parsed = parseJournalKey(obj.key);
    if (parsed) found.add(`journal/${parsed.instanceId}/`);
  }
  return [...found].sort();
}

async function discoverSignalPrefixes(bucket: R2BucketLike, instancePrefix: string): Promise<string[]> {
  const page = await bucket.list!({ prefix: instancePrefix, delimiter: "/", limit: 1000 });
  const prefixes = (page.delimitedPrefixes ?? []).filter(
    (p) => p.startsWith(instancePrefix) && p !== instancePrefix,
  );
  if (prefixes.length > 0) return prefixes.sort();
  const found = new Set<string>();
  for (const obj of page.objects) {
    const parsed = parseJournalKey(obj.key);
    if (parsed) found.add(`journal/${parsed.instanceId}/${parsed.signal}/`);
  }
  return [...found].sort();
}

/** 13-digit epoch-ms cursor; keys before it are skipped by `startAfter`. */
function timeCursor(fromMs: number): string {
  const clamped = Math.max(0, Math.floor(fromMs));
  return String(clamped).padStart(13, "0");
}

export interface ParsedJournalKey {
  instanceId: string;
  signal: string;
  receivedAt: number;
  name: string;
}

/** Strict parse of `journal/{instance}/{signal}/{receivedAt}-{rand}.bin`. */
export function parseJournalKey(key: string): ParsedJournalKey | null {
  const parts = key.split("/");
  if (parts.length !== 4 || parts[0] !== "journal") return null;
  const [, instanceId, signal, name] = parts;
  if (!instanceId || !signal || !name) return null;
  const match = /^(\d{13})-.+\.bin$/.exec(name);
  if (!match) return null;
  const receivedAt = Number.parseInt(match[1]!, 10);
  if (!Number.isFinite(receivedAt)) return null;
  return { instanceId, signal, receivedAt, name };
}

function toJournalInfo(obj: R2ObjectLike, signalPrefix: string): JournalObjectInfo | null {
  const parsed = parseJournalKey(obj.key);
  if (!parsed || !obj.key.startsWith(signalPrefix)) return null;
  const meta = obj.customMetadata ?? {};
  const received = Number.parseInt(meta.received_at ?? "", 10);
  const instanceId = meta.instance_id && meta.instance_id !== "" ? meta.instance_id : parsed.instanceId;
  const signal = meta.signal && meta.signal !== "" ? meta.signal : parsed.signal;
  return {
    key: obj.key,
    instance_id: instanceId,
    instance: meta.instance ?? "",
    signal,
    received_at: Number.isFinite(received) && received > 0 ? received : parsed.receivedAt,
    token_id: meta.token_id ?? "",
    sha256: (meta.sha256 ?? obj.sha256 ?? "").toLowerCase(),
    content_encoding: (meta.content_encoding ?? "").toLowerCase(),
    content_type: meta.content_type ?? "application/x-protobuf",
    size: obj.size,
  };
}
