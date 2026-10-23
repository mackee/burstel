// Spec 024 R7 review regression: listing discovery must not let one signal
// prefix that has already reached the exclusive upper bound hide eligible
// objects in another signal/instance prefix.

import { describe, expect, test } from "bun:test";

import { listJournalObjects, parseJournalKey } from "../src/journal-listing";
import { FakeBucket } from "./fakes";

async function put(bucket: FakeBucket, key: string, receivedAt: number): Promise<void> {
  const body = new TextEncoder().encode(key);
  await bucket.put(key, body, {
    customMetadata: {
      signal: parseJournalKey(key)!.signal,
      instance_id: parseJournalKey(key)!.instanceId,
      instance: parseJournalKey(key)!.instanceId,
      received_at: String(receivedAt),
      content_type: "application/x-protobuf",
    },
  });
}

describe("journal listing discovery", () => {
  test("an upper-bound hit in one signal prefix does not skip other prefixes", async () => {
    const bucket = new FakeBucket();
    const now = 1_700_000_000_000;
    // Sorted prefix order is journal/a/ then journal/b/. Prefix a/logs has a
    // future object first; prefix b/traces has an eligible older object.
    await put(bucket, `journal/a/logs/${now + 60_000}-future.bin`, now + 60_000);
    await put(bucket, `journal/b/traces/${now - 5_000}-eligible.bin`, now - 5_000);

    const result = await listJournalObjects(bucket, {
      fromMs: 0,
      toMs: now,
      maxObjects: 10,
      maxScan: 100,
      pageSize: 10,
    });

    expect(result.objects.map((o) => o.key)).toEqual([`journal/b/traces/${now - 5_000}-eligible.bin`]);
    // Hitting the bound is not truncation: the prefix was fully examined.
    expect(result.truncated).toBe(false);
  });

  test("the exclusive upper bound is strict and fromMs is inclusive", async () => {
    const bucket = new FakeBucket();
    const now = 1_700_000_000_000;
    await put(bucket, `journal/a/traces/${now - 1}-before.bin`, now - 1);
    await put(bucket, `journal/a/traces/${now}-at.bin`, now);

    const result = await listJournalObjects(bucket, {
      fromMs: now - 1,
      toMs: now,
      maxObjects: 10,
      pageSize: 10,
    });
    expect(result.objects.map((o) => o.key)).toEqual([`journal/a/traces/${now - 1}-before.bin`]);
  });
});
