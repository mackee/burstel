import { describe, expect, test } from "bun:test";

import { createR2Sink } from "../src/artifacts";
import type { R2BucketLike, R2MultipartUploadLike, R2ObjectBodyLike, R2ObjectLike } from "../src/artifacts";

class FakeR2Bucket implements R2BucketLike {
  readonly objects = new Map<string, Uint8Array>();
  abortCount = 0;
  partsUploaded = 0;
  completeCount = 0;
  failUploadPart = false;

  async put(
    key: string,
    value: Uint8Array | ArrayBuffer | ReadableStream<Uint8Array> | string | Blob | null,
  ): Promise<R2ObjectLike | null> {
    if (value === null) throw new Error("null body");
    const bytes = await toBytes(value);
    this.objects.set(key, bytes);
    return { key, size: bytes.byteLength, etag: `"${bytes.byteLength}"` };
  }

  async get(key: string): Promise<R2ObjectBodyLike | null> {
    const bytes = this.objects.get(key);
    if (!bytes) return null;
    return {
      key,
      size: bytes.byteLength,
      body: new Response(bytes).body!,
      text: async () => new TextDecoder().decode(bytes),
    };
  }

  async createMultipartUpload(key: string): Promise<R2MultipartUploadLike> {
    const parts: Uint8Array[] = [];
    const self = this;
    return {
      key,
      uploadId: "mpu-1",
      async uploadPart(partNumber, value) {
        self.partsUploaded += 1;
        if (self.failUploadPart) throw new Error("part upload failed");
        parts[partNumber - 1] = await toBytes(value as Uint8Array);
        return { partNumber, etag: `part-${partNumber}` };
      },
      async abort() {
        self.abortCount += 1;
      },
      async complete() {
        self.completeCount += 1;
        const total = concat(parts);
        self.objects.set(key, total);
        return { key, size: total.byteLength, etag: '"complete"' };
      },
    };
  }
}

async function toBytes(
  value: Uint8Array | ArrayBuffer | ReadableStream<Uint8Array> | string | Blob,
): Promise<Uint8Array> {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (typeof value === "string") return new TextEncoder().encode(value);
  return new Uint8Array(await new Response(value as ReadableStream).arrayBuffer());
}

function concat(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, p) => sum + p.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

function streamOf(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

describe("R2 artifact sink", () => {
  test("streams a small artifact with a single PUT and validates size", async () => {
    const bucket = new FakeR2Bucket();
    const sink = createR2Sink(bucket, { singlePutMaxBytes: 1 << 20 });
    const bytes = new Uint8Array(100).fill(7);

    const result = await sink.putArtifact("shots/a/telemetry.duckdb", {
      body: streamOf(bytes),
      size: bytes.byteLength,
      sha256: "abc",
    });

    expect(result.multipart).toBe(false);
    expect(result.size).toBe(100);
    expect(bucket.objects.get("shots/a/telemetry.duckdb")).toEqual(bytes);
  });

  test("rejects an artifact whose stored size differs from the reported size", async () => {
    const bucket = new FakeR2Bucket();
    const sink = createR2Sink(bucket, { singlePutMaxBytes: 1 << 20 });
    const bytes = new Uint8Array(100).fill(9);

    await expect(
      sink.putArtifact("shots/b/telemetry.duckdb", {
        body: streamOf(bytes),
        size: 99, // lie about the length
        sha256: "abc",
      }),
    ).rejects.toThrow(/expected 99/);
  });

  test("uses a bounded multipart upload above the single-PUT limit", async () => {
    const bucket = new FakeR2Bucket();
    const fiveMiB = 5 * 1024 * 1024;
    const sink = createR2Sink(bucket, { singlePutMaxBytes: 1024, partSizeBytes: fiveMiB });
    const bytes = new Uint8Array(11 * 1024 * 1024).fill(3);

    const result = await sink.putArtifact("shots/big/telemetry.duckdb", {
      body: streamOf(bytes),
      size: bytes.byteLength,
      sha256: "def",
    });

    expect(result.multipart).toBe(true);
    expect(bucket.partsUploaded).toBe(3); // 5 MiB + 5 MiB + 1 MiB
    expect(bucket.completeCount).toBe(1);
    expect(bucket.objects.get("shots/big/telemetry.duckdb")).toEqual(bytes);
  });

  test("aborts the multipart upload when a part fails", async () => {
    const bucket = new FakeR2Bucket();
    bucket.failUploadPart = true;
    const fiveMiB = 5 * 1024 * 1024;
    const sink = createR2Sink(bucket, { singlePutMaxBytes: 1, partSizeBytes: fiveMiB });
    const bytes = new Uint8Array(fiveMiB).fill(1);

    await expect(
      sink.putArtifact("shots/fail/telemetry.duckdb", {
        body: streamOf(bytes),
        size: bytes.byteLength,
        sha256: "xyz",
      }),
    ).rejects.toThrow(/part upload failed/);
    expect(bucket.abortCount).toBe(1);
  });
});
