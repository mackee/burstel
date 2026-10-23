// Streaming freeze of the per-shot DuckDB artifact into R2.
//
// Design goals:
//   * never buffer the whole database in Worker memory: single PUT streams the
//     artifact response body directly into R2; very large artifacts use a
//     bounded multipart upload that holds at most one part in memory;
//   * never mark a shot frozen unless the stored object size/checksum match
//     what the Go recorder reported.

export interface R2ObjectLike {
  key: string;
  size: number;
  etag?: string;
  /** Lowercase hex SHA-256 when the runtime exposes it. */
  sha256?: string;
  /** Object custom metadata (the DO writes `sha256` here on segment PUT). */
  customMetadata?: Record<string, string>;
}

export interface R2UploadedPartLike {
  partNumber: number;
  etag: string;
}

export interface R2MultipartUploadLike {
  readonly key: string;
  readonly uploadId: string;
  uploadPart(
    partNumber: number,
    value: Uint8Array | ArrayBuffer | ReadableStream<Uint8Array> | string | Blob,
  ): Promise<R2UploadedPartLike>;
  abort(): Promise<void>;
  complete(parts: Array<{ partNumber: number; etag: string }>): Promise<R2ObjectLike>;
}

export interface R2PutOptionsLike {
  sha256?: string;
  httpMetadata?: { contentType?: string; contentDisposition?: string };
  customMetadata?: Record<string, string>;
}

export interface R2ObjectBodyLike extends R2ObjectLike {
  body: ReadableStream<Uint8Array>;
  text(): Promise<string>;
  customMetadata?: Record<string, string>;
}

/** Bounded R2 listing request (spec 023, D3). */
export interface R2ListOptionsLike {
  prefix?: string;
  cursor?: string;
  /** Exclusive key cursor. Keys sort lexicographically. */
  startAfter?: string;
  limit?: number;
  /** "/" turns deeper keys into `delimitedPrefixes` instead of objects. */
  delimiter?: string;
  /** `["customMetadata"]` asks R2 to return per-object metadata. */
  include?: string[];
}

export interface R2ListResultLike {
  objects: R2ObjectLike[];
  truncated: boolean;
  cursor?: string;
  delimitedPrefixes?: string[];
}

/** Structural subset of the platform `R2Bucket` type. */
export interface R2BucketLike {
  put(
    key: string,
    value: Uint8Array | ArrayBuffer | ReadableStream<Uint8Array> | string | Blob | null,
    options?: R2PutOptionsLike,
  ): Promise<R2ObjectLike | null>;
  get(key: string): Promise<R2ObjectBodyLike | null>;
  /**
   * Metadata-only lookup. Used by the coordinator to re-verify an uploaded
   * segment's size/checksum before advertising it (spec 018). Optional so the
   * pure unit-test fakes that never publish segments can omit it.
   */
  head?(key: string): Promise<R2ObjectLike | null>;
  delete?(key: string): Promise<void>;
  /**
   * Prefix listing. Compaction discovers finalized journal objects directly
   * from R2 (spec 023, D3) instead of a per-request ledger.
   */
  list?(options?: R2ListOptionsLike): Promise<R2ListResultLike>;
  createMultipartUpload(key: string, options?: R2PutOptionsLike): Promise<R2MultipartUploadLike>;
}

export interface ArtifactSource {
  body: ReadableStream<Uint8Array> | null;
  size: number;
  /** Lowercase hex SHA-256 reported by the Go recorder. */
  sha256: string;
  contentType?: string;
}

export interface ArtifactResult {
  key: string;
  size: number;
  sha256: string;
  etag?: string;
  multipart: boolean;
}

/** A frozen artifact streamed out of R2. */
export interface ArtifactObject {
  body: ReadableStream<Uint8Array>;
  size: number;
  /** SHA-256 recorded in R2 custom metadata, when present. */
  sha256?: string;
}

export interface ArtifactSink {
  putArtifact(key: string, source: ArtifactSource): Promise<ArtifactResult>;
  putJSON(key: string, value: unknown): Promise<void>;
  getJSON<T>(key: string): Promise<T | null>;
  getArtifact(key: string): Promise<ArtifactObject | null>;
}

export interface R2SinkOptions {
  /** Objects at or below this size use a single streaming PUT. */
  singlePutMaxBytes?: number;
  /** Multipart part size; must be >= 5 MiB for all but the final part. */
  partSizeBytes?: number;
}

// R2's Workers API accepts a single PUT for objects up to 5 GiB; larger objects
// require multipart. Keep a small margin below the hard limit.
export const DEFAULT_SINGLE_PUT_MAX_BYTES = 5 * 1024 ** 3 - 1024 ** 2;
export const DEFAULT_PART_SIZE_BYTES = 64 * 1024 ** 2;

/** Builds an {@link ArtifactSink} over an R2 bucket binding. */
export function createR2Sink(bucket: R2BucketLike, options: R2SinkOptions = {}): ArtifactSink {
  const singlePutMax = options.singlePutMaxBytes ?? DEFAULT_SINGLE_PUT_MAX_BYTES;
  const partSize = options.partSizeBytes ?? DEFAULT_PART_SIZE_BYTES;

  return {
    async putArtifact(key, source) {
      const { body, size, sha256, contentType } = source;
      if (!body) throw new Error("artifact body is empty");
      if (!Number.isFinite(size) || size <= 0) {
        throw new Error(`artifact size must be a positive number, got ${size}`);
      }
      if (partSize < 5 * 1024 ** 2) {
        throw new Error("multipart part size must be at least 5 MiB");
      }

      if (size <= singlePutMax) {
        const object = await bucket.put(key, body, {
          sha256,
          httpMetadata: { contentType: contentType ?? "application/octet-stream" },
          customMetadata: { sha256 },
        });
        if (!object) {
          throw new Error("R2 PUT returned null (precondition failed)");
        }
        assertStoredSize(object.size, size, "single PUT");
        return { key, size: object.size, sha256, etag: object.etag, multipart: false };
      }

      const upload = await bucket.createMultipartUpload(key, {
        httpMetadata: { contentType: contentType ?? "application/octet-stream" },
        customMetadata: { sha256 },
      });
      try {
        const reader = new PartReader(body);
        const parts: Array<{ partNumber: number; etag: string }> = [];
        let partNumber = 1;
        let sent = 0;
        for (;;) {
          const part = await reader.nextPart(partSize);
          if (part === null) break;
          const uploaded = await upload.uploadPart(partNumber, part);
          parts.push({ partNumber: uploaded.partNumber, etag: uploaded.etag });
          sent += part.byteLength;
          partNumber += 1;
        }
        if (sent !== size) {
          throw new Error(`multipart uploaded ${sent} bytes, expected ${size}`);
        }
        const object = await upload.complete(parts);
        assertStoredSize(object.size, size, "multipart");
        return { key, size: object.size, sha256, etag: object.etag, multipart: true };
      } catch (error) {
        // Abort so a failed freeze does not leave a dangling upload. Incomplete
        // uploads are also auto-aborted by R2 after 7 days.
        await upload.abort().catch(() => undefined);
        throw error;
      }
    },

    async putJSON(key, value) {
      const object = await bucket.put(key, JSON.stringify(value), {
        httpMetadata: { contentType: "application/json" },
      });
      if (!object) throw new Error("R2 PUT of metadata returned null");
    },

    async getJSON<T>(key: string): Promise<T | null> {
      const object = await bucket.get(key);
      if (!object) return null;
      return JSON.parse(await object.text()) as T;
    },

    async getArtifact(key: string): Promise<ArtifactObject | null> {
      const object = await bucket.get(key);
      if (!object) return null;
      return {
        body: object.body,
        size: object.size,
        sha256: object.customMetadata?.["sha256"],
      };
    },
  };
}

function assertStoredSize(stored: number, expected: number, phase: string): void {
  if (stored !== expected) {
    throw new Error(`R2 ${phase} stored ${stored} bytes, expected ${expected}`);
  }
}

interface ReaderLike {
  read(): Promise<{ done: boolean; value?: Uint8Array }>;
}

/** Reads a stream in fixed-size parts without buffering more than one part. */
class PartReader {
  private readonly reader: ReaderLike;
  private leftover: Uint8Array | null = null;

  constructor(body: ReadableStream<Uint8Array>) {
    this.reader = body.getReader() as unknown as ReaderLike;
  }

  async nextPart(max: number): Promise<Uint8Array | null> {
    if (this.leftover) {
      const buf = this.leftover;
      this.leftover = null;
      if (buf.byteLength <= max) return buf;
      const part = buf.subarray(0, max);
      this.leftover = buf.subarray(max);
      return part;
    }

    const chunks: Uint8Array[] = [];
    let total = 0;
    while (total < max) {
      const { done, value } = await this.reader.read();
      if (done) break;
      if (!value || value.byteLength === 0) continue;
      const remaining = max - total;
      if (value.byteLength <= remaining) {
        chunks.push(value);
        total += value.byteLength;
      } else {
        chunks.push(value.subarray(0, remaining));
        this.leftover = value.subarray(remaining);
        total = max;
        break;
      }
    }
    if (total === 0) return null;
    if (chunks.length === 1) return chunks[0]!;
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return out;
  }
}
