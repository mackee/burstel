// Shared cryptographic primitives for the Worker, the Durable Object cores and
// the instance/token registries.
//
// Kept Cloudflare- and Node-free so the exact same code runs in production
// (Workers runtime over DO SQLite) and in unit tests (bun:sqlite).

/** Lowercase hex SHA-256 digest of a UTF-8 string. */
export async function sha256Hex(text: string): Promise<string> {
  return sha256HexBytes(new TextEncoder().encode(text));
}

/** Lowercase hex SHA-256 digest of raw bytes. */
export async function sha256HexBytes(bytes: Uint8Array): Promise<string> {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  const digest = await crypto.subtle.digest("SHA-256", copy.buffer);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Constant-time-ish string comparison folded over both lengths. Used to compare
 * a presented token's digest against every stored digest without an early
 * return that would leak which row (if any) matched.
 */
export function constantTimeEqual(a: string, b: string): boolean {
  const len = Math.max(a.length, b.length);
  let diff = a.length ^ b.length;
  for (let i = 0; i < len; i++) {
    diff |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  }
  return diff === 0;
}

/** 256 bits of CSPRNG entropy, URL-safe (no padding). */
export function randomBase64Url(bytes = 32): string {
  const buf = new Uint8Array(bytes);
  crypto.getRandomValues(buf);
  return base64Url(buf);
}

/** URL-safe base64 without padding. */
export function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Lowercase hex id of `bytes` bytes of CSPRNG entropy (default 128 bits). */
export function randomHexId(bytes = 16): string {
  const buf = new Uint8Array(bytes);
  crypto.getRandomValues(buf);
  return [...buf].map((b) => b.toString(16).padStart(2, "0")).join("");
}
