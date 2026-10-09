// Unguessable shot identifiers.
//
// A shot id is a capability: anyone holding it (plus the API token) can ingest
// telemetry for that shot. Use a CSPRNG and enough entropy that ids cannot be
// enumerated or guessed from each other.

/** 144 bits of randomness, URL-safe (no padding). */
export function newShotId(): string {
  const bytes = new Uint8Array(18);
  crypto.getRandomValues(bytes);
  return base64Url(bytes);
}

export function isShotId(value: string): boolean {
  return /^[A-Za-z0-9_-]{16,64}$/.test(value);
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
