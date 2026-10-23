// Shared segment scope constants (spec 023, D3.3).
//
// A segment aggregates one receive-time bucket across every instance. Its
// manifest `instance_id` is therefore the synthetic scope `all`, while each row
// inside the DuckDB segment keeps its real `instance_id` column.

export const MULTI_INSTANCE_SCOPE = "all";

/** True when a segment scope spans instances rather than naming one. */
export function isCrossInstanceScope(scope: string | null | undefined): boolean {
  return scope === MULTI_INSTANCE_SCOPE;
}
