/**
 * L2 → L1 input packers for the `Client` write path: turning the
 * TypeScript-facing `NewEvent` / `ExpectedVersion` shapes into the
 * positional / column shapes the stored procedures expect.
 */

import type { ExpectedVersion, NewEvent } from "../types/index.ts";

export function packEvent<E = unknown>(
  e: NewEvent<E>,
): Record<string, unknown> {
  // L2 → L1 boundary: the TypeScript surface uses `type`; the SQL
  // column is `event_type`. The rename is purely TS-facing; the L1
  // wire contract (and any deployed database schema) is unchanged.
  const out: Record<string, unknown> = {
    event_id: e.event_id,
    event_type: e.type,
    data: e.data ?? null,
  };
  if (e.metadata !== undefined) out.metadata = e.metadata;
  if (e.causation_id !== undefined) out.causation_id = e.causation_id;
  if (e.correlation_id !== undefined) out.correlation_id = e.correlation_id;
  return out;
}

export function expectedVersionParams(
  ev: ExpectedVersion,
): { type: string; version: string | null } {
  switch (ev.kind) {
    case "any":
      return { type: "any", version: null };
    case "noStream":
      return { type: "no_stream", version: null };
    case "streamExists":
      return { type: "stream_exists", version: null };
    case "exact":
      return { type: "exact", version: ev.version.toString() };
  }
}
