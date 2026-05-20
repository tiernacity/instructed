/**
 * Shared conformance-harness helpers.
 *
 * Lifted out of `append.test.ts` / `read.test.ts` / `snapshot.test.ts`
 * when `subscription-persistent.test.ts` (step 5/8) became the third
 * caller needing the same `append`-for-setup + `rejectsWithCode`
 * shape. Per-procedure thin wrappers (e.g. `readStream`, `readBatch`,
 * `recordSnapshot`) stay local to their owning test file; only the
 * cross-file shapes live here.
 *
 * The full append surface (with expected_version variants) lives in
 * `append.test.ts` because it IS the test of that surface. This
 * module exports only the `'any'` shape that other test files need
 * for setup.
 */

import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";
import type pg from "pg";

export interface InputEvent {
  event_id?: string;
  event_type: string;
  data?: unknown;
  metadata?: unknown;
  causation_id?: string | null;
  correlation_id?: string | null;
}

/**
 * Append events with `expected_version_type = 'any'`. The setup-call
 * shape needed by every non-append test file. The `event_id` defaults
 * to a fresh UUID per event.
 */
export async function appendAny(
  q: pg.ClientBase | pg.Pool,
  streamUuid: string,
  events: InputEvent[],
): Promise<void> {
  const payload = events.map((e) => ({
    event_id: e.event_id ?? randomUUID(),
    event_type: e.event_type,
    data: e.data ?? {},
    ...(e.metadata !== undefined ? { metadata: e.metadata } : {}),
    ...(e.causation_id !== undefined ? { causation_id: e.causation_id } : {}),
    ...(e.correlation_id !== undefined
      ? { correlation_id: e.correlation_id }
      : {}),
  }));
  await q.query(
    `SELECT * FROM instructed.append_to_stream($1, 'any', NULL, $2::jsonb)`,
    [streamUuid, JSON.stringify(payload)],
  );
}

/** Assert a promise rejects with a PostgresError whose `code` matches. */
export async function rejectsWithCode(
  fn: () => Promise<unknown>,
  code: string,
): Promise<void> {
  await assert.rejects(fn, (err: unknown) => {
    const e = err as { code?: unknown };
    return typeof e.code === "string" && e.code === code;
  });
}
