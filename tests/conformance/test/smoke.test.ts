/**
 * Smoke test for the conformance harness (Phase 9, step 1/8).
 *
 * Asserts only that the harness can:
 *   1. Connect to the docker-compose Postgres.
 *   2. Install `sql/instructed.sql` cleanly.
 *   3. `truncateAll` between cases without error.
 *   4. See the seeded `$all` row (stream_id = 0) afterwards.
 *
 * No INV-* coverage yet; that begins in step 2/8 (Part B — Append).
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import type pg from "pg";
import { closePool, getPool, truncateAll } from "./fixtures.ts";

describe("conformance harness — smoke", () => {
  let pool: pg.Pool;

  before(async () => {
    pool = await getPool();
  });

  beforeEach(async () => {
    await truncateAll(pool);
  });

  after(async () => {
    await closePool();
  });

  test("schema is installed and the instructed schema exists", async () => {
    const r = await pool.query<{ exists: boolean }>(
      `SELECT EXISTS(
         SELECT 1 FROM information_schema.schemata
         WHERE schema_name = 'instructed'
       ) AS exists`,
    );
    assert.equal(r.rows[0].exists, true);
  });

  test("$all stream is seeded with stream_id = 0 and version = 0", async () => {
    const r = await pool.query<{
      stream_id: string;
      stream_uuid: string;
      stream_version: string;
    }>(
      `SELECT stream_id, stream_uuid, stream_version
         FROM instructed.streams
        WHERE stream_uuid = '$all'`,
    );
    assert.equal(r.rowCount, 1);
    assert.equal(r.rows[0].stream_id, "0");
    assert.equal(r.rows[0].stream_uuid, "$all");
    assert.equal(r.rows[0].stream_version, "0");
  });

  test("every documented procedure is callable (signature smoke)", async () => {
    // Per docs/sql-contract.md the closed procedure catalogue is the
    // following. We don't invoke them with valid arguments here —
    // each gets its own INV-* coverage in steps 2-7. We only assert
    // that each routine is present and registered with the expected
    // argument count, so that a future schema rename surfaces here
    // before it surfaces as a confusing failure in a real case.
    const expected: ReadonlyArray<string> = [
      "append_to_stream",
      "read_stream",
      "read_all",
      "record_snapshot",
      "read_snapshot",
      "delete_snapshot",
      "claim_subscription",
      "release_subscription",
      "read_subscription_batch",
      "advance_subscription",
      "delete_subscription",
      // SUB-A work-queue procedures (slice 2).
      "route_batch",
      "claim_work_item",
      "extend_work_item_claim",
      "complete_work_item_projection",
      "complete_work_item_pm",
      "complete_pm_instance",
      "fail_work_item",
      "is_subscription_caught_up",
      // SUB-A slice 7: PM-state rebuild cold path.
      "list_pm_rebuild_events",
    ];

    const r = await pool.query<{ proname: string }>(
      `SELECT proname
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'instructed'
          AND p.prokind = 'f'
        ORDER BY proname`,
    );
    const found = new Set(r.rows.map((row) => row.proname));

    for (const name of expected) {
      assert.ok(
        found.has(name),
        `expected function instructed.${name} to exist`,
      );
    }
    // Arg counts are deliberately not asserted here — they are a
    // sql-contract concern, not a conformance concern. Mere presence
    // is enough for step 1/8; each procedure gets full INV-* coverage
    // in steps 2–7.
  });
});
