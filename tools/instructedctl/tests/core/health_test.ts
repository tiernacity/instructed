// Core tests for the health check.

import { assertEquals } from "@std/assert";
import { checkHealth, type HealthReport } from "../../src/core/index.ts";
import { createThrowawayDb, withThrowawayDb } from "../support.ts";

function check(report: HealthReport, name: string) {
  return report.checks.find((c) => c.name === name)!;
}

Deno.test("checkHealth: a fresh store is sound", async () => {
  const tw = await createThrowawayDb();
  try {
    const report = await withThrowawayDb(tw, checkHealth);
    assertEquals(report.ok, true);
    assertEquals(report.checks.length, 4);
  } finally {
    await tw.drop();
  }
});

Deno.test("checkHealth: flags an expired-lease zombie", async () => {
  const tw = await createThrowawayDb();
  try {
    await withThrowawayDb(tw, (db) =>
      db.exec(
        `insert into instructed.subscriptions
           (stream_id, subscription_name, last_seen, claimed_by, claim_expires_at)
         values (0, 'proj', 0, 'deadworker', now() - interval '1 hour')`,
      ));
    const report = await withThrowawayDb(tw, checkHealth);
    assertEquals(report.ok, false);
    assertEquals(check(report, "no expired-lease zombies").ok, false);
    assertEquals(check(report, "$all contiguous").ok, true);
  } finally {
    await tw.drop();
  }
});

Deno.test("checkHealth: passes contiguity with appended events", async () => {
  const tw = await createThrowawayDb();
  try {
    await withThrowawayDb(tw, (db) =>
      db.exec(
        `select instructed.append_to_stream('s-1', 'any', null,
           jsonb_build_array(jsonb_build_object(
             'event_id', gen_random_uuid(), 'event_type', 'E', 'data', '{}'::jsonb)))`,
      ));
    const report = await withThrowawayDb(tw, checkHealth);
    assertEquals(check(report, "$all contiguous").ok, true);
    assertEquals(check(report, "$all contiguous").detail, "1 events, head 1");
  } finally {
    await tw.drop();
  }
});
