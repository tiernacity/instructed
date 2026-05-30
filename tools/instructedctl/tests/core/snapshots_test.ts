// Core tests for snapshot inspection.

import { assertEquals } from "@std/assert";
import { getSnapshot } from "../../src/core/index.ts";
import { createThrowawayDb, withThrowawayDb } from "../support.ts";

Deno.test("getSnapshot: returns a snapshot, null when missing", async () => {
  const tw = await createThrowawayDb();
  try {
    await withThrowawayDb(tw, (db) =>
      db.exec(
        `select instructed.record_snapshot('acct-1', 'Account', 2,
           '{"bal":50}'::jsonb, '{"v":1}'::jsonb)`,
      ));

    const snap = await withThrowawayDb(tw, (db) => getSnapshot(db, "acct-1"));
    assertEquals(snap?.sourceUuid, "acct-1");
    assertEquals(snap?.sourceType, "Account");
    assertEquals(snap?.sourceVersion, 2);
    assertEquals(snap?.data, { bal: 50 });
    assertEquals(snap?.metadata, { v: 1 });

    const missing = await withThrowawayDb(tw, (db) => getSnapshot(db, "nope"));
    assertEquals(missing, null);
  } finally {
    await tw.drop();
  }
});
