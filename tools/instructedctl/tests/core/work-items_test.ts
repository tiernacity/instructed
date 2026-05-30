// Core tests for work-queue inspection.

import { assertEquals } from "@std/assert";
import { listFailedWorkItems, listWorkItemCounts } from "../../src/core/index.ts";
import { createThrowawayDb, type Throwaway, withThrowawayDb } from "../support.ts";

async function setup(tw: Throwaway): Promise<void> {
  await withThrowawayDb(tw, (db) =>
    db.exec(
      `insert into instructed.subscriptions (stream_id, subscription_name, last_seen)
         values (0, 'proj', 0);
       insert into instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state)
         values
           (0, 'proj', '_default', 5, 'pending'),
           (0, 'proj', '_default', 6, 'pending');
       insert into instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state,
          failed_at, error_text)
         values (0, 'proj', '_default', 4, 'failed', now(), 'boom');`,
    ));
}

Deno.test("listWorkItemCounts: counts by state with oldest active", async () => {
  const tw = await createThrowawayDb();
  try {
    await setup(tw);
    const counts = await withThrowawayDb(tw, (db) => listWorkItemCounts(db));
    assertEquals(counts.length, 1);
    const c = counts[0];
    assertEquals(c.subscriptionName, "proj");
    assertEquals(c.pending, 2);
    assertEquals(c.failed, 1);
    assertEquals(c.total, 3);
    assertEquals(c.oldestActiveEventNumber, 4);
  } finally {
    await tw.drop();
  }
});

Deno.test("listFailedWorkItems: returns failed rows with error text", async () => {
  const tw = await createThrowawayDb();
  try {
    await setup(tw);
    const failed = await withThrowawayDb(tw, (db) => listFailedWorkItems(db));
    assertEquals(failed.length, 1);
    assertEquals(failed[0].eventNumber, 4);
    assertEquals(failed[0].errorText, "boom");
    assertEquals(failed[0].partitionKey, "_default");

    const filteredEmpty = await withThrowawayDb(
      tw,
      (db) => listFailedWorkItems(db, "other"),
    );
    assertEquals(filteredEmpty, []);
  } finally {
    await tw.drop();
  }
});
