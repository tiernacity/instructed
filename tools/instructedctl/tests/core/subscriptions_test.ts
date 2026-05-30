// Core tests for subscription inspection.

import { assertEquals } from "@std/assert";
import { getSubscription, listSubscriptions } from "../../src/core/index.ts";
import { createThrowawayDb, type Throwaway, withThrowawayDb } from "../support.ts";

// Create a subscription row on $all (stream_id 0) with a given cursor.
async function makeSubscription(
  tw: Throwaway,
  name: string,
  lastSeen: number,
): Promise<void> {
  await withThrowawayDb(tw, (db) =>
    db.exec(
      `insert into instructed.subscriptions (stream_id, subscription_name, last_seen)
       values (0, '${name}', ${lastSeen})`,
    ));
}

// Bump $all's head so lag is observable.
async function setAllHead(tw: Throwaway, head: number): Promise<void> {
  await withThrowawayDb(tw, (db) =>
    db.exec(
      `update instructed.streams set stream_version = ${head} where stream_id = 0`,
    ));
}

Deno.test("listSubscriptions: empty store returns []", async () => {
  const tw = await createThrowawayDb();
  try {
    const subs = await withThrowawayDb(tw, listSubscriptions);
    assertEquals(subs, []);
  } finally {
    await tw.drop();
  }
});

Deno.test("listSubscriptions: reports cursor, stream, and lag", async () => {
  const tw = await createThrowawayDb();
  try {
    await setAllHead(tw, 10);
    await makeSubscription(tw, "projector", 4);
    const subs = await withThrowawayDb(tw, listSubscriptions);
    assertEquals(subs.length, 1);
    const s = subs[0];
    assertEquals(s.subscriptionName, "projector");
    assertEquals(s.streamUuid, "$all");
    assertEquals(s.lastSeen, 4);
    assertEquals(s.lag, 6);
    assertEquals(s.claimedBy, null);
  } finally {
    await tw.drop();
  }
});

Deno.test("getSubscription: returns the matching row, [] when missing", async () => {
  const tw = await createThrowawayDb();
  try {
    await makeSubscription(tw, "pm:transfer", 0);
    const found = await withThrowawayDb(tw, (db) => getSubscription(db, "pm:transfer"));
    assertEquals(found.length, 1);
    assertEquals(found[0].subscriptionName, "pm:transfer");

    const missing = await withThrowawayDb(tw, (db) => getSubscription(db, "nope"));
    assertEquals(missing, []);
  } finally {
    await tw.drop();
  }
});
