// Core tests for subscription inspection.

import { assertEquals, assertRejects } from "@std/assert";
import {
  claimSubscription,
  deleteSubscription,
  getSubscription,
  listSubscriptions,
  rebuildSubscription,
  releaseSubscription,
  SubscriptionNotClaimed,
  SubscriptionNotFound,
} from "../../src/core/index.ts";
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

async function insertWorkItem(
  tw: Throwaway,
  name: string,
  eventNumber: number,
): Promise<void> {
  await withThrowawayDb(tw, (db) =>
    db.exec(
      `insert into instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state)
       values (0, '${name}', '_default', ${eventNumber}, 'pending')`,
    ));
}

async function countWorkItems(tw: Throwaway, name: string): Promise<number> {
  const rows = await withThrowawayDb(tw, (db) =>
    db.query<{ n: string }>(
      `select count(*) as n from instructed.subscription_work_items
        where subscription_name = $1`,
      [name],
    ));
  return Number(rows[0].n);
}

async function cursorOf(tw: Throwaway, name: string): Promise<number> {
  const subs = await withThrowawayDb(tw, (db) => getSubscription(db, name));
  return subs[0].lastSeen;
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

Deno.test("claimSubscription: creates at origin, then reports already_claimed", async () => {
  const tw = await createThrowawayDb();
  try {
    const first = await withThrowawayDb(
      tw,
      (db) => claimSubscription(db, { name: "proj", workerId: "w1", leaseSeconds: 60 }),
    );
    assertEquals(first.result, "claimed");
    assertEquals(first.lastSeen, 0);
    assertEquals(first.claimedBy, "w1");

    const second = await withThrowawayDb(
      tw,
      (db) => claimSubscription(db, { name: "proj", workerId: "w2", leaseSeconds: 60 }),
    );
    assertEquals(second.result, "already_claimed");
    assertEquals(second.claimedBy, "w1");
  } finally {
    await tw.drop();
  }
});

Deno.test("releaseSubscription: auto-detects the holder and frees the lease", async () => {
  const tw = await createThrowawayDb();
  try {
    await withThrowawayDb(
      tw,
      (db) => claimSubscription(db, { name: "proj", workerId: "w1", leaseSeconds: 60 }),
    );
    const result = await withThrowawayDb(
      tw,
      (db) => releaseSubscription(db, { name: "proj" }),
    );
    assertEquals(result.releasedFrom, "w1");
    const subs = await withThrowawayDb(tw, (db) => getSubscription(db, "proj"));
    assertEquals(subs[0].claimedBy, null);
  } finally {
    await tw.drop();
  }
});

Deno.test("releaseSubscription: throws when not claimed / not found", async () => {
  const tw = await createThrowawayDb();
  try {
    await makeSubscription(tw, "idle", 0);
    await assertRejects(
      () => withThrowawayDb(tw, (db) => releaseSubscription(db, { name: "idle" })),
      SubscriptionNotClaimed,
    );
    await assertRejects(
      () => withThrowawayDb(tw, (db) => releaseSubscription(db, { name: "ghost" })),
      SubscriptionNotFound,
    );
  } finally {
    await tw.drop();
  }
});

Deno.test("deleteSubscription: removes the row, cascades work items, errors when missing", async () => {
  const tw = await createThrowawayDb();
  try {
    await makeSubscription(tw, "proj", 5);
    await insertWorkItem(tw, "proj", 1);
    await insertWorkItem(tw, "proj", 2);
    assertEquals(await countWorkItems(tw, "proj"), 2);

    await withThrowawayDb(tw, (db) => deleteSubscription(db, { name: "proj" }));
    assertEquals(
      (await withThrowawayDb(tw, (db) => getSubscription(db, "proj"))).length,
      0,
    );
    assertEquals(await countWorkItems(tw, "proj"), 0);

    await assertRejects(
      () => withThrowawayDb(tw, (db) => deleteSubscription(db, { name: "proj" })),
      SubscriptionNotFound,
    );
  } finally {
    await tw.drop();
  }
});

Deno.test("rebuildSubscription: resets the cursor to origin and drops work items", async () => {
  const tw = await createThrowawayDb();
  try {
    await makeSubscription(tw, "proj", 42);
    await insertWorkItem(tw, "proj", 1);
    assertEquals(await cursorOf(tw, "proj"), 42);
    assertEquals(await countWorkItems(tw, "proj"), 1);

    const result = await withThrowawayDb(
      tw,
      (db) => rebuildSubscription(db, { name: "proj" }),
    );
    assertEquals(result.existed, true);
    assertEquals(await cursorOf(tw, "proj"), 0);
    assertEquals(await countWorkItems(tw, "proj"), 0);
    const subs = await withThrowawayDb(tw, (db) => getSubscription(db, "proj"));
    assertEquals(subs[0].claimedBy, null);
  } finally {
    await tw.drop();
  }
});

Deno.test("rebuildSubscription: creates the subscription when it does not exist", async () => {
  const tw = await createThrowawayDb();
  try {
    const result = await withThrowawayDb(
      tw,
      (db) => rebuildSubscription(db, { name: "fresh" }),
    );
    assertEquals(result.existed, false);
    const subs = await withThrowawayDb(tw, (db) => getSubscription(db, "fresh"));
    assertEquals(subs.length, 1);
    assertEquals(subs[0].lastSeen, 0);
    assertEquals(subs[0].claimedBy, null);
  } finally {
    await tw.drop();
  }
});
