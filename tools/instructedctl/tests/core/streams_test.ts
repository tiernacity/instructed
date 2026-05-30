// Core tests for stream inspection and event reads.

import { assertEquals, assertRejects } from "@std/assert";
import {
  getStream,
  listStreams,
  readAll,
  readStream,
  StreamNotFound,
} from "../../src/core/index.ts";
import { createThrowawayDb, type Throwaway, withThrowawayDb } from "../support.ts";

async function append(
  tw: Throwaway,
  streamUuid: string,
  types: string[],
): Promise<void> {
  const events = types
    .map(
      (t) =>
        `jsonb_build_object('event_id', gen_random_uuid(), 'event_type', '${t}', 'data', '{}'::jsonb)`,
    )
    .join(",");
  await withThrowawayDb(tw, (db) =>
    db.exec(
      `select instructed.append_to_stream('${streamUuid}', 'any', null,
         jsonb_build_array(${events}))`,
    ));
}

Deno.test("listStreams: reports head and event count, excludes $all", async () => {
  const tw = await createThrowawayDb();
  try {
    await append(tw, "acct-1", ["Opened", "Deposited"]);
    await append(tw, "acct-2", ["Opened"]);
    const streams = await withThrowawayDb(tw, listStreams);
    assertEquals(streams.map((s) => s.streamUuid), ["acct-1", "acct-2"]);
    assertEquals(streams[0].head, 2);
    assertEquals(streams[0].eventCount, 2);
    assertEquals(streams[1].head, 1);
  } finally {
    await tw.drop();
  }
});

Deno.test("getStream: returns one stream, null when missing", async () => {
  const tw = await createThrowawayDb();
  try {
    await append(tw, "acct-1", ["Opened"]);
    const found = await withThrowawayDb(tw, (db) => getStream(db, "acct-1"));
    assertEquals(found?.streamUuid, "acct-1");
    const missing = await withThrowawayDb(tw, (db) => getStream(db, "nope"));
    assertEquals(missing, null);
  } finally {
    await tw.drop();
  }
});

Deno.test("readStream: returns events in order, throws StreamNotFound", async () => {
  const tw = await createThrowawayDb();
  try {
    await append(tw, "acct-1", ["Opened", "Deposited", "Withdrew"]);
    const events = await withThrowawayDb(
      tw,
      (db) => readStream(db, { streamUuid: "acct-1", from: 2, count: 10 }),
    );
    assertEquals(events.map((e) => e.eventType), ["Deposited", "Withdrew"]);
    assertEquals(events[0].streamVersion, 2);

    await assertRejects(
      () =>
        withThrowawayDb(
          tw,
          (db) => readStream(db, { streamUuid: "ghost", from: 1, count: 10 }),
        ),
      StreamNotFound,
    );
  } finally {
    await tw.drop();
  }
});

Deno.test("readAll: returns events across streams in event_number order", async () => {
  const tw = await createThrowawayDb();
  try {
    await append(tw, "acct-1", ["Opened", "Deposited"]);
    await append(tw, "acct-2", ["Opened"]);
    const events = await withThrowawayDb(
      tw,
      (db) => readAll(db, { from: 0, count: 10 }),
    );
    assertEquals(events.map((e) => e.eventNumber), [1, 2, 3]);
    assertEquals(events.map((e) => e.streamUuid), ["acct-1", "acct-1", "acct-2"]);

    const tail = await withThrowawayDb(tw, (db) => readAll(db, { from: 3, count: 10 }));
    assertEquals(tail.map((e) => e.eventNumber), [3]);
  } finally {
    await tw.drop();
  }
});
