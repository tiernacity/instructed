// Core tests for the schema lifecycle. These call the core functions directly
// against a throwaway database — the clean test boundary. Requires the
// docker-compose Postgres.

import { assertEquals, assertRejects } from "@std/assert";
import {
  getSchemaVersion,
  getStatus,
  installSchema,
  SchemaAlreadyInstalled,
  schemaPresent,
} from "../../src/core/index.ts";
import {
  createEmptyDb,
  createThrowawayDb,
  type Throwaway,
  withThrowawayDb,
} from "../support.ts";

async function seedEvent(tw: Throwaway, streamUuid: string): Promise<void> {
  await withThrowawayDb(tw, (db) =>
    db.query(
      `select * from instructed.append_to_stream(
         $1, 'any', null,
         jsonb_build_array(jsonb_build_object(
           'event_id', gen_random_uuid(),
           'event_type', 'DemoHappened',
           'data', '{"n":1}'::jsonb)))`,
      [streamUuid],
    ));
}

Deno.test("getSchemaVersion: returns the recorded version", async () => {
  const tw = await createThrowawayDb();
  try {
    const v = await withThrowawayDb(tw, getSchemaVersion);
    assertEquals(v, "main");
  } finally {
    await tw.drop();
  }
});

Deno.test("getStatus: empty store reports zero counts", async () => {
  const tw = await createThrowawayDb();
  try {
    const s = await withThrowawayDb(tw, getStatus);
    assertEquals(s.schemaVersion, "main");
    assertEquals(s.allHead, 0);
    assertEquals(s.streams, 0);
    assertEquals(s.events, 0);
  } finally {
    await tw.drop();
  }
});

Deno.test("getStatus: counts reflect appended events", async () => {
  const tw = await createThrowawayDb();
  try {
    await seedEvent(tw, "demo-1");
    await seedEvent(tw, "demo-2");
    const s = await withThrowawayDb(tw, getStatus);
    assertEquals(s.allHead, 2);
    assertEquals(s.streams, 2);
    assertEquals(s.events, 2);
  } finally {
    await tw.drop();
  }
});

Deno.test("schemaPresent / installSchema: fresh install", async () => {
  const tw = await createEmptyDb();
  try {
    assertEquals(await withThrowawayDb(tw, schemaPresent), false);
    const result = await withThrowawayDb(tw, (db) => installSchema(db));
    assertEquals(result.alreadyExisted, false);
    assertEquals(result.action, "installed");
    assertEquals(result.schemaVersion, "main");
    assertEquals(await withThrowawayDb(tw, schemaPresent), true);
  } finally {
    await tw.drop();
  }
});

Deno.test("installSchema: refuses when schema exists without force", async () => {
  const tw = await createEmptyDb();
  try {
    await withThrowawayDb(tw, (db) => installSchema(db));
    await assertRejects(
      () => withThrowawayDb(tw, (db) => installSchema(db)),
      SchemaAlreadyInstalled,
    );
  } finally {
    await tw.drop();
  }
});

Deno.test("installSchema --force: reinstalls and wipes data", async () => {
  const tw = await createEmptyDb();
  try {
    await withThrowawayDb(tw, (db) => installSchema(db));
    await seedEvent(tw, "demo-1");
    const result = await withThrowawayDb(
      tw,
      (db) => installSchema(db, { force: true }),
    );
    assertEquals(result.alreadyExisted, true);
    assertEquals(result.action, "reinstalled");
    const s = await withThrowawayDb(tw, getStatus);
    assertEquals(s.allHead, 0);
    assertEquals(s.events, 0);
  } finally {
    await tw.drop();
  }
});
