// Core tests for the schema lifecycle. These call the core functions directly
// against a throwaway database — the clean test boundary. Requires the
// docker-compose Postgres.

import { assertEquals, assertRejects } from "@std/assert";
import {
  bundledSchemaVersion,
  ensureSchema,
  getSchemaVersion,
  getStatus,
  installSchema,
  SchemaAlreadyInstalled,
  schemaPresent,
  SchemaVersionMismatch,
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

Deno.test("bundledSchemaVersion: matches the installed version", async () => {
  const tw = await createThrowawayDb();
  try {
    const running = await withThrowawayDb(tw, getSchemaVersion);
    assertEquals(bundledSchemaVersion(), running);
  } finally {
    await tw.drop();
  }
});

Deno.test("ensureSchema: installs on a fresh database", async () => {
  const tw = await createEmptyDb();
  try {
    assertEquals(await withThrowawayDb(tw, schemaPresent), false);
    const result = await withThrowawayDb(tw, ensureSchema);
    assertEquals(result.action, "installed");
    assertEquals(result.schemaVersion, "main");
    assertEquals(await withThrowawayDb(tw, schemaPresent), true);
  } finally {
    await tw.drop();
  }
});

Deno.test("ensureSchema: no-op when already present, preserving data", async () => {
  const tw = await createThrowawayDb();
  try {
    await seedEvent(tw, "demo-1");
    const result = await withThrowawayDb(tw, ensureSchema);
    assertEquals(result.action, "already-current");
    assertEquals(result.schemaVersion, "main");
    // Data untouched.
    const s = await withThrowawayDb(tw, getStatus);
    assertEquals(s.events, 1);
  } finally {
    await tw.drop();
  }
});

Deno.test("ensureSchema: idempotent across repeated calls", async () => {
  const tw = await createEmptyDb();
  try {
    const first = await withThrowawayDb(tw, ensureSchema);
    assertEquals(first.action, "installed");
    const second = await withThrowawayDb(tw, ensureSchema);
    assertEquals(second.action, "already-current");
    const third = await withThrowawayDb(tw, ensureSchema);
    assertEquals(third.action, "already-current");
  } finally {
    await tw.drop();
  }
});

Deno.test("ensureSchema: refuses on a version mismatch", async () => {
  const tw = await createThrowawayDb();
  try {
    // Simulate a store installed by a different tool version by rewriting the
    // version function to report something else.
    await withThrowawayDb(tw, (db) =>
      db.query(
        `create or replace function instructed.get_schema_version()
           returns text language sql as $$ select 'some-other-version'::text $$`,
      ));
    await assertRejects(
      () => withThrowawayDb(tw, ensureSchema),
      SchemaVersionMismatch,
    );
  } finally {
    await tw.drop();
  }
});
