// Integration tests for the `install` command. Each test owns a bare
// throwaway database (no schema) and drops it on teardown.
//
// Requires the docker-compose Postgres to be running.

import { assertEquals, assertStringIncludes } from "@std/assert";
import { Client } from "@db/postgres";
import { makeContext, parseArgs } from "../src/cli.ts";
import { installCommand } from "../src/commands/install.ts";
import { capture, createEmptyDb, type Throwaway } from "./support.ts";

function ctxFor(db: Throwaway, argv: string[] = []) {
  const ctx = makeContext(parseArgs(argv));
  ctx.dbConfig = db.config;
  return ctx;
}

async function schemaExists(db: Throwaway): Promise<boolean> {
  const client = new Client({
    hostname: db.config.host,
    port: db.config.port,
    user: db.config.user,
    password: db.config.password,
    database: db.config.database,
  });
  await client.connect();
  try {
    const r = await client.queryObject<{ present: boolean }>(
      `select exists(
         select 1 from information_schema.schemata
         where schema_name = 'instructed'
       ) as present`,
    );
    return r.rows[0].present;
  } finally {
    await client.end();
  }
}

Deno.test("install: applies the schema to a fresh database", async () => {
  const db = await createEmptyDb();
  try {
    assertEquals(await schemaExists(db), false);
    const { stdout, result } = await capture(() => installCommand.run(ctxFor(db)));
    assertEquals(result, 0);
    assertStringIncludes(stdout, "installed successfully");
    assertStringIncludes(stdout, "version main");
    assertEquals(await schemaExists(db), true);
  } finally {
    await db.drop();
  }
});

Deno.test("install: refuses when the schema already exists", async () => {
  const db = await createEmptyDb();
  try {
    await capture(() => installCommand.run(ctxFor(db)));
    const { stderr, result } = await capture(() => installCommand.run(ctxFor(db)));
    assertEquals(result, 1);
    assertStringIncludes(stderr, "already exists");
    assertStringIncludes(stderr, "--force");
  } finally {
    await db.drop();
  }
});

Deno.test("install --force: drops and reinstalls over an existing schema", async () => {
  const db = await createEmptyDb();
  try {
    await capture(() => installCommand.run(ctxFor(db)));
    const { stdout, result } = await capture(() =>
      installCommand.run(ctxFor(db, ["--force"]))
    );
    assertEquals(result, 0);
    assertStringIncludes(stdout, "reinstalled successfully");
    assertEquals(await schemaExists(db), true);
  } finally {
    await db.drop();
  }
});

Deno.test("install --force: wipes data from a prior install", async () => {
  const db = await createEmptyDb();
  const client = () =>
    new Client({
      hostname: db.config.host,
      port: db.config.port,
      user: db.config.user,
      password: db.config.password,
      database: db.config.database,
    });
  try {
    await capture(() => installCommand.run(ctxFor(db)));

    // Seed an event into the first install.
    const c1 = client();
    await c1.connect();
    await c1.queryArray(
      `select * from instructed.append_to_stream(
         'demo-1', 'any', null,
         jsonb_build_array(jsonb_build_object(
           'event_id', gen_random_uuid(),
           'event_type', 'DemoHappened',
           'data', '{"n":1}'::jsonb)))`,
    );
    await c1.end();

    await capture(() => installCommand.run(ctxFor(db, ["--force"])));

    // After reinstall, $all head is back to 0.
    const c2 = client();
    await c2.connect();
    const r = await c2.queryObject<{ head: bigint }>(
      "select stream_version as head from instructed.streams where stream_id = 0",
    );
    await c2.end();
    assertEquals(r.rows[0].head, 0n);
  } finally {
    await db.drop();
  }
});
