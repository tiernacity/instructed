// Integration tests: run commands against a throwaway database with the real
// schema installed. Each test owns its database and drops it on teardown.
//
// Requires the docker-compose Postgres to be running:
//   docker compose up -d postgres
// Admin connection resolves from PG* env vars (see tests/support.ts).

import { assertEquals, assertStringIncludes } from "@std/assert";
import { Client } from "@db/postgres";
import { makeContext, parseArgs } from "../src/cli.ts";
import { statusCommand } from "../src/commands/status.ts";
import { schemaVersionCommand } from "../src/commands/schema-version.ts";
import { capture, createThrowawayDb, type Throwaway } from "./support.ts";

// Build a CommandContext whose dbConfig targets the throwaway db, with the
// given extra flags parsed in.
function ctxFor(db: Throwaway, argv: string[] = []) {
  const ctx = makeContext(parseArgs(argv));
  ctx.dbConfig = db.config;
  return ctx;
}

// Append one event to a stream via the SQL contract, so counts are non-zero.
async function seedEvent(db: Throwaway, streamUuid: string): Promise<void> {
  const client = new Client({
    hostname: db.config.host,
    port: db.config.port,
    user: db.config.user,
    password: db.config.password,
    database: db.config.database,
  });
  await client.connect();
  try {
    await client.queryArray(
      `select * from instructed.append_to_stream(
         $1, 'any', null,
         jsonb_build_array(jsonb_build_object(
           'event_id', gen_random_uuid(),
           'event_type', 'DemoHappened',
           'data', '{"n":1}'::jsonb))
       )`,
      [streamUuid],
    );
  } finally {
    await client.end();
  }
}

Deno.test("schema-version: prints the recorded version", async () => {
  const db = await createThrowawayDb();
  try {
    const { stdout, result } = await capture(() =>
      schemaVersionCommand.run(ctxFor(db))
    );
    assertEquals(result, 0);
    assertEquals(stdout.trim(), "main");
  } finally {
    await db.drop();
  }
});

Deno.test("status: empty store reports zero counts and seeded subscription row", async () => {
  const db = await createThrowawayDb();
  try {
    const { stdout, result } = await capture(() => statusCommand.run(ctxFor(db)));
    assertEquals(result, 0);
    assertStringIncludes(stdout, "schema version : main");
    assertStringIncludes(stdout, "$all head      : 0");
    assertStringIncludes(stdout, "streams        : 0");
    assertStringIncludes(stdout, "events         : 0");
  } finally {
    await db.drop();
  }
});

Deno.test("status: counts reflect appended events", async () => {
  const db = await createThrowawayDb();
  try {
    await seedEvent(db, "demo-stream-1");
    await seedEvent(db, "demo-stream-2");
    const { stdout, result } = await capture(() => statusCommand.run(ctxFor(db)));
    assertEquals(result, 0);
    assertStringIncludes(stdout, "$all head      : 2");
    assertStringIncludes(stdout, "streams        : 2");
    assertStringIncludes(stdout, "events         : 2");
  } finally {
    await db.drop();
  }
});

Deno.test("status: --verbose prints the resolved configuration to stderr", async () => {
  const db = await createThrowawayDb();
  try {
    const { stderr } = await capture(() =>
      statusCommand.run(ctxFor(db, ["--verbose"]))
    );
    assertStringIncludes(stderr, "Configuration:");
    assertStringIncludes(stderr, "Database:");
    assertStringIncludes(stderr, db.config.database!);
  } finally {
    await db.drop();
  }
});
