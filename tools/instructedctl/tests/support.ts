// Test support: a throwaway PostgreSQL database per integration test, plus a
// stdout-capture helper for asserting on command output.
//
// Each integration test creates a uniquely-named database, loads
// sql/instructed.sql into it, runs against it, and drops it afterwards. This
// keeps tests isolated and leaves no residue on the shared Postgres.
//
// Admin connection (used only to CREATE/DROP the throwaway database) resolves
// from the standard libpq vars, defaulting to the repo's docker-compose
// Postgres:
//   PGHOST     (default: 127.0.0.1)
//   PGPORT     (default: 5432)
//   PGUSER     (default: postgres)
//   PGPASSWORD (default: postgres)

import { Client } from "@db/postgres";
import { fromFileUrl, join } from "@std/path";
import { type DbConfig, withDb } from "../src/cli/db.ts";
import type { Db } from "../src/core/index.ts";

export { withDb };
export type { Db, DbConfig };

const HOST = Deno.env.get("PGHOST") ?? "127.0.0.1";
const PORT = Number(Deno.env.get("PGPORT") ?? "5432");
const USER = Deno.env.get("PGUSER") ?? "postgres";
const PASSWORD = Deno.env.get("PGPASSWORD") ?? "postgres";

// Repo root relative to this file: tools/instructedctl/tests -> ../../..
const SCHEMA_PATH = join(
  fromFileUrl(import.meta.url),
  "../../../../sql/instructed.sql",
);

function adminClient(database: string): Client {
  return new Client({
    hostname: HOST,
    port: PORT,
    user: USER,
    password: PASSWORD,
    database,
  });
}

function randomDbName(): string {
  const suffix = crypto.randomUUID().replace(/-/g, "").slice(0, 12);
  return `instructedctl_test_${suffix}`;
}

// Only a-z0-9_ identifiers are generated, but guard anyway since the name is
// interpolated into DDL.
function assertSafeIdent(name: string): void {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`unsafe identifier: ${name}`);
  }
}

export interface Throwaway {
  config: DbConfig;
  drop: () => Promise<void>;
}

// Create a throwaway database with NO schema installed. Useful for testing the
// `install` command. The returned config uses discrete fields so commands
// connect to it regardless of the ambient environment.
export async function createEmptyDb(): Promise<Throwaway> {
  const name = randomDbName();
  assertSafeIdent(name);

  const admin = adminClient("postgres");
  await admin.connect();
  try {
    await admin.queryArray(`CREATE DATABASE "${name}"`);
  } finally {
    await admin.end();
  }

  return { config: configFor(name), drop: dropFor(name) };
}

// Create a throwaway database with the schema installed. The returned config
// uses discrete fields so commands connect to it regardless of the ambient
// environment.
export async function createThrowawayDb(): Promise<Throwaway> {
  const db = await createEmptyDb();
  const name = db.config.database!;

  const schema = await Deno.readTextFile(SCHEMA_PATH);
  const target = adminClient(name);
  await target.connect();
  try {
    await target.queryArray(schema);
  } finally {
    await target.end();
  }

  return db;
}

function configFor(name: string): DbConfig {
  return {
    host: HOST,
    port: PORT,
    user: USER,
    password: PASSWORD,
    database: name,
  };
}

// A connection URI for a throwaway database (for env-driven CLI smoke tests).
export function uriFor(tw: Throwaway): string {
  const c = tw.config;
  return `postgresql://${c.user}:${c.password}@${c.host}:${c.port}/${c.database}`;
}

function dropFor(name: string): () => Promise<void> {
  return async () => {
    const a = adminClient("postgres");
    await a.connect();
    try {
      // Terminate any lingering backends, then drop.
      await a.queryArray(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname = $1 AND pid <> pg_backend_pid()`,
        [name],
      );
      await a.queryArray(`DROP DATABASE IF EXISTS "${name}"`);
    } finally {
      await a.end();
    }
  };
}

// Run `fn` with a core `Db` connected to the throwaway database. This is the
// boundary core tests exercise.
export function withThrowawayDb<T>(
  tw: Throwaway,
  fn: (db: Db) => Promise<T>,
): Promise<T> {
  return withDb(tw.config, fn);
}

// Run `fn` with console.log/console.error captured. Returns the combined
// stdout and stderr lines.
export async function capture(
  fn: () => Promise<unknown> | unknown,
): Promise<{ stdout: string; stderr: string; result: unknown }> {
  const origLog = console.log;
  const origError = console.error;
  const out: string[] = [];
  const err: string[] = [];
  console.log = (...args: unknown[]) => {
    out.push(args.map(String).join(" "));
  };
  console.error = (...args: unknown[]) => {
    err.push(args.map(String).join(" "));
  };
  try {
    const result = await fn();
    return { stdout: out.join("\n"), stderr: err.join("\n"), result };
  } finally {
    console.log = origLog;
    console.error = origError;
  }
}
