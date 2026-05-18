/**
 * Test fixtures. Connects to the live Postgres provisioned by the
 * repo's docker-compose (`docker compose up -d postgres`), creates the
 * `instructed_test` database if missing, installs the schema once, and
 * truncates `instructed.*` tables between cases.
 *
 * Environment overrides:
 *   PGHOST     (default: 127.0.0.1)
 *   PGPORT     (default: 5432)
 *   PGUSER     (default: postgres)
 *   PGPASSWORD (default: postgres)
 *   PGDATABASE (default: instructed_test)
 */

import pg from "pg";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const HOST = process.env.PGHOST ?? "127.0.0.1";
const PORT = Number(process.env.PGPORT ?? 5432);
const USER = process.env.PGUSER ?? "postgres";
const PASSWORD = process.env.PGPASSWORD ?? "postgres";
const DATABASE = process.env.PGDATABASE ?? "instructed_test";

let installed = false;
let pool: pg.Pool | null = null;

async function ensureDatabase(): Promise<void> {
  const admin = new pg.Client({
    host: HOST,
    port: PORT,
    user: USER,
    password: PASSWORD,
    database: "postgres",
  });
  await admin.connect();
  try {
    const r = await admin.query<{ exists: boolean }>(
      `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1) AS exists`,
      [DATABASE],
    );
    if (!r.rows[0].exists) {
      // CREATE DATABASE doesn't accept parameters
      await admin.query(`CREATE DATABASE ${pgIdent(DATABASE)}`);
    }
  } finally {
    await admin.end();
  }
}

function pgIdent(name: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`unsafe identifier: ${name}`);
  }
  return `"${name}"`;
}

async function installSchema(p: pg.Pool): Promise<void> {
  // Re-installing the schema on every test process is the simplest way
  // to keep tests independent. Drop and recreate.
  await p.query(`DROP SCHEMA IF EXISTS instructed CASCADE`);
  const schemaPath = join(__dirname, "../../../sql/instructed.sql");
  const schema = readFileSync(schemaPath, "utf-8");
  await p.query(schema);
}

/** Get (or lazily create) the shared test pool. */
export async function getPool(): Promise<pg.Pool> {
  if (pool) return pool;
  await ensureDatabase();
  pool = new pg.Pool({
    host: HOST,
    port: PORT,
    user: USER,
    password: PASSWORD,
    database: DATABASE,
    max: 8,
  });
  if (!installed) {
    await installSchema(pool);
    installed = true;
  }
  return pool;
}

/** Truncate every instructed table between cases. */
export async function truncateAll(p: pg.Pool): Promise<void> {
  // Order doesn't matter under CASCADE, but we also reset the streams
  // identity so stream_id values are stable across cases. We re-seed
  // the `$all` row afterwards.
  await p.query(`
    TRUNCATE
      instructed.stream_events,
      instructed.events,
      instructed.snapshots,
      instructed.subscriptions,
      instructed.streams
    RESTART IDENTITY CASCADE;
    INSERT INTO instructed.streams (stream_id, stream_uuid, stream_version)
      VALUES (0, '$all', 0)
      ON CONFLICT DO NOTHING;
    SELECT setval(
      pg_get_serial_sequence('instructed.streams', 'stream_id'),
      1,
      false
    );
  `);
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
