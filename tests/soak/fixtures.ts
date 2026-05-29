/**
 * DB connection + schema install for the soak harness.
 *
 * Mirrors `sdks/typescript/test/fixtures.ts` but stands on its own
 * (the SDK fixtures live inside `node --test` territory and reset the
 * schema between cases — neither is what the soak harness wants).
 *
 * Environment overrides match the rest of the repo:
 *   PGHOST     (default: 127.0.0.1)
 *   PGPORT     (default: 5432)
 *   PGUSER     (default: postgres)
 *   PGPASSWORD (default: postgres)
 *   PGDATABASE (default: instructed_soak)
 *
 * The soak DB defaults to `instructed_soak` so it can't accidentally
 * clobber the SDK test database.
 */

import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import pg from 'pg'

const __dirname = dirname(fileURLToPath(import.meta.url))

export interface DbConfig {
  host: string
  port: number
  user: string
  password: string
  database: string
}

export function dbConfigFromEnv(): DbConfig {
  return {
    host: process.env.PGHOST ?? '127.0.0.1',
    port: Number(process.env.PGPORT ?? 5432),
    user: process.env.PGUSER ?? 'postgres',
    password: process.env.PGPASSWORD ?? 'postgres',
    database: process.env.PGDATABASE ?? 'instructed_soak',
  }
}

function pgIdent(name: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`unsafe identifier: ${name}`)
  }
  return `"${name}"`
}

async function ensureDatabase(cfg: DbConfig): Promise<void> {
  const admin = new pg.Client({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: 'postgres',
  })
  await admin.connect()
  try {
    const r = await admin.query<{ exists: boolean }>(
      `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1) AS exists`,
      [cfg.database],
    )
    if (!r.rows[0].exists) {
      await admin.query(`CREATE DATABASE ${pgIdent(cfg.database)}`)
    }
  } finally {
    await admin.end()
  }
}

/**
 * (Re)install the `instructed` schema. The soak harness always resets
 * before a run — invariant checks assume a clean baseline.
 */
export async function resetSchema(pool: pg.Pool): Promise<void> {
  await pool.query(`DROP SCHEMA IF EXISTS instructed CASCADE`)
  const schemaPath = join(__dirname, '../../sql/instructed.sql')
  const schema = readFileSync(schemaPath, 'utf-8')
  await pool.query(schema)
}

export async function makePool(cfg: DbConfig, max: number): Promise<pg.Pool> {
  await ensureDatabase(cfg)
  return new pg.Pool({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: cfg.database,
    max,
  })
}
