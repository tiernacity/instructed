/**
 * Slice 1 (SUB-A) — schema-only coverage for
 * `instructed.subscription_work_items`.
 *
 * No procedures touch this table yet (they land in slice 2). The
 * tests here assert the table, its primary key, its partial claim-
 * path index, the state CHECK, and the per-state column invariants
 * exist as documented in `sql/instructed.sql`. If a future migration
 * accidentally reshapes the table this file fails before the
 * routing/processing-worker tests do.
 */

import assert from 'node:assert/strict'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import { closePool, getPool, truncateAll } from './fixtures.ts'

void describe('SUB-A slice 1 — subscription_work_items schema', () => {
  let pool: pg.Pool

  before(async () => {
    pool = await getPool()
  })

  beforeEach(async () => {
    await truncateAll(pool)
  })

  after(async () => {
    await closePool()
  })

  void test('table exists with the documented columns', async () => {
    const r = await pool.query<{
      column_name: string
      data_type: string
      is_nullable: 'YES' | 'NO'
    }>(
      `SELECT column_name, data_type, is_nullable
         FROM information_schema.columns
        WHERE table_schema = 'instructed'
          AND table_name   = 'subscription_work_items'
        ORDER BY column_name`,
    )
    const byName = new Map(r.rows.map((row) => [row.column_name, row]))
    const expect = (name: string, type: string, nullable: 'YES' | 'NO'): void => {
      const col = byName.get(name)
      assert.ok(col, `expected column ${name}`)
      assert.equal(col.data_type, type, `column ${name} type`)
      assert.equal(col.is_nullable, nullable, `column ${name} nullable`)
    }
    expect('stream_id', 'bigint', 'NO')
    expect('subscription_name', 'text', 'NO')
    expect('partition_key', 'text', 'NO')
    expect('event_number', 'bigint', 'NO')
    expect('state', 'text', 'NO')
    expect('claimed_by', 'text', 'YES')
    expect('lease_expires_at', 'timestamp with time zone', 'YES')
    expect('failed_at', 'timestamp with time zone', 'YES')
    expect('error_text', 'text', 'YES')
    assert.equal(byName.size, 9, `unexpected extra columns: ${[...byName.keys()].sort().join(',')}`)
  })

  void test('primary key is (stream_id, subscription_name, partition_key, event_number)', async () => {
    const r = await pool.query<{ attname: string; ord: number }>(
      `SELECT a.attname, k.n AS ord
         FROM pg_constraint c
         JOIN pg_class      t ON t.oid = c.conrelid
         JOIN pg_namespace  n ON n.oid = t.relnamespace
         JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, n) ON TRUE
         JOIN pg_attribute  a ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE n.nspname = 'instructed'
          AND t.relname = 'subscription_work_items'
          AND c.contype = 'p'
        ORDER BY k.n`,
    )
    assert.deepEqual(
      r.rows.map((row) => row.attname),
      ['stream_id', 'subscription_name', 'partition_key', 'event_number'],
    )
  })

  void test('FK on (stream_id, subscription_name) cascades from subscriptions', async () => {
    const r = await pool.query<{
      confdeltype: string
      cols: string
    }>(
      `SELECT c.confdeltype,
              (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
                 FROM LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                 JOIN pg_attribute a
                   ON a.attrelid = c.conrelid AND a.attnum = k.attnum) AS cols
         FROM pg_constraint c
         JOIN pg_class      t ON t.oid = c.conrelid
         JOIN pg_namespace  n ON n.oid = t.relnamespace
        WHERE n.nspname = 'instructed'
          AND t.relname = 'subscription_work_items'
          AND c.contype = 'f'`,
    )
    assert.equal(r.rowCount, 1)
    assert.equal(r.rows[0].cols, 'stream_id,subscription_name')
    assert.equal(r.rows[0].confdeltype, 'c') // ON DELETE CASCADE
  })

  void test('partial claim-path index excludes done rows', async () => {
    const r = await pool.query<{ indexdef: string }>(
      `SELECT indexdef
         FROM pg_indexes
        WHERE schemaname = 'instructed'
          AND tablename  = 'subscription_work_items'
          AND indexname  = 'subscription_work_items_claimable'`,
    )
    assert.equal(r.rowCount, 1)
    const def = r.rows[0].indexdef.toLowerCase()
    // Shape: indexed columns include event_number (for ORDER BY in claim),
    // and the partial predicate excludes 'done'.
    assert.ok(def.includes('event_number'), `index def: ${def}`)
    assert.ok(
      def.includes("'pending'") && def.includes("'claimed'") && def.includes("'failed'"),
      `index predicate should include pending/claimed/failed: ${def}`,
    )
    assert.ok(!def.includes("'done'"), `index predicate should not mention done: ${def}`)
  })

  void test('state CHECK rejects unknown values', async () => {
    // Seed a subscription row to satisfy the FK.
    await pool.query(
      `INSERT INTO instructed.subscriptions
         (stream_id, subscription_name, last_seen)
       VALUES (0, 's', 0)`,
    )
    await assert.rejects(
      pool.query(
        `INSERT INTO instructed.subscription_work_items
           (stream_id, subscription_name, partition_key, event_number, state)
         VALUES (0, 's', 'p', 1, 'bogus')`,
      ),
      /check constraint|violates check/i,
    )
  })

  // INV-SUB-W-003 [mechanism-only]: per-state column invariants
  //   enforced by CHECK constraints (claimed iff claimed_by and
  //   lease_expires_at; failed iff failed_at; error_text only on
  //   failed rows).
  void test('per-state column invariants are enforced by CHECK constraints', async () => {
    await pool.query(
      `INSERT INTO instructed.subscriptions
         (stream_id, subscription_name, last_seen)
       VALUES (0, 's', 0)`,
    )

    // 'claimed' requires both claimed_by and lease_expires_at.
    await assert.rejects(
      pool.query(
        `INSERT INTO instructed.subscription_work_items
           (stream_id, subscription_name, partition_key, event_number, state)
         VALUES (0, 's', 'p', 1, 'claimed')`,
      ),
      /check/i,
      'claimed without claimed_by/lease should be rejected',
    )

    // 'pending' must NOT carry claimed_by.
    await assert.rejects(
      pool.query(
        `INSERT INTO instructed.subscription_work_items
           (stream_id, subscription_name, partition_key, event_number,
            state, claimed_by, lease_expires_at)
         VALUES (0, 's', 'p', 2, 'pending', 'w1', now())`,
      ),
      /check/i,
      'non-claimed row with claimed_by should be rejected',
    )

    // 'failed' requires failed_at.
    await assert.rejects(
      pool.query(
        `INSERT INTO instructed.subscription_work_items
           (stream_id, subscription_name, partition_key, event_number, state)
         VALUES (0, 's', 'p', 3, 'failed')`,
      ),
      /check/i,
      'failed without failed_at should be rejected',
    )

    // error_text only on failed.
    await assert.rejects(
      pool.query(
        `INSERT INTO instructed.subscription_work_items
           (stream_id, subscription_name, partition_key, event_number,
            state, error_text)
         VALUES (0, 's', 'p', 4, 'pending', 'boom')`,
      ),
      /check/i,
      'error_text on non-failed row should be rejected',
    )

    // Happy paths.
    await pool.query(
      `INSERT INTO instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state)
       VALUES (0, 's', 'p', 10, 'pending')`,
    )
    await pool.query(
      `INSERT INTO instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number,
          state, claimed_by, lease_expires_at)
       VALUES (0, 's', 'p', 11, 'claimed', 'w1', now() + interval '30s')`,
    )
    await pool.query(
      `INSERT INTO instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number,
          state, failed_at, error_text)
       VALUES (0, 's', 'p', 12, 'failed', now(), 'boom')`,
    )
    await pool.query(
      `INSERT INTO instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state)
       VALUES (0, 's', 'p', 13, 'done')`,
    )
  })

  void test('FK cascade: deleting a subscription removes its work items', async () => {
    await pool.query(
      `INSERT INTO instructed.subscriptions
         (stream_id, subscription_name, last_seen)
       VALUES (0, 's', 0)`,
    )
    await pool.query(
      `INSERT INTO instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state)
       VALUES (0, 's', 'p', 1, 'pending'),
              (0, 's', 'p', 2, 'pending')`,
    )
    await pool.query(
      `DELETE FROM instructed.subscriptions
        WHERE stream_id = 0 AND subscription_name = 's'`,
    )
    const r = await pool.query<{ c: string }>(
      `SELECT count(*)::text AS c FROM instructed.subscription_work_items`,
    )
    assert.equal(r.rows[0].c, '0')
  })

  void test('primary key prevents duplicate (subscription, partition, event_number)', async () => {
    await pool.query(
      `INSERT INTO instructed.subscriptions
         (stream_id, subscription_name, last_seen)
       VALUES (0, 's', 0)`,
    )
    await pool.query(
      `INSERT INTO instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state)
       VALUES (0, 's', 'p', 1, 'pending')`,
    )
    await assert.rejects(
      pool.query(
        `INSERT INTO instructed.subscription_work_items
           (stream_id, subscription_name, partition_key, event_number, state)
         VALUES (0, 's', 'p', 1, 'pending')`,
      ),
      /duplicate key|unique/i,
    )
    // Same event_number but different partition_key is fine.
    await pool.query(
      `INSERT INTO instructed.subscription_work_items
         (stream_id, subscription_name, partition_key, event_number, state)
       VALUES (0, 's', 'q', 1, 'pending')`,
    )
  })
})
