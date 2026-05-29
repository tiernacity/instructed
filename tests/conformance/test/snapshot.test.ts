/**
 * Part D — Snapshot conformance (Phase 9, step 4/8).
 *
 * Drives `instructed.record_snapshot`, `instructed.read_snapshot`,
 * and `instructed.delete_snapshot` directly via `pg` (D-0021).
 * Each `test(...)` carries one or more `// INV-SNAP-NNN` annotations
 * on the line above it.
 *
 * The procedures' full contracts live in `sql/instructed.sql`.
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import { rejectsWithCode } from './_helpers.ts'
import { closePool, getPool, truncateAll } from './fixtures.ts'

let pool: pg.Pool

before(async () => {
  pool = await getPool()
})

after(async () => {
  await closePool()
})

beforeEach(async () => {
  await truncateAll(pool)
})

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

interface SnapshotRow {
  source_uuid: string
  source_type: string
  source_version: bigint
  data: unknown
  metadata: unknown
  created_at: Date
}

async function recordSnapshot(
  sourceUuid: string,
  sourceType: string,
  sourceVersion: bigint,
  data: unknown,
  metadata: unknown = null,
): Promise<void> {
  await pool.query(`SELECT instructed.record_snapshot($1, $2, $3, $4::jsonb, $5::jsonb)`, [
    sourceUuid,
    sourceType,
    sourceVersion,
    JSON.stringify(data),
    metadata === null ? null : JSON.stringify(metadata),
  ])
}

async function readSnapshot(sourceUuid: string): Promise<SnapshotRow> {
  const r = await pool.query<{
    source_uuid: string
    source_type: string
    source_version: string
    data: unknown
    metadata: unknown
    created_at: Date
  }>(`SELECT * FROM instructed.read_snapshot($1)`, [sourceUuid])
  if (r.rowCount === 0) {
    throw new Error('readSnapshot: zero rows but no IS010 raised')
  }
  const row = r.rows[0]
  return {
    source_uuid: row.source_uuid,
    source_type: row.source_type,
    source_version: BigInt(row.source_version),
    data: row.data,
    metadata: row.metadata,
    created_at: row.created_at,
  }
}

async function deleteSnapshot(sourceUuid: string): Promise<void> {
  await pool.query(`SELECT instructed.delete_snapshot($1)`, [sourceUuid])
}

async function snapshotRowCount(): Promise<bigint> {
  const r = await pool.query<{ n: string }>(`SELECT COUNT(*)::text AS n FROM instructed.snapshots`)
  return BigInt(r.rows[0].n)
}

// =============================================================================
// At-most-one + upsert (INV-SNAP-001, INV-SNAP-002, INV-SNAP-005)
// =============================================================================

describe('record_snapshot — at-most-one + wholesale upsert', () => {
  // INV-SNAP-001: at most one snapshot per source_uuid
  test('a fresh source_uuid lands exactly one row', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 5n, { balance: 100 })
    assert.equal(await snapshotRowCount(), 1n)
  })

  // INV-SNAP-001 + INV-SNAP-005: re-recording does NOT create a second row
  //   (snapshots are not versioned history)
  test('re-recording the same source_uuid keeps the row count at 1 (no history)', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 5n, { balance: 100 })
    await recordSnapshot(id, 'Account', 6n, { balance: 110 })
    await recordSnapshot(id, 'Account', 7n, { balance: 120 })
    assert.equal(await snapshotRowCount(), 1n)
  })

  // INV-SNAP-002: record_snapshot is a FULL-ROW upsert; every field is
  //   replaced wholesale, including source_type, source_version, data,
  //   metadata, AND created_at.
  test('upsert replaces every field, including created_at', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 5n, { balance: 100 }, { v: 1 })
    const first = await readSnapshot(id)

    // Wait a hair so that `now()` on the second insert is strictly later.
    await new Promise((r) => setTimeout(r, 10))

    await recordSnapshot(
      id,
      'Ledger',
      99n,
      { balance: 7777, foo: 'bar' },
      {
        v: 2,
      },
    )
    const second = await readSnapshot(id)

    assert.equal(second.source_uuid, id)
    assert.equal(second.source_type, 'Ledger') // replaced
    assert.equal(second.source_version, 99n) // replaced
    assert.deepEqual(second.data, { balance: 7777, foo: 'bar' }) // replaced
    assert.deepEqual(second.metadata, { v: 2 }) // replaced
    assert.ok(
      second.created_at.getTime() > first.created_at.getTime(),
      `created_at must advance: was ${first.created_at.toISOString()}, is ${second.created_at.toISOString()}`,
    )
  })

  // INV-SNAP-002: upsert from null metadata to a value, and back to null
  test('upsert handles metadata transitions (null -> value -> null)', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 1n, { x: 1 }, null)
    assert.equal((await readSnapshot(id)).metadata, null)

    await recordSnapshot(id, 'Account', 2n, { x: 2 }, { tag: 'hot' })
    assert.deepEqual((await readSnapshot(id)).metadata, { tag: 'hot' })

    await recordSnapshot(id, 'Account', 3n, { x: 3 }, null)
    assert.equal((await readSnapshot(id)).metadata, null)
  })

  // INV-SNAP-001: different source_uuids are independent
  test('distinct source_uuids land distinct rows', async () => {
    const a = randomUUID()
    const b = randomUUID()
    await recordSnapshot(a, 'Account', 1n, { which: 'a' })
    await recordSnapshot(b, 'Account', 1n, { which: 'b' })
    assert.equal(await snapshotRowCount(), 2n)
    assert.deepEqual((await readSnapshot(a)).data, { which: 'a' })
    assert.deepEqual((await readSnapshot(b)).data, { which: 'b' })
  })
})

// =============================================================================
// read_snapshot (INV-SNAP-003)
// =============================================================================

describe('read_snapshot — present and missing', () => {
  // INV-SNAP-003: read of a missing source_uuid raises IS010 snapshot_not_found
  test('reading a missing source_uuid raises IS010 snapshot_not_found', async () => {
    await rejectsWithCode(() => readSnapshot(randomUUID()), 'IS010')
  })

  // INV-SNAP-002 + read: every field round-trips faithfully on read
  test('read echoes every stored field verbatim', async () => {
    const id = randomUUID()
    const data = { balance: 1234, holders: ['alice', 'bob'] }
    const meta = { snapshot_module_version: 3, tag: 'warm' }
    await recordSnapshot(id, 'Account', 42n, data, meta)
    const got = await readSnapshot(id)
    assert.equal(got.source_uuid, id)
    assert.equal(got.source_type, 'Account')
    assert.equal(got.source_version, 42n)
    assert.deepEqual(got.data, data)
    assert.deepEqual(got.metadata, meta)
    assert.ok(got.created_at instanceof Date)
  })
})

// =============================================================================
// delete_snapshot (INV-SNAP-004)
// =============================================================================

describe('delete_snapshot — idempotent', () => {
  // INV-SNAP-004: deleting an existing snapshot removes it; subsequent
  //   read raises IS010.
  test('delete removes the snapshot; subsequent read raises IS010', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 1n, { x: 1 })
    await deleteSnapshot(id)
    await rejectsWithCode(() => readSnapshot(id), 'IS010')
    assert.equal(await snapshotRowCount(), 0n)
  })

  // INV-SNAP-004: deleting a missing snapshot is a silent no-op
  //   (contrast delete_subscription, which raises IS020 per D-0009).
  test('deleting a missing snapshot returns successfully (no IS010)', async () => {
    // Must not throw.
    await deleteSnapshot(randomUUID())
    // And the table is still empty.
    assert.equal(await snapshotRowCount(), 0n)
  })

  // INV-SNAP-004 (continued): the canonical record -> delete -> read
  //   sequence from the Commanded conformance suite
  test('record -> delete -> read yields IS010 snapshot_not_found', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 1n, { x: 1 })
    await deleteSnapshot(id)
    await rejectsWithCode(() => readSnapshot(id), 'IS010')
  })

  // INV-SNAP-004: delete-then-delete (two deletes of the same key) is OK
  test('double-delete is silent (idempotent twice)', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 1n, { x: 1 })
    await deleteSnapshot(id)
    await deleteSnapshot(id)
    assert.equal(await snapshotRowCount(), 0n)
  })
})

// =============================================================================
// Advisory-only (INV-SNAP-006)
// =============================================================================

describe('snapshots are advisory (INV-SNAP-006)', () => {
  // INV-SNAP-006: snapshots are NOT required for correct aggregate
  //   reconstruction; the event stream from version 0 is always the
  //   source of truth. The store enforces no relationship between
  //   `source_uuid` and any stream; recording a snapshot for a
  //   non-existent aggregate is permitted (it is the SDK's job to
  //   keep the two in sync; see AGG-001/003 in mapping.md).
  test('a snapshot may be recorded for a source_uuid with no events / no stream', async () => {
    const id = randomUUID()
    await recordSnapshot(id, 'Account', 99n, { balance: 0 })
    const got = await readSnapshot(id)
    assert.equal(got.source_version, 99n)
    // The store has no events at all.
    const e = await pool.query<{ n: string }>(`SELECT COUNT(*)::text AS n FROM instructed.events`)
    assert.equal(BigInt(e.rows[0].n), 0n)
  })

  // INV-SNAP-006: the store does not validate snapshot.source_version
  //   against any stream's current version. The SDK is responsible for
  //   that check on read; the contract here is "stores what it is given".
  test('snapshot source_version is not validated against any stream', async () => {
    const id = randomUUID()
    // Record a snapshot at version 1_000_000 with no underlying events.
    await recordSnapshot(id, 'Account', 1_000_000n, { wild: true })
    const got = await readSnapshot(id)
    assert.equal(got.source_version, 1_000_000n)
  })
})
