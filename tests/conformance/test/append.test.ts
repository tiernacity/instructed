/**
 * Part B — Append conformance (Phase 9, step 2/8).
 *
 * Drives `instructed.append_to_stream` directly via `pg` (D-0021).
 * Each `test(...)` carries one or more `// INV-APPEND-NNN` annotations
 * on the line above it; the coverage reporter (step 8/8) scrapes
 * them.
 *
 * The procedure's full contract — inputs, outputs, error SQLSTATEs,
 * lock-acquisition order — lives in `sql/instructed.sql`. This file
 * does not paraphrase it; the assertions go straight to observable
 * post-conditions and to the documented IS* codes.
 *
 * Source checklist: COVERAGE.md maps Commanded's
 * `append_events_test_case.ex` blocks to the INV-APPEND-* IDs
 * exercised below.
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

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

type ExpectedType = 'any' | 'no_stream' | 'stream_exists' | 'exact'

interface InputEvent {
  event_id?: string
  event_type: string
  data?: unknown
  metadata?: unknown
  causation_id?: string | null
  correlation_id?: string | null
}

interface AppendedRow {
  event_id: string
  stream_version: bigint
  event_number: bigint
  created_at: Date
}

async function append(
  streamUuid: string,
  expectedType: ExpectedType,
  expectedVersion: bigint | null,
  events: InputEvent[],
  q: pg.ClientBase | pg.Pool = pool,
): Promise<AppendedRow[]> {
  const payload = events.map((e) => ({
    event_id: e.event_id ?? randomUUID(),
    event_type: e.event_type,
    data: e.data ?? {},
    ...(e.metadata !== undefined ? { metadata: e.metadata } : {}),
    ...(e.causation_id !== undefined ? { causation_id: e.causation_id } : {}),
    ...(e.correlation_id !== undefined ? { correlation_id: e.correlation_id } : {}),
  }))
  const r = await q.query<{
    event_id: string
    stream_version: string
    event_number: string
    created_at: Date
  }>(
    `SELECT event_id, stream_version, event_number, created_at
       FROM instructed.append_to_stream($1, $2, $3, $4::jsonb)`,
    [streamUuid, expectedType, expectedVersion, JSON.stringify(payload)],
  )
  return r.rows.map((row) => ({
    event_id: row.event_id,
    stream_version: BigInt(row.stream_version),
    event_number: BigInt(row.event_number),
    created_at: row.created_at,
  }))
}

/** Assert a promise rejects with a PostgresError whose `code` matches. */
async function rejectsWithCode(fn: () => Promise<unknown>, code: string): Promise<void> {
  await assert.rejects(fn, (err: unknown) => {
    const e = err as { code?: unknown }
    return typeof e.code === 'string' && e.code === code
  })
}

async function streamVersionOf(streamUuid: string): Promise<bigint | null> {
  const r = await pool.query<{ stream_version: string }>(
    `SELECT stream_version FROM instructed.streams WHERE stream_uuid = $1`,
    [streamUuid],
  )
  return r.rowCount === 0 ? null : BigInt(r.rows[0].stream_version)
}

async function allRowCount(): Promise<bigint> {
  const r = await pool.query<{ n: string }>(`SELECT COUNT(*)::text AS n FROM instructed.events`)
  return BigInt(r.rows[0].n)
}

// =============================================================================
// Identity, ordering, atomicity (INV-APPEND-001..007)
// =============================================================================

void describe('append_to_stream — identity, ordering, atomicity', () => {
  // INV-APPEND-001: every appended event has a unique event_id (echoed from caller)
  // INV-APPEND-002: first event in a stream is stream_version 1
  // INV-APPEND-003: first event in the store is event_number 1
  // INV-APPEND-005: created_at is a UTC timestamp set at append time
  void test('single-event append returns sv=1, en=1, valid created_at, echoes event_id', async () => {
    const s = randomUUID()
    const id = randomUUID()
    const before = Date.now()
    const rows = await append(s, 'no_stream', null, [
      { event_id: id, event_type: 'Created', data: { x: 1 } },
    ])
    const after = Date.now()
    assert.equal(rows.length, 1)
    assert.equal(rows[0].event_id, id)
    assert.equal(rows[0].stream_version, 1n)
    assert.equal(rows[0].event_number, 1n)
    assert.ok(rows[0].created_at instanceof Date)
    // INV-APPEND-005 explicitly tolerates clock skew. We assert the
    // timestamp is *roughly* current (±60s window absorbs realistic
    // Postgres-vs-node drift, including dockerised Postgres on
    // virtualised macOS) rather than tight bracketing. The
    // monotonicity property has its own case below.
    const t = rows[0].created_at.getTime()
    const SKEW_MS = 60_000
    assert.ok(
      t >= before - SKEW_MS && t <= after + SKEW_MS,
      `created_at ${new Date(t).toISOString()} far outside [${new Date(before).toISOString()}, ${new Date(after).toISOString()}]`,
    )
  })

  // INV-APPEND-002: contiguous per-stream stream_version starting at 1
  // INV-APPEND-003: contiguous globally-gapless event_number
  // INV-APPEND-004: contiguity within a single multi-event append
  void test('multi-event append assigns contiguous stream_version and event_number', async () => {
    const s = randomUUID()
    const rows = await append(s, 'no_stream', null, [
      { event_type: 'A' },
      { event_type: 'B' },
      { event_type: 'C' },
    ])
    assert.deepEqual(
      rows.map((r) => r.stream_version),
      [1n, 2n, 3n],
    )
    assert.deepEqual(
      rows.map((r) => r.event_number),
      [1n, 2n, 3n],
    )
    const ids = new Set(rows.map((r) => r.event_id))
    assert.equal(ids.size, 3, 'event_ids must be unique')
  })

  // INV-APPEND-003: event_number is gapless across multiple streams
  void test('event_number is globally gapless across interleaved appends to different streams', async () => {
    const a = randomUUID()
    const b = randomUUID()
    const r1 = await append(a, 'no_stream', null, [{ event_type: 'A1' }])
    const r2 = await append(b, 'no_stream', null, [{ event_type: 'B1' }, { event_type: 'B2' }])
    const r3 = await append(a, 'any', null, [{ event_type: 'A2' }])
    const r4 = await append(b, 'any', null, [{ event_type: 'B3' }])
    const ens = [...r1, ...r2, ...r3, ...r4].map((r) => r.event_number)
    assert.deepEqual(ens, [1n, 2n, 3n, 4n, 5n])
    // Per-stream versions remain independent and contiguous.
    assert.deepEqual([r1[0].stream_version, r3[0].stream_version], [1n, 2n])
    assert.deepEqual(
      [r2[0].stream_version, r2[1].stream_version, r4[0].stream_version],
      [1n, 2n, 3n],
    )
  })

  // INV-APPEND-005: created_at non-decreasing modulo clock skew
  void test('created_at is non-decreasing across appends', async () => {
    const s = randomUUID()
    const a = await append(s, 'no_stream', null, [{ event_type: 'A' }])
    const b = await append(s, 'any', null, [{ event_type: 'B' }])
    assert.ok(
      a[0].created_at.getTime() <= b[0].created_at.getTime(),
      `created_at decreased: ${a[0].created_at.toISOString()} > ${b[0].created_at.toISOString()}`,
    )
  })

  // INV-APPEND-006: atomicity — either all N persisted, or none
  // INV-APPEND-007: atomicity boundary includes the per-stream + global bumps
  void test('a failing multi-event append leaves no trace (stream version, $all version, events all untouched)', async () => {
    const s = randomUUID()
    // Seed: stream at version 1
    await append(s, 'no_stream', null, [{ event_type: 'Seed' }])
    const svBefore = await streamVersionOf(s)
    const allBefore = await streamVersionOf('$all')
    const countBefore = await allRowCount()

    // Force a mid-batch failure by re-using an event_id in a second batch.
    const reused = randomUUID()
    await append(s, 'any', null, [{ event_id: reused, event_type: 'X' }])
    // Now this batch (B,C with a re-used Y) must fail wholesale:
    await rejectsWithCode(
      () =>
        append(s, 'any', null, [
          { event_type: 'B' },
          { event_type: 'C' },
          { event_id: reused, event_type: 'Y' }, // duplicate → IS004
        ]),
      'IS004',
    )

    // Post-condition: the target stream did not advance past the prior good
    // append; neither did $all; no new event rows landed.
    const svAfter = await streamVersionOf(s)
    const allAfter = await streamVersionOf('$all')
    const countAfter = await allRowCount()
    // Prior successful 1-event append (`X`) ran between svBefore and svAfter,
    // so we should be exactly +1 on each counter from the pre-X baseline,
    // and zero from the failing batch.
    assert.equal(
      svAfter,
      svBefore! + 1n,
      'stream version moved only by the prior successful X append',
    )
    assert.equal(allAfter, allBefore! + 1n, '$all moved only by the prior successful X append')
    assert.equal(
      countAfter,
      countBefore + 1n,
      'events table grew only by the prior successful X append',
    )
  })
})

// =============================================================================
// Expected-version semantics (INV-APPEND-010..014)
// =============================================================================

void describe('append_to_stream — expected_version', () => {
  // INV-APPEND-010: :any creates the stream when missing
  void test("'any' creates the stream when missing", async () => {
    const s = randomUUID()
    const rows = await append(s, 'any', null, [{ event_type: 'A' }])
    assert.equal(rows[0].stream_version, 1n)
    assert.equal(await streamVersionOf(s), 1n)
  })

  // INV-APPEND-010: :any extends an existing stream
  void test("'any' extends an existing stream", async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    const rows = await append(s, 'any', null, [{ event_type: 'B' }, { event_type: 'C' }])
    assert.deepEqual(
      rows.map((r) => r.stream_version),
      [2n, 3n],
    )
  })

  // INV-APPEND-011: :no_stream succeeds only if the stream does not exist
  void test("'no_stream' on existing stream raises IS002 stream_exists", async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    await rejectsWithCode(() => append(s, 'no_stream', null, [{ event_type: 'B' }]), 'IS002')
  })

  // INV-APPEND-012: :stream_exists succeeds only if the stream already exists
  void test("'stream_exists' on missing stream raises IS003 stream_not_found", async () => {
    const s = randomUUID()
    await rejectsWithCode(() => append(s, 'stream_exists', null, [{ event_type: 'A' }]), 'IS003')
  })

  void test("'stream_exists' on present stream appends and bumps version", async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    const rows = await append(s, 'stream_exists', null, [{ event_type: 'B' }])
    assert.equal(rows[0].stream_version, 2n)
  })

  // INV-APPEND-013: exact V succeeds only if current_version == V at append
  void test("'exact' V matching current version appends", async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    const rows = await append(s, 'exact', 1n, [{ event_type: 'B' }])
    assert.equal(rows[0].stream_version, 2n)
  })

  // INV-APPEND-013: exact V mismatched current version raises IS001
  void test("'exact' V mismatched raises IS001 wrong_expected_version", async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    await rejectsWithCode(() => append(s, 'exact', 99n, [{ event_type: 'B' }]), 'IS001')
  })

  // INV-APPEND-014: V=0 against a non-existent stream MUST succeed (creates it)
  void test("'exact' V=0 on missing stream creates it", async () => {
    const s = randomUUID()
    const rows = await append(s, 'exact', 0n, [{ event_type: 'A' }])
    assert.equal(rows[0].stream_version, 1n)
    assert.equal(await streamVersionOf(s), 1n)
  })

  // INV-APPEND-014: V>0 against a non-existent stream MUST fail with IS001
  void test("'exact' V>0 on missing stream raises IS001", async () => {
    await rejectsWithCode(() => append(randomUUID(), 'exact', 1n, [{ event_type: 'A' }]), 'IS001')
  })

  // INV-APPEND-014: V=0 against an existing stream at version != 0 raises IS001
  void test("'exact' V=0 against a non-empty stream raises IS001", async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    await rejectsWithCode(() => append(s, 'exact', 0n, [{ event_type: 'B' }]), 'IS001')
  })
})

// =============================================================================
// Concurrency (INV-APPEND-020..022)
// =============================================================================

void describe('append_to_stream — concurrency', () => {
  // INV-APPEND-020: under concurrent 'exact' V appends, at most one succeeds
  // INV-APPEND-022 [reference-only]: realised by the (stream_id, stream_version)
  //   unique constraint — surfaces here as one IS001 reject.
  void test("two concurrent 'exact' V=0 on the same stream: exactly one wins, the other gets IS001", async () => {
    const s = randomUUID()
    // Pre-create the stream at version 0 so both attempts see the same
    // expected_version path (the create-on-V=0 branch is a separate INV-014
    // case; here we want the contended UPDATE path).
    // We do this by appending an event with 'any', then truncating just
    // the events — actually simpler: use 'no_stream' to insert without
    // appending an event… that's not a public path. The clean way is to
    // race two 'exact' V=0 calls on a fresh stream and accept that both
    // sessions are racing on the missing-stream-create branch instead,
    // which still exercises INV-APPEND-020.
    const [a, b] = await Promise.allSettled([
      append(s, 'exact', 0n, [{ event_type: 'A' }]),
      append(s, 'exact', 0n, [{ event_type: 'B' }]),
    ])
    const outcomes = [a, b].map((r) => r.status)
    assert.equal(
      outcomes.filter((s) => s === 'fulfilled').length,
      1,
      `expected exactly one winner, got ${outcomes.join(', ')}`,
    )
    const loser = a.status === 'rejected' ? a.reason : b.status === 'rejected' ? b.reason : null
    assert.ok(loser, 'expected one rejection')
    assert.equal(
      (loser as { code?: string }).code,
      'IS001',
      `loser must reject with IS001, got ${(loser as { code?: string }).code}`,
    )
    // Stream ended up at version 1, not 2.
    assert.equal(await streamVersionOf(s), 1n)
  })

  // INV-APPEND-014 / INV-APPEND-020: deterministic pinning of the
  // streams_stream_uuid_key race on the V=0 missing-stream-create
  // branch (TODO #13). Two dedicated client connections with explicit
  // BEGIN keep session 1's transaction open past session 2's SELECT
  // FOR UPDATE, forcing both into the INSERT path. Without the SQL
  // fix, session 2's INSERT raises raw SQLSTATE 23505
  // (streams_stream_uuid_key); with the fix it is translated to IS001.
  void test('deterministic streams_stream_uuid_key race: loser gets IS001, not raw 23505', async () => {
    const { Client } = await import('pg')
    const conn = {
      host: process.env.PGHOST ?? '127.0.0.1',
      port: Number(process.env.PGPORT ?? 5432),
      user: process.env.PGUSER ?? 'postgres',
      password: process.env.PGPASSWORD ?? 'postgres',
      database: process.env.PGDATABASE ?? 'instructed_test',
    }
    const c1 = new Client(conn)
    const c2 = new Client(conn)
    await c1.connect()
    await c2.connect()
    try {
      const s = randomUUID()
      // c1: open transaction, run append_to_stream to completion, but
      // do NOT commit yet. The inserted streams row is uncommitted; its
      // unique-index entry blocks any concurrent INSERT for the same
      // stream_uuid.
      await c1.query('BEGIN')
      const r1 = await append(s, 'exact', 0n, [{ event_type: 'A' }], c1)
      assert.equal(r1.length, 1)

      // c2: open transaction, kick off append_to_stream WITHOUT awaiting.
      // Inside the procedure: SELECT FOR UPDATE finds nothing (c1
      // uncommitted under read-committed isolation), falls into the
      // V=0-creates-stream branch, attempts INSERT — which blocks on
      // c1's pending unique-index entry.
      await c2.query('BEGIN')
      const p2 = append(s, 'exact', 0n, [{ event_type: 'B' }], c2)

      // Give c2 a moment to reach the blocking INSERT.
      await new Promise((r) => setTimeout(r, 50))

      // c1 commits: c2's INSERT now resolves to a unique_violation.
      await c1.query('COMMIT')

      // c2 must surface IS001, not 23505.
      let loser: unknown
      try {
        await p2
        assert.fail('c2 must reject; the stream already exists')
      } catch (err) {
        loser = err
      }
      await c2.query('ROLLBACK')

      const code = (loser as { code?: unknown }).code
      assert.equal(
        code,
        'IS001',
        `loser must reject with IS001, got ${String(code)} (raw 23505 means the SQL fix is missing)`,
      )

      // Stream is at version 1, owned by c1.
      assert.equal(await streamVersionOf(s), 1n)
    } finally {
      await c1.end()
      await c2.end()
    }
  })

  // INV-APPEND-021: concurrent 'any' to different streams: all succeed,
  //   and the global event_number sequence stays gapless (D-0012).
  void test("concurrent 'any' to different streams: all succeed, $all is gapless 1..N", async () => {
    const streams = Array.from({ length: 5 }, () => randomUUID())
    const eventsPerStream = 4
    const total = streams.length * eventsPerStream

    // Two-tier concurrency: each stream fires its events back-to-back in
    // its own task; tasks run concurrently. This produces overlapping
    // $all-row contention.
    const tasks = streams.map(async (s) => {
      const out: AppendedRow[] = []
      for (let i = 0; i < eventsPerStream; i++) {
        const r = await append(s, 'any', null, [{ event_type: `E${i}` }])
        out.push(...r)
      }
      return out
    })
    const results = (await Promise.all(tasks)).flat()
    assert.equal(results.length, total)

    // Assert $all has every event_number in [1..total] exactly once,
    // i.e. no gaps and no duplicates.
    const r = await pool.query<{ event_number: string }>(
      `SELECT stream_version AS event_number
         FROM instructed.stream_events
        WHERE stream_id = 0
        ORDER BY stream_version`,
    )
    const ens = r.rows.map((row) => Number(row.event_number))
    assert.equal(ens.length, total)
    for (let i = 0; i < total; i++) {
      assert.equal(ens[i], i + 1, `gap at position ${i}: ${ens.join(',')}`)
    }
    // And the $all row's bookkeeping agrees.
    assert.equal(await streamVersionOf('$all'), BigInt(total))
  })
})

// =============================================================================
// Duplicate event_id (INV-APPEND-030)
// =============================================================================

void describe('append_to_stream — duplicate event_id', () => {
  // INV-APPEND-030: re-appending an event_id MUST NOT silently succeed and
  //   MUST NOT duplicate. In `instructed` this is IS004 duplicate_event.
  void test('re-appending an existing event_id raises IS004 duplicate_event', async () => {
    const s = randomUUID()
    const id = randomUUID()
    await append(s, 'no_stream', null, [{ event_id: id, event_type: 'A' }])
    await rejectsWithCode(
      () => append(s, 'any', null, [{ event_id: id, event_type: 'B' }]),
      'IS004',
    )
  })

  // INV-APPEND-030: duplicate event_id within a single batch also fails
  void test('duplicate event_id within a single batch raises IS004', async () => {
    const s = randomUUID()
    const id = randomUUID()
    await rejectsWithCode(
      () =>
        append(s, 'no_stream', null, [
          { event_id: id, event_type: 'A' },
          { event_id: id, event_type: 'B' },
        ]),
      'IS004',
    )
    // And nothing landed.
    assert.equal(await streamVersionOf(s), null)
    assert.equal(await allRowCount(), 0n)
  })
})

// =============================================================================
// Immutability (INV-APPEND-040) and hard-delete (INV-APPEND-041)
// =============================================================================

void describe('append_to_stream — immutability', () => {
  // INV-APPEND-040: persisted events MUST NOT be modified by any later op.
  //   Enforced by the events_no_update / stream_events_no_update triggers;
  //   IS006 is raised on direct DDL/DML attempts. (IS006 is internal — not in
  //   the procedure-facing catalogue — but it IS the assertable post-condition
  //   for INV-APPEND-040.)
  void test('direct UPDATE on instructed.events raises IS006', async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    await rejectsWithCode(
      () => pool.query(`UPDATE instructed.events SET event_type = 'X' WHERE event_type = 'A'`),
      'IS006',
    )
  })

  // INV-APPEND-040: same for DELETE.
  void test('direct DELETE on instructed.events raises IS006', async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    await rejectsWithCode(
      () => pool.query(`DELETE FROM instructed.events WHERE event_type = 'A'`),
      'IS006',
    )
  })

  // INV-APPEND-040: stream_events is similarly immutable
  void test('direct UPDATE on instructed.stream_events raises IS006', async () => {
    const s = randomUUID()
    await append(s, 'no_stream', null, [{ event_type: 'A' }])
    await rejectsWithCode(
      () =>
        pool.query(`UPDATE instructed.stream_events SET stream_version = 999 WHERE stream_id <> 0`),
      'IS006',
    )
  })

  // INV-APPEND-041: no hard-delete path in v1 (tighter than Commanded —
  //   see mapping.md INV-APPEND-040/041 + NG-* in non-goals.md).
  //   The assertable post-condition is "no public procedure named
  //   *hard_delete* / *delete_event* / *truncate* exists in the
  //   instructed schema". This is a documentation-shape test more than
  //   a behaviour test, but it locks the absence in.
  void test('no hard-delete procedure exists in v1 (tighter than Commanded)', async () => {
    const r = await pool.query<{ proname: string }>(
      `SELECT proname
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'instructed'
          AND (
            p.proname ILIKE '%hard_delete%'
            OR p.proname ILIKE '%delete_event%'
            OR p.proname ILIKE '%truncate%'
          )`,
    )
    assert.equal(
      r.rowCount,
      0,
      `expected no hard-delete-shaped procedure; found ${r.rows.map((row) => row.proname).join(', ')}`,
    )
  })
})

// =============================================================================
// Reserved stream uuid (INV-STREAM-003 — also surfaces from append_to_stream)
// =============================================================================

void describe('append_to_stream — reserved stream uuid', () => {
  // INV-STREAM-003 / NG-0011: '$all' is reserved; user appends must fail.
  //   The full INV-STREAM-* coverage lands in step 7/8; this case lives
  //   here because the reservation is enforced at the append_to_stream
  //   surface, not by an out-of-band check.
  void test("appending to '$all' raises IS005 reserved_stream_uuid", async () => {
    await rejectsWithCode(() => append('$all', 'any', null, [{ event_type: 'A' }]), 'IS005')
  })
})
