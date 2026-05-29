/**
 * Part E — Persistent subscriptions conformance (Phase 9, step 5/8).
 *
 * Drives `instructed.claim_subscription`,
 * `instructed.release_subscription`,
 * `instructed.route_batch` (for cursor advance), and
 * `instructed.delete_subscription` directly via `pg` (D-0021).
 *
 * The pre-SUB-A single-cursor delivery procedures
 * (`read_subscription_batch` / `advance_subscription`) were removed
 * in slice A3; their delivery-order, at-least-once and
 * cursor-monotonicity invariants (INV-SUB-P-030/031/032/034) are now
 * realised by the SUB-A routing cursor + work-item queue and covered
 * in `subscription-work-items-procedures.test.ts`.
 *
 * Each `test(...)` carries one or more `// INV-SUB-P-NNN` annotations
 * on the line above it. The procedures' full contracts live in
 * `sql/instructed.sql`.
 *
 * Lease-expiry cases simulate expiry via a direct UPDATE on the
 * `subscriptions` table (lease_seconds is an integer ≥ 1; waiting
 * for real expiry would slow the suite without exercising any
 * different invariant). This is a SQL-only harness: bypassing the
 * procedure surface for setup is fair game as long as the
 * post-condition under test still goes through a procedure.
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import { appendAny, rejectsWithCode } from './_helpers.ts'
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
// Per-procedure thin wrappers
// -----------------------------------------------------------------------------

interface ClaimRow {
  result: 'claimed' | 'already_claimed'
  last_seen: bigint
  claimed_by: string
  claim_expires_at: Date
}

async function claim(
  streamUuid: string,
  name: string,
  workerId: string,
  leaseSeconds = 30,
  options: Record<string, unknown> = {},
): Promise<ClaimRow> {
  const r = await pool.query<{
    result: string
    last_seen: string
    claimed_by: string
    claim_expires_at: Date
  }>(`SELECT * FROM instructed.claim_subscription($1, $2, $3, $4, $5::jsonb)`, [
    streamUuid,
    name,
    workerId,
    leaseSeconds,
    JSON.stringify(options),
  ])
  const row = r.rows[0]
  return {
    result: row.result as 'claimed' | 'already_claimed',
    last_seen: BigInt(row.last_seen),
    claimed_by: row.claimed_by,
    claim_expires_at: row.claim_expires_at,
  }
}

async function release(streamUuid: string, name: string, workerId: string): Promise<void> {
  await pool.query(`SELECT instructed.release_subscription($1, $2, $3)`, [
    streamUuid,
    name,
    workerId,
  ])
}

// Advance the routing cursor via the SUB-A `route_batch` primitive
// (the pre-SUB-A `advance_subscription` procedure was removed in A3).
// Passing an empty decisions array moves the cursor monotonically to
// `greatest(last_seen, upToPosition)` without enqueuing work items —
// exactly the cursor-only advance these lifecycle tests need. Requires
// the caller to hold the lease; a non-holder raises IS022 and a
// missing subscription raises IS020, matching the old contract.
async function advance(
  streamUuid: string,
  name: string,
  workerId: string,
  upToPosition: bigint,
): Promise<bigint> {
  const r = await pool.query<{ new_last_seen: string }>(
    `SELECT new_last_seen
       FROM instructed.route_batch($1, $2, $3, $4, '[]'::jsonb)`,
    [streamUuid, name, workerId, upToPosition],
  )
  return BigInt(r.rows[0].new_last_seen)
}

// Reads the routing cursor directly. The read_subscription_position
// procedure was removed in A2; D-0021 permits direct table reads for
// post-condition assertions in this SQL-only harness.
async function position(streamUuid: string, name: string): Promise<bigint> {
  const r = await pool.query<{ last_seen: string }>(
    `SELECT s.last_seen::text AS last_seen
       FROM instructed.subscriptions s
       JOIN instructed.streams str ON str.stream_id = s.stream_id
      WHERE str.stream_uuid = $1 AND s.subscription_name = $2`,
    [streamUuid, name],
  )
  return BigInt(r.rows[0].last_seen)
}

// True when no subscription row exists for (stream, name).
async function subscriptionGone(streamUuid: string, name: string): Promise<boolean> {
  const r = await pool.query(
    `SELECT 1
       FROM instructed.subscriptions s
       JOIN instructed.streams str ON str.stream_id = s.stream_id
      WHERE str.stream_uuid = $1 AND s.subscription_name = $2`,
    [streamUuid, name],
  )
  return r.rows.length === 0
}

async function deleteSub(streamUuid: string, name: string): Promise<void> {
  await pool.query(`SELECT instructed.delete_subscription($1, $2)`, [streamUuid, name])
}

/** Force the lease to be expired without sleeping. */
async function expireLease(streamUuid: string, name: string): Promise<void> {
  await pool.query(
    `UPDATE instructed.subscriptions s
        SET claim_expires_at = now() - interval '1 second'
       FROM instructed.streams str
      WHERE s.stream_id = str.stream_id
        AND str.stream_uuid = $1
        AND s.subscription_name = $2`,
    [streamUuid, name],
  )
}

/** Seed N events on a fresh stream; return the stream uuid. */
async function seedStream(n: number): Promise<string> {
  const s = randomUUID()
  const events = Array.from({ length: n }, (_, i) => ({
    event_type: `E${i + 1}`,
    data: { i: i + 1 },
  }))
  await appendAny(pool, s, events)
  return s
}

// =============================================================================
// Identity and idempotent re-subscribe (INV-SUB-P-001, 002)
// =============================================================================

void describe('subscriptions — identity and re-subscribe', () => {
  // INV-SUB-P-001: identity is (stream_uuid, subscription_name)
  void test('first claim on a fresh (stream, name) creates the row and returns last_seen = 0', async () => {
    const s = await seedStream(3)
    const r = await claim(s, 'handler-a', 'worker-1')
    assert.equal(r.result, 'claimed')
    assert.equal(r.last_seen, 0n)
    assert.equal(r.claimed_by, 'worker-1')
    assert.ok(r.claim_expires_at instanceof Date)
  })

  // INV-SUB-P-002: re-claim by the same worker is idempotent — cursor preserved
  void test('re-claim by the same worker preserves last_seen and extends the lease', async () => {
    const s = await seedStream(3)
    const first = await claim(s, 'handler-a', 'worker-1')
    // Advance the cursor so the second claim's last_seen is observably non-zero.
    await advance(s, 'handler-a', 'worker-1', 2n)
    const second = await claim(s, 'handler-a', 'worker-1')
    assert.equal(second.result, 'claimed')
    assert.equal(second.last_seen, 2n)
    assert.equal(second.claimed_by, 'worker-1')
    assert.ok(
      second.claim_expires_at.getTime() >= first.claim_expires_at.getTime(),
      'lease must not move backwards across re-claim',
    )
  })

  // INV-SUB-P-001: subscription on '$all' is permitted (stream_id = 0)
  void test("a subscription on '$all' is permitted and starts at last_seen = 0", async () => {
    const r = await claim('$all', 'all-handler', 'worker-1')
    assert.equal(r.result, 'claimed')
    assert.equal(r.last_seen, 0n)
  })

  // INV-SUB-P-001 (negative): the target stream must exist (and is not $all)
  void test('claim on a non-existent stream raises IS003 stream_not_found', async () => {
    await rejectsWithCode(() => claim(randomUUID(), 'handler-a', 'worker-1'), 'IS003')
  })
})

// =============================================================================
// start_from semantics (INV-SUB-P-020, 021)
// =============================================================================

void describe('subscriptions — start_from on first vs subsequent claim', () => {
  // INV-SUB-P-020: default start_from = 'origin' → last_seen = 0
  void test("default start_from is 'origin' (last_seen = 0)", async () => {
    const s = await seedStream(5)
    const r = await claim(s, 'h', 'w1')
    assert.equal(r.last_seen, 0n)
  })

  // INV-SUB-P-020: start_from = 'current' → last_seen = current head
  void test("start_from 'current' on '$all' starts at the current event_number head", async () => {
    await seedStream(3)
    await seedStream(2) // 5 events total
    const r = await claim('$all', 'h', 'w1', 30, { start_from: 'current' })
    assert.equal(r.last_seen, 5n)
  })

  // INV-SUB-P-020: start_from = 'current' on single stream → last_seen = its current version
  void test("start_from 'current' on a single stream starts at that stream's version", async () => {
    const s = await seedStream(7)
    const r = await claim(s, 'h', 'w1', 30, { start_from: 'current' })
    assert.equal(r.last_seen, 7n)
  })

  // INV-SUB-P-020: start_from = N → last_seen = N
  void test('start_from integer N sets last_seen = N', async () => {
    const s = await seedStream(10)
    const r = await claim(s, 'h', 'w1', 30, { start_from: '3' })
    assert.equal(r.last_seen, 3n)
  })

  // INV-SUB-P-021: start_from is IGNORED on subsequent claims
  void test('start_from is ignored on subsequent claims (cursor resumes from last_seen)', async () => {
    const s = await seedStream(10)
    await claim(s, 'h', 'w1', 30, { start_from: '0' })
    await advance(s, 'h', 'w1', 4n)
    // Now re-claim with start_from = '0' (which would reset to 0 if honoured) —
    // the call must instead resume from last_seen = 4.
    const r = await claim(s, 'h', 'w1', 30, { start_from: '0' })
    assert.equal(r.last_seen, 4n)
  })

  // INV-SUB-P-020 (input validation): malformed start_from → 22023
  void test('malformed start_from raises 22023', async () => {
    const s = await seedStream(1)
    await rejectsWithCode(() => claim(s, 'h', 'w1', 30, { start_from: 'not-a-number' }), '22023')
  })
})

// =============================================================================
// Single-active-subscriber and failover (INV-SUB-P-010, 011, 012)
// =============================================================================

void describe('subscriptions — single-active-subscriber and failover', () => {
  // INV-SUB-P-010: only one live subscriber per (stream, name).
  //   In `instructed`'s lease model, a competing claim returns
  //   `result = 'already_claimed'` with the current holder reported
  //   in `claimed_by` (NOT a raised exception — this is the row-result
  //   shape per claim_subscription's contract).
  // INV-SUB-P-011 [reference-only mechanism]: Commanded uses
  //   pg_advisory_lock; `instructed` uses lease rows per D-0006.
  //   Both realise INV-SUB-P-010; only the mechanism differs.
  void test("a second claim on a live lease returns 'already_claimed' with the current holder", async () => {
    const s = await seedStream(2)
    const first = await claim(s, 'h', 'worker-A')
    assert.equal(first.result, 'claimed')

    const second = await claim(s, 'h', 'worker-B')
    assert.equal(second.result, 'already_claimed')
    assert.equal(second.claimed_by, 'worker-A') // diagnostics
    assert.equal(second.last_seen, 0n) // cursor reported, unmodified
  })

  // INV-SUB-P-012: when the holder disconnects, its slot becomes
  //   available without administrative action — realised by lease TTL.
  void test('after lease expiry, a different worker claims successfully', async () => {
    const s = await seedStream(2)
    await claim(s, 'h', 'worker-A', 30)
    // Simulate worker-A disconnecting and its lease timing out:
    await expireLease(s, 'h')
    const r = await claim(s, 'h', 'worker-B', 30)
    assert.equal(r.result, 'claimed')
    assert.equal(r.claimed_by, 'worker-B')
  })

  // INV-SUB-P-010 (idempotence under same worker): re-claim by the
  //   current holder always succeeds (used as a heartbeat fallback).
  void test("re-claim by the current holder succeeds (no 'already_claimed' against self)", async () => {
    const s = await seedStream(1)
    await claim(s, 'h', 'w1')
    const r = await claim(s, 'h', 'w1')
    assert.equal(r.result, 'claimed')
    assert.equal(r.claimed_by, 'w1')
  })

  // claim_subscription contention race (TODO #15): when the
  //   FOR UPDATE SKIP LOCKED pre-check finds zero rows because another
  //   transaction holds the row lock, the unlocked diagnostic re-read
  //   sees the row in its released between-batches state (NULL
  //   claimed_by, NULL claim_expires_at under D-0025). The procedure
  //   MUST return 'already_claimed' with NULL diagnostic fields rather
  //   than fabricating values. Two dedicated client connections with
  //   explicit BEGIN pin the race: session 1 holds a SELECT FOR UPDATE
  //   on the subscriptions row across session 2's claim attempt.
  void test("contention race: 'already_claimed' carries NULL diagnostic fields when SKIP LOCKED finds zero rows", async () => {
    const s = await seedStream(1)
    // Create the subscription row in the released state (claim then
    // release leaves claimed_by / claim_expires_at = NULL). This is
    // the D-0025 between-batches steady state.
    await claim(s, 'h', 'w-setup')
    await release(s, 'h', 'w-setup')

    const { Client } = await import('pg')
    const conn = {
      host: process.env.PGHOST ?? '127.0.0.1',
      port: Number(process.env.PGPORT ?? 5432),
      user: process.env.PGUSER ?? 'postgres',
      password: process.env.PGPASSWORD ?? 'postgres',
      database: process.env.PGDATABASE ?? 'instructed_test',
    }
    const blocker = new Client(conn)
    const claimant = new Client(conn)
    await blocker.connect()
    await claimant.connect()
    try {
      // session 1: hold a row lock on the subscriptions row across
      // session 2's call. Look the row up by (stream_uuid, name) via a
      // join to streams; the row exists from the setup above.
      await blocker.query('BEGIN')
      const lockRes = await blocker.query<{ stream_id: number }>(
        `SELECT sub.stream_id
           FROM instructed.subscriptions sub
           JOIN instructed.streams st ON st.stream_id = sub.stream_id
          WHERE st.stream_uuid = $1 AND sub.subscription_name = $2
          FOR UPDATE OF sub`,
        [s, 'h'],
      )
      assert.equal(lockRes.rows.length, 1)

      // session 2: call claim_subscription. The FOR UPDATE SKIP LOCKED
      // step inside the procedure finds zero rows (blocker holds the
      // lock); the diagnostic unlocked re-read sees the released
      // (NULL, NULL) row; the procedure returns 'already_claimed'
      // with NULL fields.
      const r = await claimant.query<{
        result: string
        last_seen: string
        claimed_by: string | null
        claim_expires_at: Date | null
      }>(`SELECT * FROM instructed.claim_subscription($1, $2, $3, $4, $5::jsonb)`, [
        s,
        'h',
        'w-claimant',
        30,
        JSON.stringify({}),
      ])
      assert.equal(r.rows.length, 1)
      const row = r.rows[0]
      assert.equal(row.result, 'already_claimed')
      assert.equal(row.claimed_by, null, 'claimed_by must be NULL when SKIP LOCKED found zero rows')
      assert.equal(
        row.claim_expires_at,
        null,
        'claim_expires_at must be NULL when SKIP LOCKED found zero rows',
      )

      await blocker.query('ROLLBACK')
    } finally {
      await blocker.end()
      await claimant.end()
    }
  })
})

// =============================================================================
// Selector (INV-SUB-P-050) — above adapter line
// =============================================================================
//
// INV-SUB-P-050: above adapter line — see D-0023 / ML-0003 / mapping.md Pass 2
//
// In Commanded, an optional `selector` predicate filters delivery; the
// subscription server still acks filtered-out events. In `instructed`
// v1, selectors are realised entirely SDK-side (the SDK reads a batch,
// runs the predicate, calls the handler on matches, and advances the
// cursor to the highest *fetched* event_number regardless of how many
// matched). The SQL surface deliberately exposes no selector
// parameter on the routing primitives and no selector column on
// `subscriptions`. Server-side selector evaluation is reserved for
// ML-0003.
//
// The omission is documented (rather than asserted as a behavioural
// case) because there is nothing for the SQL layer to assert: the
// invariant lives in the SDK and is covered by
// `sdks/typescript/test/subscription.test.ts` :: "selector (SDK-side)".
// The annotation above is what the step-8/8 coverage reporter
// recognises as "above adapter line", distinguishing it from "dropped"
// (a deliberate elimination, like INV-SUB-T-*) and from "missing" (a
// real gap).

// =============================================================================
// Lifecycle — release / delete / position (INV-SUB-P-060, 061, 062)
// =============================================================================

void describe('subscriptions — lifecycle', () => {
  // INV-SUB-P-060: release detaches the holder but preserves the cursor
  void test('release_subscription clears the holder and preserves last_seen', async () => {
    const s = await seedStream(5)
    await claim(s, 'h', 'w1')
    await advance(s, 'h', 'w1', 3n)
    await release(s, 'h', 'w1')
    // Cursor is still 3.
    assert.equal(await position(s, 'h'), 3n)
    // A new worker can claim and resume from there.
    const r = await claim(s, 'h', 'w2')
    assert.equal(r.result, 'claimed')
    assert.equal(r.last_seen, 3n)
  })

  // INV-SUB-P-060: release by a non-holder raises IS022.
  void test('release_subscription by a non-holder raises IS022', async () => {
    const s = await seedStream(1)
    await claim(s, 'h', 'w1')
    await rejectsWithCode(() => release(s, 'h', 'w2'), 'IS022')
  })

  // INV-SUB-P-060: release on a missing subscription raises IS020.
  void test('release_subscription on a missing subscription raises IS020', async () => {
    const s = await seedStream(1)
    await rejectsWithCode(() => release(s, 'no-such', 'w1'), 'IS020')
  })

  // INV-SUB-P-061: delete removes the row entirely; subsequent claim
  //   behaves as a first-create (honours start_from).
  void test('delete_subscription removes the row; next claim treats it as fresh', async () => {
    const s = await seedStream(5)
    await claim(s, 'h', 'w1')
    await advance(s, 'h', 'w1', 3n)
    await deleteSub(s, 'h')
    // The cursor is gone — the subscription row no longer exists.
    assert.equal(await subscriptionGone(s, 'h'), true)
    // A subsequent claim honours start_from (here: explicit '4').
    const r = await claim(s, 'h', 'w1', 30, { start_from: '4' })
    assert.equal(r.last_seen, 4n)
  })

  // INV-SUB-P-062 / D-0009: delete on a missing subscription raises IS020.
  //   This is tighter than Commanded's reference adapter (which is
  //   silent); `instructed` follows the abstract contract.
  void test('delete_subscription on a missing subscription raises IS020 (D-0009)', async () => {
    const s = await seedStream(1)
    await rejectsWithCode(() => deleteSub(s, 'no-such'), 'IS020')
  })

  // INV-SUB-P-062: delete on a stream that doesn't even exist → IS020.
  void test('delete_subscription on a non-existent stream raises IS020', async () => {
    await rejectsWithCode(() => deleteSub(randomUUID(), 'h'), 'IS020')
  })

  // INV-SUB-P-011 (composed lease-expiry → takeover → IS022 on
  //   original): the routing-side three-step case called out in
  //   TODO #11 / §4 gap-list item 7. SP:322 covers "different worker
  //   claims after expiry"; this one composes that with the
  //   non-holder-fails case into the single end-to-end sequence the
  //   invariant's wording promises, with *no* administrative action
  //   in between.
  void test("after lease expiry + takeover, the original holder's next op raises IS022", async () => {
    const s = await seedStream(2)
    const w1 = await claim(s, 'h', 'worker-A', 30)
    assert.equal(w1.result, 'claimed')

    // No admin action — just lease expiry.
    await expireLease(s, 'h')

    // A second worker takes over.
    const w2 = await claim(s, 'h', 'worker-B', 30)
    assert.equal(w2.result, 'claimed')
    assert.equal(w2.claimed_by, 'worker-B')

    // Original holder's *next* op on any of the routing-side
    // procedures must raise IS022 (subscription_lease_lost).
    await rejectsWithCode(() => advance(s, 'h', 'worker-A', 1n), 'IS022')
    await rejectsWithCode(() => release(s, 'h', 'worker-A'), 'IS022')
  })
})
