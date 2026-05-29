/**
 * Part E — Transient subscriptions (Phase 9, step 7/8).
 *
 * INV-SUB-T-001..005 are **dropped wholesale** in `instructed` v1 per
 * NG-0005 / D-0007. There is no fire-and-forget pub/sub primitive;
 * live-tail use cases are served by a persistent subscription with
 * `start_from: 'current'` plus a teardown call (see `mapping.md`
 * Pass 2 Part E.1).
 *
 * This file exists so that the INV-coverage matrix (step 8/8)
 * renders INV-SUB-T-* as "dropped" rather than "missing". The
 * omission is asserted concretely: no transient-subscribe procedure
 * exists in the `instructed.*` namespace.
 */

import assert from 'node:assert/strict'
import { before, after, describe, test } from 'node:test'

import type pg from 'pg'

import { closePool, getPool } from './fixtures.ts'

let pool: pg.Pool

before(async () => {
  pool = await getPool()
})

after(async () => {
  await closePool()
})

void describe('transient subscriptions — dropped wholesale (NG-0005 / D-0007)', () => {
  // INV-SUB-T-001: dropped — see NG-0005
  // INV-SUB-T-002: dropped — see NG-0005
  // INV-SUB-T-003: dropped — see NG-0005
  // INV-SUB-T-004: dropped — see NG-0005
  // INV-SUB-T-005: dropped — see NG-0005
  //
  // The Commanded transient-subscription surface (`subscribe/2` + the
  // process-mailbox `{:events, events}` delivery) has no analogue in
  // `instructed`. The omission shape: no procedure named
  // `subscribe`, `transient_subscribe`, `subscribe_transient`, or
  // similar exists in `instructed.*`. The persistent surface
  // (claim_subscription / route_batch / etc.) is the
  // only subscription primitive.
  void test('no transient-subscribe procedure exists in the instructed schema', async () => {
    const r = await pool.query<{ proname: string }>(
      `SELECT proname
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'instructed'
          AND (
            p.proname = 'subscribe'
            OR p.proname ILIKE 'transient_%'
            OR p.proname ILIKE 'subscribe_transient%'
            OR p.proname ILIKE 'pub%'
          )`,
    )
    assert.equal(
      r.rowCount,
      0,
      `expected no transient-subscribe procedure; found ${r.rows.map((row) => row.proname).join(', ')}`,
    )
  })
})
