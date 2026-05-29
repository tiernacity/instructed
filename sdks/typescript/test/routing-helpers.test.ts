/**
 * Smoke tests for the L3 routing-function combinators.
 *
 * The contract being tested is purely shape:
 *
 *   - events with `type` outside the allowed set are routed to
 *     `"ignore"` without ever calling `inner`;
 *   - events with `type` in the allowed set are passed to `inner`
 *     and its decision is returned verbatim;
 *   - the allowed-set Set is built once (lookup is O(1) per event,
 *     not a re-scan of the array).
 *
 * Storage / worker integration is exercised end-to-end via the
 * routing-worker / projection-worker / bank-account tests; this
 * file stays in-memory.
 */

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { onlyTypes } from '../src/index.ts'
import type { RecordedEvent, RoutingFn } from '../src/index.ts'

// --- minimal event union ----------------------------------------------------

type Added = { type: 'Added'; data: { n: number } }
type Removed = { type: 'Removed'; data: { n: number } }
type Pinged = { type: 'Pinged'; data: Record<string, never> }
type E = Added | Removed | Pinged

function fake(type: E['type'], data: unknown = {}): RecordedEvent<E> {
  return {
    event_id: 'id',
    event_number: 1n,
    stream_uuid: 's',
    stream_version: 1n,
    type,
    causation_id: null,
    correlation_id: null,
    data,
    metadata: null,
    created_at: new Date(0),
  } as RecordedEvent<E>
}

void describe('onlyTypes', () => {
  void test('delegates matching events to inner and ignores the rest', async () => {
    const seen: string[] = []
    const inner: RoutingFn<E> = (event) => {
      seen.push(event.type)
      return { partitionKey: 'p' }
    }
    const fn = onlyTypes<E>(['Added', 'Removed'], inner)

    assert.deepEqual(await fn(fake('Added', { n: 1 })), { partitionKey: 'p' })
    assert.deepEqual(await fn(fake('Removed', { n: 2 })), { partitionKey: 'p' })
    assert.equal(await fn(fake('Pinged')), 'ignore')
    // Inner was called only for the allowed types -- no entry for "Pinged".
    assert.deepEqual(seen, ['Added', 'Removed'])
  })

  void test("inner sees the user's typed event union (narrowing through switch)", async () => {
    // Pure compile-time check: inside `inner`, `event.data` narrows
    // per `event.type`. Runtime assertion is just that the routing
    // decision flows back unchanged.
    const fn = onlyTypes<E>(['Added'], (event) => {
      // After narrowing on `event.type === "Added"`, `event.data.n` is typed.
      if (event.type === 'Added') {
        return { partitionKey: String(event.data.n) }
      }
      return 'ignore'
    })
    assert.deepEqual(await fn(fake('Added', { n: 42 })), { partitionKey: '42' })
  })

  void test('empty types list ignores everything', async () => {
    let called = false
    const fn = onlyTypes<E>([], () => {
      called = true
      return { partitionKey: 'p' }
    })
    assert.equal(await fn(fake('Added')), 'ignore')
    assert.equal(await fn(fake('Pinged')), 'ignore')
    assert.equal(called, false)
  })
})
