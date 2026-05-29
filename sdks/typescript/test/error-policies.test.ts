/**
 * Step-5 slice 3 — retry/error-policy standard library + state
 * threading.
 *
 * Unit tests for the L3 helpers in `src/error-policies.ts` plus an
 * integration test pinning down per-work-item `PolicyState` lifecycle
 * (starts undefined; threaded forward across attempts on the same
 * work item).
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import {
  Client,
  exponentialBackoff,
  expected,
  linearBackoff,
  retryUpTo,
  startProcessingWorker,
  type ErrorPolicy,
  type Event,
} from '../src/index.ts'
import { Logger } from '../src/logger/index.ts'
import { closePool, getPool, truncateAll } from './fixtures.ts'

const ALL = '$all'

let pool: pg.Pool
let client: Client

before(async () => {
  pool = await getPool()
  client = new Client(pool)
})

after(async () => {
  await closePool()
})

beforeEach(async () => {
  await truncateAll(pool)
})

// ============================================================================
// Pure unit tests (no DB)
// ============================================================================

void describe('exponentialBackoff', () => {
  const ctxBase = { workerId: 'w', partitionKey: 'p', eventNumber: 1n, logger: Logger.noop() }

  void test('doubles per attempt, capped', async () => {
    const p = exponentialBackoff({ baseMs: 100, capMs: 1000 })
    const r1 = await p(new Error(), { ...ctxBase, attempt: 1 }, undefined)
    const r2 = await p(new Error(), { ...ctxBase, attempt: 2 }, undefined)
    const r3 = await p(new Error(), { ...ctxBase, attempt: 3 }, undefined)
    const r99 = await p(new Error(), { ...ctxBase, attempt: 99 }, undefined)
    assert.equal(r1.decision.kind, 'retry-in')
    if (r1.decision.kind === 'retry-in') assert.equal(r1.decision.delayMs, 100)
    if (r2.decision.kind === 'retry-in') assert.equal(r2.decision.delayMs, 200)
    if (r3.decision.kind === 'retry-in') assert.equal(r3.decision.delayMs, 400)
    if (r99.decision.kind === 'retry-in') assert.equal(r99.decision.delayMs, 1000)
    // Stateless: state always undefined.
    assert.equal(r1.state, undefined)
    assert.equal(r99.state, undefined)
  })

  void test('jitter samples in [0, computedDelay)', async () => {
    const p = exponentialBackoff({ baseMs: 100, capMs: 1000, jitter: true })
    // 50 samples; all in [0, 200) for attempt=2.
    for (let i = 0; i < 50; i++) {
      const r = await p(new Error(), { ...ctxBase, attempt: 2 }, undefined)
      assert.equal(r.decision.kind, 'retry-in')
      if (r.decision.kind === 'retry-in') {
        assert.ok(r.decision.delayMs >= 0, `delay ${r.decision.delayMs} < 0`)
        assert.ok(r.decision.delayMs < 200, `delay ${r.decision.delayMs} >= 200`)
      }
    }
  })

  void test('rejects bad arguments', () => {
    assert.throws(() => exponentialBackoff({ baseMs: -1, capMs: 1000 }), RangeError)
    assert.throws(() => exponentialBackoff({ baseMs: NaN, capMs: 1000 }), RangeError)
    assert.throws(() => exponentialBackoff({ baseMs: 100, capMs: -1 }), RangeError)
  })
})

void describe('linearBackoff', () => {
  const ctxBase = { workerId: 'w', partitionKey: 'p', eventNumber: 1n, logger: Logger.noop() }

  void test('grows linearly, capped', async () => {
    const p = linearBackoff({ stepMs: 50, capMs: 200 })
    const r1 = await p(new Error(), { ...ctxBase, attempt: 1 }, undefined)
    const r2 = await p(new Error(), { ...ctxBase, attempt: 2 }, undefined)
    const r4 = await p(new Error(), { ...ctxBase, attempt: 4 }, undefined)
    const r99 = await p(new Error(), { ...ctxBase, attempt: 99 }, undefined)
    if (r1.decision.kind === 'retry-in') assert.equal(r1.decision.delayMs, 50)
    if (r2.decision.kind === 'retry-in') assert.equal(r2.decision.delayMs, 100)
    if (r4.decision.kind === 'retry-in') assert.equal(r4.decision.delayMs, 200)
    if (r99.decision.kind === 'retry-in') assert.equal(r99.decision.delayMs, 200)
  })

  void test('rejects bad arguments', () => {
    assert.throws(() => linearBackoff({ stepMs: -1, capMs: 100 }), RangeError)
    assert.throws(() => linearBackoff({ stepMs: 50, capMs: NaN }), RangeError)
  })
})

void describe('retryUpTo', () => {
  const ctxBase = { workerId: 'w', partitionKey: 'p', eventNumber: 1n, logger: Logger.noop() }

  void test('delegates to inner when attempt <= max', async () => {
    const inner = exponentialBackoff({ baseMs: 100, capMs: 1000 })
    const p = retryUpTo(3, inner)
    const r1 = await p(new Error(), { ...ctxBase, attempt: 1 }, undefined)
    const r2 = await p(new Error(), { ...ctxBase, attempt: 2 }, undefined)
    const r3 = await p(new Error(), { ...ctxBase, attempt: 3 }, undefined)
    assert.equal(r1.decision.kind, 'retry-in')
    assert.equal(r2.decision.kind, 'retry-in')
    assert.equal(r3.decision.kind, 'retry-in')
  })

  void test('emits stop once attempt > max', async () => {
    const inner = exponentialBackoff({ baseMs: 100, capMs: 1000 })
    const p = retryUpTo(3, inner)
    const r4 = await p(new Error(), { ...ctxBase, attempt: 4 }, undefined)
    const r100 = await p(new Error(), { ...ctxBase, attempt: 100 }, undefined)
    assert.equal(r4.decision.kind, 'stop')
    assert.equal(r100.decision.kind, 'stop')
  })

  void test("preserves inner policy's state", async () => {
    // A stateful inner policy that counts how often it's invoked.
    interface CountState {
      calls: number
    }
    const counting: ErrorPolicy<CountState> = (_err, _ctx, state) => {
      const next = { calls: (state?.calls ?? 0) + 1 }
      return { decision: { kind: 'retry-in', delayMs: 1 }, state: next }
    }
    const p = retryUpTo(5, counting)
    let s: CountState | undefined = undefined
    for (let attempt = 1; attempt <= 3; attempt++) {
      const r = await p(new Error(), { ...ctxBase, attempt }, s)
      s = r.state
    }
    assert.deepEqual(s, { calls: 3 })
  })

  void test('rejects bad arguments', () => {
    const inner = exponentialBackoff({ baseMs: 100, capMs: 1000 })
    assert.throws(() => retryUpTo(0, inner), RangeError)
    assert.throws(() => retryUpTo(-1, inner), RangeError)
    assert.throws(() => retryUpTo(1.5, inner), RangeError)
  })
})

// ============================================================================
// Integration: per-work-item state lifecycle in the processing worker
// ============================================================================

async function ensureSubscription(name: string): Promise<void> {
  await client.claimSubscription(ALL, name, 'setup', 30)
  await client.releaseSubscription(ALL, name, 'setup')
}

async function append(streamPrefix: string, n: number): Promise<bigint[]> {
  const stream = `${streamPrefix}-${randomUUID().slice(0, 8)}`
  const rows = await client.appendToStream(
    stream,
    expected.any,
    Array.from({ length: n }, (_, i) => ({
      type: `E${i}`,
      data: { i },
    })),
  )
  return rows.map((r) => r.event_number)
}

async function route(name: string, decisions: Array<{ pk: string; en: bigint }>): Promise<void> {
  await client.claimSubscription(ALL, name, 'router', 30)
  try {
    const en = decisions.reduce((m, d) => (d.en > m ? d.en : m), decisions[0]?.en ?? 0n)
    await client.routeBatch(
      ALL,
      name,
      'router',
      en,
      decisions.map((d) => ({ partitionKey: d.pk, eventNumber: d.en })),
    )
  } finally {
    await client.releaseSubscription(ALL, name, 'router')
  }
}

void describe('processing worker — PolicyState lifecycle (slice 3)', () => {
  void test('state starts undefined and threads forward across attempts on the same item', async () => {
    const name = `ep-state-${randomUUID().slice(0, 8)}`
    await ensureSubscription(name)
    const [en] = await append('ep-state', 1)
    await route(name, [{ pk: 'p1', en }])

    // Stateful policy: records what `state` it sees on each invocation,
    // then increments and either retries or stops.
    interface Seen {
      attempt: number
      stateSeen: number | undefined
    }
    const seen: Seen[] = []
    const policy: ErrorPolicy<number> = (_err, ctx, state) => {
      seen.push({ attempt: ctx.attempt, stateSeen: state })
      const nextState = (state ?? 0) + 1
      // After 3 attempts, stop so the worker exits and the test ends.
      if (ctx.attempt >= 3) {
        return { decision: { kind: 'stop' }, state: nextState }
      }
      return {
        decision: { kind: 'retry-in', delayMs: 5 },
        state: nextState,
      }
    }

    const w = startProcessingWorker<Event, number>(client, {
      name,
      handle: async () => {
        throw new Error('always-fails')
      },
      // `stop` exits before complete fires; this completer is
      // unreachable in this test.
      complete: async () => {
        /* unreachable */
      },
      errorPolicy: policy,
    })

    await w.stopped

    // 3 invocations on the same work item, with state threading
    // 0 -> 1 -> 2 (the SDK passes back what the previous call set).
    assert.equal(seen.length, 3, `expected 3 policy invocations, got ${seen.length}`)
    assert.deepEqual(seen, [
      { attempt: 1, stateSeen: undefined },
      { attempt: 2, stateSeen: 1 },
      { attempt: 3, stateSeen: 2 },
    ])
  })
})
