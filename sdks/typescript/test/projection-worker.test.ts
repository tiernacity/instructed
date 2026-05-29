/**
 * SUB-A slice 6 — projection processing worker tests.
 *
 * Slice acceptance items (SUB-A slice 6; substrate documented in
 * docs/architecture.md "How a worker runs" / "Concurrency model"):
 *   - each `PartitionBy` mode behaves as specified:
 *       sequential = serial; per-event = max parallelism;
 *       per-key   = parallel across keys, serial within.
 *   - immediate-delete: no `done` row ever exists for a projection.
 *   - handler throw leaves the work item `claimed` under the
 *     error-policy retry loop; the DELETE never runs without a
 *     successful handler.
 *   - the DELETE is a single procedure call, not wrapped in any
 *     framework-supplied user-facing tx (D-0016).
 *   - `routingFnForPartitionBy` translation produces the documented
 *     partition keys and never emits `"ignore"`.
 *
 * Tests wire the actual SUB-A routing worker (slice 4) on top of the
 * sugar translator, so the routing + processing paths are exercised
 * end-to-end.
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import { routingFnForPartitionBy, SEQUENTIAL_PARTITION_KEY } from '../src/facade/partition-by.ts'
import { Client, expected } from '../src/index.ts'
import type { RunningWorker } from '../src/internal/running-worker.ts'
import type { Event, RecordedEvent } from '../src/types/index.ts'
import {
  startProjectionWorker,
  type ProjectionWorkerDefinition,
  type ProjectionHandlerContext,
} from '../src/workers/projection/index.ts'
import { startRoutingWorker } from '../src/workers/routing/index.ts'
import { closePool, getPool, truncateAll } from './fixtures.ts'

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

// -- helpers -----------------------------------------------------------------

async function append(streamPrefix: string, n: number): Promise<{ stream: string; ens: bigint[] }> {
  const stream = `${streamPrefix}-${randomUUID().slice(0, 8)}`
  const rows = await client.appendToStream(
    stream,
    expected.any,
    Array.from({ length: n }, (_, i) => ({
      type: `E${i}`,
      data: { i },
    })),
  )
  return { stream, ens: rows.map((r) => r.event_number) }
}

async function waitFor<T>(
  predicate: () => Promise<T | null | undefined>,
  timeoutMs = 5_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const v = await predicate()
    if (v) return v
    await new Promise((r) => setTimeout(r, 25))
  }
  throw new Error(`waitFor: timed out after ${timeoutMs}ms`)
}

async function listWorkItems(
  name: string,
): Promise<Array<{ partition_key: string; event_number: string; state: string }>> {
  const r = await pool.query<{
    partition_key: string
    event_number: string
    state: string
  }>(
    `SELECT partition_key, event_number::text AS event_number, state
       FROM instructed.subscription_work_items
      WHERE subscription_name = $1
      ORDER BY event_number`,
    [name],
  )
  return r.rows
}

async function workItemState(
  name: string,
  partitionKey: string,
  eventNumber: bigint,
): Promise<{ state: string; claimed_by: string | null } | null> {
  const r = await pool.query<{ state: string; claimed_by: string | null }>(
    `SELECT state, claimed_by FROM instructed.subscription_work_items
      WHERE subscription_name = $1
        AND partition_key = $2
        AND event_number = $3`,
    [name, partitionKey, eventNumber.toString()],
  )
  if (r.rowCount === 0) return null
  return r.rows[0]
}

interface WiredWorkers {
  routing: RunningWorker
  processing: RunningWorker
  closeAll: () => Promise<void>
}

/**
 * Start a routing worker (driven by `routeFn`) and a projection processing
 * worker for the same subscription, against the given source stream.
 * Returned `closeAll` is idempotent.
 */
function startPair<E extends Event>(
  source: string,
  def: ProjectionWorkerDefinition<E>,
  routeFn: Parameters<typeof startRoutingWorker<E>>[1]['routeFn'],
  opts: { processingCount?: number } = {},
): WiredWorkers & { processingWorkers: RunningWorker[] } {
  const routing = startRoutingWorker<E>(client, {
    name: def.name,
    stream: source,
    routeFn,
    startFrom: 'origin',
  })
  const count = opts.processingCount ?? 1
  const processingWorkers = Array.from({ length: count }, () =>
    startProjectionWorker<E>(client, { ...def, stream: source }),
  )
  return {
    routing,
    processing: processingWorkers[0],
    processingWorkers,
    async closeAll() {
      await Promise.all([routing.stop(), ...processingWorkers.map((w) => w.stop())])
    },
  }
}

// ============================================================================
// routingFnForPartitionBy — pure translation
// ============================================================================

void describe('routingFnForPartitionBy', () => {
  function fakeEvent<E extends Event = Event>(en: bigint, data: unknown = {}): RecordedEvent<E> {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- test fixture: synthetic RecordedEvent.
    return {
      event_id: 'id-' + en,
      event_number: en,
      stream_uuid: 's',
      stream_version: en,
      type: 'T',
      causation_id: null,
      correlation_id: null,
      data,
      metadata: null,
      created_at: new Date(0),
    } as RecordedEvent<E>
  }

  void test("sequential: every event routes to '_default'", async () => {
    const fn = routingFnForPartitionBy({ kind: 'sequential' })
    for (const en of [1n, 2n, 100n]) {
      const d = await fn(fakeEvent(en))
      assert.deepEqual(d, { partitionKey: SEQUENTIAL_PARTITION_KEY })
    }
  })

  void test('per-event: partition key is String(event_number)', async () => {
    const fn = routingFnForPartitionBy({ kind: 'per-event' })
    for (const en of [1n, 2n, 12345n]) {
      const d = await fn(fakeEvent(en))
      assert.deepEqual(d, { partitionKey: String(en) })
    }
  })

  void test('per-key: calls the user key function', async () => {
    const fn = routingFnForPartitionBy<{ type: string; data: { k: string } }>({
      kind: 'per-key',
      key: (e) => e.data.k,
    })
    const d = await fn(fakeEvent<{ type: string; data: { k: string } }>(1n, { k: 'alice' }))
    assert.deepEqual(d, { partitionKey: 'alice' })
  })

  void test("never emits 'ignore'", async () => {
    // The sugar layer always produces a partition; filtering belongs
    // to the raw routeFn escape hatch (option (c)).
    const seq = await routingFnForPartitionBy({ kind: 'sequential' })(fakeEvent(1n))
    const pe = await routingFnForPartitionBy({ kind: 'per-event' })(fakeEvent(1n))
    const pk = await routingFnForPartitionBy<{ type: string; data: { k: string } }>({
      kind: 'per-key',
      key: () => 'x',
    })(fakeEvent<{ type: string; data: { k: string } }>(1n, { k: 'x' }))
    assert.notEqual(seq, 'ignore')
    assert.notEqual(pe, 'ignore')
    assert.notEqual(pk, 'ignore')
  })
})

// ============================================================================
// PartitionBy modes end-to-end (routing + processing)
// ============================================================================

void describe('projection worker — sequential mode', () => {
  void test('strict-sequential delivery; one partition; serial regardless of worker count', async () => {
    const name = `proj-seq-${randomUUID().slice(0, 8)}`
    const { stream, ens } = await append('seq', 5)

    const handled: bigint[] = []
    const def: ProjectionWorkerDefinition = {
      name,
      handler: async (event) => {
        handled.push(event.event_number)
        await new Promise((r) => setTimeout(r, 5))
      },
    }
    const wired = startPair(stream, def, routingFnForPartitionBy({ kind: 'sequential' }), {
      processingCount: 3,
    })

    try {
      await waitFor(async () => (handled.length >= 5 ? true : null))
      assert.deepEqual(handled, ens)
      // Handler-end -> complete-callback-DELETE is async; wait for the
      // DELETEs to land before asserting on the table state.
      await waitFor(async () => ((await listWorkItems(name)).length === 0 ? true : null))
      assert.deepEqual(await listWorkItems(name), [])
    } finally {
      await wired.closeAll()
    }
  })

  void test("all routed work items use the synthetic '_default' partition key", async () => {
    const name = `proj-seq-pk-${randomUUID().slice(0, 8)}`
    const { stream } = await append('seq-pk', 3)

    // Block handlers so we can observe routed rows before they're deleted.
    let release!: () => void
    const block = new Promise<void>((r) => {
      release = r
    })
    const def: ProjectionWorkerDefinition = {
      name,
      handler: async () => {
        await block
      },
    }
    const wired = startPair(stream, def, routingFnForPartitionBy({ kind: 'sequential' }))

    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.length >= 3 ? rows : null
      })
      const rows = await listWorkItems(name)
      assert.ok(rows.length === 3, `expected 3 rows; got ${rows.length}`)
      for (const r of rows) {
        assert.equal(r.partition_key, SEQUENTIAL_PARTITION_KEY)
      }
    } finally {
      release()
      await wired.closeAll()
    }
  })
})

void describe('projection worker — per-event mode', () => {
  void test('each event becomes its own partition; full parallelism across workers', async () => {
    const name = `proj-pe-${randomUUID().slice(0, 8)}`
    const N = 6
    const { stream, ens } = await append('pe', N)

    const starts = new Map<bigint, number>()
    const finishes = new Map<bigint, number>()
    const def: ProjectionWorkerDefinition = {
      name,
      handler: async (event) => {
        starts.set(event.event_number, Date.now())
        await new Promise((r) => setTimeout(r, 100))
        finishes.set(event.event_number, Date.now())
      },
    }
    const wired = startPair(stream, def, routingFnForPartitionBy({ kind: 'per-event' }), {
      processingCount: N,
    })

    try {
      await waitFor(async () => (finishes.size >= N ? true : null), 10_000)
      // partition_key == String(event_number) for each; with N workers
      // and disjoint partitions, total wall-clock should be well under
      // the serial bound (N * 100ms).
      const t0 = Math.min(...starts.values())
      const t1 = Math.max(...finishes.values())
      // Some slack against test-machine load: 0.9 of the serial bound
      // is enough to demonstrate parallelism (one fully-serial run
      // would be N*100ms; we want clearly under that). Stronger
      // overlap assertions live in the timing log if needed.
      assert.ok(
        t1 - t0 < N * 100 * 0.9,
        `expected parallel run; wall=${t1 - t0}ms, serial bound=${N * 100}ms`,
      )
      // The handler-end -> complete-callback-DELETE step is async;
      // `finishes.size >= N` only proves the handlers returned. Wait
      // for the DELETEs to land before asserting on the table state.
      await waitFor(async () => ((await listWorkItems(name)).length === 0 ? true : null))
      assert.deepEqual(await listWorkItems(name), [])
      // sanity: starts cover exactly the routed ens
      assert.deepEqual(
        [...starts.keys()].sort((a, b) => Number(a - b)),
        ens,
      )
    } finally {
      await wired.closeAll()
    }
  })
})

void describe('projection worker — per-key mode', () => {
  void test('parallel across keys, serial within a key', async () => {
    const name = `proj-pk-${randomUUID().slice(0, 8)}`
    // 4 events: 2 for key 'a', 2 for key 'b', interleaved.
    const stream = `pk-${randomUUID().slice(0, 8)}`
    const rows = await client.appendToStream<{ k: string; i: number }>(stream, expected.any, [
      { type: 'E', data: { k: 'a', i: 0 } },
      { type: 'E', data: { k: 'b', i: 0 } },
      { type: 'E', data: { k: 'a', i: 1 } },
      { type: 'E', data: { k: 'b', i: 1 } },
    ])
    const ens = rows.map((r) => r.event_number)

    const events: Array<{ k: string; en: bigint; t0: number; t1: number }> = []
    const def: ProjectionWorkerDefinition<{ type: string; data: { k: string; i: number } }> = {
      name,
      handler: async (event, ctx: ProjectionHandlerContext) => {
        const t0 = Date.now()
        await new Promise((r) => setTimeout(r, 100))
        events.push({
          k: ctx.partitionKey,
          en: event.event_number,
          t0,
          t1: Date.now(),
        })
      },
    }
    const wired = startPair<{ type: string; data: { k: string; i: number } }>(
      stream,
      def,
      routingFnForPartitionBy<{ type: string; data: { k: string; i: number } }>({
        kind: 'per-key',
        key: (e) => e.data.k,
      }),
      { processingCount: 4 },
    )

    try {
      await waitFor(async () => (events.length >= 4 ? true : null), 10_000)

      // Serial within key: 'a's events in event_number order.
      const aEns = events.filter((e) => e.k === 'a').map((e) => e.en)
      const bEns = events.filter((e) => e.k === 'b').map((e) => e.en)
      assert.deepEqual(aEns, [ens[0], ens[2]])
      assert.deepEqual(bEns, [ens[1], ens[3]])

      // Parallel across keys: the second 'a' and the first 'b'
      // overlap in time, OR each key's two events run in parallel
      // with the other key's events. A practical, stable check: the
      // total wall-clock is closer to 2*100ms (per-key serial, both
      // keys in parallel) than to 4*100ms (everything serial).
      const t0 = Math.min(...events.map((e) => e.t0))
      const t1 = Math.max(...events.map((e) => e.t1))
      // Per-key serial within key (100ms x 2 = 200ms) + per-key
      // parallelism across keys means a perfect run completes in
      // ~200ms; serial-everything would be 400ms. Threshold leaves
      // headroom for test-machine load while still failing on the
      // fully-serial regression.
      assert.ok(t1 - t0 < 380, `expected per-key parallelism; wall=${t1 - t0}ms`)
      // Handler-end -> complete-callback-DELETE is async.
      await waitFor(async () => ((await listWorkItems(name)).length === 0 ? true : null))
      assert.deepEqual(await listWorkItems(name), [])
    } finally {
      await wired.closeAll()
    }
  })
})

// ============================================================================
// PRJ-E immediate-delete; D-0016 handler-opacity invariants
// ============================================================================

void describe('projection worker — PRJ-E immediate-delete', () => {
  void test('no `done` row ever persists for a projection', async () => {
    const name = `proj-del-${randomUUID().slice(0, 8)}`
    const { stream } = await append('del', 4)

    const seenStatesDuringHandle: string[] = []
    const def: ProjectionWorkerDefinition = {
      name,
      handler: async (_event, ctx) => {
        // Snapshot all rows for this subscription so we can prove no
        // `done` state ever appears (rows are either claimed-by-us or
        // pending for the queue tail).
        const r = await pool.query<{ state: string }>(
          `SELECT state FROM instructed.subscription_work_items
            WHERE subscription_name = $1`,
          [ctx.workerId.startsWith('force') ? '_' : name],
        )
        for (const row of r.rows) seenStatesDuringHandle.push(row.state)
      },
    }
    const wired = startPair(stream, def, routingFnForPartitionBy({ kind: 'sequential' }))

    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.length === 0 ? true : null
      })
      // No row ever observed in state 'done'.
      assert.ok(
        seenStatesDuringHandle.every((s) => s !== 'done'),
        `expected no 'done' rows; saw states=${seenStatesDuringHandle.join(',')}`,
      )
      // Final state: empty.
      assert.deepEqual(await listWorkItems(name), [])
    } finally {
      await wired.closeAll()
    }
  })

  void test('handler throw leaves the work item `claimed`; DELETE never runs without a successful handler', async () => {
    const name = `proj-throw-${randomUUID().slice(0, 8)}`
    const { stream, ens } = await append('th', 1)
    const [e1] = ens

    let attempts = 0
    const def: ProjectionWorkerDefinition = {
      name,
      handler: async () => {
        attempts += 1
        throw new Error('handler-fails-once')
      },
      // Tight retry-in so we observe multiple attempts quickly.
      errorPolicy: () => ({
        decision: { kind: 'retry-in', delayMs: 30 },
        state: undefined,
      }),
    }
    const wired = startPair(stream, def, routingFnForPartitionBy({ kind: 'sequential' }))

    try {
      // Wait for at least 3 attempts; throughout that time the row
      // must NOT be deleted.
      await waitFor(async () => (attempts >= 3 ? true : null))
      const s = await workItemState(name, SEQUENTIAL_PARTITION_KEY, e1)
      assert.ok(s, 'work item must still exist after handler throws')
      assert.equal(s.state, 'claimed')
      assert.notEqual(s.state, 'done')
    } finally {
      await wired.closeAll()
    }
  })
})

void describe('projection worker — D-0016 handler opacity', () => {
  void test('ProjectionHandlerContext exposes no tx / Queryable', async () => {
    // Compile-time guard re-checked at runtime: only the documented
    // fields are present. This protects against future regressions
    // where someone adds a `tx` field thinking it harmless.
    const name = `proj-ctx-${randomUUID().slice(0, 8)}`
    const { stream } = await append('ctx', 1)

    let captured: ProjectionHandlerContext | null = null
    const def: ProjectionWorkerDefinition = {
      name,
      handler: async (_event, ctx) => {
        captured = ctx
      },
    }
    const wired = startPair(stream, def, routingFnForPartitionBy({ kind: 'sequential' }))

    try {
      await waitFor(async () => (captured ? true : null))
      const ctx = captured!
      assert.deepEqual(Object.keys(ctx).sort(), [
        'attempt',
        'eventNumber',
        'logger',
        'partitionKey',
        'signal',
        'workerId',
      ])
      // Belt and braces: explicit field-absence checks.
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- test: inspect the handler context as a bag to assert field absence.
      const bag = ctx as unknown as Record<string, unknown>
      assert.equal(bag.tx, undefined)
      assert.equal(bag.client, undefined)
      assert.equal(bag.query, undefined)
    } finally {
      await wired.closeAll()
    }
  })
})
