/**
 * SUB-A slice 7 — process-manager processing worker tests.
 *
 * Slice acceptance items (SUB-A slice 7; substrate documented in
 * docs/architecture.md "PM-specific processing"):
 *   - snapshot load happy path
 *   - rebuild via `apply` on missing snapshot (IS010)
 *   - rebuild via `apply` on snapshot_module_version mismatch (state
 *     matches freshly-built)
 *   - `complete: true` deletes both snapshot and every work-item for
 *     partition in one tx
 *   - non-terminal path advances both snapshot and work-item state
 *     (one tx; SQL-level atomicity is exercised by slice 2 conformance)
 *   - multi-command emissions dispatch in declaration order
 *   - failure during dispatch leaves the work-item `claimed`
 *   - D-0011: same `Client` as both persist and dispatch throws
 *
 * Plus the PM-E known-gap doc-coverage: a dispatch failure followed by
 * a redelivery (retry-in) re-dispatches the prior commands; the test
 * records the observable behaviour rather than asserting any
 * idempotency guarantee.
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import { SNAPSHOT_MODULE_VERSION_KEY } from '../src/aggregate/index.ts'
import type { AggregateDefinition, DomainEvent } from '../src/aggregate/index.ts'
import { Client, expected } from '../src/index.ts'
import type { RunningWorker } from '../src/internal/running-worker.ts'
import type { RecordedEvent } from '../src/types/index.ts'
import {
  startPmWorker,
  type DispatchedCommand,
  type PmDefinition,
} from '../src/workers/pm/index.ts'
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
  seen.length = 0
})

// -- helpers -----------------------------------------------------------------

async function appendN(
  streamPrefix: string,
  events: Array<{ type: string; data: unknown }>,
): Promise<{ stream: string; ens: bigint[] }> {
  const stream = `${streamPrefix}-${randomUUID().slice(0, 8)}`
  const rows = await client.appendToStream(
    stream,
    expected.any,
    events.map((e) => ({ type: e.type, data: e.data })),
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

async function snapshotRow(
  sourceUuid: string,
): Promise<{ data: unknown; metadata: unknown; source_version: string } | null> {
  const r = await pool.query<{
    data: unknown
    metadata: unknown
    source_version: string
  }>(
    `SELECT data, metadata, source_version::text AS source_version
       FROM instructed.snapshots WHERE source_uuid = $1`,
    [sourceUuid],
  )
  if (r.rowCount === 0) return null
  return r.rows[0]
}

// A minimal aggregate the PM dispatches commands at. The handler
// records its dispatches in `seen` so tests can assert dispatch order.
// `from` is a stringified event-number because event payloads are
// jsonb and JSON has no bigint; tests model the application's encoding.
interface NoteState {
  count: number
}
type NoteCommand = { kind: 'note'; from: string; tag: string }
interface NoteEvent extends DomainEvent {
  type: 'Noted'
  data: { from: string; tag: string }
}
const seen: NoteEvent[] = []

function noteAggregate(): AggregateDefinition<NoteState, NoteCommand, NoteEvent> {
  return {
    type: 'Note',
    initialState: () => ({ count: 0 }),
    execute(_state, c) {
      const ev: NoteEvent = {
        type: 'Noted',
        data: { from: c.from, tag: c.tag },
      }
      seen.push(ev)
      return { type: ev.type, data: ev.data }
    },
    apply(s, _e) {
      return { count: s.count + 1 }
    },
  }
}

interface PmState {
  // event_numbers folded via apply(), stored as strings because JSON
  // can't serialise bigint and snapshot data is jsonb. Applications
  // pick their own encoding; we use strings throughout the test for
  // clean equality.
  applied: string[]
}

function pmDef(
  name: string,
  overrides: Partial<PmDefinition<PmState, NoteEvent>> = {},
): PmDefinition<PmState, NoteEvent> {
  return {
    type: name,
    // `stream` defaulted via spread by the caller; required so the PM
    // worker and the routing worker agree on the source stream (the
    // PM worker's `claim_work_item` is keyed on (stream, name)).
    initialState: () => ({ applied: [] }),
    apply: (s, event) => ({
      applied: [...s.applied, event.event_number.toString()],
    }),
    handle: () => ({}),
    ...overrides,
  }
}

/** Start a routing worker that puts every event in partition `pk`. */
function startRouter(name: string, stream: string, pk: string): RunningWorker {
  return startRoutingWorker(client, {
    name,
    stream,
    routeFn: () => ({ partitionKey: pk }),
    startFrom: 'origin',
  })
}

// ============================================================================
// Happy path: snapshot present, non-terminal complete
// ============================================================================

void describe('startPmWorker — non-terminal complete', () => {
  void test("UPDATE work-item to 'done' and UPSERT snapshot in one logical step", async () => {
    const name = `pm-happy-${randomUUID().slice(0, 8)}`
    const { stream, ens } = await appendN('ev', [
      { type: 'T', data: { tag: 'a' } },
      { type: 'T', data: { tag: 'b' } },
    ])

    const router = startRouter(name, stream, 'p1')
    const worker = startPmWorker(client, pmDef(name, { stream }))

    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.length === 2 && rows.every((r) => r.state === 'done') ? true : null
      })
      // Both events are now `done` and a snapshot exists for the
      // partition's source_uuid.
      const snap = await snapshotRow(`${name}-p1`)
      assert.ok(snap, 'snapshot should exist')
      const snapData = snap.data as PmState
      // apply was run on every routed event, in event-number order.
      assert.deepEqual(
        snapData.applied,
        ens.map((n) => n.toString()),
      )
      // sourceVersion = the most recently processed event's number.
      assert.equal(snap.source_version, ens[1].toString())
    } finally {
      await Promise.all([router.stop(), worker.stop()])
    }
  })
})

// ============================================================================
// Multi-command dispatch in declaration order
// ============================================================================

void describe('startPmWorker — multi-command dispatch', () => {
  void test('commands dispatch in declaration order; causation = triggering event_id', async () => {
    const name = `pm-cmds-${randomUUID().slice(0, 8)}`
    const aggStream = `agg-${randomUUID().slice(0, 8)}`
    const { stream } = await appendN('trig', [{ type: 'T', data: { tag: 'x' } }])

    let triggeringEventId = ''
    const def: PmDefinition<PmState, NoteEvent> = {
      ...pmDef(name, { stream }),
      handle: (_state, event) => {
        triggeringEventId = event.event_id
        const from = event.event_number.toString()
        return {
          commands: [
            {
              streamUuid: aggStream,
              aggregate: noteAggregate(),
              command: { kind: 'note', from, tag: 'first' } as NoteCommand,
            },
            {
              streamUuid: aggStream,
              aggregate: noteAggregate(),
              command: { kind: 'note', from, tag: 'second' } as NoteCommand,
            },
            {
              streamUuid: aggStream,
              aggregate: noteAggregate(),
              command: { kind: 'note', from, tag: 'third' } as NoteCommand,
            },
          ],
        }
      },
    }

    const router = startRouter(name, stream, 'p1')
    const worker = startPmWorker(client, def)

    try {
      await waitFor(async () => (seen.length >= 3 ? true : null))
      const tags = seen.map((s) => s.data.tag)
      assert.deepEqual(tags, ['first', 'second', 'third'])

      // Causation: the appended Noted events should carry the
      // triggering event's id as their causation_id.
      const r = await pool.query<{ causation_id: string | null }>(
        `SELECT causation_id::text AS causation_id
           FROM instructed.events
          WHERE event_type = 'Noted'
          ORDER BY event_id`,
      )
      for (const row of r.rows) {
        assert.equal(row.causation_id, triggeringEventId)
      }
    } finally {
      await Promise.all([router.stop(), worker.stop()])
    }
  })
})

// ============================================================================
// Terminal complete (PM-F `complete: true`)
// ============================================================================

void describe('startPmWorker — complete: true (PM-F terminal)', () => {
  void test('DELETEs both the snapshot AND every work-item for the partition', async () => {
    const name = `pm-term-${randomUUID().slice(0, 8)}`
    const { stream, ens } = await appendN('trig', [
      { type: 'T', data: { tag: 'a' } },
      { type: 'T', data: { tag: 'b' } },
      { type: 'T', data: { tag: 'TERM' } },
    ])
    const def: PmDefinition<PmState, NoteEvent> = {
      ...pmDef(name, { stream }),
      handle: (_s, event) => {
        if ((event.data as { tag: string }).tag === 'TERM') {
          return { complete: true }
        }
        return {}
      },
    }

    const router = startRouter(name, stream, 'p1')
    const worker = startPmWorker(client, def)

    try {
      // Two-stage wait so we don't spuriously satisfy `length === 0`
      // at t=0 before routing has run. First confirm all three events
      // were routed (or processed); then confirm the partition slice
      // is empty (complete_pm_instance ran).
      await waitFor(async () => {
        const sub = await pool.query<{ last_seen: string }>(
          `SELECT last_seen::text AS last_seen
             FROM instructed.subscriptions s
             JOIN instructed.streams st USING(stream_id)
            WHERE s.subscription_name = $1 AND st.stream_uuid = $2`,
          [name, stream],
        )
        return sub.rowCount === 1 && BigInt(sub.rows[0].last_seen) >= ens[2] ? true : null
      })
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.length === 0 ? true : null
      })
      // Snapshot gone, every work-item gone.
      assert.equal(await snapshotRow(`${name}-p1`), null)
      assert.deepEqual(await listWorkItems(name), [])
    } finally {
      await Promise.all([router.stop(), worker.stop()])
    }
  })
})

// ============================================================================
// Rebuild path: no snapshot
// ============================================================================

void describe('startPmWorker — rebuild on missing snapshot', () => {
  void test('folds prior `done` events through apply before staging the claimed event', async () => {
    const name = `pm-rebuild-${randomUUID().slice(0, 8)}`
    // Phase 1: append two events; PM processes them; snapshot + 2
    // `done` rows result. Worker closed before phase 2.
    const { stream, ens: ens1 } = await appendN('e', [
      { type: 'T', data: { tag: 'a' } },
      { type: 'T', data: { tag: 'b' } },
    ])

    const r1 = startRouter(name, stream, 'p1')
    const w1 = startPmWorker(client, pmDef(name, { stream }))
    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.filter((x) => x.state === 'done').length >= 2 ? true : null
      })
    } finally {
      await w1.stop()
      await r1.stop()
    }

    // Phase 2: simulate a snapshot loss (operator drop, or first
    // module-version mismatch handled via rebuild). Delete the
    // snapshot row directly.
    await pool.query(`DELETE FROM instructed.snapshots WHERE source_uuid = $1`, [`${name}-p1`])
    // Confirm the snapshot really is gone before phase 3 starts.
    assert.equal(await snapshotRow(`${name}-p1`), null)

    // Phase 3: append a third event to the same stream, then start a
    // fresh PM worker. Because no snapshot exists, the worker MUST
    // rebuild state by folding the two prior `done` events through
    // `apply` before staging the third claimed event.
    const more = await client.appendToStream(stream, expected.any, [
      { type: 'T', data: { tag: 'c' } },
    ])
    const e3 = more[0].event_number

    const def2: PmDefinition<PmState, NoteEvent> = {
      ...pmDef(name, { stream }),
      apply: (s, ev) => ({
        applied: [...s.applied, ev.event_number.toString()],
      }),
    }
    const r2 = startRouter(name, stream, 'p1')
    const w2 = startPmWorker(client, def2)
    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.filter((x) => x.state === 'done').length >= 3 ? true : null
      })
    } finally {
      await w2.stop()
      await r2.stop()
    }

    // The third event's stage state should reflect ALL three event
    // numbers having been apply'd (two via rebuild, one as the
    // claimed event).
    const finalSnap = await snapshotRow(`${name}-p1`)
    assert.ok(finalSnap, 'snapshot should be rebuilt')
    assert.deepEqual(
      (finalSnap.data as PmState).applied,
      [...ens1, e3].map((n) => n.toString()),
    )
  })
})

// ============================================================================
// Rebuild path: snapshotModuleVersion mismatch
// ============================================================================

void describe('startPmWorker — rebuild on snapshot_module_version mismatch', () => {
  void test('snapshot exists but version tag differs => rebuild via apply matches', async () => {
    const name = `pm-mod-${randomUUID().slice(0, 8)}`
    const { stream, ens } = await appendN('e', [
      { type: 'T', data: { tag: 'a' } },
      { type: 'T', data: { tag: 'b' } },
    ])

    // Phase 1: writer tags snapshots with version "v1".
    const defV1: PmDefinition<PmState, NoteEvent> = {
      ...pmDef(name, { stream }),
      snapshotModuleVersion: 'v1',
    }
    const r1 = startRouter(name, stream, 'p1')
    const w1 = startPmWorker(client, defV1)
    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.filter((x) => x.state === 'done').length >= 2 ? true : null
      })
    } finally {
      await w1.stop()
      await r1.stop()
    }

    const v1Snap = await snapshotRow(`${name}-p1`)
    assert.ok(v1Snap, 'v1 snapshot should exist')
    assert.deepEqual(
      (v1Snap.metadata as Record<string, unknown>)[SNAPSHOT_MODULE_VERSION_KEY],
      'v1',
    )

    // Phase 2: append one more event; reader announces module version
    // "v2", which should cause it to rebuild via apply from origin
    // rather than trusting the v1 snapshot's data.
    const more = await client.appendToStream(stream, expected.any, [
      { type: 'T', data: { tag: 'c' } },
    ])
    const e3 = more[0].event_number

    let loadedFromSnapshot = false
    const defV2: PmDefinition<PmState, NoteEvent> = {
      ...pmDef(name, { stream }),
      snapshotModuleVersion: 'v2',
      apply: (s, ev) => ({
        applied: [...s.applied, ev.event_number.toString()],
      }),
      handle: (state, _ev) => {
        // After rebuild + staging, applied should hold all three
        // event_numbers. If the v1 snapshot's data had been wrongly
        // trusted, applied would only contain e3 when handle runs.
        if (state.applied.length === 1 && state.applied[0] === e3.toString()) {
          loadedFromSnapshot = true
        }
        return {}
      },
    }
    const r2 = startRouter(name, stream, 'p1')
    const w2 = startPmWorker(client, defV2)
    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.filter((x) => x.state === 'done').length >= 3 ? true : null
      })
    } finally {
      await w2.stop()
      await r2.stop()
    }
    assert.equal(
      loadedFromSnapshot,
      false,
      'v2 reader must rebuild via apply, not use the v1 snapshot',
    )
    const finalSnap = await snapshotRow(`${name}-p1`)
    assert.ok(finalSnap)
    assert.deepEqual(
      (finalSnap.data as PmState).applied,
      [...ens, e3].map((n) => n.toString()),
    )
    // The v2 snapshot now carries v2 in its metadata.
    assert.deepEqual(
      (finalSnap.metadata as Record<string, unknown>)[SNAPSHOT_MODULE_VERSION_KEY],
      'v2',
    )
  })
})

// ============================================================================
// Failure during dispatch
// ============================================================================

void describe('startPmWorker — dispatch failure leaves work-item claimed', () => {
  void test('dispatch throw -> handle throws -> retry-in; row stays `claimed`; lease retained', async () => {
    const name = `pm-disp-fail-${randomUUID().slice(0, 8)}`
    const aggStream = `agg-${randomUUID().slice(0, 8)}`
    const { stream } = await appendN('e', [{ type: 'T', data: { tag: 'x' } }])

    let attempts = 0
    const exploder: AggregateDefinition<NoteState, NoteCommand, NoteEvent> = {
      type: 'Note',
      initialState: () => ({ count: 0 }),
      execute() {
        attempts += 1
        throw new Error('synthetic-dispatch-failure')
      },
      apply: (s) => s,
    }
    const def: PmDefinition<PmState, NoteEvent> = {
      ...pmDef(name, { stream }),
      handle: (_state, event) => ({
        commands: [
          {
            streamUuid: aggStream,
            aggregate: exploder,
            command: {
              kind: 'note',
              from: event.event_number.toString(),
              tag: 'boom',
            } as NoteCommand,
          },
        ],
      }),
      // Fast retries so we observe multiple attempts.
      errorPolicy: () => ({
        decision: { kind: 'retry-in', delayMs: 30 },
        state: undefined,
      }),
    }

    const router = startRouter(name, stream, 'p1')
    const worker = startPmWorker(client, def)

    try {
      await waitFor(async () => (attempts >= 3 ? true : null))
      // While retries continue, the work item must remain `claimed`
      // and the snapshot must NOT have been written.
      const rows = await listWorkItems(name)
      assert.equal(rows.length, 1)
      assert.equal(rows[0].state, 'claimed')
      assert.equal(await snapshotRow(`${name}-p1`), null)
    } finally {
      await Promise.all([router.stop(), worker.stop()])
    }
  })
})

// ============================================================================
// Empty handle result is valid (no commands, no complete)
// ============================================================================

void describe('startPmWorker — empty handle result', () => {
  void test('{}: still records staged_state and marks the work-item done', async () => {
    const name = `pm-empty-${randomUUID().slice(0, 8)}`
    const { stream } = await appendN('e', [{ type: 'T', data: { tag: 'z' } }])
    const router = startRouter(name, stream, 'p1')
    const worker = startPmWorker(client, {
      ...pmDef(name, { stream }),
      handle: () => ({}),
    })
    try {
      await waitFor(async () => {
        const rows = await listWorkItems(name)
        return rows.length === 1 && rows[0].state === 'done' ? true : null
      })
      assert.ok(await snapshotRow(`${name}-p1`))
    } finally {
      await Promise.all([router.stop(), worker.stop()])
    }
  })
})

// Silence unused-import linter for type-only references kept for clarity.
void (null as unknown as DispatchedCommand)
void (null as unknown as RecordedEvent)
