/**
 * Layer 1: aggregate runner tests.
 *
 * Focus: the OCC-retry contract (D-0005 / mapping.md AGG-010 / §3 layer 1).
 * Two concurrent writers race; the loser sees IS001 internally and retries
 * with reloaded state, then succeeds. Causation / correlation defaulting
 * (§11.8), snapshot policy (§6), and the no-op short-circuit are covered
 * alongside.
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import {
  Client,
  everyN,
  expected,
  RetryBudgetExhausted,
  runCommand,
  runCommandWithSnapshots,
  WrongExpectedVersion,
} from '../src/index.ts'
import type { AggregateDefinition, DomainEvent } from '../src/index.ts'
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

// --- a minimal counter aggregate -------------------------------------------

interface CounterState {
  value: number
}
type CounterCommand = { kind: 'add'; n: number } | { kind: 'noop' }
interface CounterEvent extends DomainEvent {
  type: 'Added' | 'Seed'
  data: { n: number } | Record<string, never>
}

function counter(): AggregateDefinition<CounterState, CounterCommand, CounterEvent> {
  return {
    type: 'Counter',
    initialState: () => ({ value: 0 }),
    execute(state, command) {
      if (command.kind === 'noop') return []
      return {
        type: 'Added',
        data: { n: command.n },
      }
    },
    apply(state, event) {
      if (event.type === 'Added') {
        const n = (event.data as { n: number }).n
        return { value: state.value + n }
      }
      return state
    },
  }
}

// Seed a stream at version 1 with an event the aggregate ignores. Establishes
// the stream so the OCC tests don't race the no-stream/create path.
async function seed(streamUuid: string): Promise<void> {
  await client.appendToStream(streamUuid, expected.noStream, [{ type: 'Seed', data: {} }])
}

// ---------------------------------------------------------------------------

void describe('runCommand — happy path', () => {
  void test('appends an event, returns AppendedEvent rows', async () => {
    const s = randomUUID()
    await seed(s)
    const rows = await runCommand(client, counter(), s, { kind: 'add', n: 3 })
    assert.equal(rows.length, 1)
    assert.equal(rows[0].stream_version, 2n)
  })

  void test('no-op (execute returns []) does not append and returns []', async () => {
    const s = randomUUID()
    await seed(s)
    const rows = await runCommand(client, counter(), s, { kind: 'noop' })
    assert.deepEqual(rows, [])
    const events = await client.readStream(s, 0n, 10)
    assert.equal(events.length, 1) // only the Seed event
  })

  void test('loads state from prior events and applies command on top', async () => {
    const s = randomUUID()
    await seed(s)
    await runCommand(client, counter(), s, { kind: 'add', n: 2 })
    await runCommand(client, counter(), s, { kind: 'add', n: 5 })
    const events = await client.readStream(s, 0n, 10)
    assert.equal(events.length, 3) // Seed + 2 Added
    assert.equal(events[2].stream_version, 3n)
    assert.deepEqual(events[2].data, { n: 5 })
  })

  void test('fills causation_id with commandId; correlation_id from opts (§11.8)', async () => {
    const s = randomUUID()
    await seed(s)
    const cmdId = randomUUID()
    const corrId = randomUUID()
    await runCommand(
      client,
      counter(),
      s,
      { kind: 'add', n: 1 },
      {
        commandId: cmdId,
        correlationId: corrId,
      },
    )
    const events = await client.readStream(s, 0n, 10)
    const added = events[1]
    assert.equal(added.causation_id, cmdId)
    assert.equal(added.correlation_id, corrId)
  })

  void test('respects explicit causation_id / correlation_id on a NewEvent', async () => {
    const s = randomUUID()
    await seed(s)
    const explicitCausation = randomUUID()
    const explicitCorrelation = randomUUID()
    const def: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      execute(_state, _cmd) {
        return {
          type: 'Added',
          data: { n: 1 },
          causation_id: explicitCausation,
          correlation_id: explicitCorrelation,
        }
      },
    }
    await runCommand(
      client,
      def,
      s,
      { kind: 'add', n: 1 },
      {
        // SDK defaults should NOT override explicit values.
        correlationId: randomUUID(),
      },
    )
    const events = await client.readStream(s, 0n, 10)
    assert.equal(events[1].causation_id, explicitCausation)
    assert.equal(events[1].correlation_id, explicitCorrelation)
  })

  void test('apply receives DomainEvent shape, not RecordedEvent (§11.3)', async () => {
    const s = randomUUID()
    await seed(s)
    await runCommand(client, counter(), s, { kind: 'add', n: 7 })

    // Reload via a fresh aggregate that asserts the shape its apply sees.
    const observed: any[] = []
    const probe: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      apply(state, event) {
        observed.push(event)
        return counter().apply(state, event)
      },
    }
    await runCommand(client, probe, s, { kind: 'add', n: 1 })

    // Two events folded (Seed + Added). Each carries only {type,data,metadata}.
    assert.equal(observed.length, 2)
    for (const e of observed) {
      assert.deepEqual(Object.keys(e).sort(), ['data', 'metadata', 'type'])
    }
    assert.equal(observed[0].type, 'Seed')
    assert.equal(observed[1].type, 'Added')
    assert.deepEqual(observed[1].data, { n: 7 })
  })
})

void describe('runCommand — OCC retry (D-0005 / AGG-010)', () => {
  void test('two concurrent writers: one wins, the other retries and succeeds', async () => {
    const s = randomUUID()
    await seed(s)

    // Two concurrent commands at the SDK's default retryBudget (5).
    // Both load the stream at version 1, both compute exact(1). One
    // append wins; the other gets IS001, the SDK reloads, re-executes,
    // re-appends at exact(2), and succeeds.
    const [a, b] = await Promise.all([
      runCommand(client, counter(), s, { kind: 'add', n: 10 }),
      runCommand(client, counter(), s, { kind: 'add', n: 20 }),
    ])

    // Both commands succeeded.
    assert.equal(a.length, 1)
    assert.equal(b.length, 1)

    // The stream now has Seed (v1) + two Added events (v2, v3).
    const events = await client.readStream(s, 0n, 10)
    assert.equal(events.length, 3)
    assert.equal(events[0].type, 'Seed')
    assert.equal(events[1].type, 'Added')
    assert.equal(events[1].stream_version, 2n)
    assert.equal(events[2].type, 'Added')
    assert.equal(events[2].stream_version, 3n)

    // Aggregate state derived by re-loading is the sum, regardless of
    // who won — proves the loser re-folded the winner's event before
    // computing its own.
    const loaded = await loadFresh(s)
    assert.equal(loaded.value, 30)
  })

  void test('retryBudget: 0 surfaces RetryBudgetExhausted under contention', async () => {
    const s = randomUUID()
    await seed(s)

    // Spawn enough concurrent commands with retryBudget: 0 that at least
    // one MUST race a peer for the same version slot and lose. With one
    // append slot per stream version and no retries, every loser must
    // surface RetryBudgetExhausted carrying a WrongExpectedVersion.
    const N = 8
    const results = await Promise.allSettled(
      Array.from({ length: N }, () =>
        runCommand(
          client,
          counter(),
          s,
          { kind: 'add', n: 1 },
          {
            retryBudget: 0,
          },
        ),
      ),
    )
    const rejected = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected')
    assert.ok(
      rejected.length >= 1,
      `expected at least one RetryBudgetExhausted; all ${N} succeeded`,
    )
    for (const r of rejected) {
      assert.ok(
        r.reason instanceof RetryBudgetExhausted,
        `expected RetryBudgetExhausted, got ${(r.reason as Error)?.constructor?.name}`,
      )
      const re = r.reason as RetryBudgetExhausted
      assert.equal(re.attempts, 1)
      assert.ok(re.lastError instanceof WrongExpectedVersion)
    }
  })

  void test('explicit expectedVersion disables retry (D-0019)', async () => {
    const s = randomUUID()
    await seed(s)
    // Stream is at version 1. Ask the SDK to assert version 99.
    // It should NOT retry; the WrongExpectedVersion surfaces directly.
    await assert.rejects(
      () =>
        runCommand(
          client,
          counter(),
          s,
          { kind: 'add', n: 1 },
          {
            expectedVersion: expected.exact(99n),
            retryBudget: 5,
          },
        ),
      (err) => err instanceof WrongExpectedVersion && err.code === 'IS001',
    )
  })
})

void describe('runCommandWithSnapshots — snapshot policy (§6)', () => {
  // Snapshot orchestration moved to L3 per step-5 slice 2; the L2
  // `runCommand` no longer invokes `def.snapshotPolicy`. These tests
  // exercise the L3 wrapper, which is what `Instructed.dispatch` and
  // the PM worker delegate to.
  void test('everyN(n) writes a snapshot after the threshold is crossed', async () => {
    const s = randomUUID()
    await seed(s)
    const def: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      // Snapshot once we've seen ≥ 2 events since the last snapshot
      // (so the first command, which puts the stream at Seed+Added=2,
      // crosses the threshold).
      snapshotPolicy: everyN(2),
    }
    await runCommandWithSnapshots(client, def, s, { kind: 'add', n: 4 })
    const snap = await client.readSnapshot<CounterState>(s)
    assert.equal(snap.sourceType, 'Counter')
    assert.equal(snap.sourceVersion, 2n)
    assert.deepEqual(snap.data, { value: 4 })
  })

  void test('subsequent loads use the snapshot (no full re-fold)', async () => {
    const s = randomUUID()
    await seed(s)
    // Snapshot after every command so the second command's load hits a
    // fresh snapshot and reads zero unseen events.
    const def: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotPolicy: everyN(1),
    }
    await runCommandWithSnapshots(client, def, s, { kind: 'add', n: 3 })

    // Probe: count apply() calls on the second run. If the snapshot is
    // used, apply is only called once (for the just-appended Added(5)
    // folded through apply to produce the staged snapshot state). If
    // the snapshot were ignored, apply would also be called for the
    // Seed event and the first Added event = 3 total.
    let appliesSeen = 0
    const probe: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...def,
      apply(state, event) {
        appliesSeen += 1
        return counter().apply(state, event)
      },
    }
    await runCommandWithSnapshots(client, probe, s, { kind: 'add', n: 5 })
    assert.equal(appliesSeen, 1)
    const snap = await client.readSnapshot<CounterState>(s)
    assert.equal(snap.data.value, 8)
    assert.equal(snap.sourceVersion, 3n)
  })

  void test('L2 runCommand does NOT invoke snapshotPolicy', async () => {
    // Step-5 slice 2 contract: snapshot orchestration is L3, not L2.
    // A direct call to runCommand must not write a snapshot even if
    // the def declares a policy.
    const s = randomUUID()
    await seed(s)
    const def: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotPolicy: everyN(1),
    }
    await runCommand(client, def, s, { kind: 'add', n: 1 })
    await assert.rejects(
      () => client.readSnapshot<CounterState>(s),
      (err: Error) => err.name === 'SnapshotNotFound',
    )
  })
})

void describe('aggregate snapshot module versioning (SNAP-002, TODO #5)', () => {
  // Generalises the PM substrate's module-version mechanism to
  // aggregates. The metadata key (`SNAPSHOT_MODULE_VERSION_KEY`)
  // is shared between L2 aggregate and L2 PM substrate.
  // Comparison is strict: undefined matches only undefined.

  void test('matching version: snapshot is used', async () => {
    const s = randomUUID()
    await seed(s)
    const def: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotPolicy: everyN(1),
      snapshotModuleVersion: 'v1',
    }
    // First command writes a snapshot stamped with "v1".
    await runCommandWithSnapshots(client, def, s, { kind: 'add', n: 3 })
    const snap = await client.readSnapshot<CounterState>(s)
    assert.deepEqual((snap.metadata as Record<string, unknown>)['snapshot_module_version'], 'v1')

    // Second command with the same version: apply runs only for the
    // newly-appended event (1 call), not for the loaded events from
    // origin (which would be 2 calls: Seed + Added).
    let appliesSeen = 0
    const probe: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...def,
      apply(state, event) {
        appliesSeen += 1
        return counter().apply(state, event)
      },
    }
    await runCommandWithSnapshots(client, probe, s, { kind: 'add', n: 5 })
    assert.equal(appliesSeen, 1, 'snapshot should have been used')
  })

  void test('mismatched version: snapshot discarded, full replay from origin', async () => {
    const s = randomUUID()
    await seed(s)

    // Phase 1: write a snapshot with version "v1".
    const v1: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotPolicy: everyN(1),
      snapshotModuleVersion: 'v1',
    }
    await runCommandWithSnapshots(client, v1, s, { kind: 'add', n: 7 })
    // Sanity: snapshot exists with v1 stamped.
    const v1Snap = await client.readSnapshot<CounterState>(s)
    assert.equal((v1Snap.metadata as Record<string, unknown>)['snapshot_module_version'], 'v1')

    // Phase 2: a NEW def with version "v2" loads. The v1 snapshot
    // is mismatched -> discarded; the load pages all events from
    // origin (Seed + Added = 2 events).
    let appliesSeen = 0
    const v2: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotPolicy: everyN(1),
      snapshotModuleVersion: 'v2',
      apply(state, event) {
        appliesSeen += 1
        return counter().apply(state, event)
      },
    }
    await runCommandWithSnapshots(client, v2, s, { kind: 'add', n: 4 })

    // load -> 2 applies (Seed, Added(7)); the snapshot was rejected.
    // The `apply` count includes only the loaded events; the
    // post-append fold in `runCommandAndApply` adds 1 more for the
    // just-appended Added(4) = 3 total.
    assert.equal(
      appliesSeen,
      3,
      `expected full replay (2 loaded + 1 post-append fold = 3); got ${appliesSeen}`,
    )

    // Final state is correct: 0 + 7 + 4 = 11.
    const final = await client.readSnapshot<CounterState>(s)
    assert.equal(final.data.value, 11)
    // And the snapshot is now stamped with v2.
    assert.equal((final.metadata as Record<string, unknown>)['snapshot_module_version'], 'v2')
  })

  void test('strict: snapshot has version but def does not -> mismatch', async () => {
    const s = randomUUID()
    await seed(s)

    // Phase 1: write with v1.
    const versioned: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotPolicy: everyN(1),
      snapshotModuleVersion: 'v1',
    }
    await runCommandWithSnapshots(client, versioned, s, { kind: 'add', n: 1 })

    // Phase 2: def WITHOUT a version. The v1-stamped snapshot is
    // mismatched (strict semantics: "absent on one side" counts).
    let appliesSeen = 0
    const unversioned: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      apply(state, event) {
        appliesSeen += 1
        return counter().apply(state, event)
      },
    }
    await runCommand(client, unversioned, s, { kind: 'add', n: 2 })
    // Full replay: Seed + Added(1) = 2 loaded events.
    assert.equal(appliesSeen, 2, 'v1 snapshot should be rejected by unversioned def')
  })

  void test('strict: def has version but snapshot does not -> mismatch', async () => {
    const s = randomUUID()
    await seed(s)

    // Phase 1: write WITHOUT a version (no metadata stamped).
    const unversioned: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotPolicy: everyN(1),
    }
    await runCommandWithSnapshots(client, unversioned, s, { kind: 'add', n: 1 })
    const u = await client.readSnapshot<CounterState>(s)
    assert.equal(u.metadata, null, 'unversioned snapshot has no metadata')

    // Phase 2: def WITH "v1". The unversioned snapshot is mismatched.
    let appliesSeen = 0
    const versioned: AggregateDefinition<CounterState, CounterCommand, CounterEvent> = {
      ...counter(),
      snapshotModuleVersion: 'v1',
      apply(state, event) {
        appliesSeen += 1
        return counter().apply(state, event)
      },
    }
    await runCommand(client, versioned, s, { kind: 'add', n: 2 })
    // Full replay: Seed + Added(1) = 2 loaded events.
    assert.equal(appliesSeen, 2, 'unversioned snapshot should be rejected by versioned def')
  })
})

// ---- helpers --------------------------------------------------------------

async function loadFresh(streamUuid: string): Promise<CounterState> {
  // Hand-load by reading the stream and folding; mirrors runCommand's
  // load step but doesn't append. Used only by tests.
  const def = counter()
  let state = def.initialState()
  const events = await client.readStream(streamUuid, 0n, 1000)
  for (const e of events) {
    state = def.apply(state, {
      type: e.type as CounterEvent['type'],
      data: e.data as CounterEvent['data'],
      metadata: e.metadata,
    })
  }
  return state
}
