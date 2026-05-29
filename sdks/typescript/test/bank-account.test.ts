/**
 * Bank-account integration test -- standalone facade exercise.
 *
 * Slice 9 note: this test was previously coupled to the
 * `examples/bank-account/` module. The example will be migrated to
 * the SUB-A registration shapes in slice 10; for slice 9 we inline a
 * minimal copy of the relevant domain code so the SDK test suite is
 * not blocked on the example migration and so the test compiles
 * against the new `Instructed.register` /
 * `registerProcessManager` surfaces.
 *
 * Exercises the example end-to-end against the docker-compose
 * Postgres: Account + Transfer aggregates, a Balances projection,
 * and a Transfer PM (compensation-by-refusal per D-0011).
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import {
  Instructed,
  SnapshotNotFound,
  type AggregateDefinition,
  type DispatchedCommand,
  type RecordedEvent,
  type RoutingFn,
} from '../src/index.ts'
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

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((r) => setTimeout(r, 25))
  }
  throw new Error(`timeout waiting for ${label}`)
}

// ===========================================================================
// Inlined domain code (test-local copy of examples/bank-account/).
// Slice 10 migrates the example proper; this copy lives here only so the
// SDK test suite is standalone.
// ===========================================================================

// ---- Account aggregate ---------------------------------------------------

interface AccountState {
  opened: boolean
  owner: string | null
  balance: number
}

type AccountEvent =
  | { type: 'AccountOpened'; data: { owner: string } }
  | { type: 'Deposited'; data: { amount: number; transferId?: string } }
  | { type: 'Withdrawn'; data: { amount: number; transferId?: string; to?: string } }
  | {
      type: 'WithdrawalRefused'
      data: { reason: string; amount: number; transferId?: string }
    }

type AccountCommand =
  | { kind: 'Open'; owner: string }
  | { kind: 'Deposit'; amount: number; transferId?: string }
  | { kind: 'Withdraw'; amount: number; transferId?: string; to?: string }

const Account: AggregateDefinition<AccountState, AccountCommand, AccountEvent> = {
  type: 'Account',
  initialState: () => ({ opened: false, owner: null, balance: 0 }),
  apply(state, event) {
    switch (event.type) {
      case 'AccountOpened':
        return { ...state, opened: true, owner: event.data.owner }
      case 'Deposited':
        return { ...state, balance: state.balance + event.data.amount }
      case 'Withdrawn':
        return { ...state, balance: state.balance - event.data.amount }
      case 'WithdrawalRefused':
        return state
    }
  },
  execute(state, command) {
    switch (command.kind) {
      case 'Open':
        if (state.opened) throw new Error('account already open')
        return { type: 'AccountOpened', data: { owner: command.owner } }
      case 'Deposit':
        if (!state.opened) throw new Error('account not open')
        return {
          type: 'Deposited',
          data: { amount: command.amount, transferId: command.transferId },
        }
      case 'Withdraw':
        if (!state.opened) throw new Error('account not open')
        if (state.balance < command.amount) {
          return {
            type: 'WithdrawalRefused',
            data: {
              reason: 'insufficient funds',
              amount: command.amount,
              transferId: command.transferId,
            },
          }
        }
        return {
          type: 'Withdrawn',
          data: {
            amount: command.amount,
            transferId: command.transferId,
            to: command.to,
          },
        }
    }
  },
}

// ---- Transfer aggregate --------------------------------------------------

interface TransferState {
  requested: boolean
}
interface TransferRequestedData {
  from: string
  to: string
  amount: number
  transferId: string
}
type TransferEvent = { type: 'TransferRequested'; data: TransferRequestedData }
type TransferCommand = {
  kind: 'Request'
  from: string
  to: string
  amount: number
  transferId: string
}

const Transfer: AggregateDefinition<TransferState, TransferCommand, TransferEvent> = {
  type: 'Transfer',
  initialState: () => ({ requested: false }),
  apply(state, event) {
    if (event.type === 'TransferRequested') return { requested: true }
    return state
  },
  execute(state, command) {
    if (state.requested) throw new Error('transfer already requested')
    return {
      type: 'TransferRequested',
      data: {
        from: command.from,
        to: command.to,
        amount: command.amount,
        transferId: command.transferId,
      },
    }
  },
}

// ---- Balances projection (in-memory) -------------------------------------

interface BalancesView {
  balance: Map<string, number>
  lastEventByAccount: Map<string, bigint>
}

function newBalancesView(): BalancesView {
  return { balance: new Map(), lastEventByAccount: new Map() }
}

const BALANCES_TYPES = new Set(['AccountOpened', 'Deposited', 'Withdrawn'])

const balancesRouteFn: RoutingFn = (e) =>
  BALANCES_TYPES.has(e.type) ? { partitionKey: '_default' } : 'ignore'

function balancesHandler(view: BalancesView) {
  return async (event: RecordedEvent) => {
    const last = view.lastEventByAccount.get(event.stream_uuid) ?? -1n
    if (event.event_number <= last) return // idempotent
    const data = event.data as { amount?: number }
    switch (event.type) {
      case 'AccountOpened':
        if (!view.balance.has(event.stream_uuid)) {
          view.balance.set(event.stream_uuid, 0)
        }
        break
      case 'Deposited': {
        const cur = view.balance.get(event.stream_uuid) ?? 0
        view.balance.set(event.stream_uuid, cur + (data.amount ?? 0))
        break
      }
      case 'Withdrawn': {
        const cur = view.balance.get(event.stream_uuid) ?? 0
        view.balance.set(event.stream_uuid, cur - (data.amount ?? 0))
        break
      }
    }
    view.lastEventByAccount.set(event.stream_uuid, event.event_number)
  }
}

// ---- TransferProcessManager (PM-F + PM-C shape) --------------------------

// Stream-name prefix is derived from the aggregate definition's
// `streamName(id)` (default `${type}-${id}`); kept here purely for
// the PM's reverse-lookup of `from` out of the source stream's name
// when the event payload doesn't carry it.
const ACCOUNT_STREAM_PREFIX = 'Account-'

type TransferStage =
  | { stage: 'starting' }
  | { stage: 'debited'; from: string; to: string; amount: number; transferId: string }
  | { stage: 'done' }
  | { stage: 'refunded'; reason: string }

const TRANSFER_PM_NAME = 'TransferProcessManager'

function transferIdOf(event: { data: unknown }): string | null {
  const d = event.data as { transferId?: string } | null
  return d?.transferId ?? null
}

const transferRouteFn: RoutingFn = (e) => {
  switch (e.type) {
    case 'TransferRequested': {
      const id = (e.data as { transferId?: string }).transferId
      return id ? { partitionKey: id } : 'ignore'
    }
    case 'Withdrawn':
    case 'Deposited':
    case 'WithdrawalRefused': {
      const id = transferIdOf(e)
      return id ? { partitionKey: id } : 'ignore'
    }
    default:
      return 'ignore'
  }
}

function transferApply(state: TransferStage, event: RecordedEvent): TransferStage {
  switch (event.type) {
    case 'Withdrawn': {
      const d = event.data as { amount: number; transferId?: string; to?: string }
      if (!d.to || !d.transferId) return state
      return {
        stage: 'debited',
        from: event.stream_uuid.replace(ACCOUNT_STREAM_PREFIX, ''),
        to: d.to,
        amount: d.amount,
        transferId: d.transferId,
      }
    }
    case 'Deposited':
      return { stage: 'done' }
    case 'WithdrawalRefused': {
      const d = event.data as { reason: string }
      return { stage: 'refunded', reason: d.reason }
    }
    default:
      return state
  }
}

async function transferHandle(
  _state: TransferStage,
  event: RecordedEvent,
): Promise<{ commands?: DispatchedCommand[]; complete?: boolean }> {
  switch (event.type) {
    case 'TransferRequested': {
      const d = event.data as TransferRequestedData
      return {
        commands: [
          {
            streamUuid: `Account-${d.from}`,
            aggregate: Account,
            command: {
              kind: 'Withdraw',
              amount: d.amount,
              transferId: d.transferId,
              to: d.to,
            } as AccountCommand,
          },
        ],
      }
    }
    case 'Withdrawn': {
      const d = event.data as { amount: number; transferId?: string; to?: string }
      if (!d.to || !d.transferId) return {}
      return {
        commands: [
          {
            streamUuid: `Account-${d.to}`,
            aggregate: Account,
            command: {
              kind: 'Deposit',
              amount: d.amount,
              transferId: d.transferId,
            } as AccountCommand,
          },
        ],
      }
    }
    case 'Deposited':
      // Successful path terminates the PM partition.
      return { complete: true }
    case 'WithdrawalRefused':
      // No compensating command needed per D-0011: the debit never
      // happened. Terminate the partition.
      return { complete: true }
    default:
      return {}
  }
}

// ===========================================================================
// The test
// ===========================================================================

void describe('bank-account end-to-end (standalone)', () => {
  void test('successful transfer + refused transfer', async () => {
    const view = newBalancesView()
    const app = new Instructed({ db: pool })
      .register(Account)
      .register(Transfer)
      .register({
        type: 'Balances',
        routeFn: balancesRouteFn,
        handler: balancesHandler(view),
      })
      .register({
        type: TRANSFER_PM_NAME,
        routeFn: transferRouteFn,
        initialState: () => ({ stage: 'starting' }),
        apply: transferApply,
        handle: transferHandle,
      })

    const worker = await app.poll({
      defaults: { pollInterval: 25, heartbeatInterval: 1_000 },
    })
    try {
      const alice = randomUUID()
      const bob = randomUUID()
      // Stream names are derived from the aggregate definition's
      // `streamName(id)` (defaulting to `${type}-${id}`); tests only
      // ever pass the bare id.
      const aliceStream = `Account-${alice}`
      const bobStream = `Account-${bob}`

      await app.dispatch('Account', alice, { kind: 'Open', owner: 'alice' })
      await app.dispatch('Account', bob, { kind: 'Open', owner: 'bob' })
      await app.dispatch(
        'Account',
        alice,
        { kind: 'Deposit', amount: 1_000 },
        { consistency: ['Balances'], consistencyTimeout: 10_000 },
      )
      assert.equal(view.balance.get(aliceStream), 1_000)
      assert.equal(view.balance.get(bobStream) ?? 0, 0)

      // ---- successful transfer ----
      const transferOk = randomUUID()
      await app.dispatch('Transfer', transferOk, {
        kind: 'Request',
        from: alice,
        to: bob,
        amount: 300,
        transferId: transferOk,
      })
      await waitFor(() => view.balance.get(bobStream) === 300, 10_000, 'bob to be credited 300')
      await waitFor(() => view.balance.get(aliceStream) === 700, 10_000, 'alice to be debited 300')
      // The PM instance reached `complete: true` on the Deposited
      // event; the snapshot must be gone.
      await waitFor(
        async () => {
          try {
            await app.client().readSnapshot(`${TRANSFER_PM_NAME}-${transferOk}`)
            return false
          } catch (err) {
            return err instanceof SnapshotNotFound
          }
        },
        10_000,
        'successful-transfer PM to complete',
      )

      // ---- refused transfer ----
      const transferKo = randomUUID()
      await app.dispatch('Transfer', transferKo, {
        kind: 'Request',
        from: bob,
        to: alice,
        amount: 5_000, // bob only has 300
        transferId: transferKo,
      })
      await waitFor(
        async () => {
          const ev = await app.client().readStream(bobStream, 1n, 50)
          return ev.some((e) => e.type === 'WithdrawalRefused')
        },
        10_000,
        "WithdrawalRefused on bob's stream",
      )
      await waitFor(
        async () => {
          try {
            await app.client().readSnapshot(`${TRANSFER_PM_NAME}-${transferKo}`)
            return false
          } catch (err) {
            return err instanceof SnapshotNotFound
          }
        },
        10_000,
        'refused-transfer PM to complete',
      )

      assert.equal(view.balance.get(aliceStream), 700)
      assert.equal(view.balance.get(bobStream), 300)

      const bobEvents = await app.client().readStream(bobStream, 1n, 50)
      const types = bobEvents.map((e) => e.type)
      assert.deepEqual(types, ['AccountOpened', 'Deposited', 'WithdrawalRefused'])
    } finally {
      await worker.stop()
    }
  })
})
