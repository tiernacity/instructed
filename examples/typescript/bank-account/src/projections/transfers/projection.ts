/**
 * Transfers projection — writes through to `bank_account.transfers`.
 *
 * Per-transfer partitioning: each transfer is its own partition,
 * so multiple concurrent transfers project in parallel across
 * however many workers are running. All three events for one
 * transfer (Requested / Completed / Failed) land on the same
 * partition, preserving order.
 *
 * Idempotency: `last_event_number` guard on every write (in
 * `queries.ts`).
 */

import { onlyTypes } from 'instructed-sdk'
import type { ProjectionDefinition, RoutingFn } from 'instructed-sdk'
import type pg from 'pg'

import {
  type TransferEvent,
  TransferRequested,
  TransferCompleted,
  TransferFailed,
} from '../../events/transfer/index.ts'
import { markCompleted, markFailed, upsertRequested } from './queries.ts'

export const Transfers = 'Transfers' as const

const transfersRouteFn: RoutingFn<TransferEvent> = onlyTypes<TransferEvent>(
  [TransferRequested, TransferCompleted, TransferFailed],
  (event) => ({ partitionKey: event.data.transferId }),
)

export function transfersProjection(pool: pg.Pool): ProjectionDefinition<TransferEvent> {
  return {
    type: Transfers,
    stream: '$all',
    routeFn: transfersRouteFn,
    async handler(event) {
      const n = event.event_number
      switch (event.type) {
        case TransferRequested:
          await upsertRequested(pool, {
            transferId: event.data.transferId,
            from: event.data.from,
            to: event.data.to,
            amount: event.data.amount,
            requestedAt: event.created_at,
            eventNumber: n,
          })
          return
        case TransferCompleted:
          await markCompleted(pool, {
            transferId: event.data.transferId,
            eventNumber: n,
          })
          return
        case TransferFailed:
          await markFailed(pool, {
            transferId: event.data.transferId,
            reason: event.data.reason,
            eventNumber: n,
          })
          return
      }
    },
  }
}
