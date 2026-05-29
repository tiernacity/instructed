/**
 * `Transfer` aggregate — models the *lifecycle* of a single transfer.
 *
 * Lifecycle:
 *
 *   RequestTransfer        -> TransferRequested
 *   MarkTransferCompleted  -> TransferCompleted
 *   MarkTransferFailed     -> TransferFailed { reason }
 *
 * The PM dispatches `MarkTransferCompleted` once the destination
 * `DepositToAccount` lands, or `MarkTransferFailed` once an
 * `AccountWithdrawalRefused` lands. Both are terminal — further
 * mark-commands are idempotent no-ops so the PM is safe across
 * redelivery.
 *
 * This shape lets the Transfers projection report a real outcome
 * for every transfer, and gives operators a clean stream-per-
 * transfer audit trail without scanning Account streams.
 */

import type { AggregateDefinition } from 'instructed-sdk'

import {
  type TransferCommand,
  RequestTransfer,
  MarkTransferCompleted,
  MarkTransferFailed,
} from '../commands/transfer/index.ts'
import {
  type TransferEvent,
  TransferRequested,
  TransferCompleted,
  TransferFailed,
} from '../events/transfer/index.ts'

export type TransferStage = 'none' | 'requested' | 'completed' | 'failed'

export interface TransferState {
  stage: TransferStage
  from?: string
  to?: string
  amount?: number
  transferId?: string
  reason?: string
}

export const Transfer: AggregateDefinition<TransferState, TransferCommand, TransferEvent> = {
  type: 'Transfer',
  initialState: () => ({ stage: 'none' }),

  apply(state, event) {
    switch (event.type) {
      case TransferRequested:
        return {
          stage: 'requested',
          from: event.data.from,
          to: event.data.to,
          amount: event.data.amount,
          transferId: event.data.transferId,
        }
      case TransferCompleted:
        return { ...state, stage: 'completed' }
      case TransferFailed:
        return { ...state, stage: 'failed', reason: event.data.reason }
    }
  },

  execute(state, command) {
    switch (command.type) {
      case RequestTransfer:
        if (state.stage !== 'none') {
          throw new Error(`transfer already ${state.stage}`)
        }
        return {
          type: TransferRequested,
          data: {
            from: command.from,
            to: command.to,
            amount: command.amount,
            transferId: command.transferId,
          },
        }
      case MarkTransferCompleted:
        if (state.stage === 'completed') return [] // idempotent
        if (state.stage !== 'requested') {
          throw new Error(`cannot complete transfer in stage '${state.stage}'`)
        }
        return {
          type: TransferCompleted,
          data: { transferId: command.transferId },
        }
      case MarkTransferFailed:
        if (state.stage === 'failed') return [] // idempotent
        if (state.stage !== 'requested') {
          throw new Error(`cannot fail transfer in stage '${state.stage}'`)
        }
        return {
          type: TransferFailed,
          data: { transferId: command.transferId, reason: command.reason },
        }
    }
  },
}
