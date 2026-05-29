/**
 * `Account` aggregate — the canonical bank-account example.
 *
 * Pure domain folds. The SDK tracks version; user code reads no
 * connection / transaction handle. `AccountWithdrawalRefused` is
 * an emitted event, not a thrown error: it records that a
 * withdrawal was tried and refused so the TransferProcessManager
 * can react via its routes (D-0011).
 *
 * Stream naming defaults to `Account-<id>` via the SDK's
 * `prefixType(def.type)` helper. Apps identify aggregates by
 * `(type, id)`; stream names are storage-layer detail.
 */

import type { AggregateDefinition } from 'instructed-sdk'

import {
  type AccountCommand,
  OpenAccount,
  DepositToAccount,
  WithdrawFromAccount,
} from '../commands/account/index.ts'
import {
  type AccountEvent,
  AccountOpened,
  AccountDepositedTo,
  AccountWithdrawnFrom,
  AccountWithdrawalRefused,
} from '../events/account/index.ts'

export interface AccountState {
  opened: boolean
  owner: string | null
  balance: number
}

export const Account: AggregateDefinition<AccountState, AccountCommand, AccountEvent> = {
  type: 'Account',
  initialState: () => ({ opened: false, owner: null, balance: 0 }),

  apply(state, event) {
    switch (event.type) {
      case AccountOpened:
        return { ...state, opened: true, owner: event.data.owner }
      case AccountDepositedTo:
        return { ...state, balance: state.balance + event.data.amount }
      case AccountWithdrawnFrom:
        return { ...state, balance: state.balance - event.data.amount }
      case AccountWithdrawalRefused:
        return state
    }
  },

  execute(state, command) {
    switch (command.type) {
      case OpenAccount:
        if (state.opened) throw new Error('account already open')
        return {
          type: AccountOpened,
          data: { owner: command.owner },
        }
      case DepositToAccount:
        if (!state.opened) throw new Error('account not open')
        return {
          type: AccountDepositedTo,
          data: { amount: command.amount, transferId: command.transferId },
        }
      case WithdrawFromAccount:
        if (!state.opened) throw new Error('account not open')
        if (state.balance < command.amount) {
          // D-0011: refusal is a domain event, not an exception.
          // The TransferProcessManager observes it via its routes
          // and stops the process without ever needing a
          // compensating command.
          return {
            type: AccountWithdrawalRefused,
            data: {
              reason: 'insufficient funds',
              amount: command.amount,
              transferId: command.transferId,
            },
          }
        }
        return {
          type: AccountWithdrawnFrom,
          data: {
            amount: command.amount,
            transferId: command.transferId,
            to: command.to,
          },
        }
    }
  },
}
