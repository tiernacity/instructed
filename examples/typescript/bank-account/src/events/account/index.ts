/**
 * Account event barrel. Re-exports the four account events so
 * aggregates / projections / process-managers can import the
 * whole family in one line:
 *
 *     import { AccountOpened, AccountDepositedTo, ... } from
 *       "../events/account/index.ts";
 *
 * Also exports the `AccountEvent` union — pass it as the generic
 * to `RecordedEvent<AccountEvent>` for type-narrowed `event.data`
 * inside switches on `event.type`.
 */
export { AccountOpened } from './account-opened.ts'
export { AccountDepositedTo } from './account-deposited-to.ts'
export { AccountWithdrawnFrom } from './account-withdrawn-from.ts'
export { AccountWithdrawalRefused } from './account-withdrawal-refused.ts'

import type { AccountDepositedTo } from './account-deposited-to.ts'
import type { AccountOpened } from './account-opened.ts'
import type { AccountWithdrawalRefused } from './account-withdrawal-refused.ts'
import type { AccountWithdrawnFrom } from './account-withdrawn-from.ts'

export type AccountEvent =
  | AccountOpened
  | AccountDepositedTo
  | AccountWithdrawnFrom
  | AccountWithdrawalRefused
