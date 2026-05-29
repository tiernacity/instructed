/**
 * `AccountWithdrawnFrom` — funds removed from an account.
 *
 * `transferId` + `to` are set when the withdrawal is the debit leg
 * of a money transfer; the TransferProcessManager observes this
 * event, then dispatches `DepositToAccount` on `to`.
 */
export const AccountWithdrawnFrom = 'AccountWithdrawnFrom' as const
export type AccountWithdrawnFrom = {
  type: typeof AccountWithdrawnFrom
  data: { amount: number; transferId?: string; to?: string }
}
