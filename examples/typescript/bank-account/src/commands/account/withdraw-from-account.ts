/**
 * `WithdrawFromAccount` — debit `amount` from `accountId`.
 *
 * `transferId` + `to` are set by the TransferProcessManager when
 * the withdrawal is the source leg of a transfer. The aggregate's
 * `execute` emits `AccountWithdrawalRefused` (not a thrown error)
 * when the balance is insufficient, per D-0011.
 */
export const WithdrawFromAccount = 'WithdrawFromAccount' as const
export type WithdrawFromAccount = {
  type: typeof WithdrawFromAccount
  accountId: string
  amount: number
  transferId?: string
  to?: string
}
