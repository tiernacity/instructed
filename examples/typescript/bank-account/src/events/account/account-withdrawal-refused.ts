/**
 * `AccountWithdrawalRefused` — a withdrawal was attempted but the
 * account had insufficient funds. Per D-0011 refusal is a domain
 * event (recorded on the source account stream), not a thrown
 * exception: the TransferProcessManager observes it on the same
 * stream as `AccountWithdrawnFrom` and decides whether to mark
 * the transfer failed.
 */
export const AccountWithdrawalRefused = 'AccountWithdrawalRefused' as const
export type AccountWithdrawalRefused = {
  type: typeof AccountWithdrawalRefused
  data: { reason: string; amount: number; transferId?: string }
}
