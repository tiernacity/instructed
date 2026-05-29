/**
 * `TransferFailed` — terminal failure state for a Transfer.
 * Written when the PM observes `AccountWithdrawalRefused` (the
 * source account had insufficient funds; per D-0011 the debit
 * never happened, so no compensating Account command).
 */
export const TransferFailed = 'TransferFailed' as const
export type TransferFailed = {
  type: typeof TransferFailed
  data: { transferId: string; reason: string }
}
