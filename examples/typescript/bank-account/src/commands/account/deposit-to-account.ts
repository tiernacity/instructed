/**
 * `DepositToAccount` — credit `amount` to `accountId`.
 * `transferId` is set by the TransferProcessManager when the
 * deposit is the destination leg of a transfer.
 */
export const DepositToAccount = "DepositToAccount" as const;
export type DepositToAccount = {
  type: typeof DepositToAccount;
  accountId: string;
  amount: number;
  transferId?: string;
};
