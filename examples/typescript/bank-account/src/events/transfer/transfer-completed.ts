/**
 * `TransferCompleted` — terminal success state for a Transfer.
 * Written when the PM's destination-side `DepositToAccount` lands.
 */
export const TransferCompleted = "TransferCompleted" as const;
export type TransferCompleted = {
  type: typeof TransferCompleted;
  data: { transferId: string };
};
