/**
 * `AccountDepositedTo` — funds added to an account.
 *
 * `transferId` is set when the deposit is part of a money transfer
 * (the TransferProcessManager dispatches a `DepositToAccount`
 * command on the receiving account carrying the transfer id); it
 * is absent for direct deposits.
 */
export const AccountDepositedTo = "AccountDepositedTo" as const;
export type AccountDepositedTo = {
  type: typeof AccountDepositedTo;
  data: { amount: number; transferId?: string };
};
