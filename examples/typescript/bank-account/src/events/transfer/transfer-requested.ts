/**
 * `TransferRequested` — the first event on every Transfer stream.
 * The TransferProcessManager observes this and orchestrates the
 * Withdraw/Deposit pair on the participating accounts.
 */
export const TransferRequested = "TransferRequested" as const;
export type TransferRequested = {
  type: typeof TransferRequested;
  data: { from: string; to: string; amount: number; transferId: string };
};
