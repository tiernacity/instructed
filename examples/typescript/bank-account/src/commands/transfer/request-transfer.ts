/**
 * `RequestTransfer` — create a new Transfer aggregate instance and
 * start its lifecycle. `transferId` doubles as both the aggregate
 * id (used by the command router) and the correlation key that
 * the TransferProcessManager partitions on.
 */
export const RequestTransfer = "RequestTransfer" as const;
export type RequestTransfer = {
  type: typeof RequestTransfer;
  transferId: string;
  from: string;
  to: string;
  amount: number;
};
