/**
 * `MarkTransferCompleted` — terminal-success command dispatched by
 * the TransferProcessManager when the destination-side
 * `DepositToAccount` lands. Idempotent at the aggregate
 * (`MarkCompleted` on an already-completed transfer is a no-op).
 */
export const MarkTransferCompleted = "MarkTransferCompleted" as const;
export type MarkTransferCompleted = {
  type: typeof MarkTransferCompleted;
  transferId: string;
};
