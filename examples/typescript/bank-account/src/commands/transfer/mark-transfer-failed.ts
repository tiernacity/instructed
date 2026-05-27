/**
 * `MarkTransferFailed` — terminal-failure command dispatched by
 * the TransferProcessManager when `AccountWithdrawalRefused` is
 * observed. Idempotent at the aggregate.
 */
export const MarkTransferFailed = "MarkTransferFailed" as const;
export type MarkTransferFailed = {
  type: typeof MarkTransferFailed;
  transferId: string;
  reason: string;
};
