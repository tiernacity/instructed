/**
 * Transfer command barrel + the `TransferCommand` union.
 */
export { RequestTransfer } from './request-transfer.ts'
export { MarkTransferCompleted } from './mark-transfer-completed.ts'
export { MarkTransferFailed } from './mark-transfer-failed.ts'

import type { MarkTransferCompleted } from './mark-transfer-completed.ts'
import type { MarkTransferFailed } from './mark-transfer-failed.ts'
import type { RequestTransfer } from './request-transfer.ts'

export type TransferCommand = RequestTransfer | MarkTransferCompleted | MarkTransferFailed
