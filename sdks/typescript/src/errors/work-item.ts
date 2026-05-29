/** L1 — IS030: work-item errors (SUB-A). */

import { InstructedError } from './base.ts'

/**
 * Raised by `complete_work_item_*` and `fail_work_item` when the caller is
 * not (or no longer) the row's claimant. Covers both "row gone" (a
 * takeover worker already terminal-deleted it) and "claimed_by mismatch"
 * (the lease expired and another worker took over). Either way the
 * contract is: stop processing.
 */
export class WorkItemLeaseLost extends InstructedError {
  readonly streamUuid?: string
  readonly subscriptionName?: string
  readonly partitionKey?: string
  readonly eventNumber?: bigint
  readonly holder?: string
  constructor(
    message: string,
    opts: {
      code?: string
      detail?: string
      hint?: string
      streamUuid?: string
      subscriptionName?: string
      partitionKey?: string
      eventNumber?: bigint
      holder?: string
      cause?: unknown
    } = {},
  ) {
    super(message, opts)
    this.streamUuid = opts.streamUuid
    this.subscriptionName = opts.subscriptionName
    this.partitionKey = opts.partitionKey
    this.eventNumber = opts.eventNumber
    this.holder = opts.holder
  }
}
