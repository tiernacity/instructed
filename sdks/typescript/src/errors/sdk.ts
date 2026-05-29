/**
 * SDK-level errors with no SQLSTATE — emitted by runtime code, not by
 * SQL. (The plan's tentative `consistency.ts` name; widened to `sdk.ts`
 * because the file also holds the L2 retry-exhaustion and the L3 facade
 * unknown-aggregate errors, not only the consistency ones.)
 *
 *   - `RetryBudgetExhausted` — L2; emitted by the aggregate retry loop
 *     (`runCommand` in `aggregate.ts`). Re-exported from `instructed-sdk/core`.
 *   - `ConsistencyTimeout`, `ConsistencyTargetError` — L3; emitted by
 *     `consistency.ts` (`waitForProjection`).
 *   - `UnknownAggregateType` — L3; emitted by the `Instructed` facade.
 *
 * The three L3 classes are re-exported only from the bare `instructed-sdk`
 * entry, not from `/core`.
 */

import { InstructedError } from './base.ts'

export class RetryBudgetExhausted extends InstructedError {
  readonly attempts: number
  readonly lastError: unknown
  constructor(message: string, opts: { attempts: number; lastError: unknown }) {
    super(message, { cause: opts.lastError })
    this.attempts = opts.attempts
    this.lastError = opts.lastError
  }
}

export class ConsistencyTimeout extends InstructedError {
  readonly waitedMs: number
  readonly missing: string[]
  constructor(message: string, opts: { waitedMs: number; missing: string[] }) {
    super(message)
    this.waitedMs = opts.waitedMs
    this.missing = opts.missing
  }
}

/**
 * Raised synchronously by `waitForProjection` (CON-B) when a
 * per-stream `SubscriptionRef` targets a stream that none of the
 * appended events touched. Comparing the subscription's `last_seen`
 * (in its own stream's coordinate space) against the appended
 * events' target (in the appended stream's coordinate space) would
 * silently produce wrong answers; raising up front prevents that.
 *
 * `$all` refs are never rejected by this check, regardless of which
 * streams were appended to.
 */
export class ConsistencyTargetError extends InstructedError {
  readonly subscriptionStream: string
  readonly subscriptionName: string
  readonly appendedStreams: string[]
  constructor(
    message: string,
    opts: {
      subscriptionStream: string
      subscriptionName: string
      appendedStreams: string[]
    },
  ) {
    super(message)
    this.subscriptionStream = opts.subscriptionStream
    this.subscriptionName = opts.subscriptionName
    this.appendedStreams = opts.appendedStreams
  }
}

export class UnknownAggregateType extends InstructedError {
  readonly aggregateType: string
  constructor(aggregateType: string) {
    super(`Unknown aggregate type: ${aggregateType}`)
    this.aggregateType = aggregateType
  }
}
