/**
 * L1-internal — PG error translation.
 *
 * `mapPgError` and `MapPgErrorContext` are used by `Client` (and by
 * `internal/with-transaction.ts`) to translate raw `pg` errors at the
 * call site. Neither is re-exported from the package's public entries;
 * consumers wanting custom translation should write their own.
 *
 * The closed error set per procedure is documented in
 * `sql/instructed.sql`. Standard Postgres errors (connection loss,
 * serialization_failure, …) pass through unwrapped — they are
 * infrastructure failures, not contract failures.
 */

import {
  WrongExpectedVersion,
  StreamExists,
  StreamNotFound,
  DuplicateEvent,
  ReservedStreamUuid,
  AppendOnlyViolation,
} from './append.ts'
import { InvalidParameterValue } from './base.ts'
import { SnapshotNotFound } from './snapshot.ts'
import {
  SubscriptionNotFound,
  SubscriptionAlreadyClaimed,
  SubscriptionLeaseLost,
} from './subscription.ts'
import { WorkItemLeaseLost } from './work-item.ts'

interface PgErrorLike {
  code?: string
  message?: string
  detail?: string
  hint?: string
  severity?: string
}

/** Pull a hint from the server message in the form "actual N, expected M". */
function parseWrongExpectedVersion(message: string | undefined): {
  actualVersion?: bigint
  expectedVersion?: bigint
} {
  if (!message) return {}
  const m = message.match(/actual\s+(-?\d+),\s*expected\s+(-?\d+)/)
  if (!m) return {}
  try {
    return {
      actualVersion: BigInt(m[1]),
      expectedVersion: BigInt(m[2]),
    }
  } catch {
    return {}
  }
}

export interface MapPgErrorContext {
  /** The stream the SDK was operating on, if known at call site. */
  streamUuid?: string
  /** The subscription name, if known at call site. */
  subscriptionName?: string
  /** The snapshot source_uuid, if known at call site. */
  sourceUuid?: string
  /** The work-item partition, if known at call site (SUB-A). */
  partitionKey?: string
  /** The work-item event_number, if known at call site (SUB-A). */
  eventNumber?: bigint
}

/**
 * Translate a Postgres error to the matching InstructedError subclass.
 *
 * Non-IS errors (and the catch-all 22023) are wrapped without preserving
 * the original error class, so callers can `instanceof InstructedError`.
 * If the error has no SQLSTATE at all (a transport / driver error), it is
 * returned unchanged.
 */
export function mapPgError(err: unknown, ctx: MapPgErrorContext = {}): unknown {
  if (!err || typeof err !== 'object') return err
  const pgErr = err as PgErrorLike
  const code = pgErr.code
  if (!code) return err

  const base = {
    code,
    detail: pgErr.detail,
    hint: pgErr.hint,
    cause: err,
  } as const
  const msg = pgErr.message ?? `instructed error ${code}`

  switch (code) {
    case 'IS001': {
      const parsed = parseWrongExpectedVersion(pgErr.message)
      return new WrongExpectedVersion(msg, { ...base, ...parsed })
    }
    case 'IS002':
      return new StreamExists(msg, { ...base, streamUuid: ctx.streamUuid })
    case 'IS003':
      return new StreamNotFound(msg, { ...base, streamUuid: ctx.streamUuid })
    case 'IS004':
      return new DuplicateEvent(msg, base)
    case 'IS005':
      return new ReservedStreamUuid(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
      })
    case 'IS006':
      return new AppendOnlyViolation(msg, base)
    case 'IS010':
      return new SnapshotNotFound(msg, {
        ...base,
        sourceUuid: ctx.sourceUuid,
      })
    case 'IS020':
      return new SubscriptionNotFound(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
      })
    case 'IS021':
      return new SubscriptionAlreadyClaimed(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
      })
    case 'IS022':
      return new SubscriptionLeaseLost(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
      })
    case 'IS030':
      return new WorkItemLeaseLost(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
        partitionKey: ctx.partitionKey,
        eventNumber: ctx.eventNumber,
      })
    case '22023':
      return new InvalidParameterValue(msg, base)
    default:
      // Standard Postgres errors (08xxx connection, 40001 serialization
      // failure, etc.) pass through unwrapped. They are infrastructure
      // failures, not contract failures.
      return err
  }
}
