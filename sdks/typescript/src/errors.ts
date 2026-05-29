/**
 * SQLSTATE → typed Error subclass translation.
 *
 * The closed error set per procedure is documented in
 * `sql/instructed.sql`. Standard Postgres errors (connection loss,
 * serialization_failure, …) pass through unwrapped — they are
 * infrastructure failures, not contract failures.
 *
 * **Layer membership** (per [D-0027](../../../docs/decisions.md#d-0027)):
 *
 *   - **L1 (procedure bindings)** classes are SQLSTATE-bound and form
 *     part of the contract every SDK port must reproduce. The base
 *     `InstructedError`, the IS00x append errors, `SnapshotNotFound`,
 *     the IS02x subscription errors, `WorkItemLeaseLost`, and
 *     `InvalidParameterValue` are all L1. They are re-exported from
 *     `instructed-sdk/core`.
 *   - **L2 (core behaviours)** classes are emitted by SDK runtime
 *     code, not by SQL. `RetryBudgetExhausted` is the only L2 entry
 *     (aggregate retry loop). Re-exported from `instructed-sdk/core`.
 *   - **L3 (conveniences)** classes are emitted by the idiomatic
 *     facade. `ConsistencyTimeout`, `ConsistencyTargetError`,
 *     `UnknownAggregateType`. Re-exported only from the bare
 *     `instructed-sdk` entry, not from `/core`.
 *   - `mapPgError` and `MapPgErrorContext` are L1-internal: used by
 *     `Client` to translate errors at the call site; not re-exported
 *     from any public entry.
 *
 * Section headers below tag each block; the layer split is wired in
 * `src/core.ts` and `src/index.ts`.
 */

export class InstructedError extends Error {
  /** The SQLSTATE that produced this error, if any. */
  readonly code?: string;
  /** Original Postgres error message detail, if present. */
  readonly detail?: string;
  /** Original Postgres error hint, if present. */
  readonly hint?: string;

  constructor(
    message: string,
    options?: { code?: string; detail?: string; hint?: string; cause?: unknown },
  ) {
    super(message, options?.cause ? { cause: options.cause } : undefined);
    this.name = new.target.name;
    this.code = options?.code;
    this.detail = options?.detail;
    this.hint = options?.hint;
  }
}

// ---- L1 — IS00x: append-path errors ----------------------------------------

export class AppendError extends InstructedError {}

export class WrongExpectedVersion extends AppendError {
  /** Parsed from the server message when possible. */
  readonly actualVersion?: bigint;
  readonly expectedVersion?: bigint;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      actualVersion?: bigint;
      expectedVersion?: bigint;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.actualVersion = opts.actualVersion;
    this.expectedVersion = opts.expectedVersion;
  }
}

export class StreamExists extends AppendError {
  readonly streamUuid?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      streamUuid?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.streamUuid = opts.streamUuid;
  }
}

/**
 * Raised by append_to_stream(stream_exists), read_stream, and
 * claim_subscription when the stream does not exist.
 */
export class StreamNotFound extends AppendError {
  readonly streamUuid?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      streamUuid?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.streamUuid = opts.streamUuid;
  }
}

export class DuplicateEvent extends AppendError {
  readonly eventId?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      eventId?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.eventId = opts.eventId;
  }
}

export class ReservedStreamUuid extends AppendError {
  readonly streamUuid?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      streamUuid?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.streamUuid = opts.streamUuid;
  }
}

/**
 * IS006. Fires only on direct table manipulation (UPDATE / DELETE on
 * events / stream_events); the procedures never raise this. Surfaced as
 * a named class so a user bypassing the contract sees a useful name.
 */
export class AppendOnlyViolation extends InstructedError {}

// ---- L1 — IS010: snapshot errors -------------------------------------------

export class SnapshotNotFound extends InstructedError {
  readonly sourceUuid?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      sourceUuid?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.sourceUuid = opts.sourceUuid;
  }
}

// ---- L1 — IS020 / IS021 / IS022: subscription errors -----------------------

export class SubscriptionError extends InstructedError {
  readonly streamUuid?: string;
  readonly subscriptionName?: string;
  readonly holder?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      streamUuid?: string;
      subscriptionName?: string;
      holder?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.streamUuid = opts.streamUuid;
    this.subscriptionName = opts.subscriptionName;
    this.holder = opts.holder;
  }
}

export class SubscriptionNotFound extends SubscriptionError {}
/** Reserved by the SQL catalogue; never thrown in v1. */
export class SubscriptionAlreadyClaimed extends SubscriptionError {}
export class SubscriptionLeaseLost extends SubscriptionError {}

// ---- L1 — IS030: work-item errors (SUB-A) ---------------------------------

/**
 * Raised by `complete_work_item_*` and `fail_work_item` when the caller is
 * not (or no longer) the row's claimant. Covers both "row gone" (a
 * takeover worker already terminal-deleted it) and "claimed_by mismatch"
 * (the lease expired and another worker took over). Either way the
 * contract is: stop processing.
 */
export class WorkItemLeaseLost extends InstructedError {
  readonly streamUuid?: string;
  readonly subscriptionName?: string;
  readonly partitionKey?: string;
  readonly eventNumber?: bigint;
  readonly holder?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      streamUuid?: string;
      subscriptionName?: string;
      partitionKey?: string;
      eventNumber?: bigint;
      holder?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.streamUuid = opts.streamUuid;
    this.subscriptionName = opts.subscriptionName;
    this.partitionKey = opts.partitionKey;
    this.eventNumber = opts.eventNumber;
    this.holder = opts.holder;
  }
}

// ---- L1 — 22023: invalid parameter value -----------------------------------

export class InvalidParameterValue extends InstructedError {}

// ---- L2 / L3 — SDK-level (no SQLSTATE) ------------------------------------
//
// `RetryBudgetExhausted` is L2: emitted by the aggregate retry loop
// (`runCommand` in `aggregate.ts`). Belongs in `instructed-sdk/core`.
//
// `ConsistencyTimeout`, `ConsistencyTargetError`, `UnknownAggregateType`
// are L3: emitted by the convenience layer (`consistency.ts` and
// `instructed.ts`). Available only via the bare `instructed-sdk`
// entry.

export class RetryBudgetExhausted extends InstructedError {
  readonly attempts: number;
  readonly lastError: unknown;
  constructor(message: string, opts: { attempts: number; lastError: unknown }) {
    super(message, { cause: opts.lastError });
    this.attempts = opts.attempts;
    this.lastError = opts.lastError;
  }
}

export class ConsistencyTimeout extends InstructedError {
  readonly waitedMs: number;
  readonly missing: string[];
  constructor(
    message: string,
    opts: { waitedMs: number; missing: string[] },
  ) {
    super(message);
    this.waitedMs = opts.waitedMs;
    this.missing = opts.missing;
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
  readonly subscriptionStream: string;
  readonly subscriptionName: string;
  readonly appendedStreams: string[];
  constructor(
    message: string,
    opts: {
      subscriptionStream: string;
      subscriptionName: string;
      appendedStreams: string[];
    },
  ) {
    super(message);
    this.subscriptionStream = opts.subscriptionStream;
    this.subscriptionName = opts.subscriptionName;
    this.appendedStreams = opts.appendedStreams;
  }
}

export class UnknownAggregateType extends InstructedError {
  readonly aggregateType: string;
  constructor(aggregateType: string) {
    super(`Unknown aggregate type: ${aggregateType}`);
    this.aggregateType = aggregateType;
  }
}

// ---- L1-internal — PG error translation ------------------------------------
//
// `mapPgError` and `MapPgErrorContext` are used by `Client` (and by
// `internal/with-transaction.ts`) to translate raw `pg` errors at the
// call site. Neither is re-exported from the package's public
// entries; consumers wanting custom translation should write their
// own.

interface PgErrorLike {
  code?: string;
  message?: string;
  detail?: string;
  hint?: string;
  severity?: string;
}

/** Pull a hint from the server message in the form "actual N, expected M". */
function parseWrongExpectedVersion(message: string | undefined): {
  actualVersion?: bigint;
  expectedVersion?: bigint;
} {
  if (!message) return {};
  const m = message.match(/actual\s+(-?\d+),\s*expected\s+(-?\d+)/);
  if (!m) return {};
  try {
    return {
      actualVersion: BigInt(m[1]),
      expectedVersion: BigInt(m[2]),
    };
  } catch {
    return {};
  }
}

export interface MapPgErrorContext {
  /** The stream the SDK was operating on, if known at call site. */
  streamUuid?: string;
  /** The subscription name, if known at call site. */
  subscriptionName?: string;
  /** The snapshot source_uuid, if known at call site. */
  sourceUuid?: string;
  /** The work-item partition, if known at call site (SUB-A). */
  partitionKey?: string;
  /** The work-item event_number, if known at call site (SUB-A). */
  eventNumber?: bigint;
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
  if (!err || typeof err !== "object") return err;
  const pgErr = err as PgErrorLike;
  const code = pgErr.code;
  if (!code) return err;

  const base = {
    code,
    detail: pgErr.detail,
    hint: pgErr.hint,
    cause: err,
  } as const;
  const msg = pgErr.message ?? `instructed error ${code}`;

  switch (code) {
    case "IS001": {
      const parsed = parseWrongExpectedVersion(pgErr.message);
      return new WrongExpectedVersion(msg, { ...base, ...parsed });
    }
    case "IS002":
      return new StreamExists(msg, { ...base, streamUuid: ctx.streamUuid });
    case "IS003":
      return new StreamNotFound(msg, { ...base, streamUuid: ctx.streamUuid });
    case "IS004":
      return new DuplicateEvent(msg, base);
    case "IS005":
      return new ReservedStreamUuid(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
      });
    case "IS006":
      return new AppendOnlyViolation(msg, base);
    case "IS010":
      return new SnapshotNotFound(msg, {
        ...base,
        sourceUuid: ctx.sourceUuid,
      });
    case "IS020":
      return new SubscriptionNotFound(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
      });
    case "IS021":
      return new SubscriptionAlreadyClaimed(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
      });
    case "IS022":
      return new SubscriptionLeaseLost(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
      });
    case "IS030":
      return new WorkItemLeaseLost(msg, {
        ...base,
        streamUuid: ctx.streamUuid,
        subscriptionName: ctx.subscriptionName,
        partitionKey: ctx.partitionKey,
        eventNumber: ctx.eventNumber,
      });
    case "22023":
      return new InvalidParameterValue(msg, base);
    default:
      // Standard Postgres errors (08xxx connection, 40001 serialization
      // failure, etc.) pass through unwrapped. They are infrastructure
      // failures, not contract failures.
      return err;
  }
}
