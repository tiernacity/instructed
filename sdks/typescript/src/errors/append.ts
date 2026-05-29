/** L1 — IS00x: append-path errors. */

import { InstructedError } from './base.ts'

export class AppendError extends InstructedError {}

export class WrongExpectedVersion extends AppendError {
  /** Parsed from the server message when possible. */
  readonly actualVersion?: bigint
  readonly expectedVersion?: bigint
  constructor(
    message: string,
    opts: {
      code?: string
      detail?: string
      hint?: string
      actualVersion?: bigint
      expectedVersion?: bigint
      cause?: unknown
    } = {},
  ) {
    super(message, opts)
    this.actualVersion = opts.actualVersion
    this.expectedVersion = opts.expectedVersion
  }
}

export class StreamExists extends AppendError {
  readonly streamUuid?: string
  constructor(
    message: string,
    opts: {
      code?: string
      detail?: string
      hint?: string
      streamUuid?: string
      cause?: unknown
    } = {},
  ) {
    super(message, opts)
    this.streamUuid = opts.streamUuid
  }
}

/**
 * Raised by append_to_stream(stream_exists), read_stream, and
 * claim_subscription when the stream does not exist.
 */
export class StreamNotFound extends AppendError {
  readonly streamUuid?: string
  constructor(
    message: string,
    opts: {
      code?: string
      detail?: string
      hint?: string
      streamUuid?: string
      cause?: unknown
    } = {},
  ) {
    super(message, opts)
    this.streamUuid = opts.streamUuid
  }
}

export class DuplicateEvent extends AppendError {
  readonly eventId?: string
  constructor(
    message: string,
    opts: {
      code?: string
      detail?: string
      hint?: string
      eventId?: string
      cause?: unknown
    } = {},
  ) {
    super(message, opts)
    this.eventId = opts.eventId
  }
}

export class ReservedStreamUuid extends AppendError {
  readonly streamUuid?: string
  constructor(
    message: string,
    opts: {
      code?: string
      detail?: string
      hint?: string
      streamUuid?: string
      cause?: unknown
    } = {},
  ) {
    super(message, opts)
    this.streamUuid = opts.streamUuid
  }
}

/**
 * IS006. Fires only on direct table manipulation (UPDATE / DELETE on
 * events / stream_events); the procedures never raise this. Surfaced as
 * a named class so a user bypassing the contract sees a useful name.
 */
export class AppendOnlyViolation extends InstructedError {}
