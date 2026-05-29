/**
 * The base `InstructedError` and the generic `InvalidParameterValue`
 * (22023) error.
 *
 * Every typed SDK error subclasses `InstructedError`, so callers can
 * `instanceof InstructedError` to catch the whole family. See
 * `errors/index.ts` for the full layer map and `map-pg-error.ts` for
 * the SQLSTATE → class translation.
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

// ---- L1 — 22023: invalid parameter value -----------------------------------

export class InvalidParameterValue extends InstructedError {}
