/** L1 — IS010: snapshot errors. */

import { InstructedError } from "./base.ts";

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
