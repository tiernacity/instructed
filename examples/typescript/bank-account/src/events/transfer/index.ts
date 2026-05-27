/**
 * Transfer event barrel. Mirrors the account-event barrel; see
 * its module comment for the rationale.
 */
export { TransferRequested } from "./transfer-requested.ts";
export { TransferCompleted } from "./transfer-completed.ts";
export { TransferFailed } from "./transfer-failed.ts";

import type { TransferRequested } from "./transfer-requested.ts";
import type { TransferCompleted } from "./transfer-completed.ts";
import type { TransferFailed } from "./transfer-failed.ts";

export type TransferEvent =
  | TransferRequested
  | TransferCompleted
  | TransferFailed;
