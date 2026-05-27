/**
 * Account command barrel. Re-exports the three account commands
 * plus the `AccountCommand` union for use as the second-position
 * generic on `AggregateDefinition<S, C, E>` and as the
 * command-router type parameter.
 */
export { OpenAccount } from "./open-account.ts";
export { DepositToAccount } from "./deposit-to-account.ts";
export { WithdrawFromAccount } from "./withdraw-from-account.ts";

import type { OpenAccount } from "./open-account.ts";
import type { DepositToAccount } from "./deposit-to-account.ts";
import type { WithdrawFromAccount } from "./withdraw-from-account.ts";

export type AccountCommand =
  | OpenAccount
  | DepositToAccount
  | WithdrawFromAccount;
