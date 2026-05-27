/**
 * The application's command router.
 *
 * Maps each command type to the aggregate that owns it and to an
 * `id(cmd)` extractor that pulls the aggregate id out of the
 * command payload. Build-once / register-once: every script that
 * dispatches commands or runs the PM worker registers this same
 * router so the lean `dispatch(command)` shape and the PM's lean
 * `DispatchedCommand` shape both work.
 *
 * Why a separate router and not a method on Command? See
 * `command-router.ts` in the SDK: commands are plain data and
 * cross language / process boundaries; pinning aggregate identity
 * to the command type at declaration time would couple two
 * concerns. Routing is a deployment-level lookup.
 */

import { commandRouter, type CommandRouter } from "instructed-sdk";
import { Account } from "./aggregates/account.ts";
import { Transfer } from "./aggregates/transfer.ts";
import {
  type AccountCommand,
  OpenAccount,
  DepositToAccount,
  WithdrawFromAccount,
} from "./commands/account/index.ts";
import {
  type TransferCommand,
  RequestTransfer,
  MarkTransferCompleted,
  MarkTransferFailed,
} from "./commands/transfer/index.ts";

export type AppCommand = AccountCommand | TransferCommand;

export const appCommandRouter: CommandRouter = commandRouter<AppCommand>({
  [OpenAccount]:            { aggregate: Account,  id: (c) => c.accountId },
  [DepositToAccount]:       { aggregate: Account,  id: (c) => c.accountId },
  [WithdrawFromAccount]:    { aggregate: Account,  id: (c) => c.accountId },
  [RequestTransfer]:        { aggregate: Transfer, id: (c) => c.transferId },
  [MarkTransferCompleted]:  { aggregate: Transfer, id: (c) => c.transferId },
  [MarkTransferFailed]:     { aggregate: Transfer, id: (c) => c.transferId },
});
