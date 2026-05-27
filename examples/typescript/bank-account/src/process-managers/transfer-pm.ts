/**
 * `TransferProcessManager` — the canonical money-transfer saga.
 *
 *   TransferRequested        -> partition by transferId; WithdrawFromAccount(from)
 *   AccountWithdrawnFrom     -> same partition;           DepositToAccount(to)
 *   AccountDepositedTo       -> same partition;           MarkTransferCompleted; complete
 *   AccountWithdrawalRefused -> same partition;           MarkTransferFailed;    complete
 *
 * The PM dispatches *two* commands at termination so the Transfer
 * aggregate carries a real outcome event for every transfer (no
 * compensating Account command per D-0011 — the debit never
 * happened, there's nothing to undo on Account).
 *
 * `handle` returns **lean commands** (bare `Command` objects, not
 * `{ aggregate, streamUuid, command }`): the PM worker resolves
 * each through the app's registered command router. No stream
 * names appear in PM code.
 */

import { onlyTypes } from "instructed-sdk";
import type {
  DispatchedCommand,
  ProcessManagerDefinition,
  RecordedEvent,
  RoutingFn,
} from "instructed-sdk";

import {
  type AccountEvent,
  AccountDepositedTo,
  AccountWithdrawnFrom,
  AccountWithdrawalRefused,
} from "../events/account/index.ts";
import {
  type TransferEvent,
  TransferRequested,
} from "../events/transfer/index.ts";
import {
  DepositToAccount,
  WithdrawFromAccount,
} from "../commands/account/index.ts";
import {
  MarkTransferCompleted,
  MarkTransferFailed,
} from "../commands/transfer/index.ts";

/** Events the PM cares about — a slice of both aggregates' streams. */
type PmEvent =
  | TransferRequested
  | AccountWithdrawnFrom
  | AccountDepositedTo
  | AccountWithdrawalRefused;

export type TransferPmStage =
  | { stage: "starting" }
  | { stage: "debited"; transferId: string }
  | { stage: "completed" }
  | { stage: "failed"; reason: string };

export const TransferProcessManager = "TransferProcessManager" as const;

/**
 * Routing decision: ignore everything but the PM's four event
 * types; for each of those, partition by the carried transferId.
 * Events without a transferId (e.g. a direct deposit that isn't
 * part of a transfer) drop out via `"ignore"`.
 */
const transferRouteFn: RoutingFn<PmEvent> = onlyTypes<PmEvent>(
  [
    TransferRequested,
    AccountWithdrawnFrom,
    AccountDepositedTo,
    AccountWithdrawalRefused,
  ],
  (event) => {
    // Inside the inner, `event.data` narrows by event.type.
    switch (event.type) {
      case TransferRequested:
        return { partitionKey: event.data.transferId };
      case AccountWithdrawnFrom:
      case AccountDepositedTo:
      case AccountWithdrawalRefused:
        return event.data.transferId
          ? { partitionKey: event.data.transferId }
          : "ignore";
    }
  },
);

function transferApply(
  state: TransferPmStage,
  event: RecordedEvent<PmEvent>,
): TransferPmStage {
  switch (event.type) {
    case AccountWithdrawnFrom:
      // The PM only routes here when transferId is set (see routeFn),
      // so the narrowing is safe at runtime; TS's discriminated
      // union still requires the optional-check or a cast.
      return event.data.transferId
        ? { stage: "debited", transferId: event.data.transferId }
        : state;
    case AccountDepositedTo:
      return { stage: "completed" };
    case AccountWithdrawalRefused:
      return { stage: "failed", reason: event.data.reason };
    default:
      return state;
  }
}

async function transferHandle(
  _state: TransferPmStage,
  event: RecordedEvent<PmEvent>,
): Promise<{ commands?: DispatchedCommand[]; complete?: boolean }> {
  switch (event.type) {
    case TransferRequested: {
      const { from, to, amount, transferId } = event.data;
      return {
        commands: [
          {
            type: WithdrawFromAccount,
            accountId: from,
            amount,
            transferId,
            to,
          },
        ],
      };
    }
    case AccountWithdrawnFrom: {
      const { amount, transferId, to } = event.data;
      if (!to || !transferId) return {};
      return {
        commands: [
          { type: DepositToAccount, accountId: to, amount, transferId },
        ],
      };
    }
    case AccountDepositedTo: {
      const { transferId } = event.data;
      if (!transferId) return { complete: true };
      return {
        commands: [{ type: MarkTransferCompleted, transferId }],
        complete: true,
      };
    }
    case AccountWithdrawalRefused: {
      const { transferId, reason } = event.data;
      if (!transferId) return { complete: true };
      return {
        commands: [{ type: MarkTransferFailed, transferId, reason }],
        complete: true,
      };
    }
  }
}

export function transferProcessManager(): ProcessManagerDefinition<
  TransferPmStage,
  PmEvent
> {
  return {
    type: TransferProcessManager,
    stream: "$all",
    routeFn: transferRouteFn,
    initialState: () => ({ stage: "starting" }),
    apply: transferApply,
    handle: transferHandle,
  };
}
