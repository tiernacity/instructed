/**
 * TransferProcessManager — the canonical money-transfer saga.
 *
 *   TransferRequested  -> partition by transferId; Withdraw(from)
 *   Withdrawn          -> same partition;          Deposit(to)
 *   Deposited          -> same partition;          MarkCompleted(transfer); complete
 *   WithdrawalRefused  -> same partition;          MarkFailed(transfer);    complete
 *
 * The PM dispatches *two* commands at termination so the Transfer
 * aggregate carries a real outcome event for every transfer (no
 * compensating Account command per D-0011 — the debit never
 * happened, there's nothing to undo on Account).
 */

import type {
  DispatchedCommand,
  ProcessManagerDefinition,
  RecordedEvent,
  RoutingFn,
} from "instructed-sdk";
import { Account, type AccountCommand } from "./account.ts";
import { Transfer, type TransferCommand } from "./transfer.ts";

export type TransferPmStage =
  | { stage: "starting" }
  | { stage: "debited"; transferId: string }
  | { stage: "completed" }
  | { stage: "failed"; reason: string };

// Stream-name prefixes match each aggregate's default `streamName`
// (`${type}-${id}`). Step 4 of the SDK refactor will replace these
// PM-emitted `streamUuid`s with a command router so the PM only
// names a target by `(aggregateType, id)` and the SDK derives the
// stream; for now these literals make the PM's plumbing visible.
const ACCOUNT_STREAM_PREFIX = "Account-";
const TRANSFER_STREAM_PREFIX = "Transfer-";

export const TRANSFER_PM_NAME = "TransferProcessManager";

function transferIdOf(event: { data: unknown }): string | null {
  const d = event.data as { transferId?: string } | null;
  return d?.transferId ?? null;
}

const transferRouteFn: RoutingFn = (event) => {
  switch (event.type) {
    case "TransferRequested":
    case "Withdrawn":
    case "Deposited":
    case "WithdrawalRefused": {
      const id = transferIdOf(event);
      return id ? { partitionKey: id } : "ignore";
    }
    default:
      return "ignore";
  }
};

function transferApply(
  state: TransferPmStage,
  event: RecordedEvent,
): TransferPmStage {
  switch (event.type) {
    case "Withdrawn": {
      const d = event.data as { transferId?: string };
      if (!d.transferId) return state;
      return { stage: "debited", transferId: d.transferId };
    }
    case "Deposited":
      return { stage: "completed" };
    case "WithdrawalRefused": {
      const d = event.data as { reason: string };
      return { stage: "failed", reason: d.reason };
    }
    default:
      return state;
  }
}

async function transferHandle(
  _state: TransferPmStage,
  event: RecordedEvent,
): Promise<{ commands?: DispatchedCommand[]; complete?: boolean }> {
  switch (event.type) {
    case "TransferRequested": {
      const d = event.data as {
        from: string;
        to: string;
        amount: number;
        transferId: string;
      };
      return {
        commands: [
          {
            streamUuid: `${ACCOUNT_STREAM_PREFIX}${d.from}`,
            aggregate: Account,
            command: {
              kind: "Withdraw",
              amount: d.amount,
              transferId: d.transferId,
              to: d.to,
            } satisfies AccountCommand,
          },
        ],
      };
    }
    case "Withdrawn": {
      const d = event.data as {
        amount: number;
        transferId?: string;
        to?: string;
      };
      if (!d.to || !d.transferId) return {};
      return {
        commands: [
          {
            streamUuid: `${ACCOUNT_STREAM_PREFIX}${d.to}`,
            aggregate: Account,
            command: {
              kind: "Deposit",
              amount: d.amount,
              transferId: d.transferId,
            } satisfies AccountCommand,
          },
        ],
      };
    }
    case "Deposited": {
      const d = event.data as { transferId?: string };
      if (!d.transferId) return { complete: true };
      return {
        commands: [
          {
            streamUuid: `${TRANSFER_STREAM_PREFIX}${d.transferId}`,
            aggregate: Transfer,
            command: {
              kind: "MarkCompleted",
              transferId: d.transferId,
            } satisfies TransferCommand,
          },
        ],
        complete: true,
      };
    }
    case "WithdrawalRefused": {
      const d = event.data as { transferId?: string; reason: string };
      if (!d.transferId) return { complete: true };
      return {
        commands: [
          {
            streamUuid: `${TRANSFER_STREAM_PREFIX}${d.transferId}`,
            aggregate: Transfer,
            command: {
              kind: "MarkFailed",
              transferId: d.transferId,
              reason: d.reason,
            } satisfies TransferCommand,
          },
        ],
        complete: true,
      };
    }
    default:
      return {};
  }
}

export function transferProcessManager(): ProcessManagerDefinition<TransferPmStage> {
  return {
    type: TRANSFER_PM_NAME,
    stream: "$all",
    routeFn: transferRouteFn,
    initialState: () => ({ stage: "starting" }),
    apply: transferApply,
    handle: transferHandle,
  };
}
