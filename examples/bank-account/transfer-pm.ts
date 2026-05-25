/**
 * TransferProcessManager -- the canonical money-transfer saga.
 *
 * Routing (D-0011 compensation via refusal, no compensating command):
 *
 *   TransferRequested  -> partition by transferId; dispatch Withdraw(from)
 *   Withdrawn          -> same partition;          dispatch Deposit(to)
 *   Deposited          -> same partition;          complete: true (success)
 *   WithdrawalRefused  -> same partition;          complete: true (the
 *                                                  debit never happened, so
 *                                                  there's nothing to undo)
 *
 * Routing uses the `transferId` carried in each event's data field
 * so multiple transfers can interleave on the same accounts without
 * the PM confusing their state.
 *
 * SUB-A registration shape (PM-F + PM-C):
 *   - `routeFn` decides which PM partition an event belongs to
 *     (or `"ignore"` to skip). It replaces the legacy `routes`
 *     map; there is no `start` / `continue` / `stop` directive
 *     enum -- *every routed event* gets a work item, lifecycle is
 *     a return-value concern in `handle`.
 *   - `apply` is the pure PM-C state fold (no I/O, no commands).
 *     It runs during rebuild and on the triggering event before
 *     `handle`.
 *   - `handle` returns `{ commands?, complete? }`. `complete: true`
 *     is PM-F's terminal signal: the snapshot + every work-item
 *     for the partition are DELETEd in one transaction.
 */

import type {
  DispatchedCommand,
  RecordedEvent,
  RegisterProcessManagerInput,
  RoutingFn,
} from "../../sdks/typescript/src/index.ts";
import { Account, type AccountCommand } from "./account.ts";

export type TransferStage =
  | { stage: "starting" }
  | {
      stage: "debited";
      from: string;
      to: string;
      amount: number;
      transferId: string;
    }
  | { stage: "done" }
  | { stage: "refunded"; reason: string };

const ACCOUNT_STREAM_PREFIX = "account-";

export const TRANSFER_PM_NAME = "TransferProcessManager";

function transferIdOf(event: { data: unknown }): string | null {
  const d = event.data as { transferId?: string } | null;
  return d?.transferId ?? null;
}

const transferRouteFn: RoutingFn = (event) => {
  switch (event.event_type) {
    case "TransferRequested": {
      const id = (event.data as { transferId?: string }).transferId;
      return id ? { partitionKey: id } : "ignore";
    }
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
  state: TransferStage,
  event: RecordedEvent,
): TransferStage {
  switch (event.event_type) {
    case "Withdrawn": {
      const d = event.data as {
        amount: number;
        transferId?: string;
        to?: string;
      };
      if (!d.to || !d.transferId) return state;
      return {
        stage: "debited",
        from: event.stream_uuid.replace(ACCOUNT_STREAM_PREFIX, ""),
        to: d.to,
        amount: d.amount,
        transferId: d.transferId,
      };
    }
    case "Deposited":
      return { stage: "done" };
    case "WithdrawalRefused": {
      const d = event.data as { reason: string };
      return { stage: "refunded", reason: d.reason };
    }
    default:
      return state;
  }
}

async function transferHandle(
  _state: TransferStage,
  event: RecordedEvent,
): Promise<{ commands?: DispatchedCommand[]; complete?: boolean }> {
  switch (event.event_type) {
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
            } as AccountCommand,
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
            } as AccountCommand,
          },
        ],
      };
    }
    case "Deposited":
      // Successful path terminates the PM partition.
      return { complete: true };
    case "WithdrawalRefused":
      // No compensating command needed (D-0011): the debit never
      // happened. Terminate the partition.
      return { complete: true };
    default:
      return {};
  }
}

/**
 * Build the registration payload for
 * `Instructed.registerProcessManager`.
 */
export function transferProcessManager(): RegisterProcessManagerInput<TransferStage> {
  return {
    stream: "$all",
    routeFn: transferRouteFn,
    initialState: () => ({ stage: "starting" }),
    apply: transferApply,
    handle: transferHandle,
  };
}
