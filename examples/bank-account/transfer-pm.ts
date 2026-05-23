/**
 * TransferProcessManager — the canonical money-transfer saga.
 *
 * Routing (D-0011 compensation via refusal, no compensating command):
 *
 *   TransferRequested  -> start    ; dispatch Withdraw(from)
 *   Withdrawn          -> continue ; dispatch Deposit(to)
 *   Deposited          -> stop     ; transfer succeeded
 *   WithdrawalRefused  -> stop     ; nothing to compensate, the
 *                                    debit never happened.
 *
 * Routing uses the `transferId` carried in each event's data field
 * so multiple transfers can interleave on the same accounts without
 * the PM confusing their state.
 */

import type { ProcessManagerDefinition } from "../../sdks/typescript/src/index.ts";
import { Account } from "./account.ts";

export type TransferStage =
  | { stage: "starting" }
  | { stage: "debited"; from: string; to: string; amount: number; transferId: string }
  | { stage: "done" }
  | { stage: "refunded"; reason: string };

const ACCOUNT_STREAM_PREFIX = "account-";

function transferIdOf(event: { data: unknown }): string | null {
  const d = event.data as { transferId?: string } | null;
  return d?.transferId ?? null;
}

export function transferProcessManager(): ProcessManagerDefinition<TransferStage> {
  return {
    name: "TransferProcessManager",
    stream: "$all",
    routes: {
      TransferRequested: (e) => ({
        kind: "start",
        processId: ((e.data as { transferId?: string }).transferId) ?? "unknown",
      }),
      Withdrawn: (e) => {
        const id = transferIdOf(e);
        return id ? { kind: "continue", processId: id } : { kind: "ignore" };
      },
      Deposited: (e) => {
        const id = transferIdOf(e);
        return id ? { kind: "stop", processId: id } : { kind: "ignore" };
      },
      WithdrawalRefused: (e) => {
        const id = transferIdOf(e);
        return id ? { kind: "stop", processId: id } : { kind: "ignore" };
      },
    },
    initialState: () => ({ stage: "starting" }),
    async handle(state, event) {
      const raw = event.data as unknown;
      switch (event.event_type) {
        case "TransferRequested": {
          const d = raw as {
            from: string;
            to: string;
            amount: number;
            transferId: string;
          };
          return {
            state,
            commands: [
              {
                streamUuid: `${ACCOUNT_STREAM_PREFIX}${d.from}`,
                aggregate: Account,
                command: {
                  kind: "Withdraw",
                  amount: d.amount,
                  transferId: d.transferId,
                  to: d.to,
                },
              },
            ],
          };
        }
        case "Withdrawn": {
          const d = raw as {
            amount: number;
            transferId?: string;
            to?: string;
          };
          if (!d.to || !d.transferId) return { state };
          return {
            state: {
              stage: "debited",
              from: event.stream_uuid.replace(ACCOUNT_STREAM_PREFIX, ""),
              to: d.to,
              amount: d.amount,
              transferId: d.transferId,
            },
            commands: [
              {
                streamUuid: `${ACCOUNT_STREAM_PREFIX}${d.to}`,
                aggregate: Account,
                command: {
                  kind: "Deposit",
                  amount: d.amount,
                  transferId: d.transferId,
                },
              },
            ],
          };
        }
        case "Deposited":
          return { state: { stage: "done" } };
        case "WithdrawalRefused": {
          const d = raw as { reason: string };
          return { state: { stage: "refunded", reason: d.reason } };
        }
        default:
          // AccountOpened or any unrouted type; defensive.
          return { state };
      }
    },
  };
}
