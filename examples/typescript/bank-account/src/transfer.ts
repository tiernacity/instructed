/**
 * Transfer aggregate — models the *lifecycle* of a single transfer.
 *
 * Lifecycle:
 *
 *   Request        -> TransferRequested            (stage = requested)
 *   MarkCompleted  -> TransferCompleted            (stage = completed)
 *   MarkFailed     -> TransferFailed { reason }    (stage = failed)
 *
 * The PM dispatches `MarkCompleted` once the destination Deposit
 * lands, or `MarkFailed` once a `WithdrawalRefused` lands. Both
 * are terminal — further mark-commands are rejected so the PM is
 * idempotent across redelivery.
 *
 * This shape lets the Transfers projection report a real outcome
 * for every transfer, and gives operators a clean stream-per-
 * transfer audit trail without scanning Account streams.
 */

import type { AggregateDefinition } from "instructed-sdk";

export type TransferStage = "none" | "requested" | "completed" | "failed";

export interface TransferState {
  stage: TransferStage;
  from?: string;
  to?: string;
  amount?: number;
  transferId?: string;
  reason?: string;
}

export interface TransferRequestedData {
  from: string;
  to: string;
  amount: number;
  transferId: string;
}

export type TransferEvent =
  | { type: "TransferRequested"; data: TransferRequestedData }
  | { type: "TransferCompleted"; data: { transferId: string } }
  | {
      type: "TransferFailed";
      data: { transferId: string; reason: string };
    };

export type TransferCommand =
  | {
      kind: "Request";
      from: string;
      to: string;
      amount: number;
      transferId: string;
    }
  | { kind: "MarkCompleted"; transferId: string }
  | { kind: "MarkFailed"; transferId: string; reason: string };

export const Transfer: AggregateDefinition<
  TransferState,
  TransferCommand,
  TransferEvent
> = {
  type: "Transfer",
  initialState: () => ({ stage: "none" }),

  apply(state, event) {
    switch (event.type) {
      case "TransferRequested":
        return {
          stage: "requested",
          from: event.data.from,
          to: event.data.to,
          amount: event.data.amount,
          transferId: event.data.transferId,
        };
      case "TransferCompleted":
        return { ...state, stage: "completed" };
      case "TransferFailed":
        return { ...state, stage: "failed", reason: event.data.reason };
    }
  },

  execute(state, command) {
    switch (command.kind) {
      case "Request":
        if (state.stage !== "none") {
          throw new Error(`transfer already ${state.stage}`);
        }
        return {
          type: "TransferRequested",
          data: {
            from: command.from,
            to: command.to,
            amount: command.amount,
            transferId: command.transferId,
          },
        };
      case "MarkCompleted":
        if (state.stage === "completed") return []; // idempotent
        if (state.stage !== "requested") {
          throw new Error(
            `cannot complete transfer in stage '${state.stage}'`,
          );
        }
        return {
          type: "TransferCompleted",
          data: { transferId: command.transferId },
        };
      case "MarkFailed":
        if (state.stage === "failed") return []; // idempotent
        if (state.stage !== "requested") {
          throw new Error(`cannot fail transfer in stage '${state.stage}'`);
        }
        return {
          type: "TransferFailed",
          data: { transferId: command.transferId, reason: command.reason },
        };
    }
  },
};
