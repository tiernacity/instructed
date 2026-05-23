/**
 * Transfer aggregate — emits the `TransferRequested` event that
 * kicks off the TransferProcessManager. Trivial; exists so the
 * example never appends events outside an aggregate.
 */

import type { AggregateDefinition } from "../../sdks/typescript/src/index.ts";

export interface TransferState {
  requested: boolean;
}

export interface TransferRequestedData {
  from: string;
  to: string;
  amount: number;
  transferId: string;
}

export type TransferEvent = {
  type: "TransferRequested";
  data: TransferRequestedData;
};

export type TransferCommand = {
  kind: "Request";
  from: string;
  to: string;
  amount: number;
  transferId: string;
};

export const Transfer: AggregateDefinition<
  TransferState,
  TransferCommand,
  TransferEvent
> = {
  type: "Transfer",
  initialState: () => ({ requested: false }),
  apply(state, event) {
    if (event.type === "TransferRequested") return { requested: true };
    return state;
  },
  execute(state, command) {
    if (state.requested) throw new Error("transfer already requested");
    const data: TransferRequestedData = {
      from: command.from,
      to: command.to,
      amount: command.amount,
      transferId: command.transferId,
    };
    return {
      event_type: "TransferRequested",
      data,
    };
  },
};
