/**
 * Account aggregate — the canonical bank-account example.
 *
 * Pure domain folds. The SDK tracks version; the user code reads no
 * connection / transaction handle. `WithdrawalRefused` is an emitted
 * event, not a thrown error: it records that a withdrawal was tried
 * and refused so the TransferProcessManager can react via its routes.
 */

import type { AggregateDefinition } from "instructed-sdk";

export interface AccountState {
  opened: boolean;
  owner: string | null;
  balance: number;
}

export type AccountEvent =
  | { type: "AccountOpened"; data: { owner: string } }
  | { type: "Deposited"; data: { amount: number; transferId?: string } }
  | { type: "Withdrawn"; data: { amount: number; transferId?: string; to?: string } }
  | {
      type: "WithdrawalRefused";
      data: { reason: string; amount: number; transferId?: string };
    };

export type AccountCommand =
  | { kind: "Open"; owner: string }
  | { kind: "Deposit"; amount: number; transferId?: string }
  | { kind: "Withdraw"; amount: number; transferId?: string; to?: string };

export const Account: AggregateDefinition<
  AccountState,
  AccountCommand,
  AccountEvent
> = {
  type: "Account",
  initialState: () => ({ opened: false, owner: null, balance: 0 }),

  apply(state, event) {
    switch (event.type) {
      case "AccountOpened":
        return { ...state, opened: true, owner: event.data.owner };
      case "Deposited":
        return { ...state, balance: state.balance + event.data.amount };
      case "Withdrawn":
        return { ...state, balance: state.balance - event.data.amount };
      case "WithdrawalRefused":
        return state;
    }
  },

  execute(state, command) {
    switch (command.kind) {
      case "Open":
        if (state.opened) throw new Error("account already open");
        return {
          event_type: "AccountOpened",
          data: { owner: command.owner },
        };
      case "Deposit":
        if (!state.opened) throw new Error("account not open");
        return {
          event_type: "Deposited",
          data: { amount: command.amount, transferId: command.transferId },
        };
      case "Withdraw":
        if (!state.opened) throw new Error("account not open");
        if (state.balance < command.amount) {
          // Compensation per D-0011: refusal is a domain event, not
          // an exception; the TransferProcessManager observes it via
          // its routes and stops the process without ever needing
          // a compensating command.
          return {
            event_type: "WithdrawalRefused",
            data: {
              reason: "insufficient funds",
              amount: command.amount,
              transferId: command.transferId,
            },
          };
        }
        return {
          event_type: "Withdrawn",
          data: {
            amount: command.amount,
            transferId: command.transferId,
            to: command.to,
          },
        };
    }
  },
};
