/**
 * Balances projection — a `$all` projection that keeps a per-account
 * balance in memory.
 *
 * In production this would be an idempotent UPSERT into a real
 * read-store (Postgres, Elasticsearch, Redis, etc.); the in-memory
 * map is sufficient for the example and shows that the SDK has no
 * opinion about projection storage. Per D-0016 the projection
 * handler is opaque to the SDK -- the handler receives the event
 * and a minimal context (no tx, no Queryable, no framework-owned
 * resource).
 *
 * SUB-A registration shape (PRJ-A):
 *   - The legacy `selector` parameter is replaced by a `routeFn`
 *     that returns `"ignore"` for events we don't care about and
 *     `{ partitionKey }` for events we do. Returning the same
 *     partition for every routed event preserves the
 *     strict-sequential delivery the legacy projection had.
 *   - `handler` is the renamed `handle`.
 *
 * Idempotency: the `lastEventByAccount` guard skips redeliveries
 * (at-least-once delivery is part of the contract).
 */

import type {
  RecordedEvent,
  RegisterProjectionInput,
  RoutingFn,
} from "../../sdks/typescript/src/index.ts";

export interface BalancesView {
  /** account stream uuid -> current balance */
  balance: Map<string, number>;
  /** account stream uuid -> the last event_number folded in */
  lastEventByAccount: Map<string, bigint>;
}

export function newBalancesView(): BalancesView {
  return { balance: new Map(), lastEventByAccount: new Map() };
}

const BALANCES_EVENT_TYPES = new Set([
  "AccountOpened",
  "Deposited",
  "Withdrawn",
]);

/**
 * Routing-side filter (the legacy `selector`'s role). Sequential
 * partition: every routed event lands on the same partition key,
 * so the processing worker delivers them in event_number order
 * with no per-event parallelism. Equivalent to the legacy
 * single-cursor projection's strict-serial guarantee.
 */
const balancesRouteFn: RoutingFn = (event) =>
  BALANCES_EVENT_TYPES.has(event.event_type)
    ? { partitionKey: "_default" }
    : "ignore";

/**
 * Build the registration payload for `Instructed.registerProjection`.
 * The `view` is captured by the handler closure; the SDK never
 * touches it.
 */
export function balancesProjection(
  view: BalancesView,
): RegisterProjectionInput {
  return {
    stream: "$all",
    routeFn: balancesRouteFn,
    async handler(event: RecordedEvent) {
      const last = view.lastEventByAccount.get(event.stream_uuid) ?? -1n;
      if (event.event_number <= last) return; // idempotent: skip
      const data = event.data as { amount?: number };
      switch (event.event_type) {
        case "AccountOpened":
          if (!view.balance.has(event.stream_uuid)) {
            view.balance.set(event.stream_uuid, 0);
          }
          break;
        case "Deposited": {
          const cur = view.balance.get(event.stream_uuid) ?? 0;
          view.balance.set(event.stream_uuid, cur + (data.amount ?? 0));
          break;
        }
        case "Withdrawn": {
          const cur = view.balance.get(event.stream_uuid) ?? 0;
          view.balance.set(event.stream_uuid, cur - (data.amount ?? 0));
          break;
        }
      }
      view.lastEventByAccount.set(event.stream_uuid, event.event_number);
    },
  };
}

/** Subscription name -- exported so consumers can wait on it. */
export const BALANCES_SUBSCRIPTION_NAME = "Balances";
