/**
 * Balances projection — a $all projection that keeps a per-account
 * balance in memory. In production this would be an idempotent UPSERT
 * into Postgres / Elasticsearch / wherever; the in-memory map is
 * sufficient for the example and shows that the SDK has no opinion
 * about projection storage.
 *
 * Idempotency: the `lastEventByAccount` guard skips redeliveries.
 */

import type { ProjectionDefinition, RecordedEvent } from "../../sdks/typescript/src/index.ts";

export interface BalancesView {
  /** account stream uuid -> current balance */
  balance: Map<string, number>;
  /** account stream uuid -> the last event_number folded in */
  lastEventByAccount: Map<string, bigint>;
}

export function newBalancesView(): BalancesView {
  return { balance: new Map(), lastEventByAccount: new Map() };
}

export function balancesProjection(
  view: BalancesView,
  name = "Balances",
): ProjectionDefinition {
  return {
    name,
    stream: "$all",
    selector: (e) =>
      e.event_type === "AccountOpened" ||
      e.event_type === "Deposited" ||
      e.event_type === "Withdrawn",
    async handle(event: RecordedEvent) {
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
