/**
 * Balances projection — writes through to `bank_account.balances`.
 *
 * Per-stream partitioning: the routing layer assigns one partition
 * per account stream. Events for the same account stay serial
 * (correctness); events for different accounts run concurrently
 * across however many processing workers you've spun up.
 *
 * Idempotency: every write guards on `last_event_number`, so a
 * redelivered event is a no-op regardless of how the SDK's
 * at-least-once contract has delivered it. The SQL guards live
 * in `queries.ts`.
 *
 * Per D-0016 the projection handler receives no DB handle from
 * the SDK; the application owns its read-store connection. The
 * handler closes over a `pg.Pool` supplied by the caller
 * (`scripts/projection-balances.ts`).
 */

import { onlyTypes } from "instructed-sdk";
import type { ProjectionDefinition, RoutingFn } from "instructed-sdk";
import type pg from "pg";

import {
  type AccountEvent,
  AccountOpened,
  AccountDepositedTo,
  AccountWithdrawnFrom,
} from "../../events/account/index.ts";
import { bumpBalance, upsertOpened } from "./queries.ts";

export const Balances = "Balances" as const;

/**
 * Per-stream routing: one partition key per account, restricted to
 * the three event types we care about. `onlyTypes` builds the
 * allow-set once at factory time; per-event cost is one Set lookup.
 */
const balancesRouteFn: RoutingFn<AccountEvent> = onlyTypes<AccountEvent>(
  [AccountOpened, AccountDepositedTo, AccountWithdrawnFrom],
  (event) => ({ partitionKey: event.stream_uuid }),
);

export function balancesProjection(
  pool: pg.Pool,
): ProjectionDefinition<AccountEvent> {
  return {
    type: Balances,
    stream: "$all",
    routeFn: balancesRouteFn,
    async handler(event) {
      const n = event.event_number;
      const streamUuid = event.stream_uuid;
      switch (event.type) {
        case AccountOpened:
          // First event on the stream — UPSERT; the ON CONFLICT
          // branch uses the same last_event_number guard so
          // redelivery is a no-op once later events have advanced
          // the row.
          await upsertOpened(pool, {
            streamUuid,
            owner: event.data.owner,
            eventNumber: n,
          });
          return;
        case AccountDepositedTo:
          await bumpBalance(pool, {
            streamUuid,
            delta: event.data.amount,
            eventNumber: n,
          });
          return;
        case AccountWithdrawnFrom:
          await bumpBalance(pool, {
            streamUuid,
            delta: -event.data.amount,
            eventNumber: n,
          });
          return;
      }
    },
  };
}
