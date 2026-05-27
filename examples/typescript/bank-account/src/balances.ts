/**
 * Balances projection -- writes through to `bank_account.balances`.
 *
 * Per-stream partitioning: the routing layer assigns one partition
 * per account stream. Events for the same account stay serial
 * (correctness); events for different accounts run concurrently
 * across however many processing workers you've spun up.
 *
 * Idempotency: every write guards on `last_event_number`, so a
 * redelivered event is a no-op regardless of how the SDK's
 * at-least-once contract has delivered it.
 *
 * Per D-0016 the projection handler receives no DB handle from
 * the SDK -- the application owns its read-store connection.
 * The handler closes over a `pg.Pool` supplied by the caller
 * (`scripts/projection-balances.ts`).
 */

import type pg from "pg";
import type {
  ProjectionDefinition,
  RecordedEvent,
  RoutingFn,
} from "instructed-sdk";

export const BALANCES_SUBSCRIPTION_NAME = "Balances";

const BALANCES_EVENT_TYPES = new Set([
  "AccountOpened",
  "Deposited",
  "Withdrawn",
]);

/**
 * Per-stream routing: one partition key per account. Different
 * accounts can be projected concurrently across multiple processing
 * workers; the same account is always serial.
 */
const balancesRouteFn: RoutingFn = (event) =>
  BALANCES_EVENT_TYPES.has(event.type)
    ? { partitionKey: event.stream_uuid }
    : "ignore";

export function balancesProjection(
  pool: pg.Pool,
): ProjectionDefinition {
  return {
    type: BALANCES_SUBSCRIPTION_NAME,
    stream: "$all",
    routeFn: balancesRouteFn,
    async handler(event: RecordedEvent) {
      const n = event.event_number;
      const stream = event.stream_uuid;

      switch (event.type) {
        case "AccountOpened": {
          const { owner } = event.data as { owner: string };
          // First event on the stream -- INSERT, with the same
          // last_event_number guard in the ON CONFLICT branch in
          // case redelivery races against a later event already
          // landed.
          await pool.query(
            `insert into bank_account.balances
               (stream_uuid, owner, balance, last_event_number)
             values ($1, $2, 0, $3)
             on conflict (stream_uuid) do update
             set owner = excluded.owner,
                 last_event_number = excluded.last_event_number
             where excluded.last_event_number
                 > bank_account.balances.last_event_number`,
            [stream, owner, n],
          );
          return;
        }
        case "Deposited": {
          const { amount } = event.data as { amount: number };
          await pool.query(
            `update bank_account.balances
                set balance = balance + $2,
                    last_event_number = $3
              where stream_uuid = $1
                and $3 > last_event_number`,
            [stream, amount, n],
          );
          return;
        }
        case "Withdrawn": {
          const { amount } = event.data as { amount: number };
          await pool.query(
            `update bank_account.balances
                set balance = balance - $2,
                    last_event_number = $3
              where stream_uuid = $1
                and $3 > last_event_number`,
            [stream, amount, n],
          );
          return;
        }
      }
    },
  };
}

/**
 * Read the current balances view. Sorted by stream_uuid for stable
 * CLI rendering.
 */
export async function readBalances(
  pool: pg.Pool,
): Promise<{ stream: string; owner: string | null; balance: number }[]> {
  const r = await pool.query<{
    stream_uuid: string;
    owner: string | null;
    balance: string;
  }>(
    `select stream_uuid, owner, balance
       from bank_account.balances
      order by stream_uuid`,
  );
  return r.rows.map((row) => ({
    stream: row.stream_uuid,
    owner: row.owner,
    balance: Number(row.balance),
  }));
}
