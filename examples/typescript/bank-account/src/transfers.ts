/**
 * Transfers projection -- writes through to `bank_account.transfers`.
 *
 * Per-transfer partitioning: each transfer is its own partition,
 * so multiple concurrent transfers project in parallel across
 * however many workers are running. All three events for one
 * transfer (Requested / Completed / Failed) land on the same
 * partition, preserving order.
 *
 * Idempotency: `last_event_number` guard on every write.
 */

import type pg from "pg";
import type {
  ProjectionDefinition,
  RecordedEvent,
  RoutingFn,
} from "instructed-sdk";

export const TRANSFERS_SUBSCRIPTION_NAME = "Transfers";

const TRANSFER_EVENT_TYPES = new Set([
  "TransferRequested",
  "TransferCompleted",
  "TransferFailed",
]);

const transfersRouteFn: RoutingFn = (event) => {
  if (!TRANSFER_EVENT_TYPES.has(event.type)) return "ignore";
  const id = (event.data as { transferId?: string }).transferId;
  return id ? { partitionKey: id } : "ignore";
};

export function transfersProjection(
  pool: pg.Pool,
): ProjectionDefinition {
  return {
    type: TRANSFERS_SUBSCRIPTION_NAME,
    stream: "$all",
    routeFn: transfersRouteFn,
    async handler(event: RecordedEvent) {
      const n = event.event_number;
      const data = event.data as Record<string, unknown>;
      const transferId = data.transferId as string;

      switch (event.type) {
        case "TransferRequested": {
          const from = data.from as string;
          const to = data.to as string;
          const amount = data.amount as number;
          await pool.query(
            `insert into bank_account.transfers
               (transfer_id, from_account, to_account, amount, status,
                requested_at, last_event_number)
             values ($1, $2, $3, $4, 'requested', $5, $6)
             on conflict (transfer_id) do update
             set from_account = excluded.from_account,
                 to_account = excluded.to_account,
                 amount = excluded.amount,
                 requested_at = excluded.requested_at,
                 last_event_number = excluded.last_event_number
             where excluded.last_event_number
                 > bank_account.transfers.last_event_number`,
            [transferId, from, to, amount, event.created_at, n],
          );
          return;
        }
        case "TransferCompleted":
          await pool.query(
            `update bank_account.transfers
                set status = 'completed',
                    last_event_number = $2
              where transfer_id = $1
                and $2 > last_event_number`,
            [transferId, n],
          );
          return;
        case "TransferFailed":
          await pool.query(
            `update bank_account.transfers
                set status = 'failed',
                    reason = $2,
                    last_event_number = $3
              where transfer_id = $1
                and $3 > last_event_number`,
            [transferId, data.reason as string, n],
          );
          return;
      }
    },
  };
}

export interface TransferRow {
  transferId: string;
  from: string;
  to: string;
  amount: number;
  status: "requested" | "completed" | "failed";
  reason: string | null;
  requestedAt: Date;
}

/**
 * Returns the `limit` most-recent transfers, ordered newest-first
 * by `requested_at` (the timestamp of the originating
 * TransferRequested event).
 */
export async function readTransfers(
  pool: pg.Pool,
  limit = 5,
): Promise<TransferRow[]> {
  const r = await pool.query<{
    transfer_id: string;
    from_account: string;
    to_account: string;
    amount: string;
    status: TransferRow["status"];
    reason: string | null;
    requested_at: Date;
  }>(
    `select transfer_id, from_account, to_account, amount, status, reason,
            requested_at
       from bank_account.transfers
      order by requested_at desc
      limit $1`,
    [limit],
  );
  return r.rows.map((row) => ({
    transferId: row.transfer_id,
    from: row.from_account,
    to: row.to_account,
    amount: Number(row.amount),
    status: row.status,
    reason: row.reason,
    requestedAt: row.requested_at,
  }));
}
