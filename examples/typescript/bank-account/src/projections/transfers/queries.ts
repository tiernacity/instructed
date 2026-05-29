/**
 * Read-store queries and write helpers for the Transfers projection.
 *
 * Same idempotency story as the Balances queries: every write
 * guards on `last_event_number`. See `projection.ts` next door for
 * the projection definition.
 */

import type pg from 'pg'

export interface TransferRow {
  transferId: string
  from: string
  to: string
  amount: number
  status: 'requested' | 'completed' | 'failed'
  reason: string | null
  requestedAt: Date
}

/** UPSERT a transfer row when `TransferRequested` is observed. */
export async function upsertRequested(
  pool: pg.Pool,
  args: {
    transferId: string
    from: string
    to: string
    amount: number
    requestedAt: Date
    eventNumber: bigint
  },
): Promise<void> {
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
    [args.transferId, args.from, args.to, args.amount, args.requestedAt, args.eventNumber],
  )
}

/** Flip the status to 'completed', guarded by last_event_number. */
export async function markCompleted(
  pool: pg.Pool,
  args: { transferId: string; eventNumber: bigint },
): Promise<void> {
  await pool.query(
    `update bank_account.transfers
        set status = 'completed',
            last_event_number = $2
      where transfer_id = $1
        and $2 > last_event_number`,
    [args.transferId, args.eventNumber],
  )
}

/** Flip the status to 'failed' (with reason), guarded. */
export async function markFailed(
  pool: pg.Pool,
  args: { transferId: string; reason: string; eventNumber: bigint },
): Promise<void> {
  await pool.query(
    `update bank_account.transfers
        set status = 'failed',
            reason = $2,
            last_event_number = $3
      where transfer_id = $1
        and $3 > last_event_number`,
    [args.transferId, args.reason, args.eventNumber],
  )
}

/**
 * Returns the `limit` most-recent transfers, ordered newest-first
 * by `requested_at` (the timestamp of the originating
 * TransferRequested event).
 */
export async function readTransfers(pool: pg.Pool, limit = 5): Promise<TransferRow[]> {
  const r = await pool.query<{
    transfer_id: string
    from_account: string
    to_account: string
    amount: string
    status: TransferRow['status']
    reason: string | null
    requested_at: Date
  }>(
    `select transfer_id, from_account, to_account, amount, status, reason,
            requested_at
       from bank_account.transfers
      order by requested_at desc
      limit $1`,
    [limit],
  )
  return r.rows.map((row) => ({
    transferId: row.transfer_id,
    from: row.from_account,
    to: row.to_account,
    amount: Number(row.amount),
    status: row.status,
    reason: row.reason,
    requestedAt: row.requested_at,
  }))
}
