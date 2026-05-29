/**
 * Read-store queries and write helpers for the Balances projection.
 *
 * Pulled into its own file so the projection definition (which
 * lives next door in `projection.ts`) reads as the *shape* of the
 * projection — what events it consumes and how it partitions —
 * while the SQL lives here. Per D-0016 the projection handler
 * receives no DB handle from the SDK; the application owns its
 * read-store connection. The handler closes over a `pg.Pool`
 * supplied by the script that constructs it.
 */

import type pg from 'pg'

/**
 * Upsert a balances row for a newly-opened account. The
 * ON CONFLICT branch carries the same `last_event_number` guard
 * the UPDATE helpers use, so a redelivered AccountOpened lands as
 * a no-op if a later event has already advanced the row.
 */
export async function upsertOpened(
  pool: pg.Pool,
  args: { streamUuid: string; owner: string; eventNumber: bigint },
): Promise<void> {
  await pool.query(
    `insert into bank_account.balances
       (stream_uuid, owner, balance, last_event_number)
     values ($1, $2, 0, $3)
     on conflict (stream_uuid) do update
     set owner = excluded.owner,
         last_event_number = excluded.last_event_number
     where excluded.last_event_number
         > bank_account.balances.last_event_number`,
    [args.streamUuid, args.owner, args.eventNumber],
  )
}

/** Add `delta` to the balance, guarded by last_event_number. */
export async function bumpBalance(
  pool: pg.Pool,
  args: { streamUuid: string; delta: number; eventNumber: bigint },
): Promise<void> {
  await pool.query(
    `update bank_account.balances
        set balance = balance + $2,
            last_event_number = $3
      where stream_uuid = $1
        and $3 > last_event_number`,
    [args.streamUuid, args.delta, args.eventNumber],
  )
}

/**
 * Read the current balances view. Sorted by stream_uuid for stable
 * CLI rendering.
 */
export async function readBalances(
  pool: pg.Pool,
): Promise<{ stream: string; owner: string | null; balance: number }[]> {
  const r = await pool.query<{
    stream_uuid: string
    owner: string | null
    balance: string
  }>(
    `select stream_uuid, owner, balance
       from bank_account.balances
      order by stream_uuid`,
  )
  return r.rows.map((row) => ({
    stream: row.stream_uuid,
    owner: row.owner,
    balance: Number(row.balance),
  }))
}
