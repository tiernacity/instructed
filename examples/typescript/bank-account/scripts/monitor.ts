/**
 * `npm run monitor` -- poll the two projection read-stores and
 * keep the terminal up to date with the current accounts /
 * balances and the most recent transfers.
 *
 *   npm run monitor
 *
 * Read-only: the monitor never writes. Run it alongside
 * `npm run workers` (which produces the read-store rows the
 * monitor displays). Stop with Ctrl-C.
 *
 * The monitor is intentionally not an SDK consumer -- it doesn't
 * register aggregates / projections / PMs, and it never touches
 * the `instructed.*` event store. It is a pure application-side
 * view over the read-store tables (`bank_account.balances` and
 * `bank_account.transfers`), which is exactly what a real
 * application's CLI / web dashboard would be.
 */

import pg from 'pg'

import { PG_URL, waitForShutdown } from '../src/common.ts'
import { readBalances } from '../src/projections/balances/queries.ts'
import { readTransfers, type TransferRow } from '../src/projections/transfers/queries.ts'

const POLL_INTERVAL_MS = 1_000
const RECENT_TRANSFERS = 10

function renderBalances(rows: { stream: string; owner: string | null; balance: number }[]): string {
  if (rows.length === 0) return '  (no accounts yet)'
  const w = rows.reduce((m, r) => Math.max(m, (r.owner ?? r.stream).length), 0)
  return rows
    .map((r) => {
      const label = (r.owner ?? r.stream).padEnd(w)
      return `  ${label}  ${r.balance.toString().padStart(10)}`
    })
    .join('\n')
}

function renderTransfers(rows: TransferRow[]): string {
  if (rows.length === 0) return '  (no transfers yet)'
  return rows
    .map((r) => {
      const t = r.requestedAt.toISOString().slice(11, 19) // HH:MM:SS
      const id = r.transferId.slice(0, 8)
      const status = r.status === 'failed' ? `failed (${r.reason ?? '?'})` : r.status
      return `  ${t}  ${id}  ${r.from} -> ${r.to}  ${r.amount}  ${status}`
    })
    .join('\n')
}

async function tick(pool: pg.Pool): Promise<void> {
  // Both queries in parallel; each owns its connection from the
  // pool. A failure on one still renders the other.
  const [balancesR, transfersR] = await Promise.allSettled([
    readBalances(pool),
    readTransfers(pool, RECENT_TRANSFERS),
  ])

  // Clear screen + home cursor.
  process.stdout.write('\x1b[2J\x1b[H')
  process.stdout.write(`bank-account monitor  --  ${new Date().toISOString()}\n`)
  process.stdout.write(`source: ${PG_URL}\n\n`)

  process.stdout.write('Balances\n')
  if (balancesR.status === 'fulfilled') {
    process.stdout.write(`${renderBalances(balancesR.value)}\n`)
  } else {
    process.stdout.write(`  [error] ${balancesR.reason}\n`)
  }

  process.stdout.write(`\nRecent transfers (latest ${RECENT_TRANSFERS})\n`)
  if (transfersR.status === 'fulfilled') {
    process.stdout.write(`${renderTransfers(transfersR.value)}\n`)
  } else {
    process.stdout.write(`  [error] ${transfersR.reason}\n`)
  }
}

async function main(): Promise<void> {
  const pool = new pg.Pool({ connectionString: PG_URL })
  let stopping = false
  try {
    // First render immediately so the user sees something on launch
    // even before the first interval elapses.
    await tick(pool)
    const timer = setInterval(() => {
      if (stopping) return
      tick(pool).catch((err) => {
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- caught value coerced to Error for diagnostic output.
        process.stderr.write(`monitor tick failed: ${(err as Error).message}\n`)
      })
    }, POLL_INTERVAL_MS)
    await waitForShutdown()
    stopping = true
    clearInterval(timer)
  } finally {
    await pool.end()
  }
}

main().catch((err) => {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- caught value coerced to Error for diagnostic output.
  process.stderr.write(`monitor failed: ${(err as Error).stack ?? err}\n`)
  process.exit(1)
})
