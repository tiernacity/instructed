/**
 * Request a transfer between two accounts.
 *
 *   npm run transfer <from> <to> <amount>
 *
 * Each invocation creates a fresh transfer with a random
 * transferId so successive transfers between the same pair are
 * tracked independently by the PM and the Transfers projection.
 */

import { randomUUID } from 'node:crypto'

import { Instructed } from 'instructed-sdk'
import pg from 'pg'

import { Transfer } from '../src/aggregates/transfer.ts'
import { appCommandRouter } from '../src/command-router.ts'
import { RequestTransfer } from '../src/commands/transfer/index.ts'
import { PG_URL, requireArg } from '../src/common.ts'

async function main(): Promise<void> {
  const from = requireArg(process.argv, 2, 'from')
  const to = requireArg(process.argv, 3, 'to')
  const amount = Number(requireArg(process.argv, 4, 'amount'))
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error(`invalid amount: ${process.argv[4]}`)
  }

  const transferId = randomUUID()
  const pool = new pg.Pool({ connectionString: PG_URL })
  try {
    const app = new Instructed({ db: pool }).register(Transfer).register(appCommandRouter)
    await app.dispatch({
      type: RequestTransfer,
      transferId,
      from,
      to,
      amount,
    })
    process.stdout.write(
      `requested transfer ${transferId.slice(0, 8)}  ${from} -> ${to}  ${amount}\n`,
    )
  } finally {
    await pool.end()
  }
}

main().catch((err) => {
  process.stderr.write(`transfer failed: ${(err as Error).message}\n`)
  process.exit(1)
})
