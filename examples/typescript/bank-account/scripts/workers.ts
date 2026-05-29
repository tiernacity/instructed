/**
 * `npm run workers` -- one process that runs every long-lived
 * worker the application needs: both projections (Balances,
 * Transfers) and the TransferProcessManager.
 *
 *   npm run workers
 *
 * Per-worker partitioning still applies (per-stream for Balances,
 * per-transfer for Transfers and the PM), so multiple `npm run
 * workers` processes can be launched side by side for HA / scale.
 * Lease takeover and partition-level fan-out happen automatically.
 *
 * Observability is via the SDK's pluggable logger surface
 * (`instructed-sdk`'s `ILoggerImpl`). We wire a `trace` sink that
 * filters down to the per-event "work done" lines so the terminal
 * shows a heartbeat of activity without the deeper poll-loop
 * chatter. `info` / `warn` / `error` go to the corresponding
 * `console` methods. With a non-`trace` logger the SDK pays only
 * the cost of an arrow allocation per skipped trace site
 * (`logger.ts`).
 */

import { Instructed } from 'instructed-sdk'
import pg from 'pg'

import { Account } from '../src/aggregates/account.ts'
import { Transfer } from '../src/aggregates/transfer.ts'
import { appCommandRouter } from '../src/command-router.ts'
import { PG_URL, waitForShutdown } from '../src/common.ts'
import { transferProcessManager } from '../src/process-managers/transfer-pm.ts'
import { balancesProjection } from '../src/projections/balances/projection.ts'
import { transfersProjection } from '../src/projections/transfers/projection.ts'

/**
 * Filter the SDK's `trace` stream down to lines that announce
 * completed work. The SDK emits a handful of trace shapes; we
 * keep the ones a developer reading the log most plausibly wants
 * to see ("a PM handled an event and dispatched N commands"; "a
 * PM dispatched a specific command"). Polling / claim / release
 * chatter is dropped.
 */
function isWorkDone(msg: string): boolean {
  return msg.includes('pm handle:') || msg.includes('pm dispatch:')
}

async function main(): Promise<void> {
  const pool = new pg.Pool({ connectionString: PG_URL })
  try {
    const app = new Instructed({
      db: pool,
      logger: {
        info: (m) => process.stdout.write(`${m}\n`),
        warn: (m) => process.stderr.write(`WARN  ${m}\n`),
        error: (m) => process.stderr.write(`ERROR ${m}\n`),
        trace: (m) => {
          if (isWorkDone(m)) process.stdout.write(`${m}\n`)
        },
      },
    })
      // Aggregates + command router: the PM needs these registered
      // so its lean `DispatchedCommand`s resolve to the right
      // aggregate and stream.
      .register(Account)
      .register(Transfer)
      .register(appCommandRouter)
      // Projections: each owns its read-store writes via the
      // application-supplied pool (D-0016).
      .register(balancesProjection(pool), {
        onError: (err: Error) => process.stderr.write(`  [Balances error] ${err.message}\n`),
      })
      .register(transfersProjection(pool), {
        onError: (err: Error) => process.stderr.write(`  [Transfers error] ${err.message}\n`),
      })
      // Process manager: saga state lives in `instructed.snapshots`
      // via the PM-C primitive, so multi-process is safe with no
      // extra bookkeeping.
      .register(transferProcessManager(), {
        onError: (err: Error) => process.stderr.write(`  [PM error] ${err.message}\n`),
      })

    const worker = await app.poll({
      defaults: { pollInterval: 50, heartbeatInterval: 1_000 },
    })
    process.stdout.write('workers started -- Ctrl-C to stop\n')

    await Promise.race([waitForShutdown(), worker.stopped])
    await worker.stop()
  } finally {
    await pool.end()
  }
}

main().catch((err) => {
  process.stderr.write(`workers failed: ${(err as Error).stack ?? err}\n`)
  process.exit(1)
})
