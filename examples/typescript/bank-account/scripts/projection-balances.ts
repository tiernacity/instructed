/**
 * Runs the Balances projection in its own process. Periodically
 * reads the `bank_account.balances` table and prints it. Stop
 * with Ctrl-C.
 *
 *   npm run projection:balances
 *
 * Multiple copies of this process can run concurrently against
 * the same DB. Per-stream partitioning gives parallel projection
 * for unrelated accounts; the same account stays serial.
 */

import pg from "pg";
import { Instructed } from "instructed-sdk";
import { PG_URL, installSignalHandlers } from "../src/common.ts";
import { BALANCES_SUBSCRIPTION_NAME, balancesProjection, readBalances }
  from "../src/balances.ts";

const PRINT_INTERVAL_MS = 2_000;

async function main(): Promise<void> {
  // The projection's read-store pool is application-owned (D-0016):
  // it's *not* the SDK's connection, and the SDK gives the handler
  // no DB handle. We close it on shutdown.
  const pool = new pg.Pool({ connectionString: PG_URL });

  const app = new Instructed({ db: PG_URL });
  app.registerProjection(balancesProjection(pool), {
    pollInterval: 50,
    heartbeatInterval: 1_000,
    onError: (err: Error) =>
      process.stderr.write(`  [Balances error] ${err.message}\n`),
  });

  const worker = await app.startWorker();
  process.stdout.write(
    `[Balances] worker started; refreshing every ${PRINT_INTERVAL_MS}ms\n`,
  );

  const ticker = setInterval(async () => {
    try {
      const rows = await readBalances(pool);
      process.stdout.write("\x1b[2J\x1b[H");
      if (rows.length === 0) {
        process.stdout.write("[Balances] (empty)\n");
        return;
      }
      const lines = rows
        .map(
          (r) =>
            `  ${r.stream}${r.owner ? ` (${r.owner})` : ""}: ${r.balance}`,
        )
        .join("\n");
      process.stdout.write(`[Balances]\n${lines}\n`);
    } catch (err) {
      process.stderr.write(`[Balances] read failed: ${(err as Error).message}\n`);
    }
  }, PRINT_INTERVAL_MS);

  installSignalHandlers(async () => {
    clearInterval(ticker);
    await worker.close();
    await app.close();
    await pool.end();
  });

  await worker.stopped;
}

main().catch((err) => {
  process.stderr.write(
    `projection-balances failed: ${(err as Error).stack ?? err}\n`,
  );
  process.exit(1);
});
