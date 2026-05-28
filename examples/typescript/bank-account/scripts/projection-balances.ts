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
import { PG_URL, waitForShutdown } from "../src/common.ts";
import { balancesProjection } from "../src/projections/balances/projection.ts";
import { readBalances } from "../src/projections/balances/queries.ts";

const PRINT_INTERVAL_MS = 2_000;

async function main(): Promise<void> {
  // One application-owned pool, shared by the SDK (event store) and
  // the projection handler (read store). The SDK is opaque to the
  // pool's other uses; the application is responsible for `pool.end()`
  // on shutdown.
  const pool = new pg.Pool({ connectionString: PG_URL });
  try {
    const app = new Instructed({ db: pool }).register(balancesProjection(pool), {
      onError: (err: Error) =>
        process.stderr.write(`  [Balances error] ${err.message}\n`),
    });

    const worker = await app.poll({
      defaults: { pollInterval: 50, heartbeatInterval: 1_000 },
    });
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
        process.stderr.write(
          `[Balances] read failed: ${(err as Error).message}\n`,
        );
      }
    }, PRINT_INTERVAL_MS);

    // Exit on whichever happens first: a shutdown signal, or the
    // worker terminating on its own (e.g. an unrecoverable error
    // escaped its error policy).
    await Promise.race([waitForShutdown(), worker.stopped]);
    clearInterval(ticker);
    await worker.stop();
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  process.stderr.write(
    `projection-balances failed: ${(err as Error).stack ?? err}\n`,
  );
  process.exit(1);
});
