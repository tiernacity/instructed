/**
 * Runs the Transfers projection in its own process. Periodically
 * reads the `bank_account.transfers` table and prints it. Stop
 * with Ctrl-C.
 *
 *   npm run projection:transfers
 *
 * Multiple copies can run concurrently; per-transfer partitioning
 * lets unrelated transfers project in parallel.
 */

import pg from "pg";
import { Instructed } from "instructed-sdk";
import { PG_URL, installSignalHandlers } from "../src/common.ts";
import { transfersProjection } from "../src/projections/transfers/projection.ts";
import { readTransfers } from "../src/projections/transfers/queries.ts";

const PRINT_INTERVAL_MS = 2_000;

async function main(): Promise<void> {
  const pool = new pg.Pool({ connectionString: PG_URL });

  const app = new Instructed({ db: PG_URL });
  app.registerProjection(transfersProjection(pool), {
    pollInterval: 50,
    heartbeatInterval: 1_000,
    onError: (err: Error) =>
      process.stderr.write(`  [Transfers error] ${err.message}\n`),
  });

  const worker = await app.startWorker();
  process.stdout.write(
    `[Transfers] worker started; refreshing every ${PRINT_INTERVAL_MS}ms\n`,
  );

  const ticker = setInterval(async () => {
    try {
      const rows = await readTransfers(pool, 5);
      process.stdout.write("\x1b[2J\x1b[H");
      if (rows.length === 0) {
        process.stdout.write("[Transfers] (empty)\n");
        return;
      }
      const lines = rows
        .map((r) => {
          const t = r.requestedAt.toISOString().slice(11, 19); // HH:MM:SS
          const tail = r.status === "failed" ? ` (${r.reason ?? "?"})` : "";
          return `  ${t}  ${r.transferId.slice(0, 8)}  ${r.from} -> ${r.to}  ${r.amount}  ${r.status}${tail}`;
        })
        .join("\n");
      process.stdout.write(`[Transfers] (5 most recent)\n${lines}\n`);
    } catch (err) {
      process.stderr.write(
        `[Transfers] read failed: ${(err as Error).message}\n`,
      );
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
    `projection-transfers failed: ${(err as Error).stack ?? err}\n`,
  );
  process.exit(1);
});
