/**
 * Runs the TransferProcessManager in its own process. The PM
 * needs Account + Transfer aggregates registered and the
 * application's command router registered, because its `handle`
 * returns lean commands (bare `Command` objects) that the PM
 * worker resolves via the router.
 *
 *   npm run pm:transfer
 *
 * Per-event observability (commands emitted, completion flag,
 * handler errors) is provided by the SDK's logger surface: a
 * wired `trace` sink sees `pm handle: ...` and `pm dispatch: ...`
 * lines per event, and the registered `onError` callback fires
 * on every escaped handler exception. With the SDK's default
 * error policy (exponential backoff, retry forever) an error
 * here means the PM is stuck retrying that step until the
 * underlying problem is fixed.
 */

import pg from "pg";
import { Instructed } from "instructed-sdk";

import { PG_URL, waitForShutdown } from "../src/common.ts";
import { Account } from "../src/aggregates/account.ts";
import { Transfer } from "../src/aggregates/transfer.ts";
import { appCommandRouter } from "../src/command-router.ts";
import {
  TransferProcessManager,
  transferProcessManager,
} from "../src/process-managers/transfer-pm.ts";

async function main(): Promise<void> {
  const pool = new pg.Pool({ connectionString: PG_URL });
  try {
    const app = new Instructed({ db: pool })
      .register(Account)
      .register(Transfer)
      .register(appCommandRouter)
      .register(transferProcessManager(), {
        onError: (err: Error) =>
          process.stderr.write(`  [PM error] ${err.message}\n`),
      });

    const worker = await app.poll({
      defaults: { pollInterval: 50, heartbeatInterval: 1_000 },
    });
    process.stdout.write(`[${TransferProcessManager}] worker started\n`);

    await Promise.race([waitForShutdown(), worker.stopped]);
    await worker.stop();
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  process.stderr.write(`pm-transfer failed: ${(err as Error).stack ?? err}\n`);
  process.exit(1);
});
