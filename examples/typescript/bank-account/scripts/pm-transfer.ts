/**
 * Runs the TransferProcessManager in its own process. The PM
 * needs Account + Transfer aggregates registered and the
 * application's command router registered, because its `handle`
 * returns lean commands (bare `Command` objects) that the PM
 * worker resolves via the router.
 *
 *   npm run pm:transfer
 *
 * Logs every event the PM handles (with the commands it produced
 * and whether the partition completed) and every error escaping
 * the handler. With the SDK's default error policy (exponential
 * backoff, retry forever) an error here means the PM is stuck
 * retrying that step until the underlying problem is fixed.
 */

import pg from "pg";
import { Instructed } from "instructed-sdk";
import type {
  DispatchedCommand,
  DispatchedCommandExplicit,
  ProcessManagerDefinition,
} from "instructed-sdk";

/** Structural guard for the explicit dispatch shape. */
function isExplicit(c: DispatchedCommand): c is DispatchedCommandExplicit {
  return (
    typeof (c as DispatchedCommandExplicit).streamUuid === "string" &&
    typeof (c as DispatchedCommandExplicit).aggregate === "object" &&
    (c as DispatchedCommandExplicit).aggregate !== null
  );
}

import { PG_URL, waitForShutdown } from "../src/common.ts";
import { Account } from "../src/aggregates/account.ts";
import { Transfer } from "../src/aggregates/transfer.ts";
import { appCommandRouter } from "../src/command-router.ts";
import {
  TransferProcessManager,
  transferProcessManager,
  type TransferPmStage,
} from "../src/process-managers/transfer-pm.ts";

/**
 * Wrap the domain PM definition with a trace layer. The wrapping
 * lives in the script (not in the PM file) because logging is a
 * deployment concern, not a domain concern.
 */
function withTrace<E extends import("instructed-sdk").Event>(
  base: ProcessManagerDefinition<TransferPmStage, E>,
): ProcessManagerDefinition<TransferPmStage, E> {
  const baseHandle = base.handle;
  return {
    ...base,
    async handle(state, event, ctx) {
      const tag = `${event.type}#${event.event_number}`;
      try {
        const result = await baseHandle(state, event, ctx);
        const cmds = result.commands ?? [];
        const cmdSummary =
          cmds
            .map((c: DispatchedCommand) =>
              // PM emits lean commands (no `aggregate` field);
              // the type alone is enough to identify it in logs.
              isExplicit(c)
                ? `${c.aggregate.type}.${(c.command as { type?: string }).type ?? "?"}`
                : String(c.type),
            )
            .join(", ") || "(no commands)";
        const tail = result.complete ? " [complete]" : "";
        process.stdout.write(`  ${tag} -> ${cmdSummary}${tail}\n`);
        return result;
      } catch (err) {
        process.stderr.write(
          `  ${tag} !! ${(err as Error).message} (will retry per error policy)\n`,
        );
        throw err;
      }
    },
  };
}

async function main(): Promise<void> {
  const pool = new pg.Pool({ connectionString: PG_URL });
  try {
    const app = new Instructed({ db: pool })
      .register(Account)
      .register(Transfer)
      .register(appCommandRouter)
      .register(withTrace(transferProcessManager()), {
        pollInterval: 50,
        heartbeatInterval: 1_000,
        onError: (err: Error) =>
          process.stderr.write(`  [PM error] ${err.message}\n`),
      });

    const worker = await app.poll();
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
