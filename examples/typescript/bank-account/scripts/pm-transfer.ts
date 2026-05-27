/**
 * Runs the TransferProcessManager in its own process. The PM
 * needs the Account and Transfer aggregates registered because
 * it dispatches commands against them.
 *
 *   npm run pm:transfer
 *
 * Logs every event the PM handles (with the commands it produced
 * and whether the partition completed) and every error escaping
 * the handler. With the SDK's default error policy (exponential
 * backoff, retry forever) an error here means the PM is stuck
 * retrying that step until the underlying problem -- e.g. a
 * destination account that was never opened -- is fixed.
 */

import { Instructed } from "instructed-sdk";
import type {
  DispatchedCommand,
  ProcessManagerDefinition,
} from "instructed-sdk";
import { PG_URL, installSignalHandlers } from "../src/common.ts";
import { Account } from "../src/account.ts";
import { Transfer } from "../src/transfer.ts";
import {
  TRANSFER_PM_NAME,
  transferProcessManager,
  type TransferPmStage,
} from "../src/transfer-pm.ts";

/**
 * Wrap the domain PM definition with a trace layer. The wrapping
 * lives in the script (not in `transfer-pm.ts`) because logging
 * is a deployment concern, not a domain concern.
 */
function withTrace(
  base: ProcessManagerDefinition<TransferPmStage>,
): ProcessManagerDefinition<TransferPmStage> {
  const baseHandle = base.handle;
  return {
    ...base,
    async handle(state, event, ctx) {
      const tag = `${event.type}#${event.event_number}`;
      try {
        const result = await baseHandle(state, event, ctx);
        const cmds = result.commands ?? [];
        const cmdSummary = cmds
          .map((c: DispatchedCommand) => {
            // DispatchedCommand is a union: explicit
            // (`{ aggregate, streamUuid, command }`) or lean
            // (a bare `Command` resolved via the router at
            // dispatch time). The trace logger handles both.
            if ("aggregate" in c) {
              const k = (c.command as { kind?: string }).kind ?? "?";
              return `${c.aggregate.type}.${k}(${c.streamUuid})`;
            }
            return `${c.type}`;
          })
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
  const app = new Instructed({ db: PG_URL });
  app.registerAggregate(Account);
  app.registerAggregate(Transfer);
  app.registerProcessManager(withTrace(transferProcessManager()), {
    pollInterval: 50,
    heartbeatInterval: 1_000,
    onError: (err: Error) =>
      process.stderr.write(`  [PM error] ${err.message}\n`),
  });

  const worker = await app.startWorker();
  process.stdout.write(`[${TRANSFER_PM_NAME}] worker started\n`);

  installSignalHandlers(async () => {
    await worker.close();
    await app.close();
  });

  await worker.stopped;
}

main().catch((err) => {
  process.stderr.write(`pm-transfer failed: ${(err as Error).stack ?? err}\n`);
  process.exit(1);
});
