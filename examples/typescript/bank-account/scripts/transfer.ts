/**
 * Request a transfer between two accounts.
 *
 *   npm run transfer <from> <to> <amount>
 *
 * Each invocation creates a fresh transfer with a random transferId
 * so successive transfers between the same pair are tracked
 * independently by the PM and the Transfers projection.
 */

import { randomUUID } from "node:crypto";
import { Instructed } from "instructed-sdk";
import { PG_URL, requireArg } from "../src/common.ts";
import { Transfer } from "../src/transfer.ts";

async function main(): Promise<void> {
  const from = requireArg(process.argv, 2, "from");
  const to = requireArg(process.argv, 3, "to");
  const amount = Number(requireArg(process.argv, 4, "amount"));
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error(`invalid amount: ${process.argv[4]}`);
  }

  const transferId = randomUUID();
  const app = new Instructed({ db: PG_URL });
  app.registerAggregate(Transfer);
  try {
    await app.dispatch("Transfer", transferId, {
      kind: "Request",
      from,
      to,
      amount,
      transferId,
    });
    process.stdout.write(
      `requested transfer ${transferId.slice(0, 8)}  ${from} -> ${to}  ${amount}\n`,
    );
  } finally {
    await app.close();
  }
}

main().catch((err) => {
  process.stderr.write(`transfer failed: ${(err as Error).message}\n`);
  process.exit(1);
});
