/**
 * Deposit funds into an existing account.
 *
 *   npm run deposit <name> <amount>
 */

import pg from "pg";
import { Instructed } from "instructed-sdk";
import { PG_URL, requireArg } from "../src/common.ts";
import { Account } from "../src/aggregates/account.ts";
import { DepositToAccount } from "../src/commands/account/index.ts";
import { appCommandRouter } from "../src/command-router.ts";

async function main(): Promise<void> {
  const name = requireArg(process.argv, 2, "name");
  const amount = Number(requireArg(process.argv, 3, "amount"));
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error(`invalid amount: ${process.argv[3]}`);
  }

  const pool = new pg.Pool({ connectionString: PG_URL });
  try {
    const app = new Instructed({ db: pool })
      .register(Account)
      .register(appCommandRouter);
    await app.dispatch({
      type: DepositToAccount,
      accountId: name,
      amount,
    });
    process.stdout.write(`deposited ${amount} to "${name}"\n`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  process.stderr.write(`deposit failed: ${(err as Error).message}\n`);
  process.exit(1);
});
