/**
 * Open a new account.
 *
 *   npm run open-account <name>
 *
 * Uses the lean `dispatch(command)` overload. The command router
 * (in `src/command-router.ts`) resolves `OpenAccount` to the
 * `Account` aggregate with id taken from `command.accountId`.
 */

import { Instructed } from "instructed-sdk";
import { PG_URL, requireArg } from "../src/common.ts";
import { Account } from "../src/aggregates/account.ts";
import { OpenAccount } from "../src/commands/account/index.ts";
import { appCommandRouter } from "../src/command-router.ts";

async function main(): Promise<void> {
  const name = requireArg(process.argv, 2, "name");
  const app = new Instructed({ db: PG_URL });
  app.registerAggregate(Account);
  app.registerCommandRouter(appCommandRouter);
  try {
    await app.dispatch({
      type: OpenAccount,
      accountId: name,
      owner: name,
    });
    process.stdout.write(`opened account "${name}"\n`);
  } finally {
    await app.close();
  }
}

main().catch((err) => {
  process.stderr.write(`open-account failed: ${(err as Error).message}\n`);
  process.exit(1);
});
