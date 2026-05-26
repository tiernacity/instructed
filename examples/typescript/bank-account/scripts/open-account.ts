/**
 * Open a new account.
 *
 *   npm run open-account <name>
 */

import { Instructed } from "instructed-sdk";
import { PG_URL, accountStream, requireArg } from "../src/common.ts";
import { Account } from "../src/account.ts";

async function main(): Promise<void> {
  const name = requireArg(process.argv, 2, "name");
  const app = new Instructed({ db: PG_URL });
  app.registerAggregate(Account);
  try {
    await app.dispatch("Account", accountStream(name), {
      kind: "Open",
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
