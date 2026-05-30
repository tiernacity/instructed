// `subscriptions` command group: list / get. Lifecycle actions (release /
// delete / claim / rebuild) land in a later slice.

import { Command } from "@cliffy/command";
import { getSubscription, listSubscriptions } from "../../core/index.ts";
import { type GlobalOptions, runWith } from "../options.ts";
import { printJson, printTable } from "../output.ts";
import type { SubscriptionSummary } from "../../core/index.ts";

function iso(d: Date | null): string | null {
  return d ? d.toISOString() : null;
}

function table(subs: SubscriptionSummary[]): void {
  printTable(
    ["NAME", "STREAM", "LAST SEEN", "LAG", "CLAIMED BY", "LEASE EXPIRES"],
    subs.map((s) => [
      s.subscriptionName,
      s.streamUuid,
      s.lastSeen,
      s.lag,
      s.claimedBy,
      iso(s.claimExpiresAt),
    ]),
  );
}

export function subscriptionsCommand() {
  return new Command()
    .description("Inspect subscriptions")
    .default("list")
    .command(
      "list",
      new Command()
        .description("List subscriptions with cursor, claim, and lag")
        .alias("ls")
        .action(async (opts) => {
          const g = opts as unknown as GlobalOptions;
          const subs = await runWith(g, listSubscriptions);
          if (g.json) {
            printJson(subs);
            return;
          }
          table(subs);
        }),
    )
    .command(
      "get",
      new Command()
        .description("Show a subscription by name")
        .arguments("<name:string>")
        .action(async (opts, name) => {
          const g = opts as unknown as GlobalOptions;
          const subs = await runWith(
            g,
            (db) => getSubscription(db, name as string),
          );
          if (g.json) {
            printJson(subs);
            return;
          }
          if (subs.length === 0) {
            console.error(`Subscription '${name}' not found`);
            Deno.exit(1);
          }
          table(subs);
        }),
    );
}
