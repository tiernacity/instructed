// `work-items` command group: list (counts by state) / failed (failed rows
// with error text). The skip-with-audit escape hatch is intentionally absent
// pending its SQL procedure (TODO #7).

import { Command } from "@cliffy/command";
import { listFailedWorkItems, listWorkItemCounts } from "../../core/index.ts";
import { action, type GlobalOptions, runWith } from "../options.ts";
import { printJson, printTable } from "../output.ts";

type SubOpt = GlobalOptions & { subscription?: string };

export function workItemsCommand() {
  return new Command()
    .description("Inspect the work queue")
    .default("list")
    .command(
      "list",
      new Command()
        .description("Work-item counts by state, per subscription")
        .alias("ls")
        .option(
          "--subscription <name:string>",
          "Restrict to one subscription",
        )
        .action((opts) =>
          action(async () => {
            const o = opts as unknown as SubOpt;
            const counts = await runWith(o, (db) =>
              listWorkItemCounts(db, o.subscription));
            if (o.json) {
              printJson(counts);
              return;
            }
            printTable(
              [
                "SUBSCRIPTION",
                "PENDING",
                "CLAIMED",
                "FAILED",
                "DONE",
                "TOTAL",
                "OLDEST ACTIVE",
              ],
              counts.map((c) => [
                c.subscriptionName,
                c.pending,
                c.claimed,
                c.failed,
                c.done,
                c.total,
                c.oldestActiveEventNumber,
              ]),
            );
          })
        ),
    )
    .command(
      "failed",
      new Command()
        .description("List failed work items with their error text")
        .option(
          "--subscription <name:string>",
          "Restrict to one subscription",
        )
        .action((opts) =>
          action(async () => {
            const o = opts as unknown as SubOpt;
            const failed = await runWith(o, (db) =>
              listFailedWorkItems(db, o.subscription));
            if (o.json) {
              printJson(failed);
              return;
            }
            printTable(
              ["SUBSCRIPTION", "PARTITION", "EVENT#", "FAILED AT", "ERROR"],
              failed.map((f) => [
                f.subscriptionName,
                f.partitionKey,
                f.eventNumber,
                f.failedAt ? f.failedAt.toISOString() : null,
                f.errorText,
              ]),
            );
          })
        ),
    );
}
