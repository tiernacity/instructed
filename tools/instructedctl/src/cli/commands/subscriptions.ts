// `subscriptions` command group: list / get / release / delete / claim /
// rebuild. Thin Cliffy wrappers over the core subscription API.

import { Command } from "@cliffy/command";
import {
  claimSubscription,
  deleteSubscription,
  getSubscription,
  listSubscriptions,
  rebuildSubscription,
  releaseSubscription,
} from "../../core/index.ts";
import { action, type GlobalOptions, runWith } from "../options.ts";
import { printJson, printKeyValue, printTable } from "../output.ts";
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

// Shared option: which stream the subscription rides (default $all).
type StreamOpt = { stream?: string };
function streamOf(opts: StreamOpt): string {
  return opts.stream ?? "$all";
}

export function subscriptionsCommand() {
  return new Command()
    .description("Inspect and manage subscriptions")
    .default("list")
    .command(
      "list",
      new Command()
        .description("List subscriptions with cursor, claim, and lag")
        .alias("ls")
        .action((opts) =>
          action(async () => {
            const g = opts as unknown as GlobalOptions;
            const subs = await runWith(g, listSubscriptions);
            if (g.json) printJson(subs);
            else table(subs);
          })
        ),
    )
    .command(
      "get",
      new Command()
        .description("Show a subscription by name")
        .arguments("<name:string>")
        .action((opts, name) =>
          action(async () => {
            const g = opts as unknown as GlobalOptions;
            const subs = await runWith(g, (db) => getSubscription(db, name));
            if (g.json) {
              printJson(subs);
              return;
            }
            if (subs.length === 0) {
              console.error(`Subscription '${name}' not found`);
              Deno.exit(1);
            }
            table(subs);
          })
        ),
    )
    .command(
      "release",
      new Command()
        .description("Release a stuck claim (defaults to the current holder)")
        .arguments("<name:string>")
        .option("--stream <uuid:string>", "Stream the subscription rides", {
          default: "$all",
        })
        .option(
          "--worker-id <id:string>",
          "Release on behalf of this holder (default: auto-detect)",
        )
        .action((opts, name) =>
          action(async () => {
            const o = opts as unknown as GlobalOptions & StreamOpt & {
              workerId?: string;
            };
            const result = await runWith(o, (db) =>
              releaseSubscription(db, {
                name,
                streamUuid: streamOf(o),
                workerId: o.workerId,
              }));
            if (o.json) printJson(result);
            else {console.log(
                `Released '${name}' (was held by ${result.releasedFrom})`,
              );}
          })
        ),
    )
    .command(
      "delete",
      new Command()
        .description("Delete a subscription (cascades its work-item rows)")
        .alias("rm")
        .arguments("<name:string>")
        .option("--stream <uuid:string>", "Stream the subscription rides", {
          default: "$all",
        })
        .option("--yes", "Confirm the deletion (required)")
        .action((opts, name) =>
          action(async () => {
            const o = opts as unknown as GlobalOptions & StreamOpt & {
              yes?: boolean;
            };
            if (!o.yes) {
              console.error(
                `Refusing to delete '${name}' without --yes ` +
                  "(this removes the cursor and all its work items).",
              );
              Deno.exit(1);
            }
            await runWith(o, (db) =>
              deleteSubscription(db, { name, streamUuid: streamOf(o) }));
            if (o.json) {
              printJson({ deleted: name });
            } else console.log(`Deleted subscription '${name}'`);
          })
        ),
    )
    .command(
      "claim",
      new Command()
        .description("Diagnostic: take or report a subscription's lease")
        .arguments("<name:string>")
        .option("--stream <uuid:string>", "Stream the subscription rides", {
          default: "$all",
        })
        .option("--worker-id <id:string>", "Worker identifier to claim as", {
          default: "instructedctl",
        })
        .option("--lease-seconds <n:integer>", "Lease duration in seconds", {
          default: 60,
        })
        .option(
          "--start-from <s:string>",
          "Initial cursor when creating: origin | current | <integer>",
        )
        .action((opts, name) =>
          action(async () => {
            const o = opts as unknown as GlobalOptions & StreamOpt & {
              workerId: string;
              leaseSeconds: number;
              startFrom?: string;
            };
            const result = await runWith(o, (db) =>
              claimSubscription(db, {
                name,
                streamUuid: streamOf(o),
                workerId: o.workerId,
                leaseSeconds: o.leaseSeconds,
                startFrom: o.startFrom,
              }));
            if (o.json) {
              printJson(result);
              return;
            }
            printKeyValue([
              ["result", result.result],
              ["last seen", result.lastSeen],
              ["claimed by", result.claimedBy],
              ["lease expires", iso(result.claimExpiresAt)],
            ]);
          })
        ),
    )
    .command(
      "rebuild",
      new Command()
        .description(
          "Forget a subscription's state so a worker re-routes from origin",
        )
        .arguments("<name:string>")
        .option("--stream <uuid:string>", "Stream the subscription rides", {
          default: "$all",
        })
        .option("--yes", "Confirm the rebuild (required)")
        .action((opts, name) =>
          action(async () => {
            const o = opts as unknown as GlobalOptions & StreamOpt & {
              yes?: boolean;
            };
            if (!o.yes) {
              console.error(
                `Refusing to rebuild '${name}' without --yes ` +
                  "(this resets the cursor to origin; stop the worker first, " +
                  "and wipe the read store separately).",
              );
              Deno.exit(1);
            }
            const result = await runWith(o, (db) =>
              rebuildSubscription(db, { name, streamUuid: streamOf(o) }));
            if (o.json) {
              printJson({ rebuilt: name, ...result });
            } else {console.log(
                `Rebuilt '${name}' — cursor reset to origin` +
                  (result.existed ? "" : " (subscription did not previously exist)"),
              );}
          })
        ),
    );
}
