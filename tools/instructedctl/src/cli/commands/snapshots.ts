// `snapshots` command group: get.

import { Command } from "@cliffy/command";
import { getSnapshot } from "../../core/index.ts";
import { action, type GlobalOptions, runWith } from "../options.ts";
import { printJson, printKeyValue } from "../output.ts";

export function snapshotsCommand() {
  return new Command()
    .description("Inspect snapshots")
    .default("get")
    .command(
      "get",
      new Command()
        .description("Show a snapshot by source uuid")
        .arguments("<source-uuid:string>")
        .action((opts, sourceUuid) =>
          action(async () => {
            const g = opts as unknown as GlobalOptions;
            const snap = await runWith(g, (db) => getSnapshot(db, sourceUuid));
            if (g.json) {
              printJson(snap);
              return;
            }
            if (snap === null) {
              console.error(`Snapshot '${sourceUuid}' not found`);
              Deno.exit(1);
            }
            printKeyValue([
              ["source uuid", snap.sourceUuid],
              ["source type", snap.sourceType],
              ["source version", snap.sourceVersion],
              ["created at", snap.createdAt.toISOString()],
              ["data", JSON.stringify(snap.data)],
              [
                "metadata",
                snap.metadata === null ? null : JSON.stringify(snap.metadata),
              ],
            ]);
          })
        ),
    );
}
