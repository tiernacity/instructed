// `schema` command group: status / version / install. Thin Cliffy wrappers
// that call core and render the result.

import { Command } from "@cliffy/command";
import {
  ensureSchema,
  getSchemaVersion,
  getStatus,
  installSchema,
  SchemaAlreadyInstalled,
  SchemaVersionMismatch,
} from "../../core/index.ts";
import { type GlobalOptions, runWith } from "../options.ts";
import { printJson, printKeyValue } from "../output.ts";

export function schemaCommand() {
  return new Command()
    .description("Schema lifecycle and store status")
    .default("status")
    .command(
      "status",
      new Command()
        .description("Show schema version and a high-level store summary")
        .action(async (opts) => {
          const g = opts as unknown as GlobalOptions;
          const status = await runWith(g, getStatus);
          if (g.json) {
            printJson(status);
            return;
          }
          printKeyValue([
            ["schema version", status.schemaVersion],
            ["$all head", status.allHead],
            ["streams", status.streams],
            ["events", status.events],
            ["subscriptions", status.subscriptions],
          ]);
        }),
    )
    .command(
      "version",
      new Command()
        .description("Print the recorded instructed schema version")
        .action(async (opts) => {
          const version = await runWith(
            opts as unknown as GlobalOptions,
            getSchemaVersion,
          );
          console.log(version);
        }),
    )
    .command(
      "install",
      new Command()
        .description("Install the instructed schema into a database")
        .option(
          "-f, --force",
          "Drop the existing 'instructed' schema (CASCADE) and reinstall. " +
            "Destroys all data.",
        )
        .action(async (opts) => {
          const g = opts as unknown as GlobalOptions & { force?: boolean };
          try {
            const result = await runWith(
              g,
              (db) => installSchema(db, { force: g.force }),
            );
            if (g.json) {
              printJson(result);
              return;
            }
            console.log(
              `instructed schema ${result.action} successfully ` +
                `(version ${result.schemaVersion})`,
            );
          } catch (err) {
            if (err instanceof SchemaAlreadyInstalled) {
              console.error(
                "Error: the 'instructed' schema already exists. " +
                  "Re-run with --force to drop and reinstall (this destroys all data).",
              );
              Deno.exit(1);
            }
            throw err;
          }
        }),
    )
    .command(
      "ensure",
      new Command()
        .description(
          "Idempotently install the schema: install if absent, no-op if " +
            "already present at this version. Never destroys data. Safe to " +
            "run on every deploy.",
        )
        .action(async (opts) => {
          const g = opts as unknown as GlobalOptions;
          try {
            const result = await runWith(g, ensureSchema);
            if (g.json) {
              printJson(result);
              return;
            }
            console.log(
              result.action === "installed"
                ? `instructed schema installed successfully ` +
                  `(version ${result.schemaVersion})`
                : `instructed schema already current ` +
                  `(version ${result.schemaVersion}); nothing to do`,
            );
          } catch (err) {
            if (err instanceof SchemaVersionMismatch) {
              console.error(
                `Error: ${err.message} ` +
                  `Use 'schema install --force' to drop and reinstall ` +
                  `(this destroys all data).`,
              );
              Deno.exit(1);
            }
            throw err;
          }
        }),
    );
}
