// `install` — apply the embedded sql/instructed.sql to a fresh database.
// Analogous to `absurdctl init`.
//
// The schema creates tables with bare `create table` (no `if not exists`), so
// it is meant for a clean database. If the `instructed` schema already exists,
// install refuses unless `--force` is given; `--force` drops the schema
// (CASCADE) and reinstalls. The SQL is embedded in the binary (see
// src/schema.ts), so this works with no file on disk.

import type { Command, CommandContext } from "../cli.ts";
import { describeConfig, withClient } from "../db.ts";
import { flagBool } from "../cli.ts";
import { SCHEMA_SQL } from "../schema.ts";

async function run(ctx: CommandContext): Promise<number> {
  const force = flagBool(ctx.args, "force", "f");

  if (ctx.verbose) {
    console.error("Configuration:");
    for (const [label, value] of describeConfig(ctx.dbConfig)) {
      console.error(`  ${label}: ${value}`);
    }
  }

  return await withClient(ctx.dbConfig, async (client) => {
    const existing = await client.queryObject<{ present: boolean }>(
      `select exists(
         select 1 from information_schema.schemata
         where schema_name = 'instructed'
       ) as present`,
    );
    const alreadyInstalled = existing.rows[0].present;

    if (alreadyInstalled && !force) {
      console.error(
        "Error: the 'instructed' schema already exists. " +
          "Re-run with --force to drop and reinstall (this destroys all data).",
      );
      return 1;
    }

    if (alreadyInstalled && force) {
      if (ctx.verbose) console.error("Dropping existing 'instructed' schema...");
      await client.queryArray(`drop schema instructed cascade`);
    }

    await client.queryArray(SCHEMA_SQL);

    const version = await client.queryObject<{ version: string }>(
      "select instructed.get_schema_version() as version",
    );
    const action = alreadyInstalled ? "reinstalled" : "installed";
    console.log(
      `instructed schema ${action} successfully (version ${version.rows[0].version})`,
    );
    return 0;
  });
}

export const installCommand: Command = {
  name: "install",
  summary: "Install the instructed schema into a database",
  usage: `Usage: instructedctl install [--force] [DB OPTIONS]

Applies the embedded sql/instructed.sql to the target database.

The schema is meant for a clean database. If the 'instructed' schema already
exists, install refuses unless --force is given.

  -f, --force   Drop the existing 'instructed' schema (CASCADE) and reinstall.
                This destroys all data in the schema.`,
  run,
};
