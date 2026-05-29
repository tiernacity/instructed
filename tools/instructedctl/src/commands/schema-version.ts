// `schema-version` — print the recorded schema version and exit. Mirrors
// `absurdctl schema-version`. Reads the value from
// instructed.get_schema_version() (sql/instructed.sql).

import type { Command, CommandContext } from "../cli.ts";
import { withClient } from "../db.ts";

async function run(ctx: CommandContext): Promise<number> {
  return await withClient(ctx.dbConfig, async (client) => {
    const result = await client.queryObject<{ version: string }>(
      "select instructed.get_schema_version() as version",
    );
    console.log(result.rows[0].version);
    return 0;
  });
}

export const schemaVersionCommand: Command = {
  name: "schema-version",
  summary: "Show the recorded instructed schema version",
  usage: `Usage: instructedctl schema-version [DB OPTIONS]

Prints the value returned by instructed.get_schema_version().`,
  run,
};
