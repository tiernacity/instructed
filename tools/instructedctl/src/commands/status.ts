// `status` — connect to the store and report a quick summary: the installed
// schema version and high-level row counts. A future pass will expand this
// into the full health check described in TODO #7 ($all contiguity, orphaned
// stream_events rows, expired-lease zombies).

import type { Command, CommandContext } from "../cli.ts";
import { describeConfig, withClient } from "../db.ts";

async function run(ctx: CommandContext): Promise<number> {
  if (ctx.verbose) {
    console.error("Configuration:");
    for (const [label, value] of describeConfig(ctx.dbConfig)) {
      console.error(`  ${label}: ${value}`);
    }
  }

  return await withClient(ctx.dbConfig, async (client) => {
    const schema = await client.queryObject<{ version: string }>(
      "select instructed.get_schema_version() as version",
    );
    const counts = await client.queryObject<{
      streams: bigint;
      events: bigint;
      subscriptions: bigint;
    }>(
      `select
         (select count(*) from instructed.streams where stream_id <> 0) as streams,
         (select count(*) from instructed.events) as events,
         (select count(*) from instructed.subscriptions) as subscriptions`,
    );
    const head = await client.queryObject<{ head: bigint }>(
      "select stream_version as head from instructed.streams where stream_id = 0",
    );

    const row = counts.rows[0];
    console.log(`schema version : ${schema.rows[0].version}`);
    console.log(`$all head      : ${head.rows[0].head}`);
    console.log(`streams        : ${row.streams}`);
    console.log(`events         : ${row.events}`);
    console.log(`subscriptions  : ${row.subscriptions}`);
    return 0;
  });
}

export const statusCommand: Command = {
  name: "status",
  summary: "Show schema version and a high-level store summary",
  usage: `Usage: instructedctl status [DB OPTIONS]

Connects to the store and prints the installed schema version, the $all head
version, and high-level row counts.`,
  run,
};
