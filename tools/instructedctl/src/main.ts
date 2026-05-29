#!/usr/bin/env -S deno run -A
// instructedctl — administrative CLI for an `instructed` deployment.
//
// Connects directly to PostgreSQL (no application SDK dependency) and lets an
// operator inspect and manage a store without writing ad-hoc SQL. Modelled on
// absurdctl. See README.md for the command surface and build instructions.

import { DB_OPTIONS_HELP, flagBool, makeContext, parseArgs } from "./cli.ts";
import { COMMANDS, findCommand } from "./registry.ts";

const VERSION = "0.0.0";

function showHelp(): void {
  const width = Math.max(...COMMANDS.map((c) => c.name.length));
  const lines = COMMANDS.map(
    (c) => `  ${c.name.padEnd(width)}  ${c.summary}`,
  );
  console.log(`Usage: instructedctl COMMAND [OPTIONS]

An administrative CLI for managing an instructed deployment.

Commands:
${lines.join("\n")}
  help${" ".repeat(Math.max(0, width - 4))}  Show this help message

Run 'instructedctl COMMAND --help' for more information on a command.

${DB_OPTIONS_HELP}`);
}

async function main(): Promise<number> {
  const argv = Deno.args;

  if (
    argv.length === 0 || argv[0] === "help" || argv[0] === "--help" || argv[0] === "-h"
  ) {
    showHelp();
    return 0;
  }

  if (argv[0] === "--version" || argv[0] === "version") {
    console.log(VERSION);
    return 0;
  }

  const name = argv[0];
  const command = findCommand(name);
  if (!command) {
    console.error(`Unknown command: ${name}\n`);
    showHelp();
    return 1;
  }

  const args = parseArgs(argv.slice(1));
  if (flagBool(args, "help")) {
    console.log(command.usage ?? `Usage: instructedctl ${command.name} [OPTIONS]`);
    return 0;
  }

  const ctx = makeContext(args);
  try {
    return await command.run(ctx);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`Error: ${message}`);
    return 1;
  }
}

if (import.meta.main) {
  Deno.exit(await main());
}
