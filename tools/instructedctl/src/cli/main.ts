#!/usr/bin/env -S deno run -A
// instructedctl — administrative CLI for an `instructed` deployment.
//
// This module is a thin wrapper: it declares the command tree and global
// options, then delegates to the core API (src/core) for all behaviour.
// Commands connect directly to PostgreSQL; no application SDK dependency.

import { Command } from "@cliffy/command";
import { schemaCommand } from "./commands/schema.ts";
import { subscriptionsCommand } from "./commands/subscriptions.ts";

const VERSION = "0.0.0";

export function buildCli(): Command {
  // Cliffy's fluent generics don't unify a root carrying global options with
  // sub-commands built in separate factory functions. We build the root, then
  // mount the groups via an `any`-typed handle so the (purely type-level)
  // generic mismatch at the boundary doesn't block the build. Runtime
  // behaviour and global-option inheritance are unaffected.
  const cli = new Command()
    .name("instructedctl")
    .version(VERSION)
    .description("Administrative CLI for an instructed deployment")
    // Free up -h for --host (libpq convention); help is --help only.
    .helpOption("--help", "Show this help")
    // Global DB connection options, inherited by every subcommand. Resolution
    // precedence: flag > INSTRUCTED_DATABASE_URL > PGDATABASE > default.
    .globalOption(
      "-d, --database <uri:string>",
      "Database name or PostgreSQL connection URI",
    )
    .globalOption("-h, --host <host:string>", "Database host (default: localhost)")
    .globalOption("-p, --port <port:integer>", "Database port (default: 5432)")
    .globalOption("-U, --user <user:string>", "Database user")
    .globalOption("--verbose", "Print the resolved connection configuration")
    .globalOption("--json", "Emit machine-readable JSON instead of a table")
    .action(function () {
      this.showHelp();
    });

  // deno-lint-ignore no-explicit-any
  const mount = cli as any;
  mount.command("schema", schemaCommand());
  mount.command("subscriptions", subscriptionsCommand()).alias("subs");
  // The root carries a global-options generic that doesn't unify with the bare
  // `Command` return type; the cast is the type-level bridge (see note above).
  return cli as unknown as Command;
}

if (import.meta.main) {
  try {
    await buildCli().parse(Deno.args);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`Error: ${message}`);
    Deno.exit(1);
  }
}
