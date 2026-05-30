// Shared shape for the global options Cliffy parses on the root command, and
// helpers to turn them into a DbConfig and run a core function against a
// connected adapter.

import { configFromOptions, type DbConfig, describeConfig, withDb } from "./db.ts";
import type { Db } from "../core/index.ts";

// The global options declared with `.globalOption(...)` on the root command.
// Cliffy merges these into every subcommand action's options argument.
export interface GlobalOptions {
  database?: string;
  host?: string;
  port?: number;
  user?: string;
  verbose?: boolean;
  json?: boolean;
}

export function dbConfig(opts: GlobalOptions): DbConfig {
  return configFromOptions({
    database: opts.database,
    host: opts.host,
    port: opts.port,
    user: opts.user,
  });
}

// Connect using the resolved config and run `fn` with a core `Db`. Prints the
// resolved configuration first when --verbose is set.
export function runWith<T>(
  opts: GlobalOptions,
  fn: (db: Db) => Promise<T>,
): Promise<T> {
  const config = dbConfig(opts);
  if (opts.verbose) {
    console.error("Configuration:");
    for (const [label, value] of describeConfig(config)) {
      console.error(`  ${label}: ${value}`);
    }
  }
  return withDb(config, fn);
}
