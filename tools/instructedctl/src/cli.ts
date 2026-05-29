// CLI framework: a small command registry, argument parsing, and the shared
// database-option surface every command inherits. Commands register a name, a
// one-line summary, and a run function. main.ts wires the registry to argv.

import { configFromOptions, type DbConfig, type DbOptions } from "./db.ts";

export interface ParsedArgs {
  // Positional arguments, in order.
  positionals: string[];
  // Long/short flags. A flag with no value is recorded as `true`.
  flags: Map<string, string | true>;
}

export interface CommandContext {
  args: ParsedArgs;
  // Resolved database configuration, shared across all commands.
  dbConfig: DbConfig;
  // Whether --verbose was supplied.
  verbose: boolean;
}

export interface Command {
  name: string;
  summary: string;
  // Longer usage text shown by `instructedctl <cmd> --help`.
  usage?: string;
  run: (ctx: CommandContext) => Promise<number> | number;
}

// Parse a raw argv slice into positionals and flags. Supports:
//   --flag            -> flag = true
//   --flag value      -> flag = "value"
//   --flag=value      -> flag = "value"
//   -f                -> short flag, treated like --f
export function parseArgs(argv: string[]): ParsedArgs {
  const positionals: string[] = [];
  const flags = new Map<string, string | true>();

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === "--") {
      positionals.push(...argv.slice(i + 1));
      break;
    }
    if (token.startsWith("--")) {
      const body = token.slice(2);
      const eq = body.indexOf("=");
      if (eq >= 0) {
        flags.set(body.slice(0, eq), body.slice(eq + 1));
      } else {
        const next = argv[i + 1];
        if (next !== undefined && !next.startsWith("-")) {
          flags.set(body, next);
          i++;
        } else {
          flags.set(body, true);
        }
      }
    } else if (token.startsWith("-") && token.length > 1) {
      const body = token.slice(1);
      const next = argv[i + 1];
      if (next !== undefined && !next.startsWith("-")) {
        flags.set(body, next);
        i++;
      } else {
        flags.set(body, true);
      }
    } else {
      positionals.push(token);
    }
  }

  return { positionals, flags };
}

function flagString(args: ParsedArgs, ...names: string[]): string | undefined {
  for (const name of names) {
    const value = args.flags.get(name);
    if (typeof value === "string") return value;
  }
  return undefined;
}

export function flagBool(args: ParsedArgs, ...names: string[]): boolean {
  return names.some((name) => args.flags.has(name));
}

// Build a CommandContext from parsed args, resolving the shared db options.
export function makeContext(args: ParsedArgs): CommandContext {
  const dbOptions: DbOptions = {
    database: flagString(args, "database", "d"),
    host: flagString(args, "host", "h"),
    port: flagString(args, "port", "p"),
    user: flagString(args, "user", "U"),
  };
  return {
    args,
    dbConfig: configFromOptions(dbOptions),
    verbose: flagBool(args, "verbose", "v"),
  };
}

export const DB_OPTIONS_HELP = `Database options (shared by all commands):
  -d, --database  Database name or PostgreSQL connection URI
  -h, --host      Database host (default: localhost)
  -p, --port      Database port (default: 5432)
  -U, --user      Database user
  -v, --verbose   Print the resolved connection configuration

Environment variables (precedence: --database > INSTRUCTED_DATABASE_URL > PGDATABASE):
  INSTRUCTED_DATABASE_URL  PostgreSQL connection URI
  PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD  Standard libpq variables`;
