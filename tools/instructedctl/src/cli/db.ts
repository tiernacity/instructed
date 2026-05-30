// CLI database wiring: resolve connection settings from flags/env, adapt a
// `@db/postgres` Client to the core `Db` interface, and run a function with a
// connected adapter.

import { Client } from "@db/postgres";
import type { Db } from "../core/index.ts";

export interface DbConfig {
  uri?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
}

// Global DB options parsed by Cliffy (see ../cli/main.ts).
export interface DbOptions {
  database?: string;
  host?: string;
  port?: number;
  user?: string;
}

const DEFAULT_URI = "postgresql://localhost/instructed";

function looksLikeUri(value: string): boolean {
  return /^postgres(ql)?:\/\//i.test(value);
}

// Precedence: explicit flag, then INSTRUCTED_DATABASE_URL, then PGDATABASE,
// then a local default.
function resolveDatabaseArgument(explicit?: string): string {
  return (
    explicit ||
    Deno.env.get("INSTRUCTED_DATABASE_URL") ||
    Deno.env.get("PGDATABASE") ||
    DEFAULT_URI
  );
}

export function configFromOptions(options: DbOptions): DbConfig {
  const database = resolveDatabaseArgument(options.database);

  if (looksLikeUri(database)) {
    return { uri: database };
  }

  return {
    host: options.host || Deno.env.get("PGHOST") || "localhost",
    port: options.port || Number(Deno.env.get("PGPORT") || "5432"),
    user: options.user || Deno.env.get("PGUSER") || Deno.env.get("USER") || "",
    password: Deno.env.get("PGPASSWORD") || undefined,
    database,
  };
}

// Remove the password from a URI for safe display.
export function sanitizeUri(uri: string): string {
  try {
    const parsed = new URL(uri);
    if (parsed.password) parsed.password = "****";
    return parsed.toString();
  } catch {
    return uri;
  }
}

export function describeConfig(config: DbConfig): Array<[string, string]> {
  if (config.uri) return [["URI", sanitizeUri(config.uri)]];
  return [
    ["Host", String(config.host)],
    ["Port", String(config.port)],
    ["User", String(config.user)],
    ["Database", String(config.database)],
  ];
}

// Adapt a connected `@db/postgres` Client to the core `Db` interface.
class PgDb implements Db {
  constructor(private client: Client) {}

  async query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    const res = await this.client.queryObject<Record<string, unknown>>(
      sql,
      params as unknown[],
    );
    return res.rows as T[];
  }

  async exec(sql: string): Promise<void> {
    await this.client.queryArray(sql);
  }
}

function newClient(config: DbConfig): Client {
  return config.uri ? new Client(config.uri) : new Client({
    hostname: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
  });
}

// Connect, run `fn` with a core `Db`, and always close the connection.
export async function withDb<T>(
  config: DbConfig,
  fn: (db: Db) => Promise<T>,
): Promise<T> {
  const client = newClient(config);
  await client.connect();
  try {
    return await fn(new PgDb(client));
  } finally {
    await client.end();
  }
}
