// Database connection: resolution of connection settings from flags and the
// environment, plus a thin query helper used by every command. instructedctl
// connects directly to PostgreSQL and does not depend on any application SDK.

import { Client } from "@db/postgres";

export interface DbConfig {
  // When a connection URI is supplied (via --database or INSTRUCTED_DATABASE_URL
  // / PGDATABASE holding a URI) we pass it through verbatim. Otherwise we build
  // a connection from discrete host/port/user/database fields.
  uri?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
}

export interface DbOptions {
  database?: string;
  host?: string;
  port?: string;
  user?: string;
}

const DEFAULT_URI = "postgresql://localhost/instructed";

function looksLikeUri(value: string): boolean {
  return /^postgres(ql)?:\/\//i.test(value);
}

// Resolve the database argument with the same precedence absurdctl uses:
// explicit flag, then INSTRUCTED_DATABASE_URL, then PGDATABASE, then a local
// default.
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
    port: Number(options.port || Deno.env.get("PGPORT") || "5432"),
    user: options.user || Deno.env.get("PGUSER") || Deno.env.get("USER") || "",
    password: Deno.env.get("PGPASSWORD") || undefined,
    database,
  };
}

// Remove the password from a URI for safe display in verbose output.
export function sanitizeUri(uri: string): string {
  try {
    const parsed = new URL(uri);
    if (parsed.password) {
      parsed.password = "****";
    }
    return parsed.toString();
  } catch {
    return uri;
  }
}

export function describeConfig(config: DbConfig): Array<[string, string]> {
  if (config.uri) {
    return [["URI", sanitizeUri(config.uri)]];
  }
  return [
    ["Host", String(config.host)],
    ["Port", String(config.port)],
    ["User", String(config.user)],
    ["Database", String(config.database)],
  ];
}

export async function connect(config: DbConfig): Promise<Client> {
  const client = config.uri ? new Client(config.uri) : new Client({
    hostname: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
  });
  await client.connect();
  return client;
}

// Run a function with a connected client, guaranteeing the connection is
// closed afterwards.
export async function withClient<T>(
  config: DbConfig,
  fn: (client: Client) => Promise<T>,
): Promise<T> {
  const client = await connect(config);
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}
