// The database boundary the core depends on.
//
// Core functions take a `Db` and return typed data. They never import a
// concrete driver, never format output, and never exit the process — so the
// same core is consumable from the CLI, a future web UI, or a test that
// supplies any adapter. The CLI provides a `@db/postgres`-backed adapter (see
// ../cli/db.ts).
export interface Db {
  // Run a query that returns rows, mapped to objects of type T. Parameters are
  // positional ($1, $2, ...).
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  // Run one or more statements for their side effects (DDL, multi-statement
  // scripts). No rows are returned.
  exec(sql: string): Promise<void>;
}
