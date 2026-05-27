/**
 * Shared helpers for the bank-account example scripts.
 *
 * Default connection points at the isolated docker-compose Postgres
 * shipped under `examples/typescript/bank-account/docker-compose.yaml`
 * (port 5433, database `bank_account`). Override with
 * `INSTRUCTED_DATABASE_URL` to point at any conformant store.
 */

export const PG_URL =
  process.env.INSTRUCTED_DATABASE_URL ??
  "postgresql://postgres:postgres@127.0.0.1:5433/bank_account";

/**
 * Resolves on the first SIGINT or SIGTERM. Callers race this against
 * `worker.stopped` and clean up in a `finally`; no `process.exit()`
 * is needed — letting `main()` return cleanly is preferable because
 * it lets `finally` clauses (pool close, file flush, etc.) complete
 * without the brutal short-circuit `process.exit()` imposes.
 *
 * Listeners are registered with `process.once` so the handler is
 * idempotent across spurious double-signals.
 */
export function waitForShutdown(): Promise<NodeJS.Signals> {
  return new Promise((resolve) => {
    const stop = (sig: NodeJS.Signals): void => {
      process.stderr.write(`\n[${sig}] shutting down…\n`);
      resolve(sig);
    };
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
  });
}

export function requireArg(argv: string[], index: number, name: string): string {
  const v = argv[index];
  if (!v) {
    process.stderr.write(`missing argument: <${name}>\n`);
    process.exit(2);
  }
  return v;
}

// Stream-key helpers removed: identifying aggregates by their
// (type, id) pair and letting the SDK derive the stream is the
// preferred application-level interface; see `AggregateDefinition.streamName`.
// The Account aggregate defaults to `Account-<id>`; Transfer to
// `Transfer-<id>`.
