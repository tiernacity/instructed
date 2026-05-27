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

export function installSignalHandlers(onStop: () => Promise<void> | void): void {
  let stopping = false;
  const stop = async (sig: NodeJS.Signals) => {
    if (stopping) return;
    stopping = true;
    process.stderr.write(`\n[${sig}] shutting down…\n`);
    try {
      await onStop();
    } catch (err) {
      process.stderr.write(`shutdown error: ${(err as Error).message}\n`);
    }
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
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
