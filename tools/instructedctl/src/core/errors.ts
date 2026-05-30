// Typed core errors. Core functions translate the store's SQLSTATEs into these
// so consumers (the CLI, a future web UI) can branch on error type without
// importing a Postgres driver or parsing messages.

export class SubscriptionNotFound extends Error {
  override readonly name = "SubscriptionNotFound";
  constructor(public subscriptionName: string, public streamUuid: string) {
    super(`subscription '${subscriptionName}' on ${streamUuid} not found`);
  }
}

export class SubscriptionLeaseLost extends Error {
  override readonly name = "SubscriptionLeaseLost";
  constructor(public subscriptionName: string, public streamUuid: string) {
    super(
      `subscription '${subscriptionName}' on ${streamUuid}: lease lost ` +
        `(held by another worker)`,
    );
  }
}

export class StreamNotFound extends Error {
  override readonly name = "StreamNotFound";
  constructor(public streamUuid: string) {
    super(`stream '${streamUuid}' not found`);
  }
}

export class SubscriptionNotClaimed extends Error {
  override readonly name = "SubscriptionNotClaimed";
  constructor(public subscriptionName: string, public streamUuid: string) {
    super(
      `subscription '${subscriptionName}' on ${streamUuid} is not currently claimed`,
    );
  }
}

// Extract a Postgres SQLSTATE from an unknown error without depending on a
// specific driver. `@db/postgres` exposes it at `err.fields.code`; node-style
// drivers use `err.code`.
export function sqlstateOf(err: unknown): string | undefined {
  if (typeof err !== "object" || err === null) return undefined;
  const fields = (err as { fields?: { code?: string } }).fields;
  if (fields?.code) return fields.code;
  const code = (err as { code?: string }).code;
  return typeof code === "string" ? code : undefined;
}
