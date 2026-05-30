// The instructed core API — the consumable surface. Anything that wants to
// drive an instructed store (the CLI, a future web UI) imports from here and
// supplies a `Db` adapter.

export type { Db } from "./db.ts";
export type { InstallResult, StoreStatus, SubscriptionSummary } from "./types.ts";

export {
  getSchemaVersion,
  getStatus,
  installSchema,
  SchemaAlreadyInstalled,
  schemaPresent,
} from "./schema.ts";

export { getSubscription, listSubscriptions } from "./subscriptions.ts";
