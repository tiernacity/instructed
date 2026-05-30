// The instructed core API — the consumable surface. Anything that wants to
// drive an instructed store (the CLI, a future web UI) imports from here and
// supplies a `Db` adapter.

export type { Db } from "./db.ts";
export type {
  ClaimResult,
  InstallResult,
  StoreStatus,
  SubscriptionSummary,
} from "./types.ts";

export {
  SubscriptionLeaseLost,
  SubscriptionNotClaimed,
  SubscriptionNotFound,
} from "./errors.ts";

export {
  getSchemaVersion,
  getStatus,
  installSchema,
  SchemaAlreadyInstalled,
  schemaPresent,
} from "./schema.ts";

export {
  claimSubscription,
  deleteSubscription,
  getSubscription,
  listSubscriptions,
  REBUILD_WORKER_ID,
  rebuildSubscription,
  releaseSubscription,
  type SubscriptionRef,
} from "./subscriptions.ts";
