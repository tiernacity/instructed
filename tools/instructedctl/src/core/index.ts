// The instructed core API — the consumable surface. Anything that wants to
// drive an instructed store (the CLI, a future web UI) imports from here and
// supplies a `Db` adapter.

export type { Db } from "./db.ts";
export type {
  ClaimResult,
  EnsureResult,
  EventRecord,
  FailedWorkItem,
  HealthCheck,
  HealthReport,
  InstallResult,
  Snapshot,
  StoreStatus,
  StreamSummary,
  SubscriptionSummary,
  WorkItemCounts,
} from "./types.ts";

export {
  StreamNotFound,
  SubscriptionLeaseLost,
  SubscriptionNotClaimed,
  SubscriptionNotFound,
} from "./errors.ts";

export { getStream, listStreams, readAll, readStream } from "./streams.ts";
export { getSnapshot } from "./snapshots.ts";
export { listFailedWorkItems, listWorkItemCounts } from "./work-items.ts";
export { checkHealth } from "./health.ts";

export {
  bundledSchemaVersion,
  ensureSchema,
  getSchemaVersion,
  getStatus,
  installSchema,
  SchemaAlreadyInstalled,
  schemaPresent,
  SchemaVersionMismatch,
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
