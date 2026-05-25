/**
 * instructed TypeScript SDK — public surface.
 *
 * See sdks/typescript/README.md for the layered design. This file
 * re-exports only the public contract; everything under `src/internal/`
 * is private.
 *
 * SUB-A note (slice 9): the legacy `startProjection` /
 * `startProcessManager` worker functions and the
 * `ProjectionDefinition` / `ProcessManagerDefinition` shapes are
 * removed. Subscriptions now run on the work-queue substrate -- see
 * `startRoutingWorker` + `startProjectionWorker` / `startPmWorker`.
 * The layer-5 `Instructed.registerProjection` /
 * `registerProcessManager` shapes also changed (breaking).
 */

export { Client, type ClientOptions } from "./client.ts";
export {
  runCommand,
  everyN,
  DEFAULT_RETRY_BUDGET,
  type AggregateDefinition,
  type RunCommandOptions,
  type SnapshotPolicy,
  type DomainEvent,
} from "./aggregate.ts";

// SUB-A: routing worker (slice 4).
export {
  startRoutingWorker,
  DEFAULT_ROUTING_BATCH_SIZE,
  DEFAULT_ROUTING_LEASE_SECONDS,
  DEFAULT_ROUTING_POLL_INTERVAL_MS,
  type RoutingDecision,
  type RoutingFn,
  type RoutingDefinition,
  type RoutingWorkerOptions,
} from "./routing-worker.ts";

// SUB-A: processing worker (slice 5) -- kind-agnostic poll loop and
// SUB-B error-policy primitives. Most users use the projection / PM
// adapters below; this is exposed for advanced cases.
export {
  startProcessingWorker,
  DEFAULT_PROCESSING_LEASE_SECONDS,
  DEFAULT_PROCESSING_POLL_INTERVAL_MS,
  DEFAULT_ERROR_POLICY,
  type ProcessingHandler,
  type ProcessingCompleter,
  type ProcessingHandlerContext,
  type ProcessingWorkerDefinition,
  type ProcessingWorkerOptions,
  type ErrorPolicy,
  type ErrorPolicyDecision,
  type ErrorPolicyContext,
} from "./processing-worker.ts";

// SUB-A: projection processing worker (slice 6).
export {
  startProjectionWorker,
  routingFnForPartitionBy,
  SEQUENTIAL_PARTITION_KEY,
  type PartitionBy,
  type ProjectionHandler,
  type ProjectionHandlerContext,
  type ProjectionDefinition,
  type ProjectionWorkerOptions,
} from "./projection-worker.ts";

// SUB-A: PM processing worker (slice 7).
export {
  startPmWorker,
  PM_SNAPSHOT_MODULE_VERSION_KEY,
  type PmDefinition,
  type PmHandleResult,
  type PmHandlerContext,
  type PmWorkerOptions,
  type DispatchedCommand,
} from "./pm-worker.ts";

// Shared worker handle.
export type { RunningWorker } from "./internal/running-worker.ts";

// SUB-A: consistency wait (slice 8).
export {
  waitForProjection,
  DEFAULT_WAIT_POLL_INTERVAL_MS,
  DEFAULT_WAIT_TIMEOUT_MS,
  type SubscriptionRef,
  type WaitForProjectionOptions,
} from "./consistency.ts";

// Layer 5 facade.
export {
  Instructed,
  type InstructedOptions,
  type InstructedDefaults,
  type RegistrationOptions,
  type RegisterProjectionInput,
  type RegisterProcessManagerInput,
  type DispatchOptions,
} from "./instructed.ts";

export * from "./errors.ts";
export * from "./types.ts";
