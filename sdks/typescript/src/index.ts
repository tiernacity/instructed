/**
 * instructed TypeScript SDK — public surface.
 *
 * See docs/sdk-design.md for the layered design. This file re-exports
 * only the public contract; everything under `src/internal/` is private.
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
export {
  startProjection,
  DEFAULT_BATCH_SIZE,
  DEFAULT_LEASE_SECONDS,
  DEFAULT_POLL_INTERVAL_MS,
  type ProjectionDefinition,
  type ProjectionHandler,
  type ProjectionWorkerOptions,
  type HandlerContext,
  type RunningWorker,
} from "./subscription.ts";
export {
  startProcessManager,
  type ProcessManagerDefinition,
  type ProcessManagerHandlerResult,
  type ProcessManagerWorkerOptions,
  type RouteFn,
  type RouteResult,
  type DispatchedCommand,
} from "./process-manager.ts";
export {
  waitForProjection,
  DEFAULT_WAIT_POLL_INTERVAL_MS,
  DEFAULT_WAIT_TIMEOUT_MS,
  type SubscriptionRef,
  type WaitForProjectionOptions,
} from "./consistency.ts";
export * from "./errors.ts";
export * from "./types.ts";
