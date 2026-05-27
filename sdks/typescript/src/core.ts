/**
 * `instructed-sdk/core` \u2014 the L1 + L2 surface.
 *
 * This is the porting-checklist inventory: the exports every
 * language SDK must reproduce in some shape. Per
 * [D-0027](../../../docs/decisions.md#d-0027) the TypeScript SDK
 * ships as one package; this module is the sub-path entry that
 * names exactly what is contract and what is idiomatic facade.
 *
 *   - **L1 \u2014 procedure bindings.** Thin wrappers over
 *     `instructed.*` stored procedures. SQLSTATE-bound error
 *     classes. Wire-shape contracts.
 *   - **L2 \u2014 core behaviours.** The aggregate load/execute/append
 *     loop with OCC retry. The routing worker (D-0025 per-batch
 *     claim/release). The processing worker (per-item lease +
 *     heartbeat) and its kind-specific projection / PM adapters.
 *
 * What is *not* exported from here:
 *
 *   - `waitForProjection` and `SubscriptionRef` (L3 \u2014
 *     `consistency.ts`). Mechanism is the L1
 *     `is_subscription_caught_up` predicate (available on
 *     `Client`); the wait shape is a convenience.
 *   - `Instructed` and its registration / dispatch surface
 *     (L3 \u2014 `instructed.ts`).
 *   - `ConsistencyTimeout`, `ConsistencyTargetError`,
 *     `UnknownAggregateType`, `HandlerError` (L3; emitted by the
 *     facade).
 *   - `mapPgError`, `MapPgErrorContext` (L1-internal).
 *
 * Application code that wants the conventional one-import shape
 * uses `instructed-sdk` (the bare entry). Code building its own
 * L3 facade uses this entry (`instructed-sdk/core`).
 */

// ----------------------------------------------------------------------------
// L1 \u2014 procedure bindings
// ----------------------------------------------------------------------------

export { Client, type ClientOptions } from "./client.ts";

// L1 \u2014 SQLSTATE-bound error classes. `RetryBudgetExhausted` (L2) is
// re-exported below in the L2 group; the L3 error classes are not
// re-exported here at all (see `instructed-sdk` bare entry).
export {
  // base
  InstructedError,
  // IS00x \u2014 append path
  AppendError,
  WrongExpectedVersion,
  StreamExists,
  StreamNotFound,
  DuplicateEvent,
  ReservedStreamUuid,
  AppendOnlyViolation,
  // IS010 \u2014 snapshots
  SnapshotNotFound,
  // IS020 / IS021 / IS022 \u2014 subscriptions
  SubscriptionError,
  SubscriptionNotFound,
  SubscriptionAlreadyClaimed,
  SubscriptionLeaseLost,
  // IS030 \u2014 work items (SUB-A)
  WorkItemLeaseLost,
  // 22023
  InvalidParameterValue,
} from "./errors.ts";

// L1 \u2014 wire-shape contracts.
export * from "./types.ts";

// ----------------------------------------------------------------------------
// L2 \u2014 core behaviours
// ----------------------------------------------------------------------------

// Aggregate load/execute/append loop with OCC retry.
//
// `runCommand` is the simple form (returns just appended events);
// `runCommandAndApply` returns the same plus the post-append
// staged state for callers that want to do follow-up work without
// re-loading. Neither invokes snapshot policy — see
// `runCommandWithSnapshots` (L3, in `instructed-sdk` bare entry).
//
// `SnapshotPolicy<S>` and `everyN` are the contract + standard
// library halves of the snapshot-policy extension point; they
// live here because the contract is what porters reproduce, but
// the *invocation* is L3 (see `aggregate-snapshots.ts`).
export {
  runCommand,
  runCommandAndApply,
  everyN,
  DEFAULT_RETRY_BUDGET,
  type AggregateDefinition,
  type RanCommand,
  type RunCommandOptions,
  type SnapshotPolicy,
  type DomainEvent,
} from "./aggregate.ts";

// L2 retry-exhaustion class (emitted by `runCommand`).
export { RetryBudgetExhausted } from "./errors.ts";

// Routing worker (D-0025 per-batch claim/release).
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

// Processing worker (kind-agnostic poll loop + SUB-B error policy).
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

// Projection processing-worker adapter. The `PartitionBy` sugar
// (`routingFnForPartitionBy`, `SEQUENTIAL_PARTITION_KEY`, `PartitionBy`)
// lives in `src/partition-by.ts` and is L3-only; consumers using
// `instructed-sdk/core` write their own `RoutingFn` directly.
export {
  startProjectionWorker,
  type ProjectionHandler,
  type ProjectionHandlerContext,
  type ProjectionDefinition,
  type ProjectionWorkerOptions,
} from "./projection-worker.ts";

// PM processing-worker adapter.
export {
  startPmWorker,
  PM_SNAPSHOT_MODULE_VERSION_KEY,
  type PmDefinition,
  type PmHandleResult,
  type PmHandlerContext,
  type PmWorkerOptions,
  type DispatchedCommand,
} from "./pm-worker.ts";

// Shared worker handle (L2 interface; the loop functions above all
// return one).
export type { RunningWorker } from "./internal/running-worker.ts";
