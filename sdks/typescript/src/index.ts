/**
 * `instructed-sdk` \u2014 the full TypeScript SDK surface.
 *
 * Per [D-0027](../../../docs/decisions.md#d-0027) the SDK is one
 * package with two entry points:
 *
 *   - `instructed-sdk` (this file) \u2014 L1 + L2 + L3; the conventional
 *     entry. Application code that uses the `Instructed` facade,
 *     `waitForProjection`, the `PartitionBy` sugar, etc. imports from
 *     here.
 *   - `instructed-sdk/core` (`src/core.ts`) \u2014 L1 + L2 only; the
 *     porting-checklist inventory. For consumers building their own
 *     L3 facade.
 *
 * The two layers below are kept legible by section header even at the
 * bare entry: the first re-exports everything from `./core.ts` (L1 +
 * L2), the second adds the L3 conveniences on top.
 *
 * See also `docs/todo/sdk-rework.md` for the annotated export map and
 * `sdks/typescript/README.md` for the user-facing layer description.
 */

// ----------------------------------------------------------------------------
// L1 + L2 \u2014 procedure bindings + core behaviours
//
// Re-exported verbatim from the `instructed-sdk/core` sub-path entry.
// Adding or removing a symbol from `core.ts` automatically updates the
// bare entry; that's the source of truth for the porting-checklist
// surface.
// ----------------------------------------------------------------------------

export * from "./core.ts";

// ----------------------------------------------------------------------------
// L3 \u2014 conveniences
//
// The idiomatic facade and the consistency-on-dispatch helper. None of
// these are part of the porting-checklist surface; a Python / Go /
// Elixir port may ship something quite different here and still be
// conformant. See `SDK-REWORK-NOTES.md` \u00a72 and D-0027.
// ----------------------------------------------------------------------------

// `PartitionBy` sugar over a routing-layer `RoutingFn`. Pure L3 --
// lives in its own file so the file boundary matches the layer
// boundary.
export {
  routingFnForPartitionBy,
  SEQUENTIAL_PARTITION_KEY,
  type PartitionBy,
} from "./partition-by.ts";

// `runCommandWithSnapshots`: L3 orchestrator that wraps the L2
// `runCommandAndApply` primitive with best-effort snapshot writes
// per `def.snapshotPolicy`. The `Instructed` facade and the PM
// worker both delegate to this; direct callers who want snapshot
// orchestration without the facade also use this entry.
export { runCommandWithSnapshots } from "./aggregate-snapshots.ts";

// PM worker wrapper (L3): thin convenience over `startPmSubstrate`
// (L2, exported from `instructed-sdk/core`) that interprets a
// user-returned `commands` list and dispatches each command via
// `runCommandWithSnapshots` between `handle` and the substrate's
// snapshot+ack tx. Idiomatic-not-required: a port may ship a
// different shape for "PM handler returns commands" and still be
// conformant. The `Instructed` facade and direct callers wanting
// the by-value commands shape use this entry.
export {
  startPmWorker,
  type PmDefinition,
  type PmHandleResult,
  type PmHandlerContext,
  type PmWorkerOptions,
  type DispatchedCommand,
} from "./pm-worker.ts";

// Retry/error-policy standard library (L3). The contract
// (`ErrorPolicy<PolicyState>`) and the observable default
// (`DEFAULT_ERROR_POLICY`) live in `processing-worker.ts` (L2);
// these are the composable helpers users reach for when writing
// non-default policies. See `error-policies.ts` module comment
// for composition examples.
export {
  exponentialBackoff,
  linearBackoff,
  retryUpTo,
  type ExponentialBackoffOptions,
  type LinearBackoffOptions,
} from "./error-policies.ts";

// Consistency-on-dispatch wait (polls the L1 `is_subscription_caught_up`
// predicate). ML-0002 may eventually rework the mechanism into
// LISTEN/NOTIFY; the L3 shape is intended to stay stable across that
// change.
export {
  waitForProjection,
  DEFAULT_WAIT_POLL_INTERVAL_MS,
  DEFAULT_WAIT_TIMEOUT_MS,
  type SubscriptionRef,
  type WaitForProjectionOptions,
} from "./consistency.ts";

// L3 error classes (emitted only by the facade / consistency helpers).
export {
  ConsistencyTimeout,
  ConsistencyTargetError,
  UnknownAggregateType,
} from "./errors.ts";

// The `Instructed` facade: by-name aggregate dispatch, projection / PM
// registration, single `startWorker()`, single `close()`.
export {
  Instructed,
  type InstructedOptions,
  type InstructedDefaults,
  type RegistrationOptions,
  type RegisterProjectionInput,
  type RegisterProcessManagerInput,
  type DispatchOptions,
} from "./instructed.ts";
