/**
 * SQLSTATE → typed Error subclass translation (barrel).
 *
 * This is a barrel: it contains nothing but re-exports. Each group lives
 * in its own file:
 *
 *   - `base.ts`         — `InstructedError`, `InvalidParameterValue`
 *   - `append.ts`       — IS00x append-path errors
 *   - `snapshot.ts`     — IS010 snapshot error
 *   - `subscription.ts` — IS02x subscription errors
 *   - `work-item.ts`    — IS030 work-item error
 *   - `sdk.ts`          — SDK-level (no SQLSTATE) errors
 *   - `map-pg-error.ts` — the L1-internal `mapPgError` / `MapPgErrorContext`
 *
 * **Layer membership** (per [D-0027](../../../docs/decisions.md#d-0027)):
 *
 *   - **L1 (procedure bindings)** classes are SQLSTATE-bound and form
 *     part of the contract every SDK port must reproduce. The base
 *     `InstructedError`, the IS00x append errors, `SnapshotNotFound`,
 *     the IS02x subscription errors, `WorkItemLeaseLost`, and
 *     `InvalidParameterValue` are all L1. Re-exported from `instructed-sdk/core`.
 *   - **L2 (core behaviours)** — `RetryBudgetExhausted` (aggregate retry
 *     loop). Re-exported from `instructed-sdk/core`.
 *   - **L3 (conveniences)** — `ConsistencyTimeout`, `ConsistencyTargetError`,
 *     `UnknownAggregateType`. Re-exported only from the bare `instructed-sdk`
 *     entry, not from `/core`.
 *   - `mapPgError` / `MapPgErrorContext` are L1-internal: not re-exported
 *     from any public entry.
 *
 * The layer split is wired in `src/core.ts` and `src/index.ts`.
 */

export { InstructedError, InvalidParameterValue } from "./base.ts";
export {
  AppendError,
  WrongExpectedVersion,
  StreamExists,
  StreamNotFound,
  DuplicateEvent,
  ReservedStreamUuid,
  AppendOnlyViolation,
} from "./append.ts";
export { SnapshotNotFound } from "./snapshot.ts";
export {
  SubscriptionError,
  SubscriptionNotFound,
  SubscriptionAlreadyClaimed,
  SubscriptionLeaseLost,
} from "./subscription.ts";
export { WorkItemLeaseLost } from "./work-item.ts";
export {
  RetryBudgetExhausted,
  ConsistencyTimeout,
  ConsistencyTargetError,
  UnknownAggregateType,
} from "./sdk.ts";
export { mapPgError } from "./map-pg-error.ts";
export type { MapPgErrorContext } from "./map-pg-error.ts";
