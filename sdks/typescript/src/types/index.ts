/**
 * Public types for the instructed SDK (barrel).
 *
 * Mirrors the SQL contract in `sql/instructed.sql`. The SQL file is the
 * spec; anything here that disagrees is a bug in the SDK.
 *
 * This is a barrel: it contains nothing but re-exports. Each wire-shape
 * group lives in its own file:
 *
 *   - `queryable.ts`        — `Queryable`, `JsonValue` (foundational primitives)
 *   - `event.ts`            — `Event`, `NewEvent`, `AppendedEvent`,
 *                             `RecordedEvent`(+`RecordedEventFields`), `AppendOptions`
 *   - `command.ts`          — `Command`
 *   - `expected-version.ts` — `ExpectedVersion`, `expected`
 *   - `snapshot.ts`         — `SnapshotInput`, `Snapshot`
 *   - `subscription.ts`     — `ClaimResult`, `StartFrom`, `ClaimSubscriptionOptions`
 *   - `work-item.ts`        — SUB-A work-queue shapes
 */

export type { Queryable, JsonValue } from './queryable.ts'
export type {
  Event,
  NewEvent,
  AppendedEvent,
  RecordedEventFields,
  RecordedEvent,
  AppendOptions,
} from './event.ts'
export type { Command } from './command.ts'
export { expected } from './expected-version.ts'
export type { ExpectedVersion } from './expected-version.ts'
export type { SnapshotInput, Snapshot } from './snapshot.ts'
export type { ClaimResult, StartFrom, ClaimSubscriptionOptions } from './subscription.ts'
export type {
  RouteDecision,
  RouteBatchResult,
  ClaimedWorkItem,
  CompletePmInstanceResult,
} from './work-item.ts'
