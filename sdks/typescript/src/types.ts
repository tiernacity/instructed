/**
 * Public types for the instructed SDK.
 *
 * Mirrors the SQL contract in `sql/instructed.sql`. The SQL file is the
 * spec; anything here that disagrees is a bug in the SDK.
 */

import type * as pg from "pg";

/**
 * A connection-like object accepted by the SDK. Matches absurd's idiom:
 * any pg.Pool / pg.Client / pg.PoolClient (anything with `.query`).
 */
export type Queryable =
  | Pick<pg.Client, "query">
  | Pick<pg.PoolClient, "query">;

/** A JSON value, used for event data / metadata / snapshot data. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/**
 * Structural contract for a domain event.
 *
 * Applications declare their own event types (typically a discriminated
 * union over the literal `type` field) and pass them to SDK APIs; the
 * type-checker enforces compatibility with this shape. The SDK does NOT
 * require user code to `extends Event<...>` — `Event` is the contract,
 * not a base class.
 *
 * Recommended declaration pattern (no SDK import needed in the event
 * file itself):
 *
 *     export const AccountDepositedTo = "AccountDepositedTo" as const;
 *     export type AccountDepositedTo = {
 *       type: typeof AccountDepositedTo;
 *       data: { amount: number; transferId?: string };
 *     };
 *
 * The value-and-type-share-a-name pattern lets the same identifier
 * stand in for the literal-string discriminator (in value position)
 * and the event's TypeScript type (in type position).
 */
export interface Event<T extends string = string, D = unknown> {
  type: T;
  data: D;
}

/**
 * Structural contract for a command.
 *
 * Same pattern as {@link Event}: applications declare commands as
 * discriminated unions over the literal `type` field; SDK APIs that
 * accept commands type-check the user's union against this shape.
 */
export interface Command<T extends string = string> {
  type: T;
}

/**
 * Expected-version tag passed to `append_to_stream`.
 *
 * See `sql/instructed.sql` :: append_to_stream for the SQL
 * contract; `src/core.ts` and `src/index.ts` for the SDK's
 * export inventory.
 */
export type ExpectedVersion =
  | { kind: "any" }
  | { kind: "noStream" }
  | { kind: "streamExists" }
  | { kind: "exact"; version: bigint };

/** Constructors mirroring the SQL `expected_version_type` values. */
export const expected = {
  any: { kind: "any" } as const,
  noStream: { kind: "noStream" } as const,
  streamExists: { kind: "streamExists" } as const,
  exact(version: bigint | number): ExpectedVersion {
    return { kind: "exact", version: BigInt(version) };
  },
};

/**
 * An event the caller wants to append. The SDK fills `event_id`,
 * `causation_id`, and `correlation_id` when omitted (§11.2 / §11.8).
 *
 * Generic `E` is the application's event union (each member extending
 * {@link Event}); the type distributes over the union so each branch
 * carries its own `type` literal and `data` shape. Pass the union
 * (e.g. `NewEvent<AccountEvent>`) to get discriminated-union narrowing
 * inside switches on `type`. Default `E = Event` gives the historical
 * open shape (`type: string`, `data: unknown`).
 *
 * Note: the field is `type`, not `event_type` — the underlying SQL
 * column is `event_type`, but the SDK normalises the TypeScript
 * surface to a single `type` field. The `Client.appendToStream` and
 * `Client.readStream` boundary maps between the two (L2-only rename;
 * L1 wire / SQL column unchanged).
 */
export interface NewEvent<E = unknown> {
  event_id?: string;
  type: string;
  data: E;
  metadata?: unknown;
  causation_id?: string;
  correlation_id?: string;
}

/** One row returned by `append_to_stream`, in append order.
 *
 *  `stream_uuid` is populated client-side from the `appendToStream`
 *  argument (the SQL procedure doesn't echo it back). It is
 *  load-bearing for the CON-B cross-stream guard in
 *  `waitForProjection`. */
export interface AppendedEvent {
  event_id: string;
  stream_uuid: string;
  stream_version: bigint;
  event_number: bigint;
  created_at: Date;
}

/** Bookkeeping fields shared by every recorded event, independent of
 *  the domain payload. Internal building block for {@link RecordedEvent};
 *  not part of the porting-checklist surface. */
export interface RecordedEventFields {
  event_id: string;
  event_number: bigint;
  stream_uuid: string;
  stream_version: bigint;
  causation_id: string | null;
  correlation_id: string | null;
  metadata: unknown | null;
  created_at: Date;
}

/** A recorded event row, the shape returned by read_stream / read_all /
 *  list_pm_rebuild_events.
 *
 *  Generic `E` is the application's event union (each member extending
 *  {@link Event}); the type distributes so each branch carries its own
 *  `type` literal and `data` shape. Inside
 *  `switch (event.type) { case "Foo": ... }`, `event.data` is narrowed
 *  to the matching branch's data — no casting needed. Default
 *  `E = Event` gives the historical open shape.
 *
 *  Note: `type` (not `event_type`) — see {@link NewEvent}. */
export type RecordedEvent<E extends Event = Event> = E extends Event
  ? RecordedEventFields & { type: E["type"]; data: E["data"] }
  : never;

/** Options for `append_to_stream`. v1 has no recognised keys. */
export interface AppendOptions {
  /** Reserved for future use; currently unused. */
}

/** Input to `record_snapshot`. */
export interface SnapshotInput<S = unknown> {
  sourceUuid: string;
  sourceType: string;
  sourceVersion: bigint;
  data: S;
  metadata?: unknown;
}

/** A snapshot row as returned by `read_snapshot`. */
export interface Snapshot<S = unknown> {
  sourceUuid: string;
  sourceType: string;
  sourceVersion: bigint;
  data: S;
  metadata: unknown | null;
  createdAt: Date;
}

/**
 * Result of `claim_subscription`. The 'already_claimed' variant is
 * informational, not an error — see `sql/instructed.sql` ::
 * claim_subscription.
 */
/**
 * Result of {@link Client.claimSubscription}.
 *
 * - `'claimed'`: the caller now holds the lease. `claimedBy` is the
 *   caller's `workerId`; `claimExpiresAt` is the (server-side) lease
 *   expiry. Both fields are always populated.
 * - `'already_claimed'`: another worker (or another concurrent
 *   transaction) holds or is mid-write on the row. `claimedBy` and
 *   `claimExpiresAt` are *diagnostic* fields and **may be null** under
 *   one specific race in the SQL contract: the `FOR UPDATE SKIP LOCKED`
 *   pre-check sees the row locked and the unlocked re-read either
 *   misses the row (deleted between checks) or sees the released
 *   `(NULL, NULL)` between-batches state under D-0025. The SDK's
 *   routing-worker loop reacts to `'already_claimed'` by backing off
 *   and retrying; it does not consume the diagnostic fields. See
 *   `docs/sql-contract.md` `claim_subscription`.
 */
export type ClaimResult =
  | {
      result: "claimed";
      lastSeen: bigint;
      claimedBy: string;
      claimExpiresAt: Date;
    }
  | {
      result: "already_claimed";
      lastSeen: bigint;
      claimedBy: string | null;
      claimExpiresAt: Date | null;
    };

/** Options recognised by `claim_subscription.p_options.start_from`. */
export type StartFrom = "origin" | "current" | bigint | number;

export interface ClaimSubscriptionOptions {
  startFrom?: StartFrom;
}

// ============================================================================
// SUB-A work-queue types
// ============================================================================

/**
 * One element in `route_batch`'s decisions array. The routing worker emits
 * one of these per event that `RouteFn` decided to route (events routed to
 * `"ignore"` produce no decision).
 */
export interface RouteDecision {
  partitionKey: string;
  eventNumber: bigint;
}

/** Return type of `Client.routeBatch`. */
export interface RouteBatchResult {
  insertedCount: bigint;
  newLastSeen: bigint;
}

/** Return type of `Client.claimWorkItem` (or `null` when the queue is empty). */
export interface ClaimedWorkItem {
  partitionKey: string;
  eventNumber: bigint;
  claimedBy: string;
  leaseExpiresAt: Date;
  /** True iff this claim displaced a different worker whose lease had expired. */
  wasTakeover: boolean;
  /** The previous holder on a takeover; null otherwise. */
  priorClaimedBy: string | null;
}

/** Return type of `Client.completePmInstance`. */
export interface CompletePmInstanceResult {
  workItemsDeleted: bigint;
  snapshotDeleted: boolean;
}
