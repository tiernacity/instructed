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
 * Expected-version tag passed to `append_to_stream`.
 *
 * See `sql/instructed.sql` :: append_to_stream and §3 of sdk-design.md.
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
 */
export interface NewEvent<E = unknown> {
  event_id?: string;
  event_type: string;
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

/** A recorded event row, the shape returned by read_stream / read_all /
 *  read_subscription_batch. */
export interface RecordedEvent<E = unknown> {
  event_id: string;
  event_number: bigint;
  stream_uuid: string;
  stream_version: bigint;
  event_type: string;
  causation_id: string | null;
  correlation_id: string | null;
  data: E;
  metadata: unknown | null;
  created_at: Date;
}

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
      claimedBy: string;
      claimExpiresAt: Date;
    };

/** Options recognised by `claim_subscription.p_options.start_from`. */
export type StartFrom = "origin" | "current" | bigint | number;

export interface ClaimSubscriptionOptions {
  startFrom?: StartFrom;
  /** Reserved (ML-0013); v1 callers should omit. */
  shard?: number;
}

export interface SubscriptionShardOption {
  shard?: number;
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
