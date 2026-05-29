/**
 * SUB-A work-queue types: the routing decisions and the per-work-item
 * claim / completion wire shapes.
 */

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
