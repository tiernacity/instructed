/** Subscription claim wire shapes. */

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
      result: 'claimed'
      lastSeen: bigint
      claimedBy: string
      claimExpiresAt: Date
    }
  | {
      result: 'already_claimed'
      lastSeen: bigint
      claimedBy: string | null
      claimExpiresAt: Date | null
    }

/** Options recognised by `claim_subscription.p_options.start_from`. */
export type StartFrom = 'origin' | 'current' | bigint | number

export interface ClaimSubscriptionOptions {
  startFrom?: StartFrom
}
