/**
 * The aggregate snapshot-policy extension point: the contract
 * (`SnapshotPolicy<S>`) and the shipped standard-library policy
 * (`everyN`). See `sdks/porting-checklist.md` §4.2.
 *
 * The policy is consulted by the L3 `runCommandWithSnapshots` wrapper
 * (`snapshots.ts`), not by the L2 `runCommand` primitive.
 */

/**
 * Aggregate snapshot policy — the contract half of the
 * snapshot-policy extension point.
 *
 * `eventsSinceLast` counts events folded into the current state
 * since the last persisted snapshot (or since `initialState()`
 * for a never-snapshotted stream). The policy is consulted by the
 * L3 `runCommandWithSnapshots` wrapper, not by the L2 `runCommand`
 * primitive.
 */
export interface SnapshotPolicy<S> {
  shouldSnapshot(state: S, version: bigint, eventsSinceLast: number): boolean;
}

/**
 * Standard-library policy: snapshot once `eventsSinceLast` reaches
 * `n`. The only shipped policy as of step-5 slice 2; further
 * helpers (time-elapsed, state-size-threshold) will be added if a
 * concrete use case demands them.
 */
export function everyN<S>(n: number): SnapshotPolicy<S> {
  if (!Number.isFinite(n) || n <= 0) {
    throw new RangeError(`everyN: n must be a positive integer, got ${n}`);
  }
  return {
    shouldSnapshot: (_s, _v, eventsSinceLast) => eventsSinceLast >= n,
  };
}
