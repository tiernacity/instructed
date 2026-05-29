/**
 * L2/L3 aggregate runner (barrel).
 *
 * This is a barrel: it contains nothing but re-exports.
 *
 *   - `run-command.ts`     — L2 load→execute→append loop with OCC retry
 *                            (`runCommand`, `runCommandAndApply`) plus the
 *                            `AggregateDefinition` contract and helpers.
 *   - `snapshot-policy.ts` — the `SnapshotPolicy<S>` contract + `everyN`.
 *   - `snapshots.ts`       — L3 `runCommandWithSnapshots` orchestrator.
 *   - `snapshot-version.ts`— the SNAP-002 reserved metadata key.
 *
 * The L2/L3 layer split for the public entries is wired in `src/core.ts`
 * (L2: runCommand/runCommandAndApply/everyN/SnapshotPolicy/…) and
 * `src/index.ts` (L3: runCommandWithSnapshots).
 */

export {
  runCommand,
  runCommandAndApply,
  prefixType,
  DEFAULT_RETRY_BUDGET,
} from "./run-command.ts";
export type {
  AggregateDefinition,
  DispatchContext,
  DomainEvent,
  RanCommand,
  RunCommandOptions,
} from "./run-command.ts";
export { everyN } from "./snapshot-policy.ts";
export type { SnapshotPolicy } from "./snapshot-policy.ts";
export { runCommandWithSnapshots } from "./snapshots.ts";
export { SNAPSHOT_MODULE_VERSION_KEY } from "./snapshot-version.ts";
