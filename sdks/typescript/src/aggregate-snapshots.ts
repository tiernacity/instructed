/**
 * L3 aggregate orchestration: `runCommand` plus best-effort snapshot
 * writes.
 *
 * Snapshot policy invocation is an L3 concern. The L2 primitive
 * `runCommandAndApply` (in `aggregate.ts`) does load + execute +
 * append + OCC retry and returns the post-append state; this
 * file's `runCommandWithSnapshots` wraps that primitive and, on
 * success, invokes `def.snapshotPolicy.shouldSnapshot` and writes
 * the snapshot best-effort via a separate `recordSnapshot` call.
 *
 * # Events vs. snapshots
 *
 * Events and snapshots are different kinds of thing and warrant
 * different error responses:
 *
 *   - **Events are a correctness concern.** Event sourcing
 *     *requires* that emitted events persist; failure here must
 *     fail the command and surface to the caller. That's the L2
 *     primitive's job.
 *   - **Snapshots are a performance concern.** They exist so that
 *     loading an aggregate doesn't re-fold from origin every
 *     time. A missing or stale snapshot makes the next load
 *     slower; it does *not* break correctness. Failure here is a
 *     warning, not an error.
 *
 * Atomic persistence (single SQL call that takes both events and
 * an optional snapshot) would *couple* these failure modes,
 * forcing a snapshot-write failure to either fail the command
 * (wrong: snapshots aren't correctness-critical) or be silently
 * swallowed inside one tx (wrong: hides observable signal).
 * Keeping the two as separate L1 calls preserves the distinction.
 * See D-0019.
 *
 * # The contract this layer orchestrates
 *
 * Snapshot policy is one of the SDK's three named extension points
 * (see `sdks/porting-checklist.md` §4.2). The contract lives on
 * `AggregateDefinition.snapshotPolicy` in `aggregate.ts`; the
 * standard library currently ships `everyN(n)` only. This file is
 * the orchestrator: it decides when to call the policy, what state
 * to pass it, and what to do with the result.
 *
 * # Layer note
 *
 * Lives in its own file (separate from `aggregate.ts`) so the file
 * boundary matches the layer boundary: `aggregate.ts` is pure L2
 * (load + execute + append + OCC retry, unopinionated about
 * snapshots); this module is pure L3 (policy invocation + the
 * "snapshot is best-effort" semantics).
 */

import type { Client } from "./client.ts";
import {
  runCommandAndApply,
  type AggregateDefinition,
  type DomainEvent,
  type RunCommandOptions,
} from "./aggregate.ts";
import { DEFAULT_LOGGER_IMPL, Logger } from "./logger.ts";
import { SNAPSHOT_MODULE_VERSION_KEY } from "./snapshot-version.ts";
import type { AppendedEvent } from "./types.ts";

/**
 * Run a command against an aggregate stream with snapshot
 * orchestration. Same load + execute + append + OCC-retry
 * semantics as {@link runCommand}, plus: if `def.snapshotPolicy`
 * is set and `shouldSnapshot(state, version, eventsSinceLast)`
 * returns `true` on the post-append state, write a snapshot via
 * `recordSnapshot`.
 *
 * Snapshot writes are best-effort per D-0019: failures are reported
 * via `opts.ctx.logger.warn` (or, when no ctx is supplied, via the
 * default logger's `console.warn` per `DEFAULT_LOGGER_IMPL`) and do
 * not fail the command. The next load will
 * fall back to the previous snapshot (or full re-fold from
 * origin) and the next successful command may snapshot again.
 *
 * For a no-op command (handler returned no events), no snapshot
 * is considered: there is no new state to capture.
 *
 * Returns the appended events. A future revision may return the
 * richer {@link RanCommand} record; until a concrete caller needs
 * it, this matches `runCommand`'s shape so the migration from
 * `runCommand`-with-snapshot-policy is mechanical.
 */
export async function runCommandWithSnapshots<
  S,
  C,
  E extends DomainEvent = DomainEvent,
>(
  client: Client,
  def: AggregateDefinition<S, C, E>,
  streamUuid: string,
  command: C,
  opts: RunCommandOptions = {},
): Promise<AppendedEvent[]> {
  const result = await runCommandAndApply(client, def, streamUuid, command, opts);

  // No events appended (handler no-op): nothing to snapshot.
  if (result.appended.length === 0) return result.appended;

  // No policy declared: skip orchestration entirely.
  if (!def.snapshotPolicy) return result.appended;

  if (
    def.snapshotPolicy.shouldSnapshot(
      result.state,
      result.version,
      result.eventsSinceSnapshot,
    )
  ) {
    // SNAP-002: stamp the module-version metadata so the next
    // load can detect a shape change. Strict semantics on read:
    // a snapshot written with a version is rejected by a def
    // without one, and vice versa.
    const metadata =
      def.snapshotModuleVersion !== undefined
        ? { [SNAPSHOT_MODULE_VERSION_KEY]: def.snapshotModuleVersion }
        : undefined;
    try {
      await client.recordSnapshot({
        sourceUuid: streamUuid,
        sourceType: def.type,
        sourceVersion: result.version,
        data: result.state,
        metadata,
      });
    } catch (snapErr) {
      // Best-effort per D-0019. The load path works without the
      // snapshot — it'll re-fold from the previous snapshot (or
      // origin). Routed through the dispatch logger so the
      // application's logger sink sees it. When no ctx is supplied
      // at the L2 boundary, the fallback (set inside `executeCommand`)
      // wraps `DEFAULT_LOGGER_IMPL` so the warning still surfaces.
      const ctxLogger =
        opts.ctx?.logger ?? Logger.fromImpl(DEFAULT_LOGGER_IMPL);
      ctxLogger.warn(
        () =>
          `snapshot write failed for ${streamUuid}: ${describeError(snapErr)}`,
      );
    }
  }

  return result.appended;
}

function describeError(err: unknown): string {
  if (err instanceof Error) return err.message;
  try {
    return String(err);
  } catch {
    return "<unprintable error>";
  }
}
