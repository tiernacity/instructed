/**
 * L2 PM substrate.
 *
 * The "process-manager" half of SUB-A without the command-dispatch
 * orchestration: load PM-instance state, run `apply` over the
 * claimed event, run `handle` to get a completion signal, and
 * commit the snapshot+ack tx. The substrate is *unopinionated*
 * about what the user does in `handle` — it accepts a return shape
 * of `{ complete?: boolean }` only.
 *
 * The PM workers split across two layers (see
 * `sdks/porting-checklist.md` §3 for the required-core /
 * idiomatic-not-required treatment):
 *
 *   - **L2 substrate (this file).** Snapshot+ack tx, rebuild on
 *     snapshot miss / module-version mismatch, lease management
 *     via `startProcessingWorker`. Required-core for any port: a
 *     PM substrate is the minimal kernel a port reproduces to
 *     support process managers.
 *   - **L3 wrapper (`pm-worker.ts`).** Wraps the substrate; the
 *     user's `handle` returns `{ commands?, complete? }`; the
 *     wrapper dispatches commands via `runCommandWithSnapshots`
 *     between `handle` and the substrate's snapshot+ack.
 *     Idiomatic-not-required: a port may ship a different shape
 *     for "PM handler returns commands" (yield-style, eager
 *     dispatch, etc.) and still be conformant.
 *
 * # Lifecycle (per work item)
 *
 *   1. Claim a work item via the kind-agnostic processing-worker
 *      loop.
 *   2. Load PM-instance state for the claimed event's partition:
 *      - If a snapshot exists AND its `snapshot_module_version`
 *        (carried in metadata, the SDK-reserved key) matches
 *        `def.snapshotModuleVersion` (or both are absent), use
 *        the snapshot's `data` as state.
 *      - Otherwise rebuild: fold every previously-`done` event
 *        for the partition through `apply` from
 *        `initialState()`. Events fetched via the cold-path
 *        `list_pm_rebuild_events` SQL function. PM-C-shaped:
 *        only `apply` runs during rebuild; `handle` does not.
 *   3. Run `apply(state, claimedEvent)` → staged_state.
 *   4. Run `handle(staged_state, claimedEvent)` → `{ complete? }`.
 *   5. Commit the slice-5 `complete` callback:
 *      - If `handle` returned `{ complete: true }` →
 *        `complete_pm_instance` (DELETE snapshot + all work-items
 *        for the partition in one tx).
 *      - Else → `complete_work_item_pm` (UPDATE work item to
 *        'done' + UPSERT snapshot in one tx; snapshot payload =
 *        staged_state, source_version = claimedEvent.event_number).
 *
 * Per D-0016 the user `handle` is opaque to the SDK: it receives
 * the event and the staged state; it does NOT receive any
 * framework-owned resource.
 *
 * # PM-E gap (unchanged by this split)
 *
 * The substrate itself does no dispatch — it has no PM-E concern.
 * The L3 wrapper's dispatch loop carries the same PM-E gap as
 * before the split: re-running `handle` (SUB-B `retry-in` after a
 * post-dispatch failure, or a lease-takeover redelivery) may
 * re-issue commands that already committed at the aggregate. See
 * `docs/invariants.md` "Honest gaps in v1" entry 2 and the future
 * PM-E work for deterministic event IDs.
 */

import type { Client } from "./client.ts";
import { prefixType } from "./aggregate.ts";
import { SnapshotNotFound } from "./errors/index.ts";
import { SNAPSHOT_MODULE_VERSION_KEY } from "./snapshot-version.ts";
import {
  startProcessingWorker,
  type ErrorPolicy,
  type ProcessingHandlerContext,
  type ProcessingWorkerOptions,
} from "./processing-worker.ts";
import type { Event, RecordedEvent } from "./types/index.ts";
import type { RunningWorker } from "./internal/running-worker.ts";

// ============================================================================
// Public surface
// ============================================================================

/**
 * What a substrate `handle` returns. Strictly snapshot+ack
 * orchestration: signal whether the partition is terminated.
 * Side effects (command dispatch, external writes) are the L3
 * wrapper's responsibility and happen inside the `handle`
 * implementation the wrapper supplies to the substrate.
 */
export interface PmSubstrateHandleResult {
  /** Terminate the partition: DELETE snapshot + all work-items. */
  complete?: boolean;
}

/**
 * Context handed to a substrate `handle`. Identical to the
 * kind-agnostic processing-worker context; aliased here for the
 * substrate's self-contained public surface.
 */
export type PmSubstrateHandlerContext = ProcessingHandlerContext;

/**
 * PM substrate definition.
 *
 *   - `apply`: pure state fold. Runs during rebuild and on the
 *     claimed event before `handle`. MUST NOT have side effects.
 *   - `handle`: produce a completion signal. Opaque to the SDK
 *     per D-0016. Any side effects (command dispatch, external
 *     writes) are the caller's; the substrate sees only
 *     `{ complete? }`.
 *   - `initialState`: starting state for a brand-new partition.
 *   - `snapshotModuleVersion`: optional SDK-managed string used
 *     to detect when application-level state shape has changed
 *     and a rebuild is required (SNAP-002 / PM-C). Stored in the
 *     snapshot's `metadata.snapshot_module_version` key on
 *     write; compared on read.
 */
export interface PmSubstrateDefinition<S, E extends Event = Event, PolicyState = undefined> {
  /** PM type — doubles as the subscription name and the snapshot
   *  source_type prefix. Same role as `AggregateDefinition.type`. */
  type: string;
  /**
   * Optional encoding from a PM partition key to the snapshot
   * source_uuid. Default: `${type}-${partitionKey}` (see
   * {@link prefixType}). Rarely overridden — the source_uuid is
   * an internal storage concern; applications identify PM
   * instances by `(type, partitionKey)`.
   */
  streamName?(partitionKey: string): string;
  /** Default `$all`. */
  stream?: string;
  initialState(): S;
  apply(state: S, event: RecordedEvent<E>): S;
  handle(
    state: S,
    event: RecordedEvent<E>,
    ctx: PmSubstrateHandlerContext,
  ): Promise<PmSubstrateHandleResult> | PmSubstrateHandleResult;
  /** SDK-managed snapshot version tag; mismatch triggers rebuild. */
  snapshotModuleVersion?: string;
  /**
   * Retry/error-policy hook. Defaults to `DEFAULT_ERROR_POLICY`
   * (exponential backoff, retry forever). Type-parameterised by
   * `PolicyState` for callers writing stateful policies; defaults
   * to `ErrorPolicy<undefined>`.
   */
  errorPolicy?: ErrorPolicy<PolicyState>;
}

export type PmSubstrateOptions = ProcessingWorkerOptions;

// ============================================================================
// Implementation
// ============================================================================

interface StagedWork<S> {
  sourceUuid: string;
  stagedState: S;
  complete: boolean;
}

/**
 * Start a PM substrate worker. Caller is responsible for the
 * routing-worker side (one `startRoutingWorker` per subscription);
 * the layer-5 facade (`Instructed`) glues the two together at
 * registration time.
 *
 * Per [D-0026](../../../docs/decisions.md#d-0026) the PM persist-
 * and-ack path uses the same `client` as any caller-supplied
 * downstream work: the SQL contract's per-procedure
 * lock-acquisition orders and the pairwise disjoint lock sets are
 * what prevent deadlock, not client / pool separation.
 */
export function startPmSubstrate<S, E extends Event = Event, PolicyState = undefined>(
  client: Client,
  def: PmSubstrateDefinition<S, E, PolicyState>,
  opts: PmSubstrateOptions = {},
): RunningWorker {
  const stream = def.stream ?? "$all";

  // Per-item staged state shared between the slice-5 `handle` and
  // `complete` callbacks. Keyed by `${partitionKey}:${eventNumber}`.
  // Re-running `handle` (SUB-B retry-in) overwrites prior entries; on
  // `stop` the `complete` callback never fires and a stale entry just
  // orphans (the worker is exiting). Lease loss during heartbeat also
  // exits without running `complete`. No cleanup is required.
  const staged = new Map<string, StagedWork<S>>();
  function key(pk: string, en: bigint): string {
    return `${pk}:${en.toString()}`;
  }

  async function loadState(
    sourceUuid: string,
    claimedPartitionKey: string,
    claimedEventNumber: bigint,
  ): Promise<S> {
    // Snapshot path first; rebuild on miss or module-version mismatch.
    let snapData: S | null = null;
    let snapModuleVersion: string | undefined;
    try {
      const snap = await client.readSnapshot<S>(sourceUuid);
      snapData = snap.data;
      if (
        snap.metadata &&
        typeof snap.metadata === "object" &&
        snap.metadata !== null
      ) {
        const v = (snap.metadata as Record<string, unknown>)[
          SNAPSHOT_MODULE_VERSION_KEY
        ];
        if (typeof v === "string") snapModuleVersion = v;
      }
    } catch (err) {
      if (!(err instanceof SnapshotNotFound)) throw err;
    }

    const want = def.snapshotModuleVersion;
    const matches =
      snapData !== null &&
      (want === undefined ? snapModuleVersion === undefined : snapModuleVersion === want);

    if (matches) {
      return snapData as S;
    }

    // Rebuild: fold every prior `done` event for the partition through
    // `apply` from initialState. The claimed event itself is excluded;
    // the caller folds it after.
    const priorEvents = await client.listPmRebuildEvents<E>(
      stream,
      def.type,
      claimedPartitionKey,
      claimedEventNumber,
    );
    let state = def.initialState();
    for (const ev of priorEvents) state = def.apply(state, ev);
    return state;
  }

  const sourceUuidOf = def.streamName ?? prefixType(def.type);
  const adapted = {
    name: def.type,
    stream,
    errorPolicy: def.errorPolicy,
    handle: async (event: RecordedEvent<E>, ctx: ProcessingHandlerContext) => {
      const sourceUuid = sourceUuidOf(ctx.partitionKey);
      const baseState = await loadState(
        sourceUuid,
        ctx.partitionKey,
        ctx.eventNumber,
      );
      const stagedState = def.apply(baseState, event);
      const result = await def.handle(stagedState, event, ctx);
      // The substrate doesn't dispatch. Side effects between `handle`
      // and the snapshot+ack tx are the caller's responsibility, and
      // happen inside their `handle` implementation. If they throw,
      // we never reach `complete` -> SUB-B error-policy invoked; work
      // item stays `claimed` and the lease is held by the slice-5
      // heartbeat.
      staged.set(key(ctx.partitionKey, ctx.eventNumber), {
        sourceUuid,
        stagedState,
        complete: result.complete === true,
      });
    },
    complete: async (
      event: RecordedEvent<E>,
      ctx: ProcessingHandlerContext,
    ) => {
      const k = key(ctx.partitionKey, ctx.eventNumber);
      const entry = staged.get(k);
      if (!entry) {
        // Should never happen: `complete` only runs after `handle`
        // succeeds, and `handle` always stores an entry on success.
        throw new Error(
          `pm substrate: no staged state for ${ctx.partitionKey}/${ctx.eventNumber}; this is a bug`,
        );
      }
      staged.delete(k);
      if (entry.complete) {
        // PM-F terminal: DELETE snapshot + all work-items for the
        // partition in one tx.
        await client.completePmInstance(
          stream,
          def.type,
          ctx.partitionKey,
          entry.sourceUuid,
        );
        return;
      }
      // Non-terminal: UPDATE work item -> 'done' + UPSERT snapshot in
      // one tx. The snapshot's metadata carries the module-version
      // tag for the next state-load.
      const metadata: Record<string, unknown> = {};
      if (def.snapshotModuleVersion !== undefined) {
        metadata[SNAPSHOT_MODULE_VERSION_KEY] = def.snapshotModuleVersion;
      }
      await client.completeWorkItemPm<S>(
        stream,
        def.type,
        ctx.workerId,
        ctx.partitionKey,
        ctx.eventNumber,
        {
          sourceUuid: entry.sourceUuid,
          sourceType: def.type,
          sourceVersion: event.event_number,
          data: entry.stagedState,
          metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
        },
      );
    },
  };

  return startProcessingWorker<E, PolicyState>(client, adapted, opts);
}
