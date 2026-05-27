/**
 * SUB-A slice 7 — process-manager processing worker.
 *
 * Thin adapter over `startProcessingWorker` (slice 5) that supplies the
 * PM-side state-load + command-dispatch + snapshot-write step. The
 * worker:
 *
 *   1. Claims a work item via the kind-agnostic processing-worker loop.
 *   2. Loads PM-instance state for the claimed event's partition:
 *      - If a snapshot exists AND its `snapshot_module_version`
 *        (carried in metadata, the SDK-reserved key) matches
 *        `def.snapshotModuleVersion` (or both are absent), uses the
 *        snapshot's `data` as state.
 *      - Otherwise rebuilds: folds every previously-`done` event for
 *        the partition through the PM's `apply` callback from
 *        `initialState()`. The list of events is fetched via the
 *        cold-path `list_pm_rebuild_events` SQL function. PM-C-shaped:
 *        only `apply` runs during rebuild; `handle` does not.
 *   3. Runs `apply(state, claimedEvent)` -> staged_state.
 *   4. Runs `handle(staged_state, claimedEvent)` -> `{ commands?, complete? }`.
 *   5. Dispatches each command via `runCommandWithSnapshots` on the
 *      same `client` (so dispatched aggregates' snapshot policies
 *      fire just as they do via `Instructed.dispatch`).
 *      Causation = triggering event's `event_id`; correlation = the
 *      triggering event's `correlation_id` (D-0017). The two-pool
 *      model that existed prior to D-0026 was retired: lock-set
 *      disjointness is a property of the SQL contract's per-procedure
 *      lock-acquisition orders (see `sql/instructed.sql`), not of
 *      client identity.
 *   6. On dispatch success, the slice-5 `complete` callback fires:
 *      - If `handle` returned `{ complete: true }` ->
 *        `complete_pm_instance` (DELETE snapshot + all work-items in
 *        one tx).
 *      - Else -> `complete_work_item_pm` (UPDATE the work item to
 *        'done' + UPSERT the snapshot in one tx; snapshot payload =
 *        staged_state, source_version = claimedEvent.event_number).
 *
 * Per D-0016 the user `handle` is opaque to the SDK: it receives the
 * event and the staged state; it does NOT receive any framework-owned
 * resource. Dispatched commands run on the dispatch session.
 *
 * Known gap (PM-E, out of scope for this slice): the dispatch path is
 * not yet idempotent against redelivery. If the handler is re-invoked
 * for the same claimed event (a normal SUB-B `retry-in` after a
 * post-dispatch failure, or a lease-takeover redelivery), commands
 * dispatched in the prior attempt that already committed at the
 * aggregate will be re-dispatched and may produce duplicate events
 * (no IS004 protection without deterministic event IDs). PM-E closes
 * this; see TODO.md item "PM-E (deterministic event IDs for
 * PM-dispatched commands)" and `docs/invariants.md` "Honest gaps
 * in v1" entry 2.
 *
 * Not yet re-exported from `src/index.ts`. The layer-5 facade in
 * slice 9 will wire `registerProcessManager` here; tests import the
 * module directly.
 */

import type { Client } from "./client.ts";
import { SnapshotNotFound } from "./errors.ts";
import { type AggregateDefinition } from "./aggregate.ts";
import { runCommandWithSnapshots } from "./aggregate-snapshots.ts";
import {
  startProcessingWorker,
  type ErrorPolicy,
  type ProcessingHandlerContext,
  type ProcessingWorkerOptions,
} from "./processing-worker.ts";
import type { RecordedEvent } from "./types.ts";
import type { RunningWorker } from "./internal/running-worker.ts";

// ============================================================================
// Public surface
// ============================================================================

/**
 * A command emitted by a PM `handle`. v1 supports the by-value form
 * only; the by-name form lands with the layer-5 facade (slice 9).
 */
export interface DispatchedCommand {
  streamUuid: string;
  aggregate: AggregateDefinition<any, any, any>;
  command: unknown;
}

/**
 * What `handle` returns. An empty object is valid (no commands, no
 * complete -- just record staged_state in the snapshot and mark the
 * work item `done`).
 */
export interface PmHandleResult {
  commands?: DispatchedCommand[];
  /** Terminate the partition: DELETE snapshot + all work-items. */
  complete?: boolean;
}

export interface PmHandlerContext extends ProcessingHandlerContext {
  /** The PM's partition (PM-F: routing decides; processing reads). */
  partitionKey: string;
}

/**
 * PM definition (PM-C apply/handle split + PM-F lifecycle flag).
 *
 *   - `apply`: pure state fold. Runs during rebuild and on the
 *     claimed event before `handle`. MUST NOT have side effects.
 *   - `handle`: produce commands and/or signal completion. Opaque to
 *     the SDK per D-0016.
 *   - `initialState`: starting state for a brand-new partition.
 *   - `snapshotModuleVersion`: optional SDK-managed string used to
 *     detect when application-level state shape has changed and a
 *     rebuild is required (SNAP-002 / PM-C). Stored in the
 *     snapshot's `metadata.snapshot_module_version` key on write;
 *     compared on read.
 */
export interface PmDefinition<S, E = unknown, PolicyState = undefined> {
  name: string;
  /** Default `$all`. */
  stream?: string;
  initialState(): S;
  apply(state: S, event: RecordedEvent<E>): S;
  handle(
    state: S,
    event: RecordedEvent<E>,
    ctx: PmHandlerContext,
  ): Promise<PmHandleResult> | PmHandleResult;
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

export type PmWorkerOptions = ProcessingWorkerOptions;

/** The metadata key that carries `snapshotModuleVersion` on PM snapshots. */
export const PM_SNAPSHOT_MODULE_VERSION_KEY = "snapshot_module_version";

// ============================================================================
// Implementation
// ============================================================================

interface StagedWork<S> {
  sourceUuid: string;
  stagedState: S;
  complete: boolean;
}

/**
 * Start a PM processing worker. Caller is responsible for the
 * routing-worker side (one `startRoutingWorker` per subscription);
 * the slice-9 facade glues the two together at registration time.
 *
 * Per [D-0026](../../../docs/decisions.md#d-0026) the PM dispatch
 * path uses the same `client` as the persist-and-ack path: the SQL
 * contract's per-procedure lock-acquisition orders and the pairwise
 * disjoint lock sets are what prevent deadlock, not client / pool
 * separation.
 */
export function startPmWorker<S, E = unknown, PolicyState = undefined>(
  client: Client,
  def: PmDefinition<S, E, PolicyState>,
  opts: PmWorkerOptions = {},
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
          PM_SNAPSHOT_MODULE_VERSION_KEY
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
      def.name,
      claimedPartitionKey,
      claimedEventNumber,
    );
    let state = def.initialState();
    for (const ev of priorEvents) state = def.apply(state, ev);
    return state;
  }

  const adapted = {
    name: def.name,
    stream,
    errorPolicy: def.errorPolicy,
    handle: async (event: RecordedEvent<E>, ctx: ProcessingHandlerContext) => {
      const sourceUuid = `${def.name}-${ctx.partitionKey}`;
      const baseState = await loadState(
        sourceUuid,
        ctx.partitionKey,
        ctx.eventNumber,
      );
      const stagedState = def.apply(baseState, event);
      const result = await def.handle(stagedState, event, ctx as PmHandlerContext);
      const commands = result.commands ?? [];
      // Dispatch in declaration order. Each command runs on the
      // dispatch session (D-0011). A dispatch failure throws out of
      // this handler -> SUB-B error-policy invoked; the work item
      // stays `claimed` and the lease is held by the slice-5
      // heartbeat.
      //
      // PM-E gap: re-dispatch of already-committed commands on
      // retry-in or lease-takeover redelivery may produce duplicates
      // at the aggregate; no IS004 protection without deterministic
      // event IDs.
      for (const c of commands) {
        await runCommandWithSnapshots(client, c.aggregate, c.streamUuid, c.command, {
          causationId: event.event_id,
          correlationId: event.correlation_id ?? undefined,
        });
      }
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
          `pm worker: no staged state for ${ctx.partitionKey}/${ctx.eventNumber}; this is a bug`,
        );
      }
      staged.delete(k);
      if (entry.complete) {
        // PM-F terminal: DELETE snapshot + all work-items for the
        // partition in one tx.
        await client.completePmInstance(
          stream,
          def.name,
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
        metadata[PM_SNAPSHOT_MODULE_VERSION_KEY] = def.snapshotModuleVersion;
      }
      await client.completeWorkItemPm<S>(
        stream,
        def.name,
        ctx.workerId,
        ctx.partitionKey,
        ctx.eventNumber,
        {
          sourceUuid: entry.sourceUuid,
          sourceType: def.name,
          sourceVersion: event.event_number,
          data: entry.stagedState,
          metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
        },
      );
    },
  };

  return startProcessingWorker<E, PolicyState>(client, adapted, opts);
}
