/**
 * L3 PM worker wrapper.
 *
 * Thin wrapper over the L2 `pm-substrate.ts` that interprets a
 * user-returned `commands` list and dispatches each command via
 * `runCommandWithSnapshots` between `handle` and the substrate's
 * snapshot+ack tx. Preserves the user-facing API the SDK has
 * always shipped (a `PmDefinition.handle` returning
 * `{ commands?, complete? }`).
 *
 * This file is the L3 half of the PM L2/L3 split. The L2
 * substrate (`pm-substrate.ts`) does the snapshot+ack lifecycle
 * (required-core for any port); this wrapper keeps the by-value
 * `commands`-list shape as idiomatic TypeScript convenience. See
 * `sdks/porting-checklist.md` §3 for the cross-language
 * treatment.
 *
 * # Dispatch ordering
 *
 * Dispatch runs *between* the user's `handle` and the substrate's
 * snapshot+ack tx — same order as before the split. If a
 * dispatched command throws, the substrate's `handle` (which is
 * the wrapper this file installs) throws, the snapshot+ack tx
 * never fires, and the SUB-B error policy retries the work item.
 * The work item stays `claimed` and the lease is held by the
 * processing-worker heartbeat.
 *
 * # PM-handler-dispatch error visibility
 *
 * Errors from SDK-dispatched commands surface via the worker's
 * `onError` callback and the SUB-B error policy, **not** through
 * the user's `try/catch` around `handle`'s body. This asymmetry
 * is deliberate: a user wanting custom dispatch-error handling
 * can omit `commands` from the `handle` return and dispatch
 * directly inside `handle` using
 * `runCommandWithSnapshots` (or any other call). The escape
 * hatch is always available.
 *
 * # PM-E gap (unchanged)
 *
 * Re-dispatch of already-committed commands on `retry-in` or
 * lease-takeover redelivery may produce duplicate aggregate
 * events; no IS004 protection without deterministic event IDs.
 * See `docs/invariants.md` "Honest gaps in v1" entry 2.
 *
 * Per [D-0026](../../../docs/decisions.md#d-0026) the PM dispatch
 * path uses the same `client` as the persist-and-ack path: the
 * SQL contract's per-procedure lock-acquisition orders and the
 * pairwise disjoint lock sets are what prevent deadlock, not
 * client / pool separation.
 */

import { prefixType, type AggregateDefinition } from '../../aggregate/index.ts'
import { runCommandWithSnapshots } from '../../aggregate/index.ts'
import type { Client } from '../../client/index.ts'
import type { CommandRouter } from '../../facade/command-router.ts'
import type { RunningWorker } from '../../internal/running-worker.ts'
import type { Command } from '../../types/index.ts'
import type { Event, RecordedEvent } from '../../types/index.ts'
import type { ErrorPolicy, ProcessingHandlerContext } from '../processing/index.ts'
import {
  startPmSubstrate,
  type PmSubstrateDefinition,
  type PmSubstrateOptions,
} from './pm-substrate.ts'

// ============================================================================
// Public surface
// ============================================================================

/**
 * A command emitted by a PM `handle`. Two shapes:
 *
 *   - **Lean (recommended).** A bare {@link Command} (object with a
 *     `type` discriminator and per-command payload fields). The PM
 *     worker resolves the command through the configured
 *     {@link CommandRouter} to `(aggregateType, aggregateId)`,
 *     looks up the {@link AggregateDefinition}, and derives the
 *     underlying stream name via `def.streamName(id)`. The PM
 *     author writes plain commands and never constructs stream
 *     names. Requires a router; see
 *     {@link PmWorkerOptions.router}.
 *
 *   - **Explicit (legacy/escape hatch).** Carry the resolved
 *     `aggregate` + `streamUuid` directly. Used when no router is
 *     wired in, or for one-off dispatches that don't fit the
 *     router's static map.
 *
 * The two shapes are discriminated structurally by the presence
 * of `aggregate` and `streamUuid`.
 */
export type DispatchedCommand = (Command & { [key: string]: unknown }) | DispatchedCommandExplicit

export interface DispatchedCommandExplicit {
  streamUuid: string
  aggregate: AggregateDefinition<any, any, any>
  command: unknown
}

function isExplicitDispatch(c: DispatchedCommand): c is DispatchedCommandExplicit {
  return (
    typeof (c as DispatchedCommandExplicit).streamUuid === 'string' &&
    typeof (c as DispatchedCommandExplicit).aggregate === 'object' &&
    (c as DispatchedCommandExplicit).aggregate !== null &&
    typeof (c as DispatchedCommandExplicit).aggregate.type === 'string'
  )
}

/**
 * What `handle` returns. An empty object is valid (no commands,
 * no complete — just record staged_state in the snapshot and mark
 * the work item `done`).
 */
export interface PmHandleResult {
  commands?: DispatchedCommand[]
  /** Terminate the partition: DELETE snapshot + all work-items. */
  complete?: boolean
}

export interface PmHandlerContext extends ProcessingHandlerContext {
  /** The PM's partition (PM-F: routing decides; processing reads). */
  partitionKey: string
}

/**
 * PM definition (PM-C apply/handle split + PM-F lifecycle flag).
 *
 *   - `apply`: pure state fold. Runs during rebuild and on the
 *     claimed event before `handle`. MUST NOT have side effects.
 *   - `handle`: produce commands and/or signal completion. Opaque
 *     to the SDK per D-0016. Commands are dispatched in
 *     declaration order between `handle` and the substrate's
 *     snapshot+ack tx; a dispatch failure throws → SUB-B error
 *     policy retries.
 *   - `initialState`: starting state for a brand-new partition.
 *   - `snapshotModuleVersion`: optional SDK-managed string used
 *     to detect when application-level state shape has changed
 *     and a rebuild is required (SNAP-002 / PM-C). Stored in the
 *     snapshot's `metadata.snapshot_module_version` key on
 *     write; compared on read.
 */
export interface PmDefinition<S, E extends Event = Event, PolicyState = undefined> {
  /** PM type — doubles as the subscription name and the snapshot
   *  source_type prefix. Same role as `AggregateDefinition.type`. */
  type: string
  /** Optional source_uuid encoding from partition key. Default
   *  `${type}-${partitionKey}`. See {@link PmSubstrateDefinition}. */
  streamName?(partitionKey: string): string
  /** Default `$all`. */
  stream?: string
  initialState(): S
  apply(state: S, event: RecordedEvent<E>): S
  handle(
    state: S,
    event: RecordedEvent<E>,
    ctx: PmHandlerContext,
  ): Promise<PmHandleResult> | PmHandleResult
  /** SDK-managed snapshot version tag; mismatch triggers rebuild. */
  snapshotModuleVersion?: string
  /**
   * Retry/error-policy hook. Defaults to `DEFAULT_ERROR_POLICY`
   * (exponential backoff, retry forever). Type-parameterised by
   * `PolicyState` for callers writing stateful policies; defaults
   * to `ErrorPolicy<undefined>`.
   */
  errorPolicy?: ErrorPolicy<PolicyState>
}

/**
 * PM-worker options. Extends the L2 substrate options with the
 * L3 routing wiring needed for the lean {@link DispatchedCommand}
 * shape: a {@link CommandRouter} plus the aggregate registry the
 * router resolves into.
 *
 * Both `router` and `aggregates` are optional. If absent, the PM
 * worker can still dispatch via the explicit form; emitting a
 * lean command without a router throws at dispatch time.
 */
export interface PmWorkerOptions extends PmSubstrateOptions {
  /** Resolves lean commands to `(aggregateType, aggregateId)`. */
  router?: CommandRouter
  /** Registry consulted by the router's `aggregateType` result. */
  aggregates?: ReadonlyMap<string, AggregateDefinition<any, any, any>>
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Start a PM processing worker with command-dispatch
 * orchestration. Caller is responsible for the routing-worker
 * side (one `startRoutingWorker` per subscription); the layer-5
 * facade (`Instructed`) glues the two together at registration
 * time.
 *
 * This is the L3 wrapper over {@link startPmSubstrate}; it
 * supplies a substrate-handle that captures the user's returned
 * `commands` and dispatches each via `runCommandWithSnapshots`
 * before returning `{ complete }` to the substrate.
 */
export function startPmWorker<S, E extends Event = Event, PolicyState = undefined>(
  client: Client,
  def: PmDefinition<S, E, PolicyState>,
  opts: PmWorkerOptions = {},
): RunningWorker {
  // Translate the user-facing `PmDefinition` (handle returns
  // `{ commands?, complete? }`) into a substrate definition (handle
  // returns `{ complete? }`) by wrapping `handle` to dispatch
  // commands in declaration order between the user's `handle` and
  // the substrate's snapshot+ack tx.
  const substrateDef: PmSubstrateDefinition<S, E, PolicyState> = {
    type: def.type,
    ...(def.streamName !== undefined ? { streamName: def.streamName } : {}),
    stream: def.stream,
    initialState: def.initialState,
    apply: def.apply,
    snapshotModuleVersion: def.snapshotModuleVersion,
    errorPolicy: def.errorPolicy,
    handle: async (state, event, ctx) => {
      const result = await def.handle(state, event, ctx as PmHandlerContext)
      const commands = result.commands ?? []
      const complete = result.complete === true
      // Summary trace: replaces the script-side `withTrace` wrapper
      // that was previously the example app's only way to observe
      // PM activity. With a wired logger, every PM gets this for
      // free.
      ctx.logger.trace(
        () =>
          `pm handle: event ${event.type}#${event.event_number} -> ${commands.length} command(s); complete=${complete}`,
      )
      // Dispatch in declaration order. Each command runs on the
      // same `client` (D-0026). A dispatch failure throws out of
      // this handler -> substrate sees the throw -> SUB-B error
      // policy invoked; the work item stays `claimed` and the
      // lease is held by the slice-5 heartbeat.
      //
      // PM-E gap: re-dispatch of already-committed commands on
      // retry-in or lease-takeover redelivery may produce
      // duplicates at the aggregate; no IS004 protection without
      // deterministic event IDs.
      const dispatchCtx = { logger: ctx.logger }
      for (const c of commands) {
        const resolved = resolveDispatch(c, opts, dispatchCtx)
        // Per-command trace: aggregate type + command type are
        // enough to read a PM's behaviour off the log without
        // dumping payloads.
        const cmdType = (resolved.command as { type?: string }).type ?? '<untyped>'
        ctx.logger.trace(
          () => `pm dispatch: ${resolved.def.type}.${cmdType} -> ${resolved.streamUuid}`,
        )
        await runCommandWithSnapshots(client, resolved.def, resolved.streamUuid, resolved.command, {
          causationId: event.event_id,
          correlationId: event.correlation_id ?? undefined,
          ctx: dispatchCtx,
        })
      }
      return { complete }
    },
  }

  return startPmSubstrate<S, E, PolicyState>(client, substrateDef, opts)
}

/**
 * Resolve a {@link DispatchedCommand} (either shape) to the
 * triple needed by `runCommandWithSnapshots`. Throws on the lean
 * shape when no router/aggregates are configured, or when the
 * router names an unknown aggregate.
 */
function resolveDispatch(
  c: DispatchedCommand,
  opts: PmWorkerOptions,
  dispatchCtx: { logger: import('../../logger/index.ts').Logger },
): {
  def: AggregateDefinition<any, any, any>
  streamUuid: string
  command: unknown
} {
  if (isExplicitDispatch(c)) {
    return { def: c.aggregate, streamUuid: c.streamUuid, command: c.command }
  }
  // Lean form: resolve through the router.
  if (!opts.router || !opts.aggregates) {
    throw new Error(
      'startPmWorker: handle returned a lean command (no `aggregate`/' +
        '`streamUuid`) but no `router`/`aggregates` were supplied. ' +
        'Configure a CommandRouter (see `commandRouter()`) and pass it ' +
        'via PmWorkerOptions, or emit the explicit ' +
        '{ aggregate, streamUuid, command } shape.',
    )
  }
  const route = opts.router(c, dispatchCtx)
  const def = opts.aggregates.get(route.aggregateType)
  if (!def) {
    throw new Error(
      `startPmWorker: command router resolved "${c.type}" to ` +
        `aggregate type "${route.aggregateType}", which is not registered.`,
    )
  }
  const streamUuid = (def.streamName ?? prefixType(def.type))(route.aggregateId)
  return { def, streamUuid, command: c }
}
