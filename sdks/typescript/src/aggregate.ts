/**
 * L2 aggregate runner.
 *
 * `runCommand` realises the load → execute → append loop with OCC
 * retry (D-0005 / AGG-001..010). No aggregate cache, no advisory
 * lock, no concurrency control beyond OCC retry; the SDK encodes
 * the call sequence and nothing more.
 *
 * # Layering note (step-5 slice 2, 2026-05-27)
 *
 * Snapshot **policy invocation** is not part of `runCommand`. The
 * layering is:
 *
 *   - **L2 (this file).** `runCommand` does load + execute + append
 *     + OCC retry. `runCommandAndApply` is the same loop returning
 *     the post-append state for callers that want to do follow-up
 *     work (snapshot, projection, etc.) without re-loading.
 *     Neither function inspects or invokes `def.snapshotPolicy`.
 *   - **L3 (`aggregate-snapshots.ts`).** `runCommandWithSnapshots`
 *     wraps `runCommandAndApply` and, on success, invokes
 *     `def.snapshotPolicy.shouldSnapshot` and writes the snapshot
 *     best-effort via a separate `recordSnapshot` call (D-0019).
 *
 * The `snapshotPolicy?:` field stays on `AggregateDefinition` as a
 * **declaration** by the user; whether it fires depends on which
 * orchestrator the caller uses. `Instructed.dispatch` and the
 * PM-worker command dispatch both use the L3 wrapper, so users of
 * the facade see the same observable behaviour as before this
 * refactor.
 *
 * Events vs. snapshots: events are a *correctness* concern (must
 * persist; failure surfaces); snapshots are a *performance*
 * concern (best-effort; failure logs and continues). The two-call
 * shape preserves this distinction at the SQL boundary; bundling
 * them into one atomic write would conflate the two failure modes.
 */

import type { Client } from "./client/index.ts";
import { DEFAULT_LOGGER_IMPL, Logger } from "./logger.ts";
import {
  RetryBudgetExhausted,
  SnapshotNotFound,
  StreamNotFound,
  WrongExpectedVersion,
} from "./errors/index.ts";
import { SNAPSHOT_MODULE_VERSION_KEY } from "./snapshot-version.ts";
import { expected as ev } from "./types/index.ts";
import type {
  AppendedEvent,
  ExpectedVersion,
  NewEvent,
  RecordedEvent,
} from "./types/index.ts";

/**
 * Per-dispatch context handed to {@link AggregateDefinition.execute}
 * and {@link CommandRouter}. Currently exposes a {@link Logger}; may
 * grow other dispatch-scoped facilities (correlation ids, etc.) in
 * later iterations.
 *
 * The same context is reused across OCC retries within one dispatch.
 */
export interface DispatchContext {
  logger: Logger;
}

/**
 * The domain-event shape passed to `apply` (§11.3). The SDK projects
 * the stored {@link RecordedEvent} to this minimal object before
 * folding state — `stream_uuid`, `stream_version`, `event_number`,
 * `event_id`, `causation_id`, `correlation_id`, `created_at` are SDK
 * bookkeeping, not domain data.
 */
export interface DomainEvent {
  type: string;
  data: unknown;
  metadata?: unknown;
}

/**
 * Aggregate snapshot policy — the contract half of the
 * snapshot-policy extension point (see `sdks/porting-checklist.md`
 * §4.2).
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

/**
 * Default {@link AggregateDefinition.streamName} implementation: prefix
 * the aggregate's `type` to the caller-supplied id, joined by `-`.
 *
 * Example: an aggregate with `type: "Account"` and id `"alice"` yields
 * the stream `"Account-alice"`. Applications that want a different
 * encoding (UUIDs, slashes, namespacing across systems) override
 * `streamName` on the definition.
 *
 * Exposed so apps can build their own factory functions on top.
 */
export function prefixType(type: string): (id: string) => string {
  return (id) => `${type}-${id}`;
}

/**
 * A user-defined aggregate. `execute` is pure and produces events; `apply`
 * is pure and folds them into state. The SDK owns version tracking; user
 * code never reads or writes the per-stream version (§3 layer 1 note).
 */
export interface AggregateDefinition<S, C, E extends DomainEvent = DomainEvent> {
  /** Used as `source_type` for snapshots; also a registry key in the facade. */
  type: string;

  /**
   * Optional encoding from an aggregate id (e.g. `"alice"`) to the
   * underlying stream name (e.g. `"Account-alice"`). Default:
   * {@link prefixType}(`type`). Applications that want UUIDs, custom
   * separators, or cross-system namespacing override this; nothing in
   * the SDK or storage layer cares what the stream string looks like.
   *
   * Application code is encouraged to identify aggregates by
   * `(type, id)` and let the SDK / definition derive the stream name;
   * stream names are storage-layer concerns.
   */
  streamName?(id: string): string;

  initialState(): S;

  /**
   * Pure: produce zero or more events from `(state, command)`. Returning
   * `[]`, `undefined`, or `void` is a no-op — no append, no version bump,
   * no error.
   */
  execute(
    state: S,
    command: C,
    ctx: DispatchContext,
  ): NewEvent | NewEvent[] | undefined | void;

  /** Pure: fold one event into state. The SDK tracks version. */
  apply(state: S, event: E): S;

  /**
   * Optional snapshot policy. Declared here; invoked by the L3
   * `runCommandWithSnapshots` wrapper on success. The L2
   * `runCommand` / `runCommandAndApply` primitives ignore this
   * field (they are unopinionated about snapshot orchestration).
   */
  snapshotPolicy?: SnapshotPolicy<S>;

  /**
   * SDK-managed snapshot version tag (SNAP-002). When set:
   *
   *   - On write (via `runCommandWithSnapshots`): the value is
   *     stamped into the snapshot's metadata under
   *     `SNAPSHOT_MODULE_VERSION_KEY`.
   *   - On read (in `loadAggregate`): the metadata's value is
   *     compared strictly to this field; on mismatch (including
   *     "version present on one side and absent on the other"),
   *     the snapshot is discarded and the aggregate is rebuilt
   *     by paging events from version 0.
   *
   * Use this when changing the shape of `S` between deploys:
   * bump the version string to invalidate every previously-
   * written snapshot in one go, forcing a full replay through
   * the new `apply`. Failing to do so risks feeding stale-shape
   * state to the new `apply` — a silent correctness bug.
   *
   * Comparison is strict: leaving the field undefined and
   * encountering a snapshot whose metadata HAS a version (or
   * vice versa) counts as mismatch. This prevents
   * "accidentally adopting versioning silently."
   */
  snapshotModuleVersion?: string;
}

export interface RunCommandOptions {
  /**
   * Maximum number of retries after the first attempt. Default 5
   * (mapping.md AGG-010). `retryBudget: 0` disables retry (single
   * attempt; first IS001 raises {@link RetryBudgetExhausted}).
   */
  retryBudget?: number;

  /**
   * Explicit expected-version assertion. Defaults to `expected.exact(V)`
   * where V is the version loaded from snapshot + stream. When the
   * caller supplies an explicit value, **OCC retry is disabled** — the
   * assertion is the caller's, not the SDK's, and a mismatch surfaces
   * as the underlying `WrongExpectedVersion` rather than as a retry
   * loop. Recorded in D-0019.
   */
  expectedVersion?: ExpectedVersion;

  /**
   * Per-call command identity. Defaults to `crypto.randomUUID()`.
   * The SDK fills any unset `event.causation_id` with this value
   * unless `causationId` is supplied (§11.8 / AGG-020).
   */
  commandId?: string;

  /**
   * Explicit causation id (overrides the `commandId`-based default).
   * The PM worker passes the triggering event's `event_id` here so
   * every dispatched-command event carries `causation_id =
   * triggering_event.event_id` (§11.8 / D-0017 / PM-012). Callers
   * who omit this get the `commandId` default.
   */
  causationId?: string;

  /**
   * Optional correlation id. The SDK fills any unset
   * `event.correlation_id` with this value (§11.8 / AGG-021).
   */
  correlationId?: string;

  /**
   * Per-dispatch context threaded to `def.execute` and to the
   * snapshot-write warn site. Optional at the L2 boundary; when
   * omitted, the runner synthesises a context whose `logger` wraps
   * {@link DEFAULT_LOGGER_IMPL} — the same fallback the
   * `Instructed` facade uses when its `logger` option is omitted.
   *
   * Application code (handlers, `execute`, routers) therefore
   * always sees a real `ctx.logger`: the only question is which
   * {@link ILoggerImpl} sits behind it. Callers that want silence
   * pass `{ logger: Logger.fromImpl(NOOP_LOGGER_IMPL) }`.
   *
   * `Instructed.dispatch` always supplies a ctx carrying the app's
   * configured root logger; the fallback only fires for direct L2
   * callers (`runCommand` outside the facade) that don't supply
   * one.
   */
  ctx?: DispatchContext;
}

/** Pagination chunk size for `readStream` during load. Internal. */
const LOAD_PAGE_SIZE = 500;

/** Default retry budget — see {@link RunCommandOptions.retryBudget}. */
export const DEFAULT_RETRY_BUDGET = 5;

/**
 * Result of `runCommandAndApply`. Includes the post-append state
 * folded through `apply`, the new stream version, and the snapshot
 * baseline counter — enough information for an L3 wrapper to make
 * snapshot / projection decisions without re-loading.
 *
 * For a no-op (handler returned no events), `appended` is empty,
 * `state` and `version` reflect the loaded baseline, and
 * `eventsSinceSnapshot` is the loaded counter unchanged.
 */
export interface RanCommand<S> {
  appended: AppendedEvent[];
  /** State after folding `appended` through `apply`. */
  state: S;
  /** Stream version after append (loaded version if `appended` is empty). */
  version: bigint;
  /** Loaded `eventsSinceSnapshot` + `appended.length`. */
  eventsSinceSnapshot: number;
}

/**
 * Internal result of the load + execute + append + OCC loop —
 * before any post-append fold. Used by both `runCommand` (which
 * drops everything but `appended`) and `runCommandAndApply` (which
 * folds `filled` through `apply` to produce the staged state).
 *
 * Keeping the fold out of this helper preserves the invariant that
 * the L2 `runCommand` invokes `def.apply` **only on loaded events**
 * — it does not call `apply` against the just-appended events.
 * Callers wanting the post-append state opt in by calling
 * `runCommandAndApply`, which folds.
 */
interface ExecutedCommand<S, E extends DomainEvent> {
  appended: AppendedEvent[];
  /** Loaded baseline state (pre-append). Identical to the state passed to `execute`. */
  loadedState: S;
  /** Loaded baseline version. */
  loadedVersion: bigint;
  /** Loaded `eventsSinceSnapshot` counter. */
  loadedEventsSinceSnapshot: number;
  /** The events written, with causation / correlation defaults applied. */
  filled: NewEvent[];
  /** True iff `execute` returned no events. `appended` is then `[]`. */
  noOp: boolean;
}

async function executeCommand<S, C, E extends DomainEvent>(
  client: Client,
  def: AggregateDefinition<S, C, E>,
  streamUuid: string,
  command: C,
  opts: RunCommandOptions,
): Promise<ExecutedCommand<S, E>> {
  const retryBudget = opts.retryBudget ?? DEFAULT_RETRY_BUDGET;
  if (!Number.isInteger(retryBudget) || retryBudget < 0) {
    throw new RangeError(
      `runCommand: retryBudget must be a non-negative integer, got ${retryBudget}`,
    );
  }
  const commandId = opts.commandId ?? globalThis.crypto.randomUUID();
  // §11.8: explicit causationId wins for event defaulting; otherwise
  // commandId doubles as the causation default (AGG-020).
  const causationDefault = opts.causationId ?? commandId;
  const correlationId = opts.correlationId;
  const explicitExpected = opts.expectedVersion;

  // Reused across OCC retries (per design): logging at retry points
  // is the SDK's concern, but the per-execute context is stable.
  // The fallback mirrors the facade's default-logger behaviour:
  // info/warn/error to console, trace silent. Application code
  // dispatching through `Instructed` always gets the app's
  // configured logger; this fallback only fires for direct L2
  // callers that don't supply a ctx.
  const ctx: DispatchContext =
    opts.ctx ?? { logger: Logger.fromImpl(DEFAULT_LOGGER_IMPL) };

  let attempt = 0;
  let lastError: unknown;

  // total attempts = 1 + retryBudget (default budget 5 → up to 6 attempts).
  while (attempt <= retryBudget) {
    attempt += 1;
    try {
      const loaded = await loadAggregate(client, def, streamUuid);
      const events = normaliseEvents(def.execute(loaded.state, command, ctx));

      if (events.length === 0) {
        // Commanded no-op semantics: nothing to append.
        return {
          appended: [],
          loadedState: loaded.state,
          loadedVersion: loaded.version,
          loadedEventsSinceSnapshot: loaded.eventsSinceSnapshot,
          filled: [],
          noOp: true,
        };
      }

      // §11.8 defaulting: fill any unset causation_id with commandId,
      // any unset correlation_id with opts.correlationId. Explicit
      // caller values win verbatim.
      const filled = events.map<NewEvent>((e) => ({
        ...e,
        causation_id: e.causation_id ?? causationDefault,
        correlation_id: e.correlation_id ?? correlationId,
      }));

      const expectedVersion = explicitExpected ?? ev.exact(loaded.version);
      const appended = await client.appendToStream(
        streamUuid,
        expectedVersion,
        filled,
      );

      return {
        appended,
        loadedState: loaded.state,
        loadedVersion: loaded.version,
        loadedEventsSinceSnapshot: loaded.eventsSinceSnapshot,
        filled,
        noOp: false,
      };
    } catch (err) {
      lastError = err;
      // Retry only on default-expected-version mismatches (the SDK's
      // own OCC assertion). An explicit caller-supplied expectedVersion
      // is a deliberate assertion and is not retried (D-0019).
      if (err instanceof WrongExpectedVersion && explicitExpected === undefined) {
        if (attempt > retryBudget) break;
        continue;
      }
      throw err;
    }
  }

  throw new RetryBudgetExhausted(
    `runCommand: retry budget exhausted after ${attempt} attempt(s)`,
    { attempts: attempt, lastError },
  );
}

/**
 * Run a command against an aggregate stream.
 *
 *   1. Read snapshot (swallow {@link SnapshotNotFound}).
 *   2. Page through `readStream` from `version + 1`, folding `apply`.
 *   3. `execute(state, command)`; normalise to array. Empty → no-op.
 *   4. `appendToStream(streamUuid, expected.exact(version), events)`.
 *   5. On {@link WrongExpectedVersion} with default expectedVersion,
 *      reload and retry up to `retryBudget`.
 *
 * No aggregate cache, no advisory lock, no transaction held across
 * the user's `execute` (it's pure code). `def.apply` is invoked
 * **only during load** — the just-appended events are not folded
 * back through `apply` by this function. Callers wanting the
 * post-append state use {@link runCommandAndApply}.
 *
 * Does **not** invoke `def.snapshotPolicy`; see
 * `runCommandWithSnapshots` in `aggregate-snapshots.ts` for the
 * L3 orchestrator that does. The TypeScript SDK's `Instructed`
 * facade and the PM worker's command dispatch both use the L3
 * wrapper, so users of those layers see the same observable
 * snapshot behaviour as before the step-5 slice 2 refactor.
 */
export async function runCommand<S, C, E extends DomainEvent = DomainEvent>(
  client: Client,
  def: AggregateDefinition<S, C, E>,
  streamUuid: string,
  command: C,
  opts: RunCommandOptions = {},
): Promise<AppendedEvent[]> {
  const r = await executeCommand(client, def, streamUuid, command, opts);
  return r.appended;
}

/**
 * Run a command against an aggregate stream, returning the
 * appended events plus the post-append staged state. Same loop
 * as {@link runCommand}; additionally folds the just-appended
 * events through `def.apply` so the caller can make snapshot /
 * projection decisions without re-loading.
 *
 * The fold is the *only* observable difference from `runCommand`:
 * `apply` will be invoked once per appended event with the
 * post-execute, pre-snapshot state. `apply` MUST be pure (as
 * required for the load path); the extra invocations are
 * idempotent on state.
 *
 * Does **not** invoke `def.snapshotPolicy`; see
 * `runCommandWithSnapshots` (L3).
 */
export async function runCommandAndApply<
  S,
  C,
  E extends DomainEvent = DomainEvent,
>(
  client: Client,
  def: AggregateDefinition<S, C, E>,
  streamUuid: string,
  command: C,
  opts: RunCommandOptions = {},
): Promise<RanCommand<S>> {
  const r = await executeCommand(client, def, streamUuid, command, opts);

  if (r.noOp) {
    return {
      appended: r.appended,
      state: r.loadedState,
      version: r.loadedVersion,
      eventsSinceSnapshot: r.loadedEventsSinceSnapshot,
    };
  }

  // Fold appended events through apply to produce the staged
  // state. Cheap; `apply` is pure user code on at most a
  // command-worth of events.
  let stagedState = r.loadedState;
  for (const e of r.filled) {
    stagedState = def.apply(stagedState, {
      type: e.type,
      data: e.data,
      metadata: e.metadata,
    } as E);
  }

  return {
    appended: r.appended,
    state: stagedState,
    version: r.appended[r.appended.length - 1].stream_version,
    eventsSinceSnapshot: r.loadedEventsSinceSnapshot + r.appended.length,
  };
}

// ---- internals ----

interface LoadedAggregate<S> {
  state: S;
  version: bigint;
  /** Events folded since the snapshot baseline (or since initialState). */
  eventsSinceSnapshot: number;
}

async function loadAggregate<S, C, E extends DomainEvent>(
  client: Client,
  def: AggregateDefinition<S, C, E>,
  streamUuid: string,
): Promise<LoadedAggregate<S>> {
  let state = def.initialState();
  let version = 0n;
  let eventsSinceSnapshot = 0;

  // 1. Try snapshot. SNAP-002: read the snapshot's metadata for
  // `SNAPSHOT_MODULE_VERSION_KEY` and compare strictly against
  // `def.snapshotModuleVersion`. On mismatch (including "version
  // on one side and absent on the other"), discard the
  // snapshot's data and fall back to paging events from
  // version 0. Silent: no warning is emitted because every
  // aggregate would log on its next touch after a deliberate
  // version bump, which is noise.
  try {
    const snap = await client.readSnapshot<S>(streamUuid);
    let snapModuleVersion: string | undefined;
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
    const want = def.snapshotModuleVersion;
    const matches =
      want === undefined
        ? snapModuleVersion === undefined
        : snapModuleVersion === want;
    if (matches) {
      state = snap.data;
      version = snap.sourceVersion;
    }
    // Mismatch: leave `state` / `version` at their initial
    // values; the readStream loop below will page from 0.
  } catch (err) {
    if (!(err instanceof SnapshotNotFound)) throw err;
    // No snapshot: start from initialState() at version 0.
  }

  // 2. Page forward from version + 1, folding apply. A fresh stream
  // (no snapshot, no events yet) raises IS003 here; treat that as an
  // empty stream so runCommand can be the first writer.
  while (true) {
    let page: RecordedEvent[];
    try {
      page = await client.readStream(
        streamUuid,
        version + 1n,
        LOAD_PAGE_SIZE,
      );
    } catch (err) {
      if (err instanceof StreamNotFound && version === 0n) break;
      throw err;
    }
    if (page.length === 0) break;
    for (const row of page) {
      state = def.apply(state, recordedToDomain<E>(row));
      version = row.stream_version;
      eventsSinceSnapshot += 1;
    }
    if (page.length < LOAD_PAGE_SIZE) break;
  }

  return { state, version, eventsSinceSnapshot };
}

function recordedToDomain<E extends DomainEvent>(row: RecordedEvent): E {
  return {
    type: row.type,
    data: row.data,
    metadata: row.metadata,
  } as E;
}

function normaliseEvents(
  v: NewEvent | NewEvent[] | undefined | void,
): NewEvent[] {
  if (v == null) return [];
  if (Array.isArray(v)) return v;
  return [v];
}
