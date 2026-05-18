/**
 * Layer 1: aggregate runner.
 *
 * See docs/sdk-design.md §3 layer 1 and §11.3 / §11.8.
 *
 * runCommand realises the load → execute → append loop with OCC retry
 * (D-0005 / mapping.md AGG-010). There is no aggregate cache, no
 * advisory lock, and no concurrency control beyond OCC retry — the
 * SDK encodes the call sequence and nothing more.
 */

import type { Client } from "./client.ts";
import {
  RetryBudgetExhausted,
  SnapshotNotFound,
  StreamNotFound,
  WrongExpectedVersion,
} from "./errors.ts";
import { expected as ev } from "./types.ts";
import type {
  AppendedEvent,
  ExpectedVersion,
  NewEvent,
  RecordedEvent,
} from "./types.ts";

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
 * Snapshot policy (§6). `eventsSinceLast` counts events folded into
 * the current state since the last persisted snapshot (or since
 * `initialState()` for a never-snapshotted stream).
 */
export interface SnapshotPolicy<S> {
  shouldSnapshot(state: S, version: bigint, eventsSinceLast: number): boolean;
}

/** Convenience: snapshot every N events. */
export function everyN<S>(n: number): SnapshotPolicy<S> {
  if (!Number.isFinite(n) || n <= 0) {
    throw new RangeError(`everyN: n must be a positive integer, got ${n}`);
  }
  return {
    shouldSnapshot: (_s, _v, eventsSinceLast) => eventsSinceLast >= n,
  };
}

/**
 * A user-defined aggregate. `execute` is pure and produces events; `apply`
 * is pure and folds them into state. The SDK owns version tracking; user
 * code never reads or writes the per-stream version (§3 layer 1 note).
 */
export interface AggregateDefinition<S, C, E extends DomainEvent = DomainEvent> {
  /** Used as `source_type` for snapshots; also a registry key in the facade. */
  type: string;

  initialState(): S;

  /**
   * Pure: produce zero or more events from `(state, command)`. Returning
   * `[]`, `undefined`, or `void` is a no-op — no append, no version bump,
   * no error.
   */
  execute(
    state: S,
    command: C,
  ): NewEvent | NewEvent[] | undefined | void;

  /** Pure: fold one event into state. The SDK tracks version. */
  apply(state: S, event: E): S;

  snapshotPolicy?: SnapshotPolicy<S>;
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
}

/** Pagination chunk size for `readStream` during load. Internal. */
const LOAD_PAGE_SIZE = 500;

/** Default retry budget — see {@link RunCommandOptions.retryBudget}. */
export const DEFAULT_RETRY_BUDGET = 5;

/**
 * Run a command against an aggregate stream.
 *
 *   1. Read snapshot (swallow {@link SnapshotNotFound}).
 *   2. Page through `readStream` from `version + 1`, folding `apply`.
 *   3. `execute(state, command)`; normalise to array. Empty → no-op.
 *   4. `appendToStream(streamUuid, expected.exact(version), events)`.
 *   5. On {@link WrongExpectedVersion} with default expectedVersion,
 *      reload and retry up to `retryBudget`.
 *   6. Best-effort snapshot if `snapshotPolicy.shouldSnapshot` says so.
 *
 * No aggregate cache, no advisory lock, no transaction held across
 * the user's `execute` (it's pure code).
 */
export async function runCommand<S, C, E extends DomainEvent = DomainEvent>(
  client: Client,
  def: AggregateDefinition<S, C, E>,
  streamUuid: string,
  command: C,
  opts: RunCommandOptions = {},
): Promise<AppendedEvent[]> {
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

  let attempt = 0;
  let lastError: unknown;

  // total attempts = 1 + retryBudget (default budget 5 → up to 6 attempts).
  while (attempt <= retryBudget) {
    attempt += 1;
    try {
      const loaded = await loadAggregate(client, def, streamUuid);
      const events = normaliseEvents(def.execute(loaded.state, command));

      if (events.length === 0) {
        // Commanded no-op semantics: nothing to append.
        return [];
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

      // Snapshot policy is best-effort and runs as a follow-up call
      // (§3 layer 1, §6). Failure here MUST NOT fail the command.
      if (def.snapshotPolicy) {
        // Fold the just-appended events through apply so the snapshot
        // captures post-append state. `apply` is pure; we used `filled`
        // for append, but apply only needs the {type,data,metadata}.
        let newState = loaded.state;
        for (const e of filled) {
          newState = def.apply(newState, {
            type: e.event_type,
            data: e.data,
            metadata: e.metadata,
          } as E);
        }
        const newVersion = appended[appended.length - 1].stream_version;
        const eventsSinceSnapshot = loaded.eventsSinceSnapshot + appended.length;
        if (
          def.snapshotPolicy.shouldSnapshot(
            newState,
            newVersion,
            eventsSinceSnapshot,
          )
        ) {
          try {
            await client.recordSnapshot({
              sourceUuid: streamUuid,
              sourceType: def.type,
              sourceVersion: newVersion,
              data: newState,
            });
          } catch (snapErr) {
            // Best-effort: log and continue. The load path works
            // without a snapshot — it'll just re-fold the stream.
            // D-0019: snapshot-write failures are non-fatal and use
            // console.warn until a logger surface is added.
            // eslint-disable-next-line no-console
            console.warn(
              `[instructed] snapshot write failed for ${streamUuid}:`,
              snapErr,
            );
          }
        }
      }

      return appended;
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

  // 1. Try snapshot.
  try {
    const snap = await client.readSnapshot<S>(streamUuid);
    state = snap.data;
    version = snap.sourceVersion;
  } catch (err) {
    if (!(err instanceof SnapshotNotFound)) throw err;
    // No snapshot: start from initialState() at version 0.
  }

  // 2. Page forward from version + 1, folding apply. A fresh stream
  // (no snapshot, no events yet) raises IS003 here; treat that as an
  // empty stream so runCommand can be the first writer.
  while (true) {
    let page: RecordedEvent<unknown>[];
    try {
      page = await client.readStream<unknown>(
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
    type: row.event_type,
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
