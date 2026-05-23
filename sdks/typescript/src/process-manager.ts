/**
 * Layer 3: process-manager worker.
 *
 * See docs/sdk-design.md §3 layer 3, §11.4 (start/continue lenient),
 * §11.5 (handler-throws backoff — shared with projections), §11.7
 * (dispatch by value or by name), §11.8 / D-0017 (causation/correlation
 * propagation), §11.9 / D-0018 (worker lifecycle).
 *
 * A PM is a projection on `$all` (or a chosen stream) that:
 *   1. Routes each event by `event_type` to a per-event-type {@link RouteFn}.
 *   2. Loads PM-instance state from a snapshot (sourceUuid =
 *      `${def.name}-${processId}`) or `initialState()`.
 *   3. Calls `handle(state, event)` OUTSIDE any SDK transaction (D-0016).
 *   4. Dispatches each returned command via `runCommand` on the
 *      separate `dispatchClient` (D-0011/D-0012 lock-set disjointness).
 *      Threads causation/correlation from the triggering event.
 *   5. In one short SDK-internal transaction on `client`:
 *      record_snapshot (or delete_snapshot for `kind: 'stop'`) AND
 *      advance_subscription. The persist-and-ack tx is the *only*
 *      place the SDK wraps two procedures.
 *
 * If the same `Client` instance is supplied as both persist and
 * dispatch client, the function throws at construction (D-0011).
 */

import type { Client } from "./client.ts";
import {
  HandlerError,
  InstructedError,
  SubscriptionLeaseLost,
  SubscriptionNotFound,
} from "./errors.ts";
import { runCommand, type AggregateDefinition } from "./aggregate.ts";
import type {
  RecordedEvent,
  StartFrom,
} from "./types.ts";
import {
  DEFAULT_BATCH_SIZE,
  DEFAULT_LEASE_SECONDS,
  DEFAULT_POLL_INTERVAL_MS,
  type RunningWorker,
} from "./subscription.ts";
import { defaultWorkerId } from "./internal/worker-id.ts";
import { sleep } from "./internal/sleep.ts";
import { withTransaction } from "./internal/with-transaction.ts";

/**
 * The result of routing a single event. A bare string is sugar for
 * `{ kind: 'continue', processId: <string> }`. `null` / `undefined`
 * means "not interested", same as `{ kind: 'ignore' }`.
 *
 *   - `start`: discard any existing snapshot and start from
 *     `initialState()` (§11.4, lenient — `start!` is deferred).
 *   - `continue`: load state from the snapshot if it exists, else
 *     start from `initialState()` (§11.4, lenient).
 *   - `stop`: after `handle`, the SDK deletes the snapshot in the
 *     ack transaction. The process is over.
 *   - `ignore`: the SDK advances the cursor without calling `handle`.
 */
export type RouteResult =
  | { kind: "start"; processId: string }
  | { kind: "continue"; processId: string }
  | { kind: "stop"; processId: string }
  | { kind: "ignore" };

export type RouteFn<E = unknown> = (
  event: RecordedEvent<E>,
) => string | RouteResult | null | undefined;

/**
 * A command emitted by a PM `handle`. v1 supports the by-value form
 * only; the by-name form (§11.7) lands with the Layer 5 facade.
 */
export interface DispatchedCommand {
  streamUuid: string;
  aggregate: AggregateDefinition<any, any, any>;
  command: unknown;
}

export interface ProcessManagerHandlerResult<S> {
  state: S;
  commands?: DispatchedCommand[];
}

export interface ProcessManagerDefinition<S, E = unknown> {
  /**
   * The subscription name (per stream), the snapshot `source_type`,
   * and the snapshot-uuid prefix all share this string.
   */
  name: string;
  /** Default `$all`. */
  stream?: string;
  routes: { [eventType: string]: RouteFn<E> };
  initialState(): S;
  handle(
    state: S,
    event: RecordedEvent<E>,
  ): Promise<ProcessManagerHandlerResult<S>>;
  /** Honoured only on the first claim that creates the subscription. */
  startFrom?: StartFrom;
}

export interface ProcessManagerWorkerOptions {
  workerId?: string;
  batchSize?: number;
  leaseSeconds?: number;
  heartbeatInterval?: number;
  pollInterval?: number;
  /** Called for handler / dispatch errors and for fatal lifecycle events. */
  onError?: (err: Error) => void;
}

/** Handler-throws backoff schedule (§11.5). Shared with projections. */
const HANDLER_BACKOFF_MS: readonly number[] = [
  250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000,
];

const HEARTBEAT_RETRY_DELAY_MS = 250;

export function startProcessManager<S, E = unknown>(
  client: Client,
  dispatchClient: Client,
  def: ProcessManagerDefinition<S, E>,
  opts: ProcessManagerWorkerOptions = {},
): RunningWorker {
  if (client === dispatchClient) {
    // D-0011 / D-0012: the persist-and-ack session and the dispatch
    // session must be different so their lock sets are disjoint.
    throw new InstructedError(
      "startProcessManager: persist client and dispatch client must be different Client instances (D-0011 / D-0012)",
    );
  }

  const stream = def.stream ?? "$all";
  const workerId = opts.workerId ?? defaultWorkerId();
  const batchSize = opts.batchSize ?? DEFAULT_BATCH_SIZE;
  const leaseSeconds = opts.leaseSeconds ?? DEFAULT_LEASE_SECONDS;
  const heartbeatInterval =
    opts.heartbeatInterval ?? Math.max(1_000, (leaseSeconds * 1000) / 3);
  const pollInterval = opts.pollInterval ?? DEFAULT_POLL_INTERVAL_MS;
  const onError = opts.onError ?? noopOnError;

  const ac = new AbortController();
  const signal = ac.signal;

  let closing = false;
  let aborted = false;
  let closePromise: Promise<void> | null = null;

  let resolveStopped!: () => void;
  const stopped = new Promise<void>((res) => {
    resolveStopped = res;
  });

  function markAborted(err: Error): void {
    if (aborted) return;
    aborted = true;
    try {
      ac.abort();
    } catch {
      // ignore
    }
    safeOnError(err);
  }

  function safeOnError(err: Error): void {
    try {
      onError(err);
    } catch {
      // onError must never propagate.
    }
  }

  async function heartbeatLoop(): Promise<void> {
    while (!closing && !aborted) {
      await sleep(heartbeatInterval, signal);
      if (closing || aborted) break;
      try {
        await client.extendSubscriptionClaim(
          stream,
          def.name,
          workerId,
          leaseSeconds,
        );
        continue;
      } catch (err) {
        if (isLeaseLoss(err)) {
          markAborted(err as Error);
          return;
        }
        await sleep(HEARTBEAT_RETRY_DELAY_MS, signal);
        if (closing || aborted) return;
        try {
          await client.extendSubscriptionClaim(
            stream,
            def.name,
            workerId,
            leaseSeconds,
          );
          continue;
        } catch (err2) {
          if (!isLeaseLoss(err2)) safeOnError(err2 as Error);
          markAborted(err2 as Error);
          return;
        }
      }
    }
  }

  /**
   * Process one routed event. Returns true on success (cursor advanced
   * in the persist-and-ack tx), false on lease loss / shutdown (caller
   * breaks out of the batch). Throws nothing — handler errors are
   * converted to exponential-backoff retries (§11.5).
   *
   * Note: callers must filter `{kind: 'ignore'}` upstream. Ignored
   * events are *not* individually acked; the next routed event's tx
   * advances the cursor past them implicitly (TODO #10), and a single
   * trailing `advance_subscription` per batch covers any unrouted
   * tail.
   */
  async function processRoutedEvent(
    event: RecordedEvent<E>,
    routed: Exclude<RouteResult, { kind: "ignore" }>,
  ): Promise<boolean> {
    const sourceUuid = `${def.name}-${routed.processId}`;

    // ---- handle (with backoff on throw) ----
    let attempt = 0;
    while (!closing && !aborted) {
      // Load state per the route directive. `start` discards any
      // existing snapshot (§11.4 lenient); `continue` and `stop` load
      // it if present, else start from initialState.
      let state: S;
      try {
        if (routed.kind === "start") {
          state = def.initialState();
        } else {
          state = await loadState(client, def, sourceUuid);
        }
      } catch (err) {
        // Snapshot read failures other than IS010 are infra problems;
        // re-throw via onError + backoff.
        safeOnError(
          new HandlerError(
            `process manager ${def.name} failed to load state for ${sourceUuid}`,
            { cause: err, event },
          ),
        );
        await backoff(attempt++);
        continue;
      }

      // ---- user handle (outside any SDK tx, D-0016) ----
      let result: ProcessManagerHandlerResult<S>;
      try {
        result = await def.handle(state, event);
      } catch (err) {
        safeOnError(
          new HandlerError(
            `process manager ${def.name} handle threw on event_number ${event.event_number}`,
            { cause: err, event },
          ),
        );
        await backoff(attempt++);
        continue;
      }
      if (aborted || closing) return false;

      // ---- dispatch (on the separate session, D-0011/D-0012) ----
      // §11.8 / D-0017: SDK fills causation_id (= triggering event id)
      // and correlation_id from the triggering event.
      const commands = result.commands ?? [];
      let dispatchOk = true;
      for (const c of commands) {
        if (aborted || closing) return false;
        try {
          await runCommand(
            dispatchClient,
            c.aggregate,
            c.streamUuid,
            c.command,
            {
              causationId: event.event_id,
              correlationId: event.correlation_id ?? undefined,
            },
          );
        } catch (err) {
          safeOnError(
            new HandlerError(
              `process manager ${def.name} dispatch failed on event_number ${event.event_number}`,
              { cause: err, event },
            ),
          );
          dispatchOk = false;
          break;
        }
      }
      if (!dispatchOk) {
        // Don't advance — the event will be redelivered. PM authors
        // are expected to keep dispatched commands idempotent
        // (deterministic ids per §11.5 PM-specific note).
        await backoff(attempt++);
        continue;
      }
      if (aborted || closing) return false;

      // ---- persist-and-ack (single short SDK-internal tx) ----
      try {
        await withTransaction(client, async (tx) => {
          if (routed.kind === "stop") {
            await tx.deleteSnapshot(sourceUuid);
          } else {
            await tx.recordSnapshot({
              sourceUuid,
              sourceType: def.name,
              sourceVersion: event.event_number,
              data: result.state,
            });
          }
          await tx.advanceSubscription(
            stream,
            def.name,
            workerId,
            // event_number for $all; stream_version for per-stream
            // (sql/instructed.sql :: advance_subscription).
            stream === "$all" ? event.event_number : event.stream_version,
          );
        });
      } catch (err) {
        if (isLeaseLoss(err)) {
          markAborted(err as Error);
          return false;
        }
        safeOnError(err as Error);
        // Don't advance — try again. The handler is idempotent on
        // redelivery by contract.
        await backoff(attempt++);
        continue;
      }
      return true;
    }
    return false;
  }

  const positionOf = (e: RecordedEvent<E>): bigint =>
    stream === "$all" ? e.event_number : e.stream_version;

  async function backoff(idx: number): Promise<void> {
    const delay =
      HANDLER_BACKOFF_MS[Math.min(idx, HANDLER_BACKOFF_MS.length - 1)];
    await sleep(delay, signal);
  }

  async function loop(): Promise<void> {
    try {
      // ---- claim ----
      try {
        const claimOpts =
          def.startFrom !== undefined ? { startFrom: def.startFrom } : {};
        const r = await client.claimSubscription(
          stream,
          def.name,
          workerId,
          leaseSeconds,
          claimOpts,
        );
        if (r.result === "already_claimed") {
          safeOnError(
            new SubscriptionLeaseLost(
              `subscription ${def.name} on ${stream} is already claimed by ${r.claimedBy}`,
              {
                code: "IS022",
                streamUuid: stream,
                subscriptionName: def.name,
                holder: r.claimedBy,
              },
            ),
          );
          return;
        }
      } catch (err) {
        safeOnError(err as Error);
        return;
      }

      // ---- heartbeat ----
      const hb = heartbeatLoop().catch(() => {
        /* swallowed; heartbeatLoop sets aborted on its own */
      });

      try {
        // ---- poll loop ----
        while (!closing && !aborted) {
          let batch: RecordedEvent<E>[];
          try {
            batch = await client.readSubscriptionBatch<E>(
              stream,
              def.name,
              workerId,
              batchSize,
            );
          } catch (err) {
            if (isLeaseLoss(err)) {
              markAborted(err as Error);
              break;
            }
            safeOnError(err as Error);
            await sleep(pollInterval, signal);
            continue;
          }

          if (batch.length === 0) {
            await sleep(pollInterval, signal);
            continue;
          }

          // Coalescing walker (TODO #10 / ex-ML-0005). Ignored events
          // are not individually acked; the next routed event's
          // persist-and-ack tx covers them implicitly because
          // `advance_subscription` is monotone. Any tail of ignored
          // events at the end of the batch is flushed with one
          // `advance_subscription` after the loop.
          let pendingIgnoredTo: bigint | null = null;
          for (const event of batch) {
            if (closing || aborted) break;
            const routeFn = def.routes[event.event_type];
            const routed = routeFn
              ? normaliseRoute(routeFn(event))
              : ({ kind: "ignore" } as const);
            if (routed.kind === "ignore") {
              pendingIgnoredTo = positionOf(event);
              continue;
            }
            const ok = await processRoutedEvent(event, routed);
            if (!ok) break;
            // The routed tx's advance covers any prior ignored run.
            pendingIgnoredTo = null;
          }

          // End-of-batch trailing-ignored flush.
          if (pendingIgnoredTo !== null && !closing && !aborted) {
            try {
              await client.advanceSubscription(
                stream,
                def.name,
                workerId,
                pendingIgnoredTo,
              );
            } catch (err) {
              if (isLeaseLoss(err)) {
                markAborted(err as Error);
              } else {
                safeOnError(err as Error);
              }
            }
          }
        }
      } finally {
        ac.abort();
        await hb;
      }

      // ---- release (best-effort) ----
      try {
        await client.releaseSubscription(stream, def.name, workerId);
      } catch (err) {
        if (!isLeaseLoss(err) && !(err instanceof SubscriptionNotFound)) {
          safeOnError(err as Error);
        }
      }
    } finally {
      resolveStopped();
    }
  }

  const loopPromise = loop();
  loopPromise.catch(() => {
    /* unreachable: loop's try/finally always resolves stopped */
  });

  return {
    stopped,
    close(): Promise<void> {
      if (closePromise) return closePromise;
      closing = true;
      try {
        ac.abort();
      } catch {
        // ignore
      }
      closePromise = stopped;
      return closePromise;
    },
  };
}

// ---- helpers ----

function normaliseRoute(
  r: string | RouteResult | null | undefined,
): RouteResult {
  if (r == null) return { kind: "ignore" };
  if (typeof r === "string") return { kind: "continue", processId: r };
  return r;
}

async function loadState<S, E>(
  client: Client,
  def: ProcessManagerDefinition<S, E>,
  sourceUuid: string,
): Promise<S> {
  try {
    const snap = await client.readSnapshot<S>(sourceUuid);
    return snap.data;
  } catch (err) {
    // SnapshotNotFound → fresh state.
    if (
      err instanceof InstructedError &&
      (err as { code?: string }).code === "IS010"
    ) {
      return def.initialState();
    }
    throw err;
  }
}

function isLeaseLoss(
  err: unknown,
): err is SubscriptionLeaseLost | SubscriptionNotFound {
  return (
    err instanceof SubscriptionLeaseLost || err instanceof SubscriptionNotFound
  );
}

function noopOnError(_err: Error): void {
  /* no-op */
}
