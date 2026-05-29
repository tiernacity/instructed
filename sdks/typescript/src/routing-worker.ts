/**
 * SUB-A routing worker.
 *
 * # The routing extension point
 *
 * Routing is one of the SDK's three named extension points (see
 * `sdks/porting-checklist.md` §4.1). It follows the family pattern
 * **contract + standard library + escape hatch**:
 *
 *   - **Contract.** `RoutingFn<E>` (this file). Given a
 *     `RecordedEvent<E>`, return a `RoutingDecision` — either
 *     `{ partitionKey: string }` (route to that partition) or
 *     `"ignore"` (skip; nothing is written to the work queue for
 *     this event). MUST be pure, deterministic, no I/O, and fast
 *     enough that `batchSize × routeFn` completes inside
 *     `leaseSeconds`.
 *   - **Standard library.** `PartitionBy` + `routingFnForPartitionBy`
 *     in `partition-by.ts`. Three shipped strategies (`sequential`,
 *     `per-event`, `per-key`) cover the common cases; none of them
 *     can produce `"ignore"` (intentional simplification).
 *   - **Escape hatch.** Pass any `RoutingFn` to
 *     `startRoutingWorker` directly. Required if you need
 *     `"ignore"`-style routing-time filtering, or a strategy
 *     that doesn't fit the `PartitionBy` cases.
 *
 * Routing is **required core** (L2) — every SDK port reproduces it.
 * The `PartitionBy` sugar is **idiomatic, not required**: a port
 * may ship its own equivalent in whatever shape fits the language,
 * or omit it entirely and document the raw `RoutingFn` shape.
 *
 * # Worker semantics
 *
 * Per-batch claim/release routing worker. At any single instant there is
 * at most one routing worker holding the subscription's lease
 * (INV-SUB-P-010); the *identity* of the active worker rotates per batch
 * across whichever processes are polling. This makes work-stealing across
 * processes the natural default: spinning up a second process immediately
 * shares routing load with the first, rather than idling on
 * `already_claimed` until the first dies. See `docs/decisions.md`
 * **D-0025** for the design rationale.
 *
 * The loop:
 *
 *   1. claim_subscription(stream, name, workerId, leaseSeconds)
 *   2. if 'already_claimed' -> sleep pollInterval; goto 1
 *   3. readAll(lastSeen + 1, batchSize) (or readStream)
 *   4. if empty -> release_subscription; sleep pollInterval; goto 1
 *   5. for each event: routeFn -> decisions[]
 *   6. route_batch (atomic: cursor advance + work-item INSERTs)
 *      - on IS022 (subscription_lease_lost): drop the batch; goto 1
 *   7. release_subscription
 *   8. goto 1
 *
 * Key contract points:
 *
 *   * `routeFn` is pure user code: no I/O, no aggregate loads, **fast**.
 *     "Fast" is bounded by `leaseSeconds`: a batch whose `route_batch`
 *     fires after the lease expires gets `IS022` and the work is
 *     redone. Configure `leaseSeconds` comfortably larger than expected
 *     worst-case `batchSize × routeFn` duration; if you can't bound
 *     that, shrink `batchSize`.
 *   * A thrown routeFn stalls the worker (and is surfaced via
 *     `onError`); the alternative -- silent skip -- would violate the
 *     "no silent skip" contract.
 *   * `route_batch` commits the cursor advance and the work-item
 *     INSERTs in one transaction. This atomicity is load-bearing for
 *     waitForProjection's race safety: once `last_seen >= N` is
 *     observable, the corresponding work items are observable too.
 *   * The routing cursor is monotone; a crashed mid-batch worker that
 *     restarts re-reads the same events and the work-item PK absorbs
 *     duplicate INSERTs (`ON CONFLICT DO NOTHING` in the SQL
 *     procedure). The same property absorbs duplicates from an
 *     `IS022`-aborted batch.
 *   * The SDK does **not** heartbeat. The lease covers one batch;
 *     `route_batch` re-verifies `claimed_by` on each call, so a
 *     lost lease surfaces as `IS022` rather than via a heartbeat.
 */

import type { Client } from "./client.ts";
import { Logger } from "./logger.ts";
import { SubscriptionLeaseLost, SubscriptionNotFound } from "./errors/index.ts";
import type {
  Event,
  RecordedEvent,
  RouteDecision,
  StartFrom,
} from "./types/index.ts";
import type { RunningWorker } from "./internal/running-worker.ts";
export type { RunningWorker };
import { defaultWorkerId } from "./internal/worker-id.ts";
import { sleep } from "./internal/sleep.ts";

/**
 * Routing-extension-point output. Either route the event to the
 * named partition, or skip it. PM-F surface as seen by user code.
 */
export type RoutingDecision = { partitionKey: string } | "ignore";

/**
 * Routing-extension-point contract. Given an event from the source
 * stream, decide where it goes. MUST be pure, deterministic, no
 * I/O, and bounded in duration (see the lease-budget note in the
 * module header). A thrown `RoutingFn` stalls the worker via
 * `onError`; the alternative — silent skip — would violate the
 * "no silent skip" contract.
 *
 * See `partition-by.ts` for the shipped standard-library
 * implementations (`PartitionBy` / `routingFnForPartitionBy`).
 */
export type RoutingFn<E extends Event = Event> = (
  event: RecordedEvent<E>,
) => RoutingDecision | Promise<RoutingDecision>;

export interface RoutingDefinition<E extends Event = Event> {
  /** Subscription name. */
  name: string;
  /** Source stream; default `$all`. */
  stream?: string;
  /** Per-event routing decision. Pure, deterministic, no I/O, fast. */
  routeFn: RoutingFn<E>;
  /** Honoured only on the first claim that creates the subscription. */
  startFrom?: StartFrom;
}

export interface RoutingWorkerOptions {
  workerId?: string;
  /** Max events fetched per readAll round-trip. Default 100. */
  batchSize?: number;
  /**
   * Lease duration in seconds. Default 30. Bounds the per-batch
   * processing budget: if a batch overruns this, route_batch raises
   * IS022 and the work is redone by the next worker.
   */
  leaseSeconds?: number;
  /** Idle poll interval in ms. Default 200. */
  pollInterval?: number;
  /** Called for routing-fn errors and other fatal events. */
  onError?: (err: Error) => void;
  /**
   * Worker-scoped {@link Logger}. Defaults to {@link Logger.noop}
   * when absent (the worker is silent unless the caller wires one
   * in; the `Instructed` facade always does).
   */
  logger?: Logger;
}

// Defaults exported for re-use by the facade and tests.
export const DEFAULT_ROUTING_BATCH_SIZE = 100;
export const DEFAULT_ROUTING_LEASE_SECONDS = 30;
export const DEFAULT_ROUTING_POLL_INTERVAL_MS = 200;

export function startRoutingWorker<E extends Event = Event>(
  client: Client,
  def: RoutingDefinition<E>,
  opts: RoutingWorkerOptions = {},
): RunningWorker {
  const stream = def.stream ?? "$all";
  const workerId = opts.workerId ?? defaultWorkerId();
  const batchSize = opts.batchSize ?? DEFAULT_ROUTING_BATCH_SIZE;
  const leaseSeconds = opts.leaseSeconds ?? DEFAULT_ROUTING_LEASE_SECONDS;
  const pollInterval = opts.pollInterval ?? DEFAULT_ROUTING_POLL_INTERVAL_MS;
  const onError = opts.onError ?? noopOnError;
  const logger = opts.logger ?? Logger.noop();

  const ac = new AbortController();
  const signal = ac.signal;

  let closing = false;
  let aborted = false;
  let closePromise: Promise<void> | null = null;
  /**
   * Tracks whether we currently hold the lease. Set to true after a
   * successful claim_subscription returning 'claimed'; cleared after
   * release_subscription, IS022, or fatal error. Used by the close
   * path to decide whether to attempt a final release.
   */
  let holdingLease = false;

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
      /* ignore */
    }
    safeOnError(err);
  }

  function safeOnError(err: Error): void {
    try {
      onError(err);
    } catch {
      /* onError must never propagate */
    }
  }

  /**
   * Route the events in `batch`. Returns the decisions array and the
   * highest event_number to advance the cursor to, or null if the
   * batch must be dropped (close/abort mid-iteration, or a fatal
   * user-code error already surfaced via markAborted).
   */
  async function routeOneBatch(
    batch: RecordedEvent<E>[],
  ): Promise<{ decisions: RouteDecision[]; cursorTo: bigint } | null> {
    const decisions: RouteDecision[] = [];
    let cursorTo: bigint | null = null;
    for (const event of batch) {
      if (closing || aborted) return null;
      let d: RoutingDecision;
      try {
        d = await def.routeFn(event);
      } catch (err) {
        markAborted(
          err instanceof Error
            ? err
            : new Error(`routing worker: routeFn threw: ${String(err)}`),
        );
        return null;
      }
      if (closing || aborted) return null;
      cursorTo = event.event_number;
      if (d === "ignore") continue;
      if (!d || typeof d !== "object" || typeof d.partitionKey !== "string") {
        markAborted(
          new Error(
            `routing worker: routeFn must return "ignore" or { partitionKey: string }; got ${JSON.stringify(d)}`,
          ),
        );
        return null;
      }
      decisions.push({
        partitionKey: d.partitionKey,
        eventNumber: event.event_number,
      });
    }
    if (cursorTo === null) return null;
    return { decisions, cursorTo };
  }

  async function loop(): Promise<void> {
    try {
      while (!closing && !aborted) {
        // ---- claim ----
        let claimed: { lastSeen: bigint };
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
            // Another worker holds the lease for this batch; back off
            // and try again next tick. NOT fatal under D-0025.
            logger.trace("routing: subscription already claimed; backing off");
            await sleep(pollInterval, signal);
            continue;
          }
          claimed = { lastSeen: r.lastSeen };
          holdingLease = true;
          logger.trace(
            () => `routing: claimed; lastSeen=${claimed.lastSeen}`,
          );
        } catch (err) {
          // Genuine errors (stream not found, bad args, transport
          // failures). Surface and exit.
          safeOnError(err as Error);
          return;
        }

        if (closing || aborted) {
          await releaseQuietly();
          break;
        }

        // ---- read ----
        let batch: RecordedEvent<E>[];
        try {
          batch =
            stream === "$all"
              ? await client.readAll<E>(claimed.lastSeen + 1n, batchSize)
              : await client.readStream<E>(
                  stream,
                  claimed.lastSeen + 1n,
                  batchSize,
                );
        } catch (err) {
          safeOnError(err as Error);
          await releaseQuietly();
          await sleep(pollInterval, signal);
          continue;
        }

        if (batch.length === 0) {
          // No work; release the lease so another process can claim
          // immediately, then poll-sleep.
          logger.trace("routing: empty batch; releasing and polling");
          await releaseQuietly();
          await sleep(pollInterval, signal);
          continue;
        }
        logger.trace(
          () => `routing: read ${batch.length} events from ${stream}`,
        );

        // ---- route ----
        const routed = await routeOneBatch(batch);
        if (routed === null) {
          // Either: (a) the user-code routeFn threw and markAborted
          // fired (we'll exit the outer loop), or (b) close/abort hit
          // mid-iteration. Either way: drop the batch, release if we
          // can, exit.
          await releaseQuietly();
          break;
        }

        // ---- commit ----
        logger.trace(
          () =>
            `routing: committing ${routed.decisions.length} decision(s); cursor -> ${routed.cursorTo}`,
        );
        try {
          await client.routeBatch(
            stream,
            def.name,
            workerId,
            routed.cursorTo,
            routed.decisions,
          );
        } catch (err) {
          if (isLeaseLoss(err)) {
            // The lease expired mid-batch (likely a slow routeFn) and
            // another worker may have taken over. Drop the batch and
            // loop. Not fatal under D-0025; the work-item PK absorbs
            // any duplicates the other worker may have already
            // routed.
            holdingLease = false;
            continue;
          }
          safeOnError(err as Error);
          await releaseQuietly();
          await sleep(pollInterval, signal);
          continue;
        }

        // ---- release ----
        await releaseQuietly();
      }
    } finally {
      // Best-effort final release if we still hold the lease.
      await releaseQuietly();
      resolveStopped();
    }
  }

  async function releaseQuietly(): Promise<void> {
    if (!holdingLease) return;
    holdingLease = false;
    try {
      await client.releaseSubscription(stream, def.name, workerId);
    } catch (err) {
      if (!isLeaseLoss(err) && !(err instanceof SubscriptionNotFound)) {
        // Surface but don't abort — release is best-effort.
        safeOnError(err as Error);
      }
    }
  }

  const loopPromise = loop();
  loopPromise.catch(() => {
    /* unreachable: loop's try/finally always resolves stopped */
  });

  return {
    stopped,
    stop(): Promise<void> {
      if (closePromise) return closePromise;
      closing = true;
      try {
        ac.abort();
      } catch {
        /* ignore */
      }
      closePromise = stopped;
      return closePromise;
    },
  };
}

function noopOnError(_err: Error): void {
  /* no-op default */
}

function isLeaseLoss(
  err: unknown,
): err is SubscriptionLeaseLost | SubscriptionNotFound {
  return (
    err instanceof SubscriptionLeaseLost || err instanceof SubscriptionNotFound
  );
}
