/**
 * SUB-A routing worker (slice 4).
 *
 * One routing worker per subscription. Holds the subscription's lease
 * (via `claim_subscription` + heartbeat through `extend_subscription_claim`).
 * Reads batches from `$all` (or any single stream) past
 * `subscriptions.last_seen`, runs the user-supplied `RoutingFn` on each
 * event, and atomically writes the routing decisions + advances the
 * cursor via `route_batch`. The actual work is then picked up by
 * processing workers (slice 5+); this worker does not run handlers and
 * is unaware of subscription kind (projection vs PM).
 *
 * Per the SUB-A design:
 *   * Routing is pure user code; no I/O, no aggregate loads. If a
 *     RoutingFn throws, the worker stalls and surfaces the error via
 *     `onError` — the alternative (silent skip) violates the "no
 *     silent skip" contract.
 *   * `route_batch` commits the cursor advance and the work-item
 *     INSERTs in one transaction. This is the load-bearing atomicity
 *     for waitForProjection's race safety: once `last_seen >= N` is
 *     observable, the corresponding work items are observable too.
 *   * The routing cursor is monotone; a crashed mid-batch worker that
 *     restarts re-reads the same events and the work-item PK absorbs
 *     duplicate INSERTs (ON CONFLICT DO NOTHING in the SQL
 *     procedure).
 *
 * Lifecycle / lease handling mirror `startProjection`: claim ->
 * heartbeat -> poll loop -> release on close. Lease loss aborts the
 * loop via the shared `AbortSignal`.
 *
 * Not yet exposed via `src/index.ts`. The Layer-5 facade
 * (`Instructed.registerProjection` / `registerProcessManager`) wires
 * the routing worker together with the processing worker in slice 9;
 * tests for this slice import the module directly.
 */

import type { Client } from "./client.ts";
import { SubscriptionLeaseLost, SubscriptionNotFound } from "./errors.ts";
import type {
  RecordedEvent,
  RouteDecision,
  StartFrom,
} from "./types.ts";
import type { RunningWorker } from "./internal/running-worker.ts";
export type { RunningWorker };
import { defaultWorkerId } from "./internal/worker-id.ts";
import { sleep } from "./internal/sleep.ts";

/** PM-F routing-decision surface as seen by user code. */
export type RoutingDecision = { partitionKey: string } | "ignore";

export type RoutingFn<E = unknown> = (
  event: RecordedEvent<E>,
) => RoutingDecision | Promise<RoutingDecision>;

export interface RoutingDefinition<E = unknown> {
  /** Subscription name. */
  name: string;
  /** Source stream; default `$all`. */
  stream?: string;
  /** Per-event routing decision. Pure: no I/O, no side-effects. */
  routeFn: RoutingFn<E>;
  /** Honoured only on the first claim that creates the subscription. */
  startFrom?: StartFrom;
}

export interface RoutingWorkerOptions {
  workerId?: string;
  /** Max events fetched per readAll round-trip. Default 100. */
  batchSize?: number;
  /** Lease duration in seconds. Default 30. */
  leaseSeconds?: number;
  /** Heartbeat tick in ms. Default = `leaseSeconds * 1000 / 3`. */
  heartbeatInterval?: number;
  /** Idle poll interval in ms. Default 200. */
  pollInterval?: number;
  /** Called for routing-fn errors, lease loss, and other fatal events. */
  onError?: (err: Error) => void;
}

// Defaults exported for re-use by the facade (slice 9) and tests.
export const DEFAULT_ROUTING_BATCH_SIZE = 100;
export const DEFAULT_ROUTING_LEASE_SECONDS = 30;
export const DEFAULT_ROUTING_POLL_INTERVAL_MS = 200;

/** Single retry delay on a non-lease-loss heartbeat error. */
const HEARTBEAT_RETRY_DELAY_MS = 250;

export function startRoutingWorker<E = unknown>(
  client: Client,
  def: RoutingDefinition<E>,
  opts: RoutingWorkerOptions = {},
): RunningWorker {
  const stream = def.stream ?? "$all";
  const workerId = opts.workerId ?? defaultWorkerId();
  const batchSize = opts.batchSize ?? DEFAULT_ROUTING_BATCH_SIZE;
  const leaseSeconds = opts.leaseSeconds ?? DEFAULT_ROUTING_LEASE_SECONDS;
  const heartbeatInterval =
    opts.heartbeatInterval ?? Math.max(1_000, (leaseSeconds * 1000) / 3);
  const pollInterval = opts.pollInterval ?? DEFAULT_ROUTING_POLL_INTERVAL_MS;
  const onError = opts.onError ?? noopOnError;

  const ac = new AbortController();
  const signal = ac.signal;

  let closing = false;
  let aborted = false;
  let closePromise: Promise<void> | null = null;
  let lastSeen = 0n;

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

  async function routeOneBatch(
    batch: RecordedEvent<E>[],
  ): Promise<{ decisions: RouteDecision[]; cursorTo: bigint } | null> {
    const decisions: RouteDecision[] = [];
    let cursorTo: bigint | null = null;
    for (const event of batch) {
      if (closing || aborted) {
        // Drop the partial batch. The slice-4 acceptance contract is
        // "crash mid-batch leaves cursor un-advanced"; a graceful
        // close mid-batch behaves the same. The re-launched worker
        // re-reads from lastSeen and ON CONFLICT DO NOTHING absorbs
        // any rows that hypothetically would have been re-routed.
        return null;
      }
      let d: RoutingDecision;
      try {
        d = await def.routeFn(event);
      } catch (err) {
        // Routing is pure user code; a throw is fatal. Stop the worker
        // without advancing the cursor; an operator must fix the
        // routeFn before re-launching. (SUB-A "no silent skip".)
        markAborted(
          err instanceof Error
            ? err
            : new Error(`routing worker: routeFn threw: ${String(err)}`),
        );
        return null;
      }
      // §11.1-equivalent: if the abort fired during the routeFn await
      // and the routeFn still resolved, the SDK MUST drop the batch.
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
              `routing worker: subscription ${def.name} on ${stream} is already claimed by ${r.claimedBy}`,
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
        lastSeen = r.lastSeen;
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
            // readAll's first arg is an inclusive lower bound on
            // event_number; we want strictly after lastSeen.
            batch =
              stream === "$all"
                ? await client.readAll<E>(lastSeen + 1n, batchSize)
                : await client.readStream<E>(stream, lastSeen + 1n, batchSize);
          } catch (err) {
            safeOnError(err as Error);
            await sleep(pollInterval, signal);
            continue;
          }

          if (batch.length === 0) {
            await sleep(pollInterval, signal);
            continue;
          }

          const routed = await routeOneBatch(batch);
          if (routed === null) {
            // routeOneBatch returns null on a fatal user-code error
            // (already markAborted-ed) or when nothing was processed
            // because of close/abort mid-iteration. Either way: exit.
            break;
          }

          // Advance the cursor + insert work items atomically.
          try {
            await client.routeBatch(
              stream,
              def.name,
              workerId,
              routed.cursorTo,
              routed.decisions,
            );
            lastSeen = routed.cursorTo;
          } catch (err) {
            if (isLeaseLoss(err)) {
              markAborted(err as Error);
              break;
            }
            safeOnError(err as Error);
            // Cursor not advanced; re-route the same events next iter.
            await sleep(pollInterval, signal);
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
