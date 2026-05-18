/**
 * Layer 2: persistent-subscription worker (projections).
 *
 * See docs/sdk-design.md §3 layer 2, §8 (worker-loop diagram),
 * §11.1 (HandlerContext), §11.5 (handler-throws backoff),
 * §11.9 (lifecycle, AbortSignal, ack-on-shutdown). Per D-0016 the
 * handler is opaque to the SDK: ctx carries no connection / tx, two
 * short SDK-owned transactions bracket the handler (read tx, ack tx),
 * never around it.
 *
 * v1 selector locus is SDK-side (OQ-0003 resolution / §7): the cursor
 * advances past every fetched event regardless of selector match, only
 * the handler is gated.
 */

import type { Client } from "./client.ts";
import {
  HandlerError,
  SubscriptionLeaseLost,
  SubscriptionNotFound,
} from "./errors.ts";
import type { RecordedEvent, StartFrom } from "./types.ts";
import { defaultWorkerId } from "./internal/worker-id.ts";
import { sleep } from "./internal/sleep.ts";

export interface HandlerContext {
  workerId: string;
  position: { eventNumber: bigint; streamVersion: bigint };
  /** Aborted on graceful shutdown and on lease loss. */
  signal: AbortSignal;
}

export type ProjectionHandler<E = unknown> = (
  event: RecordedEvent<E>,
  ctx: HandlerContext,
) => Promise<void>;

export interface ProjectionDefinition<E = unknown> {
  /** The subscription name (per stream). */
  name: string;
  /** Default `$all`. */
  stream?: string;
  handle: ProjectionHandler<E>;
  /** Honoured only on the first claim that creates the subscription. */
  startFrom?: StartFrom;
  /** SDK-side post-fetch filter. The cursor advances past skipped events. */
  selector?: (e: RecordedEvent<E>) => boolean;
}

export interface ProjectionWorkerOptions {
  workerId?: string;
  /** Max events per fetch. Default 50. */
  batchSize?: number;
  /** Lease duration in seconds. Default 30. */
  leaseSeconds?: number;
  /** Heartbeat tick in ms. Default = leaseSeconds*1000 / 3. */
  heartbeatInterval?: number;
  /** Idle poll interval in ms. Default 250. */
  pollInterval?: number;
  /** Called for handler errors and for fatal lifecycle events. */
  onError?: (err: Error) => void;
}

export interface RunningWorker {
  /** Idempotent. Stops the loop, releases the lease best-effort. */
  close(): Promise<void>;
  /** Resolves when the loop has exited. Never rejects. */
  readonly stopped: Promise<void>;
}

// Defaults named here for re-use by the facade (layer 5) and tests.
export const DEFAULT_BATCH_SIZE = 50;
export const DEFAULT_LEASE_SECONDS = 30;
export const DEFAULT_POLL_INTERVAL_MS = 250;

/**
 * Handler-throws backoff schedule (§11.5). Capped at 30s; no max
 * attempts in v1 — a poison event stalls the projection by design.
 */
const HANDLER_BACKOFF_MS: readonly number[] = [
  250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000,
];

/** Single retry delay on a non-lease-loss heartbeat error (§11.9). */
const HEARTBEAT_RETRY_DELAY_MS = 250;

export function startProjection<E = unknown>(
  client: Client,
  def: ProjectionDefinition<E>,
  opts: ProjectionWorkerOptions = {},
): RunningWorker {
  const stream = def.stream ?? "$all";
  const workerId = opts.workerId ?? defaultWorkerId();
  const batchSize = opts.batchSize ?? DEFAULT_BATCH_SIZE;
  const leaseSeconds = opts.leaseSeconds ?? DEFAULT_LEASE_SECONDS;
  const heartbeatInterval =
    opts.heartbeatInterval ?? Math.max(1_000, (leaseSeconds * 1000) / 3);
  const pollInterval = opts.pollInterval ?? DEFAULT_POLL_INTERVAL_MS;
  const onError = opts.onError ?? noopOnError;

  // The AbortController fires on close() AND on lease loss (§11.1 / §11.9).
  // Same signal across all in-flight handler invocations.
  const ac = new AbortController();
  const signal = ac.signal;

  let closing = false;
  let aborted = false;
  let closePromise: Promise<void> | null = null;
  let lastPosition: { eventNumber: bigint; streamVersion: bigint } = {
    eventNumber: 0n,
    streamVersion: 0n,
  };

  let resolveStopped!: () => void;
  const stopped = new Promise<void>((res) => {
    resolveStopped = res;
  });

  /** Signal lease loss: abort the signal, set the stop flag. */
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
      // onError must never propagate; the loop is responsible for
      // staying alive while the lease is valid.
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
        // Non-lease-loss heartbeat error: one retry then escalate
        // (§11.9 / D-0018).
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

  async function runHandlerWithBackoff(event: RecordedEvent<E>): Promise<void> {
    let idx = 0;
    // No max-attempts in v1 (§11.5). Loop until success, close, or
    // lease loss.
    while (!closing && !aborted) {
      const ctx: HandlerContext = {
        workerId,
        position: {
          eventNumber: event.event_number,
          streamVersion: event.stream_version,
        },
        signal,
      };
      try {
        await def.handle(event, ctx);
        return;
      } catch (err) {
        safeOnError(
          new HandlerError(
            `projection ${def.name} handler threw on event_number ${event.event_number}`,
            { cause: err, event },
          ),
        );
        const delay =
          HANDLER_BACKOFF_MS[Math.min(idx, HANDLER_BACKOFF_MS.length - 1)];
        idx += 1;
        await sleep(delay, signal);
      }
    }
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
          // Another worker holds the lease; nothing to do. Report and exit.
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
        lastPosition = { eventNumber: r.lastSeen, streamVersion: r.lastSeen };
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

          let advanceTo: bigint | null = null;
          // advance_subscription targets event_number for $all and
          // stream_version for per-stream subscriptions (sql/instructed.sql
          // :: advance_subscription).
          const positionOf = (e: RecordedEvent<E>): bigint =>
            stream === "$all" ? e.event_number : e.stream_version;
          for (const event of batch) {
            if (closing || aborted) break;
            // SDK-side selector (§7 / OQ-0003): the cursor advances
            // past the event regardless; only the handler is gated.
            if (def.selector && !def.selector(event)) {
              advanceTo = positionOf(event);
              continue;
            }
            await runHandlerWithBackoff(event);
            if (aborted) break;
            // §11.1: if the signal fired during the handler and the
            // handler still resolved, the SDK MUST skip the advance.
            // `aborted` covers both lease-loss and close() cases.
            if (closing) break;
            advanceTo = positionOf(event);
            lastPosition = {
              eventNumber: event.event_number,
              streamVersion: event.stream_version,
            };
          }

          if (advanceTo !== null && !aborted) {
            try {
              await client.advanceSubscription(
                stream,
                def.name,
                workerId,
                advanceTo,
              );
            } catch (err) {
              if (isLeaseLoss(err)) {
                markAborted(err as Error);
                break;
              }
              safeOnError(err as Error);
              // Don't advance lastPosition; redeliver next iteration.
            }
          }
        }
      } finally {
        // Stop heartbeat before releasing.
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

  // Kick off the loop. We deliberately do not await it; close() returns
  // the same promise on every call.
  const loopPromise = loop();
  // Swallow unhandled rejection: stopped never rejects per §11.9.
  loopPromise.catch(() => {
    /* unreachable: loop's try/finally always resolves stopped */
  });

  return {
    stopped,
    close(): Promise<void> {
      if (closePromise) return closePromise;
      closing = true;
      // Abort fires immediately (§11.1 / §11.9). The SDK still awaits
      // any in-flight handler/ack via the loop's natural exit path.
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

function noopOnError(_err: Error): void {
  /* no-op default */
}

function isLeaseLoss(err: unknown): err is SubscriptionLeaseLost | SubscriptionNotFound {
  return (
    err instanceof SubscriptionLeaseLost || err instanceof SubscriptionNotFound
  );
}
