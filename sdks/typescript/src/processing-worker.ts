/**
 * SUB-A processing worker (slice 5).
 *
 * Kind-agnostic poll loop. One *or more* processing workers per
 * subscription compete for work items via `claim_work_item`; each
 * claim is fenced by a per-work-item lease that the worker renews via
 * `extend_work_item_claim` for the duration of `handle`. The
 * kind-specific terminal-success step lives in `complete` (supplied
 * by the caller); slice 6 / slice 7 ship the projection and PM
 * adapters on top of this module.
 *
 * Per-partition ordering, parallel-across-partitions throughput, the
 * lease-takeover branch, and the `failed`-row-blocks-its-partition
 * contract are all enforced by the SQL claim query (slice 2); this
 * module is purely the worker's side of the loop and the SUB-B
 * error-policy plumbing.
 *
 * Error policy (SUB-B core surface):
 *   - The handler is invoked inside a retry loop driven by an
 *     `ErrorPolicy` hook. The hook returns
 *       { kind: 'retry-in', delayMs }   -- sleep, then re-run the
 *                                          handler against the same
 *                                          claimed item.
 *       { kind: 'stop' }                -- exit the worker; leave the
 *                                          item claimed; the lease
 *                                          will expire and another
 *                                          worker may take it over.
 *   - Default policy: exponential backoff with base 100ms doubling
 *     each attempt, capped at 30s, retry forever. This is today's
 *     observable behaviour (per SUB-B "What lands" item 2) so existing
 *     consumers see no behaviour change.
 *   - Convenience wrappers (`retryUpTo`, `quarantineAfter`, ...) are
 *     out of scope here; they ship later and may map to
 *     `fail_work_item`. The slice-5 default never transitions a
 *     work item to `'failed'`.
 *
 * Not yet exported from src/index.ts; the layer-5 facade wires this
 * into `Instructed.registerProjection` / `registerProcessManager` in
 * slice 9. Tests import this module directly.
 */

import type { Client } from "./client.ts";
import {
  SubscriptionLeaseLost,
  SubscriptionNotFound,
  WorkItemLeaseLost,
} from "./errors.ts";
import type { RecordedEvent } from "./types.ts";
import type { RunningWorker } from "./internal/running-worker.ts";
export type { RunningWorker };
import { defaultWorkerId } from "./internal/worker-id.ts";
import { sleep } from "./internal/sleep.ts";

// ============================================================================
// Public surface
// ============================================================================

export interface ProcessingHandlerContext {
  workerId: string;
  partitionKey: string;
  eventNumber: bigint;
  /** 1-indexed retry attempt counter. Reset per work item. */
  attempt: number;
  /** Aborted on graceful shutdown and on lease loss. */
  signal: AbortSignal;
}

export type ProcessingHandler<E = unknown> = (
  event: RecordedEvent<E>,
  ctx: ProcessingHandlerContext,
) => Promise<void>;

/**
 * Kind-specific terminal-success step (slice 6: projection DELETE;
 * slice 7: PM update-to-done + snapshot upsert).
 */
export type ProcessingCompleter<E = unknown> = (
  event: RecordedEvent<E>,
  ctx: ProcessingHandlerContext,
) => Promise<void>;

export type ErrorPolicyDecision =
  | { kind: "retry-in"; delayMs: number }
  | { kind: "stop" };

export interface ErrorPolicyContext {
  workerId: string;
  partitionKey: string;
  eventNumber: bigint;
  /** 1-indexed: the attempt that just failed. */
  attempt: number;
}

export type ErrorPolicy = (
  err: unknown,
  ctx: ErrorPolicyContext,
) => ErrorPolicyDecision | Promise<ErrorPolicyDecision>;

export interface ProcessingWorkerDefinition<E = unknown> {
  /** Subscription name (must match the routing worker for the same sub). */
  name: string;
  /** Source stream; default `$all`. */
  stream?: string;
  handle: ProcessingHandler<E>;
  complete: ProcessingCompleter<E>;
  errorPolicy?: ErrorPolicy;
}

export interface ProcessingWorkerOptions {
  workerId?: string;
  /** Lease duration in seconds. Default 30. */
  leaseSeconds?: number;
  /** Heartbeat tick in ms. Default = `leaseSeconds * 1000 / 3`. */
  heartbeatInterval?: number;
  /** Idle poll interval in ms. Default 200. */
  pollInterval?: number;
  /** Surfaces handler errors, lease loss, and other lifecycle events. */
  onError?: (err: Error) => void;
}

export const DEFAULT_PROCESSING_LEASE_SECONDS = 30;
export const DEFAULT_PROCESSING_POLL_INTERVAL_MS = 200;

/**
 * SUB-B default error policy: exponential backoff with base 100ms,
 * doubling each attempt, capped at 30s, retry forever. Today's
 * observable behaviour (per SUB-B "What lands" item 2).
 */
export const DEFAULT_ERROR_POLICY: ErrorPolicy = (_err, ctx) => {
  const base = 100;
  const cap = 30_000;
  // attempt is 1-indexed; first retry waits base; clamp the exponent
  // so we never compute a huge intermediate even if attempt is large.
  const exp = Math.min(ctx.attempt - 1, 20);
  return { kind: "retry-in", delayMs: Math.min(cap, base * 2 ** exp) };
};

// ============================================================================
// Implementation
// ============================================================================

/** Single retry delay on a transient non-IS030 heartbeat error. */
const HEARTBEAT_RETRY_DELAY_MS = 100;

export function startProcessingWorker<E = unknown>(
  client: Client,
  def: ProcessingWorkerDefinition<E>,
  opts: ProcessingWorkerOptions = {},
): RunningWorker {
  const stream = def.stream ?? "$all";
  const workerId = opts.workerId ?? defaultWorkerId();
  const leaseSeconds = opts.leaseSeconds ?? DEFAULT_PROCESSING_LEASE_SECONDS;
  const heartbeatInterval =
    opts.heartbeatInterval ?? Math.max(1_000, (leaseSeconds * 1000) / 3);
  const pollInterval = opts.pollInterval ?? DEFAULT_PROCESSING_POLL_INTERVAL_MS;
  const errorPolicy = def.errorPolicy ?? DEFAULT_ERROR_POLICY;
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
   * Heartbeat loop scoped to a single claimed work item. Exits when:
   *  - the outer signal aborts (close / lease loss elsewhere), or
   *  - the heartbeat itself sees IS030 (we lost the lease).
   *
   * On IS030 we call `markAborted` so the in-flight handler's signal
   * fires; the handler's next `await sleep(_, signal)` (or its own
   * AbortSignal-honouring code) returns promptly.
   */
  async function heartbeatForItem(
    partitionKey: string,
    eventNumber: bigint,
    itemSignal: AbortSignal,
  ): Promise<void> {
    while (!closing && !aborted && !itemSignal.aborted) {
      await sleep(heartbeatInterval, itemSignal);
      if (closing || aborted || itemSignal.aborted) return;
      try {
        await client.extendWorkItemClaim(
          stream,
          def.name,
          workerId,
          partitionKey,
          eventNumber,
          leaseSeconds,
        );
        continue;
      } catch (err) {
        if (err instanceof WorkItemLeaseLost) {
          markAborted(err);
          return;
        }
        // One short retry on a transient (e.g. connection blip).
        await sleep(HEARTBEAT_RETRY_DELAY_MS, itemSignal);
        if (closing || aborted || itemSignal.aborted) return;
        try {
          await client.extendWorkItemClaim(
            stream,
            def.name,
            workerId,
            partitionKey,
            eventNumber,
            leaseSeconds,
          );
          continue;
        } catch (err2) {
          if (!(err2 instanceof WorkItemLeaseLost)) safeOnError(err2 as Error);
          markAborted(err2 as Error);
          return;
        }
      }
    }
  }

  /**
   * Run handle for one claimed item, honouring the error-policy hook.
   * Returns true on success, false if the worker is stopping (policy
   * said `stop`, or close/abort fired). The work item's lease is held
   * across attempts via the heartbeat; the row stays `claimed`.
   */
  async function runHandlerWithPolicy(
    event: RecordedEvent<E>,
    partitionKey: string,
    eventNumber: bigint,
  ): Promise<boolean> {
    let attempt = 1;
    while (!closing && !aborted) {
      const ctx: ProcessingHandlerContext = {
        workerId,
        partitionKey,
        eventNumber,
        attempt,
        signal,
      };
      try {
        await def.handle(event, ctx);
        return true;
      } catch (err) {
        // Surface the handler error so applications can log/observe
        // every failed attempt, not just the final outcome. Matches
        // the existing projection worker behaviour.
        safeOnError(asError(err, `handler threw on event ${eventNumber}`));
        let decision: ErrorPolicyDecision;
        try {
          decision = await errorPolicy(err, {
            workerId,
            partitionKey,
            eventNumber,
            attempt,
          });
        } catch (policyErr) {
          // A throwing error policy is itself a `stop` signal: we have
          // no defensible way to decide. Surface and exit.
          safeOnError(
            asError(policyErr, "errorPolicy itself threw; stopping worker"),
          );
          markAborted(asError(policyErr, "errorPolicy threw"));
          return false;
        }
        if (decision.kind === "stop") {
          // SUB-B `stop`: worker exits; item stays `claimed`; the
          // lease will expire and another worker may pick it up. We
          // do NOT call fail_work_item -- that's reserved for the
          // (future) `quarantineAfter` convenience wrapper.
          markAborted(asError(err, "errorPolicy returned 'stop'"));
          return false;
        }
        // retry-in
        await sleep(decision.delayMs, signal);
        attempt += 1;
      }
    }
    return false;
  }

  async function processOneItem(claim: {
    partitionKey: string;
    eventNumber: bigint;
  }): Promise<void> {
    // Fetch the event payload by primary-key lookup on $all. The
    // worker's view of the payload is MVCC; the event is immutable.
    let event: RecordedEvent<E> | null;
    try {
      const rows = await client.readAll<E>(claim.eventNumber, 1);
      event =
        rows.length > 0 && rows[0].event_number === claim.eventNumber
          ? rows[0]
          : null;
    } catch (err) {
      safeOnError(asError(err, "failed to read event payload"));
      return;
    }
    if (event === null) {
      safeOnError(
        new Error(
          `processing worker: event ${claim.eventNumber} not found in $all (race with rebuild?)`,
        ),
      );
      return;
    }

    // Per-item heartbeat. Its own AbortController fires when we're
    // done with this item so the heartbeat exits without affecting
    // the worker-wide signal.
    const itemAc = new AbortController();
    const hb = heartbeatForItem(
      claim.partitionKey,
      claim.eventNumber,
      itemAc.signal,
    ).catch(() => {
      /* heartbeat reports via markAborted / onError */
    });

    try {
      const ok = await runHandlerWithPolicy(
        event,
        claim.partitionKey,
        claim.eventNumber,
      );
      if (!ok) return;
      // Terminal success step. Kind-specific. On IS030 the lease was
      // taken over between handle and complete -- surface and stop;
      // the takeover worker (or a future redelivery) will handle it.
      try {
        await def.complete(event, {
          workerId,
          partitionKey: claim.partitionKey,
          eventNumber: claim.eventNumber,
          attempt: 1,
          signal,
        });
      } catch (err) {
        if (err instanceof WorkItemLeaseLost) {
          markAborted(err);
          return;
        }
        // Other complete errors are transient SDK problems (network
        // blip, etc.). Surface and stop -- redoing the handler
        // without redoing complete is unsafe; the lease expiry will
        // redeliver the whole item to another worker.
        safeOnError(asError(err, "complete threw"));
        markAborted(asError(err, "complete failed"));
      }
    } finally {
      itemAc.abort();
      await hb;
    }
  }

  async function loop(): Promise<void> {
    try {
      while (!closing && !aborted) {
        let claim: Awaited<ReturnType<Client["claimWorkItem"]>>;
        try {
          claim = await client.claimWorkItem(
            stream,
            def.name,
            workerId,
            leaseSeconds,
          );
        } catch (err) {
          if (err instanceof SubscriptionNotFound) {
            // Subscription doesn't exist yet (routing worker hasn't
            // created it). Treat as empty queue and try again.
            await sleep(pollInterval, signal);
            continue;
          }
          safeOnError(asError(err, "claim_work_item failed"));
          await sleep(pollInterval, signal);
          continue;
        }
        if (claim === null) {
          await sleep(pollInterval, signal);
          continue;
        }
        if (claim.wasTakeover) {
          // Informational; matches the design note. Surface only via
          // a non-fatal Error so users can log it if they want.
          safeOnError(
            new Error(
              `processing worker: took over work item ${claim.partitionKey}/${claim.eventNumber} from ${claim.priorClaimedBy}`,
            ),
          );
        }
        await processOneItem({
          partitionKey: claim.partitionKey,
          eventNumber: claim.eventNumber,
        });
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

function asError(err: unknown, prefix: string): Error {
  if (err instanceof Error) {
    return new Error(`${prefix}: ${err.message}`, { cause: err });
  }
  return new Error(`${prefix}: ${String(err)}`);
}

// Marker re-export to satisfy lint when SubscriptionLeaseLost is only
// referenced via instanceof in user-facing layers (kept for slice-9
// integration; the processing worker doesn't surface it directly).
export type { SubscriptionLeaseLost };
