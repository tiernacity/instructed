/**
 * Worker farm + failure injection (SUB-A).
 *
 * A "slot" is a logical projector or process-manager position. Under
 * SUB-A each slot spawns a *pair* of workers: a routing worker
 * (single-active per subscription -- competes for
 * `subscriptions.claim_*` and converts events into
 * `subscription_work_items` rows) and a processing worker (claims
 * work items and runs the handler -- multiple may be active
 * concurrently per subscription, distributed by `FOR UPDATE SKIP
 * LOCKED`).
 *
 * Multiple slots therefore exercise two distinct kinds of
 * concurrency at once:
 *
 *   - **Routing-side rebalancing.** Only one routing worker per
 *     subscription holds the lease at any single instant
 *     (INV-SUB-P-010). Under D-0025 the lease is claimed and
 *     released per batch, so the active worker rotates per batch
 *     across whichever slots are polling — spinning up extra slots
 *     immediately shares routing load rather than idling on
 *     `already_claimed` until the current holder dies. Slot
 *     respawn and lease theft both cause a routing handover; the
 *     latter manifests as IS022 on the in-flight route_batch and
 *     is recovered by the next claim, not by aborting the worker.
 *   - **Processing-side parallelism.** All processing workers per
 *     subscription run simultaneously, racing for work items via
 *     `claim_work_item`. The per-partition predicate keeps the
 *     within-partition ordering invariant.
 *
 * Failure injection has two flavours, unchanged in observable
 * intent from the pre-SUB-A harness:
 *
 *   1. **Respawn.** Periodically close a random slot (both workers);
 *      the slot's keep-alive recreates the pair immediately. Models
 *      "process died". On the routing side, the next idle routing
 *      worker steps up; on the processing side, in-flight items
 *      have their per-item lease expire and are taken over by other
 *      processing workers (or the respawned one).
 *   2. **Lease theft.** Periodically run an UPDATE that backdates a
 *      random subscription's `claim_expires_at`, simulating an
 *      out-of-band lease loss. Under D-0025 the routing worker no
 *      longer heartbeats; lease theft is observable to it only when
 *      it next calls `route_batch` (IS022, recoverable: drop batch
 *      and re-claim) or `claim_subscription` (sees `already_claimed`
 *      if a thief grabbed the now-expired lease). Either way the
 *      worker keeps polling. Processing workers are unaffected
 *      (they hold per-item leases on
 *      `subscription_work_items.lease_expires_at`, not on
 *      `subscriptions`).
 */

import type pg from "pg";
import type {
  Client} from "../../sdks/typescript/src/index.ts";
import {
  startPmWorker,
  startProjectionWorker,
  startRoutingWorker,
  type PmDefinition,
  type ProjectionWorkerDefinition,
  type RoutingDefinition,
  type RunningWorker,
} from "../../sdks/typescript/src/index.ts";

export interface SlotHandle {
  /** Stable label for logs / metrics. */
  readonly label: string;
  /** Stop the slot (no respawn). Idempotent. */
  close(): Promise<void>;
  /** Force the underlying worker pair to exit; the slot will respawn. */
  bounce(): Promise<void>;
}

interface SlotInternal extends SlotHandle {
  readonly subscriptionName: string;
  /** Subscription stream identifier (for lease theft). */
  readonly stream: string;
}

interface SpawnArgs {
  workerId: string;
  client: Client;
}

/**
 * Compose two workers into one `RunningWorker`. Stopped resolves
 * once both have stopped; close stops both in parallel. Matches the
 * facade's composite shape (SUB-A slice 9).
 */
function compose(a: RunningWorker, b: RunningWorker): RunningWorker {
  return {
    stopped: Promise.all([a.stopped, b.stopped]).then(() => {}),
    stop: async () => {
      await Promise.all([a.stop(), b.stop()]);
    },
  };
}

/**
 * A keep-alive wrapper that re-creates `worker` whenever it exits,
 * until `close()` is called. The `spawn` callback returns a fresh
 * `RunningWorker`; the loop owns it.
 */
function keepAlive(
  label: string,
  spawn: (args: SpawnArgs) => RunningWorker,
  client: Client,
  baseWorkerId: string,
  metrics: SoakMetrics,
): { close(): Promise<void>; bounce(): Promise<void> } {
  let stopping = false;
  let current: RunningWorker;
  let respawns = 0;
  let startedAt = Date.now();

  // Minimum lifetime between respawns. A losing slot exits the
  // claim immediately on `already_claimed`; without throttling the
  // keep-alive busy-loops respawning. Match the SDK's idle-poll
  // ballpark.
  const MIN_LIFETIME_MS = 250;

  const start = (): RunningWorker => {
    const workerId = `${baseWorkerId}-r${respawns}`;
    startedAt = Date.now();
    return spawn({ workerId, client });
  };

  current = start();

  // Re-spawn loop runs in the background.
  const loop = (async () => {
    while (!stopping) {
      try {
        await current.stopped;
      } catch {
        // RunningWorker.stopped never rejects per its contract, but
        // be defensive.
      }
      if (stopping) return;
      const lifetime = Date.now() - startedAt;
      if (lifetime < MIN_LIFETIME_MS) {
        await new Promise((r) => setTimeout(r, MIN_LIFETIME_MS - lifetime));
        if (stopping) return;
      }
      respawns += 1;
      metrics.respawns += 1;
      current = start();
    }
  })();

  return {
    async close() {
      if (stopping) {
        await loop;
        return;
      }
      stopping = true;
      await current.stop();
      await loop;
    },
    async bounce() {
      // Stopping the current pair triggers the keep-alive to respawn.
      await current.stop();
    },
  };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export interface SoakMetrics {
  commandsAttempted: number;
  commandsCompleted: number;
  commandsFailed: number;
  triggersAppended: number;
  respawns: number;
  leaseThefts: number;
  leaseChanges: number;
  /** Sampled (head, lastSeen) per subscription over time. */
  lagSamples: Array<{
    t: number;
    subscription: string;
    head: bigint;
    lastSeen: bigint;
  }>;
  startedAt: number;
}

export function newMetrics(): SoakMetrics {
  return {
    commandsAttempted: 0,
    commandsCompleted: 0,
    commandsFailed: 0,
    triggersAppended: 0,
    respawns: 0,
    leaseThefts: 0,
    leaseChanges: 0,
    lagSamples: [],
    startedAt: Date.now(),
  };
}

export interface ProjectorSlotOptions {
  client: Client;
  /** Routing-worker side of the projection. */
  routingDef: RoutingDefinition;
  /** Processing-worker side of the projection. */
  projectionDef: ProjectionWorkerDefinition;
  slotLabel: string;
  leaseSeconds: number;
  pollInterval: number;
  metrics: SoakMetrics;
}

export function projectorSlot(opts: ProjectorSlotOptions): SlotInternal {
  const stream = opts.routingDef.stream ?? "$all";
  // Processing-side heartbeat is still in effect; routing-side
  // heartbeat was removed in D-0025 (per-batch claim/release).
  const heartbeatInterval = Math.max(500, (opts.leaseSeconds * 1000) / 3);
  const handle = keepAlive(
    opts.slotLabel,
    ({ workerId, client }) => {
      const router = startRoutingWorker(client, opts.routingDef, {
        workerId: `${workerId}-r`,
        leaseSeconds: opts.leaseSeconds,
        pollInterval: opts.pollInterval,
        onError: noop,
      });
      const proc = startProjectionWorker(client, opts.projectionDef, {
        workerId: `${workerId}-p`,
        leaseSeconds: opts.leaseSeconds,
        pollInterval: opts.pollInterval,
        heartbeatInterval,
        onError: noop,
      });
      return compose(router, proc);
    },
    opts.client,
    opts.slotLabel,
    opts.metrics,
  );
  return {
    label: opts.slotLabel,
    subscriptionName: opts.routingDef.name,
    stream,
    close: handle.close,
    bounce: handle.bounce,
  };
}

export interface PmSlotOptions {
  client: Client;
  /** Routing-worker side of the PM. */
  routingDef: RoutingDefinition;
  /** Processing-worker side of the PM (apply + handle). */
  pmDef: PmDefinition<any, any>;
  slotLabel: string;
  leaseSeconds: number;
  pollInterval: number;
  metrics: SoakMetrics;
}

export function pmSlot(opts: PmSlotOptions): SlotInternal {
  const stream = opts.routingDef.stream ?? "$all";
  // Processing-side heartbeat is still in effect; routing-side
  // heartbeat was removed in D-0025 (per-batch claim/release).
  const heartbeatInterval = Math.max(500, (opts.leaseSeconds * 1000) / 3);
  const handle = keepAlive(
    opts.slotLabel,
    ({ workerId, client }) => {
      const router = startRoutingWorker(client, opts.routingDef, {
        workerId: `${workerId}-r`,
        leaseSeconds: opts.leaseSeconds,
        pollInterval: opts.pollInterval,
        onError: noop,
      });
      const proc = startPmWorker(
        client,
        opts.pmDef,
        {
          workerId: `${workerId}-p`,
          leaseSeconds: opts.leaseSeconds,
          pollInterval: opts.pollInterval,
          heartbeatInterval,
          onError: noop,
        },
      );
      return compose(router, proc);
    },
    opts.client,
    opts.slotLabel,
    opts.metrics,
  );
  return {
    label: opts.slotLabel,
    subscriptionName: opts.routingDef.name,
    stream,
    close: handle.close,
    bounce: handle.bounce,
  };
}

function noop(_err: Error): void {
  // Swallow per-handler errors here; the soak harness's invariant
  // checks catch real damage. Could surface to a log if --verbose
  // is added later.
}

// ---------------------------------------------------------------------------
// Failure injection
// ---------------------------------------------------------------------------

/** Backdate a subscription's lease so the next heartbeat sees IS022. */
export async function stealLease(
  pool: pg.Pool,
  stream: string,
  subscriptionName: string,
): Promise<void> {
  await pool.query(
    `UPDATE instructed.subscriptions s
        SET claim_expires_at = now() - interval '1 second'
      WHERE s.stream_id = (
        SELECT stream_id FROM instructed.streams WHERE stream_uuid = $1
      ) AND s.subscription_name = $2`,
    [stream, subscriptionName],
  );
}

export function pickRandom<T>(xs: readonly T[]): T {
  if (xs.length === 0) {
    throw new Error("pickRandom: empty array");
  }
  return xs[Math.floor(Math.random() * xs.length)]!;
}

/** Repeatedly call `fn` until `signal.aborted`. */
export async function intervalLoop(
  fn: () => Promise<void>,
  intervalMs: number,
  signal: { aborted: boolean },
): Promise<void> {
  while (!signal.aborted) {
    await new Promise((r) => setTimeout(r, intervalMs));
    if (signal.aborted) return;
    try {
      await fn();
    } catch {
      // Ignore: the invariant checks are the source of truth.
    }
  }
}
