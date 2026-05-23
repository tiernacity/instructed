/**
 * Worker farm + failure injection.
 *
 * A "slot" is a logical projector or process-manager position. The
 * harness keeps each slot live by respawning whenever the previous
 * worker exits. Multiple slots can target the same subscription —
 * only one wins the lease at a time, the others see `already_claimed`
 * and back off (the soak harness exercises lease rebalancing this
 * way).
 *
 * Failure injection has two flavours:
 *
 *   1. **Respawn.** Periodically close a random worker; the slot's
 *      keep-alive recreates it immediately. Models "process died".
 *   2. **Lease theft.** Periodically run an UPDATE that backdates a
 *      random subscription's `claim_expires_at`, forcing the holder's
 *      next heartbeat to detect lease loss (`IS022`) and another slot
 *      to take over. Models the rebalancing path under churn.
 */

import pg from "pg";
import {
  Client,
  startProcessManager,
  startProjection,
  type ProcessManagerDefinition,
  type ProjectionDefinition,
  type RunningWorker,
} from "../../sdks/typescript/src/index.ts";

export interface SlotHandle {
  /** Stable label for logs / metrics. */
  readonly label: string;
  /** Stop the slot (no respawn). Idempotent. */
  close(): Promise<void>;
  /** Force the underlying worker to exit; the slot will respawn. */
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
      await current.close();
      await loop;
    },
    async bounce() {
      // Closing the current worker triggers the keep-alive to respawn.
      await current.close();
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
  def: ProjectionDefinition;
  slotLabel: string;
  leaseSeconds: number;
  pollInterval: number;
  metrics: SoakMetrics;
}

export function projectorSlot(opts: ProjectorSlotOptions): SlotInternal {
  const stream = opts.def.stream ?? "$all";
  const handle = keepAlive(
    opts.slotLabel,
    ({ workerId, client }) =>
      startProjection(client, opts.def, {
        workerId,
        leaseSeconds: opts.leaseSeconds,
        pollInterval: opts.pollInterval,
        heartbeatInterval: Math.max(500, (opts.leaseSeconds * 1000) / 3),
        onError: () => {
          // Swallow per-handler errors here; the soak harness's
          // invariant checks catch real damage. Could surface to a
          // log if --verbose is added later.
        },
      }),
    opts.client,
    opts.slotLabel,
    opts.metrics,
  );
  return {
    label: opts.slotLabel,
    subscriptionName: opts.def.name,
    stream,
    close: handle.close,
    bounce: handle.bounce,
  };
}

export interface PmSlotOptions {
  client: Client;
  dispatchClient: Client;
  def: ProcessManagerDefinition<any>;
  slotLabel: string;
  leaseSeconds: number;
  pollInterval: number;
  metrics: SoakMetrics;
}

export function pmSlot(opts: PmSlotOptions): SlotInternal {
  const stream = opts.def.stream ?? "$all";
  const handle = keepAlive(
    opts.slotLabel,
    ({ workerId, client }) =>
      startProcessManager(client, opts.dispatchClient, opts.def, {
        workerId,
        leaseSeconds: opts.leaseSeconds,
        pollInterval: opts.pollInterval,
        heartbeatInterval: Math.max(500, (opts.leaseSeconds * 1000) / 3),
        onError: () => {
          // see projectorSlot
        },
      }),
    opts.client,
    opts.slotLabel,
    opts.metrics,
  );
  return {
    label: opts.slotLabel,
    subscriptionName: opts.def.name,
    stream,
    close: handle.close,
    bounce: handle.bounce,
  };
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
