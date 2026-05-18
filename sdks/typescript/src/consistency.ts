/**
 * Layer 4: consistency-on-dispatch wait.
 *
 * See docs/sdk-design.md §3 layer 4. For each named subscription,
 * polls `readSubscriptionPosition` until `lastSeen >= target`. The
 * target is:
 *
 *   - the highest `event_number` across `appended` for `$all`
 *     subscriptions, or
 *   - the matching `stream_version` for per-stream subscriptions
 *     (the subscription's `stream` is not `'$all'`).
 *
 * Throws {@link ConsistencyTimeout} on timeout (named
 * subscriptions whose cursors never reached the target are listed
 * in `missing`). No `:strong` shorthand (D-0010) — callers list the
 * subscriptions they want to wait on explicitly.
 */

import type { Client } from "./client.ts";
import { ConsistencyTimeout, InstructedError } from "./errors.ts";
import type { AppendedEvent } from "./types.ts";
import { sleep } from "./internal/sleep.ts";

export interface WaitForProjectionOptions {
  /** Poll interval in ms. Default 25. */
  pollInterval?: number;
  /** Total budget in ms before throwing. Default 5_000. */
  timeout?: number;
}

export interface SubscriptionRef {
  /** Stream the subscription is bound to. `$all` for cross-stream. */
  stream: string;
  /** Subscription name. */
  name: string;
}

export const DEFAULT_WAIT_POLL_INTERVAL_MS = 25;
export const DEFAULT_WAIT_TIMEOUT_MS = 5_000;

/**
 * Wait for every named subscription to have advanced past the
 * appended events.
 *
 * `appended` is the array returned by `appendToStream` (or a single
 * row from it). For `$all` subscriptions the target is the highest
 * `event_number` across `appended`; for a per-stream subscription
 * it is the highest `stream_version`. Both are derived from the
 * same `AppendedEvent` rows.
 */
export async function waitForProjection(
  client: Client,
  appended: AppendedEvent | AppendedEvent[],
  subscriptions: SubscriptionRef[],
  opts: WaitForProjectionOptions = {},
): Promise<void> {
  const rows = Array.isArray(appended) ? appended : [appended];
  if (rows.length === 0) return;
  if (subscriptions.length === 0) return;

  const pollInterval = opts.pollInterval ?? DEFAULT_WAIT_POLL_INTERVAL_MS;
  const timeout = opts.timeout ?? DEFAULT_WAIT_TIMEOUT_MS;

  const maxEventNumber = rows.reduce(
    (m, r) => (r.event_number > m ? r.event_number : m),
    rows[0].event_number,
  );
  const maxStreamVersion = rows.reduce(
    (m, r) => (r.stream_version > m ? r.stream_version : m),
    rows[0].stream_version,
  );

  const deadline = Date.now() + timeout;
  const remaining = new Map<string, SubscriptionRef>();
  for (const s of subscriptions) {
    remaining.set(refKey(s), s);
  }

  while (remaining.size > 0) {
    for (const [k, sub] of [...remaining]) {
      const target = sub.stream === "$all" ? maxEventNumber : maxStreamVersion;
      let lastSeen: bigint;
      try {
        const r = await client.readSubscriptionPosition(sub.stream, sub.name);
        lastSeen = r.lastSeen;
      } catch (err) {
        // A subscription that doesn't exist yet is "not caught up";
        // keep polling until it does (a worker is starting up) or
        // the timeout fires. Other contract errors surface
        // immediately.
        if (
          err instanceof InstructedError &&
          (err as { code?: string }).code === "IS020"
        ) {
          lastSeen = -1n;
        } else {
          throw err;
        }
      }
      if (lastSeen >= target) remaining.delete(k);
    }
    if (remaining.size === 0) return;

    const now = Date.now();
    if (now >= deadline) {
      throw new ConsistencyTimeout(
        `waitForProjection: timed out after ${timeout}ms waiting for ${remaining.size} subscription(s)`,
        {
          waitedMs: timeout,
          missing: [...remaining.values()].map(refKey),
        },
      );
    }
    const sleepMs = Math.min(pollInterval, deadline - now);
    await sleep(sleepMs);
  }
}

function refKey(s: SubscriptionRef): string {
  return `${s.stream}::${s.name}`;
}
