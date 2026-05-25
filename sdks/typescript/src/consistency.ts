/**
 * Layer 4: consistency-on-dispatch wait (SUB-A slice 8).
 *
 * For each named subscription, polls the SUB-A catch-up predicate
 * (`instructed.is_subscription_caught_up`, slice 2) until it returns
 * true or the timeout expires. The predicate's two conjuncts are both
 * enforced server-side:
 *
 *   1. `subscriptions.last_seen >= target` (the routing cursor has
 *      reached the appended events), AND
 *   2. no `subscription_work_items` row for the subscription with
 *      `event_number <= target` is still in a non-terminal state
 *      (`pending` / `claimed` / `failed`).
 *
 * Together, this means: every event the caller cares about has been
 * routed AND every routed work-item for that range has been
 * terminally handled (DELETEd for projections, UPDATEd to `done`
 * for PMs).
 *
 * The race-safety property at the start (a caller that appends event
 * N and immediately calls `waitForProjection(S, N)` must not observe
 * a spurious "caught-up" before routing has actually reached N) is
 * load-bearing on the routing worker's atomic `route_batch` --
 * cursor advance + work-item INSERTs commit in one transaction
 * (slice 4 invariant). Without that, the predicate could spuriously
 * return true. The slice-8 implementation simply trusts the
 * server-side predicate; race-safety is owned by the routing layer.
 *
 * The legacy stream_version-as-target behaviour for per-stream
 * subscriptions (which targeted the per-stream version under the
 * pre-SUB-A single-cursor model) is gone: under SUB-A all work
 * items carry the global `event_number`, the routing worker
 * (slice 4) advances `subscriptions.last_seen` using
 * `event.event_number` for both `$all` and per-stream sources, and
 * the predicate compares in `event_number` space throughout. The
 * `AppendedEvent` rows carry both numbers; the same wall-clock
 * moment is reached either way.
 *
 * Throws {@link ConsistencyTimeout} on timeout (named subscriptions
 * whose predicate never returned true are listed in `missing`).
 *
 * Out of scope here: the cross-stream guard (CON-B; a caller waiting
 * on a projection whose source stream is different from the one just
 * appended to can today silently get back a spurious "caught-up"
 * because the appended events were never routed to that
 * subscription). The guard ships separately.
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
 * Wait for every named subscription to be caught up past the appended
 * events. "Caught up" means the SUB-A catch-up predicate is true
 * server-side -- routing cursor has reached the target AND no
 * in-flight work-items remain at or below it.
 *
 * `appended` is the array returned by `appendToStream` (or a single
 * row from it). The target is the highest `event_number` across the
 * rows, applied uniformly to `$all` and per-stream subscriptions.
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

  const target = rows.reduce(
    (m, r) => (r.event_number > m ? r.event_number : m),
    rows[0].event_number,
  );

  const deadline = Date.now() + timeout;
  const remaining = new Map<string, SubscriptionRef>();
  for (const s of subscriptions) {
    remaining.set(refKey(s), s);
  }

  while (remaining.size > 0) {
    for (const [k, sub] of [...remaining]) {
      let caughtUp = false;
      try {
        caughtUp = await client.isSubscriptionCaughtUp(
          sub.stream,
          sub.name,
          target,
        );
      } catch (err) {
        // A subscription that doesn't exist yet (no routing worker
        // has created it) is "not caught up"; keep polling until it
        // does, or the timeout fires. Other contract errors surface
        // immediately.
        if (
          err instanceof InstructedError &&
          (err as { code?: string }).code === "IS020"
        ) {
          caughtUp = false;
        } else {
          throw err;
        }
      }
      if (caughtUp) remaining.delete(k);
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
