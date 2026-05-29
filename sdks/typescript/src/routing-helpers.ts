/**
 * L3 routing-function combinators.
 *
 * `RoutingFn` (defined in `routing-worker.ts`, L2) is a single
 * predicate-and-partition function over `RecordedEvent`. The L3
 * standard library here offers small composable wrappers around it
 * so applications don't have to hand-roll the most common shapes:
 *
 *   - {@link onlyTypes}: filter to a fixed set of event-type
 *     discriminators, delegating the partitioning decision to an
 *     inner `RoutingFn`. Replaces hand-written
 *     `const ALLOWED = new Set([...]); event => ALLOWED.has(...)`
 *     boilerplate.
 *
 * The contract layer (`RoutingFn`, `RoutingDecision`) stays at L2
 * so ports can write their own helpers; this module is the
 * idiomatic TS convenience.
 */

import type { RoutingFn } from "./workers/routing/index.ts";
import type { Event } from "./types/index.ts";

/**
 * Filter a `RoutingFn` to a fixed set of event types. Events whose
 * `type` is not in `types` are routed to `"ignore"`; matching events
 * are delegated to `inner`.
 *
 * The Set is constructed once at factory time; per-event cost is one
 * `Set.has` lookup.
 *
 * Typed against the application's event union `E`: `types` must
 * contain only discriminator literals drawn from `E["type"]`, and
 * `inner` is a `RoutingFn<E>` so the user's narrowing applies when
 * switching on `event.type`.
 *
 * @example
 *
 *     // Project balances for account events only; partition per stream.
 *     const balancesRouteFn = onlyTypes<AccountEvent>(
 *       [AccountOpened, AccountDepositedTo, AccountWithdrawnFrom],
 *       (event) => ({ partitionKey: event.stream_uuid }),
 *     );
 */
export function onlyTypes<E extends Event>(
  types: ReadonlyArray<E["type"]>,
  inner: RoutingFn<E>,
): RoutingFn<E> {
  const allowed: ReadonlySet<E["type"]> = new Set(types);
  return (event) =>
    allowed.has(event.type as E["type"]) ? inner(event) : "ignore";
}
