/**
 * Routing-extension-point standard library: `PartitionBy` modes
 * translated to routing-layer `RoutingFn`s.
 *
 * The routing extension point follows the contract + standard
 * library + escape hatch pattern (see `sdks/porting-checklist.md`
 * §4.1).
 * The contract lives in `routing-worker.ts` (`RoutingFn`,
 * `RoutingDecision`). This file is the **standard library** — the
 * shipped fixed strategies for the common cases. A consumer who
 * needs something outside the cases below uses the escape hatch
 * (pass a raw `RoutingFn` to `startRoutingWorker`).
 *
 * The three modes cover the spectrum of parallelism:
 *
 *   - `sequential` — single partition; fully serial processing.
 *   - `per-event` — each event its own partition; fully parallel.
 *   - `per-key` — user-supplied key extraction; bounded parallelism
 *     and the general case.
 *
 * Audit (step-5 slice 1, 2026-05-27): the three modes were
 * reviewed against potential additions — `per-event-type`,
 * `hash-modulo-N`, routing-time filter — and none meet the bar to
 * ship. `per-event-type` is `per-key` with
 * `event => event.event_type`; `hash-modulo-N` is `per-key` with
 * `event => String(hash(key(event)) % n)`; routing-time filtering
 * requires `"ignore"`, which `PartitionBy` deliberately can't
 * produce (escape hatch instead). The standard library stays at
 * three modes.
 *
 * Lives in its own file (separate from `projection-worker.ts`) so
 * the file boundary matches the layer boundary: every export here
 * is L3, the projection-worker adapter is L2. The `instructed-sdk`
 * bare entry re-exports this module; `instructed-sdk/core` does
 * not (per [D-0027](../../../docs/decisions.md#d-0027) and PRJ-A).
 *
 * A consumer of the bare entry uses these helpers via
 * `Instructed.registerProjection({ partitionBy: ... })`. A consumer
 * of `instructed-sdk/core` writes a raw `RoutingFn` directly --
 * the translation is straightforward enough that having it as
 * sugar over a `RoutingFn` is the right shape (rather than a
 * separate registration call on the worker).
 */

import type { RoutingFn } from "./routing-worker.ts";
import type { RecordedEvent } from "./types.ts";

/**
 * Three-mode partitioning sugar over a routing-layer `RoutingFn`. None
 * of the modes can emit `"ignore"`; if you need to filter at routing
 * time, pass a raw `RoutingFn` instead. PRJ-A.
 */
export type PartitionBy<E = unknown> =
  | { kind: "sequential" }
  | { kind: "per-event" }
  | { kind: "per-key"; key: (event: RecordedEvent<E>) => string };

/** The synthetic partition key used by `{ kind: "sequential" }`. */
export const SEQUENTIAL_PARTITION_KEY = "_default";

/**
 * Translate a `PartitionBy` into a routing-layer `RoutingFn`. The
 * returned function is pure and never produces `"ignore"`.
 *
 *   sequential -> always `{ partitionKey: '_default' }`.
 *   per-event  -> `{ partitionKey: String(event.event_number) }`.
 *   per-key    -> `{ partitionKey: key(event) }`.
 *
 * The per-key user function may throw or return a non-string value;
 * those errors propagate to the routing worker, which surfaces them
 * via `onError` and stalls (SUB-A "no silent skip"). No clever
 * recovery here.
 */
export function routingFnForPartitionBy<E>(pb: PartitionBy<E>): RoutingFn<E> {
  switch (pb.kind) {
    case "sequential":
      return () => ({ partitionKey: SEQUENTIAL_PARTITION_KEY });
    case "per-event":
      return (event) => ({ partitionKey: String(event.event_number) });
    case "per-key": {
      const key = pb.key;
      return (event) => ({ partitionKey: key(event) });
    }
  }
}
