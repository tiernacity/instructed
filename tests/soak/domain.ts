/**
 * Domain model for the soak workload — deliberately minimal.
 *
 * The shape mirrors the composed-concurrency tests in
 * `sdks/typescript/test/concurrent.test.ts` (TODO #3a) so a soak run
 * is "the 3a scenarios, but for longer, with more contention, and
 * with failure injection". Keep it simple: a Counter aggregate plus
 * a Forwarder process manager.
 *
 * **Counter aggregate.** One stream per account; commands are
 * `add{n}`; events are `Added{n}`; folded state is `{ value }`. Used
 * both as the direct target of dispatcher tasks (exercises OCC) and
 * as the downstream target of the PM (exercises cross-stream
 * coordination).
 *
 * **Forwarder PM.** Subscribed to `$all`. Routes `Triggered{n,target}`
 * events to a per-target process instance (PM-F: partitionKey is
 * `target`); the handler dispatches `add{n}` to the target account
 * stream. Non-Triggered events route to `"ignore"`, so the routing
 * worker never inserts a work item for them -- the SUB-A substrate
 * subsumes the legacy ignored-event ack coalescing (TODO #10 /
 * ex-ML-0005) at the routing layer: no work item ever exists for an
 * ignored event, so there's nothing to ack.
 *
 * The PM is long-lived: it never returns `{ complete: true }` because
 * the harness wants the snapshot to survive so the final
 * PM-FORWARD-TOTAL invariant can sum `forwarded` across every
 * partition's snapshot.
 */

import type {
  AggregateDefinition,
  DispatchedCommand,
  DomainEvent,
  PmDefinition,
  RecordedEvent,
  RoutingFn,
} from "../../sdks/typescript/src/index.ts";

// ---------------------------------------------------------------------------
// Counter aggregate
// ---------------------------------------------------------------------------

export interface CounterState {
  value: number;
}

export type CounterCommand = { kind: "add"; n: number };

export interface CounterEvent extends DomainEvent {
  type: "Added";
  data: { n: number };
}

export function counter(): AggregateDefinition<
  CounterState,
  CounterCommand,
  CounterEvent
> {
  return {
    type: "Counter",
    initialState: () => ({ value: 0 }),
    execute(_state, command) {
      return { event_type: "Added", data: { n: command.n } };
    },
    apply(state, event) {
      if (event.type === "Added") {
        return { value: state.value + event.data.n };
      }
      return state;
    },
  };
}

// ---------------------------------------------------------------------------
// Forwarder process manager
// ---------------------------------------------------------------------------

export interface ForwarderState {
  forwarded: number;
}

export interface TriggeredData {
  n: number;
  /** Account stream UUID to forward into. */
  target: string;
}

/**
 * Light-touch counters wired into the Forwarder route + handle
 * functions. Counter-based (not log-based) so a 60s run with
 * thousands of events costs ~zero stdout noise; the harness's final
 * report prints the totals and compares them against the
 * triggers_total / forwarded_total numbers to localise any loss.
 *
 * Reading the bucket meaning under SUB-A:
 *
 *   routeCalls['Triggered']  - times the *routing worker* invoked our
 *                              routeFn with a Triggered event. Should
 *                              be >= triggers_total (re-routing on
 *                              routing-worker restart is idempotent
 *                              at the work-item PK; the routeFn still
 *                              fires per re-read event).
 *   handleCalls              - times the *processing worker* entered
 *                              the PM's handle. May exceed routeCalls
 *                              under SUB-B retry-in or lease takeover
 *                              (handle re-runs per attempt).
 *   handleReturns            - times handle returned normally.
 *                              handleCalls - handleReturns = handler
 *                              throws.
 *
 * Plus a SQL query at the end that counts Added events whose
 * causation_id is a Triggered event (= PM dispatches that
 * committed to an aggregate stream). All four numbers, side by
 * side with triggers_total and forwarded_total, pin the bug to a
 * single SDK code path.
 */
export interface ForwarderCounters {
  /** Times the SDK invoked our routeFn, by event_type. */
  readonly routeCalls: Map<string, number>;
  /** Times the processing worker entered handle. */
  handleCalls: number;
  /** Times handle returned normally (not threw / not aborted mid-body). */
  handleReturns: number;
}

export function newForwarderCounters(): ForwarderCounters {
  return { routeCalls: new Map(), handleCalls: 0, handleReturns: 0 };
}

function bump(map: Map<string, number>, key: string): void {
  map.set(key, (map.get(key) ?? 0) + 1);
}

/**
 * SUB-A routing function for the Forwarder. Non-Triggered events
 * route to `"ignore"`; the routing worker writes no work item for
 * them. Triggered events route to the partition keyed by `target`.
 *
 * Counter side effect: `routeCalls['Triggered']` increments on every
 * Triggered event the routing worker sees. This runs in the routing
 * worker process; the routing-worker has at-most-one-active-worker
 * semantics per subscription, so the counter is single-writer modulo
 * routing-worker handover.
 */
export function forwarderRouteFn(
  counters?: ForwarderCounters,
): RoutingFn {
  return (event: RecordedEvent) => {
    if (event.event_type !== "Triggered") return "ignore";
    if (counters) bump(counters.routeCalls, event.event_type);
    const data = event.data as TriggeredData;
    return { partitionKey: data.target };
  };
}

/**
 * SUB-A PM definition for the Forwarder (worker-level shape;
 * consumed by `startPmWorker`).
 *
 * `apply`: pure fold over Triggered events; bumps the `forwarded`
 *   counter. Runs during PM-state rebuild (PM-C) and on the claimed
 *   event before `handle`.
 * `handle`: dispatches one `add{n}` command per Triggered event to
 *   the target account stream. Never returns `{ complete: true }` --
 *   the harness needs every partition's snapshot to persist so the
 *   final PM-FORWARD-TOTAL check can sum across them.
 */
export function forwarderPmDefinition(
  name: string,
  counters?: ForwarderCounters,
): PmDefinition<ForwarderState> {
  const Counter = counter();
  return {
    name,
    stream: "$all",
    initialState: () => ({ forwarded: 0 }),
    apply(state, event) {
      // PM-C: pure fold. Only Triggered events touch our state; the
      // routing fn ignores everything else, so in practice this only
      // sees Triggered, but defend in depth.
      if (event.event_type !== "Triggered") return state;
      return { forwarded: state.forwarded + 1 };
    },
    async handle(_state, event) {
      if (counters) counters.handleCalls += 1;
      const data = event.data as TriggeredData;
      const commands: DispatchedCommand[] = [
        {
          streamUuid: data.target,
          aggregate: Counter,
          command: { kind: "add", n: data.n } as CounterCommand,
        },
      ];
      if (counters) counters.handleReturns += 1;
      return { commands };
    },
  };
}
