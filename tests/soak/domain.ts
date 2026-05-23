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
 * events to a per-target process instance; the handler dispatches
 * `add{n}` to the target account stream. Lets us measure ignored-
 * event ack overhead (ML-0005) — the PM acks every `Added` event it
 * doesn't route.
 */

import type {
  AggregateDefinition,
  DispatchedCommand,
  DomainEvent,
  ProcessManagerDefinition,
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
 * The buckets answer one question per row in the final report:
 *
 *   routeCalls['Triggered']  - did the SDK reach our route fn?
 *   handleCalls              - did the SDK reach our handle fn?
 *   handleReturns            - did handle complete without throwing?
 *
 * Plus a SQL query at the end that counts Added events whose
 * causation_id is a Triggered event (= PM dispatches that
 * committed to the aggregate stream). All four numbers, side by
 * side with triggers_total and forwarded_total, pin the bug to a
 * single SDK code path.
 */
export interface ForwarderCounters {
  /** Times the SDK invoked our route fn, by event_type. */
  readonly routeCalls: Map<string, number>;
  /** Times handle was entered. */
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
 * Build a Forwarder PM with a deterministic name. One PM type
 * subscribes to `$all` and routes by `target`, so multiple process
 * instances coexist on the same subscription.
 *
 * When `counters` is provided, the route and handle hooks update it
 * (in-process JS numbers; no I/O).
 */
export function forwarder(
  name: string,
  counters?: ForwarderCounters,
): ProcessManagerDefinition<ForwarderState> {
  const Counter = counter();
  return {
    name,
    stream: "$all",
    routes: {
      Triggered: (event) => {
        if (counters) bump(counters.routeCalls, event.event_type);
        const data = event.data as TriggeredData;
        return { kind: "continue", processId: data.target };
      },
    },
    initialState: () => ({ forwarded: 0 }),
    async handle(state, event) {
      if (counters) counters.handleCalls += 1;
      const data = event.data as TriggeredData;
      const commands: DispatchedCommand[] = [
        {
          streamUuid: data.target,
          aggregate: Counter,
          command: { kind: "add", n: data.n } as CounterCommand,
        },
      ];
      const result = {
        state: { forwarded: state.forwarded + 1 },
        commands,
      };
      if (counters) counters.handleReturns += 1;
      return result;
    },
  };
}
