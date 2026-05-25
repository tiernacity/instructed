# Upgrade note — SUB-A + PM-F + PRJ-A

**Audience:** anyone with running code against a pre-SUB-A build of
the TS SDK.
**Scope:** the breaking-API-change matrix for the layer-5 facade
(`Instructed.registerProjection` / `registerProcessManager`) plus the
underlying worker-loop primitives. v1 has not shipped a release; this
note exists so that anyone who pulled `main` before SUB-A can migrate
in one pass.

## TL;DR

- The single-cursor subscription model is gone. Both projections and
  process managers now ride the work-queue substrate: a **routing
  worker** turns events into per-partition work items; a
  **processing worker** claims and runs them. The split is invisible
  at the facade.
- **Projections** (PRJ-A) no longer take a `selector`; routing-side
  filtering is expressed via a `routeFn` returning `"ignore"`. They
  gain a three-mode `partitionBy` sugar (`sequential`, `per-event`,
  `per-key`).
- **Process managers** (PM-F + PM-C) lose the Commanded-style
  directive enum (`start` / `continue` / `stop` / `false`). Routing
  reduces to `{ partitionKey } | "ignore"`; instance lifecycle moves
  into `handle`'s return value (`{ complete?: boolean }`). The legacy
  single-`handle` callback splits into `apply` (pure state fold) +
  `handle` (commands + lifecycle).

The behaviour set has not shrunk. Every prior shape has a
mechanically-derivable new shape; the table below shows each one.

---

## 1. Projection registration (PRJ-A)

### Before

```ts
const def: ProjectionDefinition = {
  name: "Balances",
  stream: "$all",
  selector: (e) =>
    e.event_type === "AccountOpened" ||
    e.event_type === "Deposited" ||
    e.event_type === "Withdrawn",
  async handle(event) {
    // ... fold into view
  },
};
app.registerProjection(def, { pollInterval: 50 });
```

### After

```ts
app.registerProjection(
  "Balances",
  {
    stream: "$all",
    // PRJ-A: the legacy `selector` is recovered by a routeFn that
    // returns "ignore" for unrelated events. `_default` is the
    // sequential-partition convention; routed events run serially
    // in event_number order.
    routeFn: (event) =>
      event.event_type === "AccountOpened" ||
      event.event_type === "Deposited" ||
      event.event_type === "Withdrawn"
        ? { partitionKey: "_default" }
        : "ignore",
    async handler(event) {
      // ... fold into view
    },
  },
  { pollInterval: 50 },
);
```

What changed:

| Legacy                              | New                                                                  |
| ----------------------------------- | -------------------------------------------------------------------- |
| `registerProjection(def, opts)`     | `registerProjection(name, input, opts)` — `name` is now positional   |
| `selector: (e) => boolean`          | `routeFn: (e) => { partitionKey } \| "ignore"`                       |
| `handle`                            | `handler` (renamed for symmetry with the PM `handle`)                |
| implicit strict-sequential delivery | explicit `partitionBy: { kind: 'sequential' }` (the default)         |
| n/a                                 | new sugar: `{ kind: 'per-event' }` (max parallelism), `{ kind: 'per-key', key }` (parallel across keys, serial within) |

`partitionBy` and `routeFn` are mutually exclusive — use one or the
other.

### Three-mode partitioning sugar

`partitionBy` is sugar over a `RoutingFn`. The three modes:

```ts
// strict-serial: every routed event lands on the same partition.
// Equivalent to the legacy single-cursor projection.
app.registerProjection("S", {
  partitionBy: { kind: "sequential" },
  handler: async (e) => { /* ... */ },
});

// max parallelism: each event lands on its own partition.
app.registerProjection("PE", {
  partitionBy: { kind: "per-event" },
  handler: async (e) => { /* ... */ },
});

// parallel across keys, serial within a key. The user supplies the
// key extractor; events sharing a key are delivered in order.
app.registerProjection("PK", {
  partitionBy: {
    kind: "per-key",
    key: (e) => (e.data as { tenantId: string }).tenantId,
  },
  handler: async (e) => { /* ... */ },
});
```

None of the three sugar modes can emit `"ignore"`. If you need
routing-side filtering plus a non-sequential partition shape, write
the `routeFn` directly.

---

## 2. Process-manager registration (PM-F + PM-C)

### Before

```ts
const pm: ProcessManagerDefinition<TransferStage> = {
  name: "TransferProcessManager",
  stream: "$all",
  // Commanded-style directive set: 4 routing shapes + ignore.
  routes: {
    TransferRequested: (e) => ({ kind: "start", processId: e.data.transferId }),
    Withdrawn:         (e) => ({ kind: "continue", processId: e.data.transferId }),
    Deposited:         (e) => ({ kind: "stop",   processId: e.data.transferId }),
    WithdrawalRefused: (e) => ({ kind: "stop",   processId: e.data.transferId }),
  },
  initialState: () => ({ stage: "starting" }),
  // Single callback: receives state + event, returns next state + commands.
  // No `apply` / `handle` split; no `complete` flag.
  async handle(state, event) {
    return { state: nextState, commands: [/* ... */] };
  },
};
app.registerProcessManager(pm, { pollInterval: 50 });
```

### After

```ts
app.registerProcessManager(
  "TransferProcessManager",
  {
    stream: "$all",
    // PM-F: routing is just (event) -> partitionKey | "ignore".
    // No start/continue/stop enum -- every routed event becomes a
    // work item.
    routeFn: (e) => {
      const id = (e.data as { transferId?: string }).transferId;
      return id ? { partitionKey: id } : "ignore";
    },
    initialState: () => ({ stage: "starting" }),
    // PM-C: pure state fold. Runs during rebuild and on the
    // triggering event before `handle`. MUST NOT have side effects.
    apply: (state, event) => {
      // ... return next state derived from event
    },
    // PM-C + PM-F: commands and lifecycle. `complete: true`
    // DELETEs snapshot + all work-items for the partition in one tx.
    handle: async (state, event) => {
      // ... compute commands / completion
      return { commands: [/* ... */], complete: event.event_type === "Deposited" };
    },
  },
  { pollInterval: 50 },
);
```

### How each Commanded directive collapses

| Commanded                    | New shape                                                                                                       |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `{ kind: 'start', processId }`   | `routeFn → { partitionKey: processId }`; processor loads/initialises (no special routing case for "first event") |
| `{ kind: 'continue', processId }` | same — no distinction at routing                                                                                |
| `{ kind: 'start!', processId }`   | `handle` raises if `state !== initialState()`                                                                   |
| `{ kind: 'continue!', processId }`| `handle` raises if `state === initialState()`                                                                   |
| `{ kind: 'stop', processId }`     | `handle` returns `{ complete: true }`                                                                           |
| `{ kind: 'ignore' }` / `false` | `routeFn → "ignore"`                                                                                            |

### Why the split

Routing now decides "which work-item row, if any?"; processing
decides "what to do with this row, and is the instance done after?".
The directive enum bundled the two; the SUB-A worker split forced the
separation. See `docs/decisions.md` D-0011 / D-0012 for the
lock-set-disjointness rationale and `docs/decisions.md` D-0018
for the design rationale.

### `apply` is mandatory

`apply` runs during PM-state **rebuild** when the snapshot is missing
or carries a `snapshot_module_version` that no longer matches
`def.snapshotModuleVersion`. It also runs on the triggering event
before `handle` to produce the "staged" state. Splitting it out (PM-C)
means the rebuild path is no worse than the aggregate-rebuild path:
the framework can replay state without ever invoking `handle` and
without re-dispatching commands.

If your legacy PM had no recovery story — `handle` did everything,
state was best-effort — the cheap migration is:

```ts
apply: (state, event) => state,   // no-op fold
handle: async (state, event) => {
  // ...whatever the legacy handle did, returning commands and
  //   complete as appropriate. Snapshot is opaque; rebuild will
  //   start from initialState() on miss.
},
```

This is correct but pessimistic: every claimed event triggers a full
rebuild from `initialState()` because `apply` doesn't actually fold
state. Worth investing the time to write a real `apply` if you have
PM instances with non-trivial event counts.

### `complete: true` semantics

On `handle` returning `{ complete: true }`, the processing worker
DELETEs the snapshot row AND every work-item for the partition
(including the triggering one) in one transaction. Future events to a
`complete`-d partition route as normal and `apply` runs from
`initialState()` again. Applications that need "permanently
terminated" semantics either encode it in their own state (in `apply`,
so it survives replay) or shape `routeFn` to stop matching once the
upstream events that mark terminal have passed.

---

## 3. Worker primitives

If you were using `startProjection` / `startProcessManager` directly
(without the `Instructed` facade), those exports are **removed**. The
SUB-A replacements:

```ts
import {
  startRoutingWorker,
  startProjectionWorker,
  startPmWorker,
  routingFnForPartitionBy,
} from "instructed";

// Projection: one routing worker + one processing worker per
// subscription, both addressing the same (stream, name) pair.
const router = startRoutingWorker(client, {
  name: "Balances",
  stream: "$all",
  routeFn: routingFnForPartitionBy({ kind: "sequential" }),
});
const proc = startProjectionWorker(client, {
  name: "Balances",
  stream: "$all",
  handler: async (event) => { /* ... */ },
});

// PM: same pattern; the processing side takes both the persist client
// and a separate dispatch client (D-0011 / D-0012).
const pmRouter = startRoutingWorker(client, { name, stream, routeFn });
const pmProc   = startPmWorker(client, dispatchClient, {
  name, stream, initialState, apply, handle,
});
```

The `Instructed` facade hides this; use it unless you have a specific
reason to wire the workers by hand.

---

## 4. Removed exports (quick reference)

These symbols are gone from `instructed`'s public surface:

| Removed                           | Replaced by                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `startProjection`                 | `startRoutingWorker` + `startProjectionWorker`                               |
| `startProcessManager`             | `startRoutingWorker` + `startPmWorker`                                       |
| `ProjectionDefinition` (legacy)   | `RegisterProjectionInput` (facade) or `ProjectionDefinition` from `projection-worker.ts` (worker) |
| `ProcessManagerDefinition`        | `RegisterProcessManagerInput` (facade) or `PmDefinition` from `pm-worker.ts` (worker) |
| `ProjectionHandler.handle`        | `ProjectionHandler` (the same callback, but now lives in `routeFn`/`handler` shape) |
| `ProcessManagerHandlerResult`     | `PmHandleResult` (now `{ commands?, complete? }`)                            |
| `RouteResult` (PM)                | `RoutingDecision` (`{ partitionKey } \| "ignore"`)                           |
| `routes` map on PM                | `routeFn` callback                                                           |
| `selector` on projection          | `routeFn` returning `"ignore"`                                               |

Worker-level types you may want for advanced use are still exported:
`RoutingFn`, `RoutingDefinition`, `RoutingWorkerOptions`,
`ProcessingWorkerOptions`, `ErrorPolicy`, `ErrorPolicyDecision`,
`PartitionBy`, `routingFnForPartitionBy`, `SEQUENTIAL_PARTITION_KEY`,
`PM_SNAPSHOT_MODULE_VERSION_KEY`, etc. See `src/index.ts`.

---

## 5. What did NOT change

- `registerAggregate` / `dispatch` / `client()` / `close()` are
  unchanged.
- `dispatch(..., { consistency: [...], consistencyTimeout })` is
  unchanged. The wait now polls the SUB-A catch-up predicate
  (`is_subscription_caught_up`) rather than the legacy single-cursor
  read, but the public shape and the timeout semantics are
  identical.
- `waitForProjection` is unchanged at the call site. Per-stream
  subscriptions now wait in `event_number` space (was
  `stream_version`); each `AppendedEvent` carries both numbers so
  the same wall-clock moment is reached.
- Causation / correlation propagation through PM-dispatched commands
  is unchanged (D-0017).
- The dispatch-pool isolation rules (D-0011 / D-0012) are unchanged:
  the PM worker still requires `dispatchDb` distinct from `db` when
  both are supplied as raw `Pool`/`Queryable` instances.

---

## 6. Reference: the bank-account example

`examples/bank-account/` is the canonical migrated example. It uses:

- a sequential projection with `routeFn` filtering (`balances.ts`),
- a PM with `routeFn` + `apply` + `handle` and `complete: true`
  termination (`transfer-pm.ts`),
- and the facade-level `dispatch(..., { consistency: [...] })` wait
  (`main.ts`).

Read those three files alongside this note for a worked example of
each migration shape.
