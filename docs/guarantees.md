# Guarantees — Phase 3

A mechanical catalogue of the guarantees Commanded provides on top of
the bare event-store adapter contract. Phase 2 (`invariants.md`)
covered what the *store* must do. This document covers what *the
layers above* — aggregate process, snapshotting policy, event handler,
process manager, subscriptions registry — must do, in order for
applications written against Commanded to behave correctly.

These are the guarantees `instructed` must either reproduce (in SQL
+ SDK), deliberately weaken, or drop. The decision for each is Phase
4 work; this document only catalogues.

## Conventions

- Each numbered guarantee has a stable identifier
  (`AGG-001`, `HND-001`, `PM-001`, `CON-001`, `DSP-001`).
- Where Commanded provides a guarantee through an OTP mechanism that
  has no Postgres analogue, the mechanism is flagged
  **[beam-mechanism]** so it is clear we need a different realisation
  rather than a port.
- "Aggregate" means a single instance, identified by `aggregate_uuid`,
  whose state is a fold over its event stream.

Sources:

- `commanded/lib/commanded/aggregates/{aggregate,aggregate_state_builder,
  execution_context,aggregate_lifespan,default_lifespan,multi}.ex`
- `commanded/lib/commanded/event/handler.ex`
- `commanded/lib/commanded/process_managers/{process_manager,
  process_manager_instance,process_router}.ex`
- `commanded/lib/commanded/subscriptions.ex` and
  `commanded/lib/commanded/snapshotting.ex`
- The Commanded guides (`Aggregates.md`, `Commands.md`, `Events.md`,
  `Process Managers.md`, `Read Model Projections.md`).

---

## Part A — Aggregate execution

The aggregate is the unit that enforces business invariants. Commands
arrive, are evaluated against the current aggregate state, and either
produce events or fail. Commanded's aggregate process (`Aggregate`
GenServer, `AggregateStateBuilder`, `ExecutionContext`) implements
the load → execute → append cycle.

### Hydration

- **AGG-001** — On first activation of an aggregate instance, its
  state MUST be reconstructed by:
  1. Optionally reading a snapshot for `aggregate_uuid` from the
     event store, and starting from `(snapshot.data,
     snapshot.source_version)`.
  2. Streaming events from `aggregate_uuid` starting at
     `start_version = snapshot_version + 1` (or `0` if no snapshot),
     in order, applying each via `aggregate_module.apply(state,
     event.data)`.
  3. Setting `aggregate_version` to the highest `stream_version`
     applied.
- **AGG-002** — `apply/2` MUST NOT fail. Once an event is recorded,
  it cannot be rejected during replay.
- **AGG-003** — If the read snapshot fails validation
  (e.g. `snapshot_module_version` mismatch — see SNAP-002), the
  snapshot MUST be ignored and the aggregate hydrated from the full
  event stream as if no snapshot existed.
- **AGG-004** **[beam-mechanism]** — Commanded keeps the hydrated
  aggregate state in a long-lived GenServer process so subsequent
  commands skip rehydration. This is an *optimisation*, not a
  semantic guarantee; an implementation that rehydrates on every
  command is observably equivalent. (The cost is performance only,
  unless an application is relying on the GenServer's per-instance
  serialisation — see AGG-010.)

### Command execution

- **AGG-005** — A command is evaluated by calling either
  `handler_module.handle(state, command)` or
  `aggregate_module.execute(state, command)` (the choice is the
  caller's; the contract is the same).
- **AGG-006** — The return value of the command function is one of:
  - `:ok`, `nil`, `[]`, `{:ok, []}` → no events
  - `%Event{}`, `[%Event{}, ...]`, `{:ok, %Event{}}`,
    `{:ok, [%Event{}, ...]}` → one or more events
  - `{:error, reason}` → command rejected; no events; no state change
  - `%Commanded.Aggregate.Multi{}` → multi-step; see AGG-008
  - raised exception → command failed with stacktrace; no events; no
    state change; aggregate process may be terminated per lifespan
- **AGG-007** — Events produced by a command MUST be folded into the
  in-memory aggregate state via `apply/2` *before* the next command
  is processed, and they MUST be the same events that were appended
  to the store. The local fold and the persisted stream MUST agree.
- **AGG-008** — `Commanded.Aggregate.Multi` allows a command to
  produce events in stages where each stage may inspect the state
  resulting from the previous stage's events. The contract: the
  aggregate state observable inside stage N is the result of
  applying all events from stages 1..N-1 on top of the
  pre-command state. Either *all* stages' events are persisted, or
  none are.

### Optimistic locking and retry

- **AGG-009** — The append at the end of a successful command MUST
  use `expected_version = aggregate_version` (the version as
  observed at the start of command evaluation). This is the
  optimistic-locking step; INV-APPEND-013 in `invariants.md`
  guarantees that conflicting concurrent appends fail.
- **AGG-010** — On `{:error, :wrong_expected_version}` from append,
  the aggregate MUST:
  1. Stream any new events from the store (from the current
     in-memory version onwards) and fold them into state.
  2. Re-evaluate the command against the new state.
  3. Re-attempt the append.
  Up to `retry_attempts` (configurable, default 0 — i.e. no retry
  by default).
- **AGG-011** **[beam-mechanism]** — Commands targeting the same
  aggregate instance are serialised by the GenServer mailbox. In
  Commanded, two concurrent dispatches to the same `aggregate_uuid`
  do not race — they queue. `instructed` does not get this for
  free; equivalent serialisation must come from optimistic-lock
  retry (INV-APPEND-013 + AGG-010), or from a per-aggregate
  advisory lock during the load-execute-append window, or be
  documented as a behaviour difference.

### Causation, correlation, metadata

- **AGG-020** — Every event produced by a command MUST be appended
  with `causation_id = command_uuid`, `correlation_id =
  caller-supplied`, and `metadata = caller-supplied map`. These
  values flow through `ExecutionContext` and `EventData`.
- **AGG-021** — All events produced by a single command share the
  same `causation_id` and `correlation_id`.

### Concurrent observers

- **AGG-025** **[beam-mechanism]** — In Commanded, the aggregate
  process also *subscribes* to its own stream
  (`EventStore.subscribe(application, aggregate_uuid)`) so that if
  events are appended to the stream by some other process (e.g. a
  direct event-store write), the GenServer applies them and bumps
  its version. This keeps the in-memory state honest if multiple
  Commanded nodes happen to dispatch to the same aggregate without
  cluster coordination.
  Without an in-memory cache, `instructed` does not need this: every
  command load reads from the store. The guarantee disappears with
  the mechanism, harmlessly.

### Lifespan

- **AGG-030** **[beam-mechanism]** — `AggregateLifespan` controls
  how long the in-memory aggregate process stays alive after each
  command, event, or error. Default is `:infinity`. Values: a
  millisecond inactivity timeout, `:hibernate`, `:stop`,
  `{:stop, reason}`. Without in-memory caching this entire concept
  is unnecessary.

---

## Part B — Snapshotting policy

The store-side contract (Part D of `invariants.md`) is just an
upsert-keyed-by-source_uuid. The policy for *when* to snapshot, and
how to validate them on read, sits above the store.

- **SNAP-001** — Snapshots are taken according to a policy attached
  to each aggregate module: `snapshot_every: N` triggers a snapshot
  after every N events appended (`source_version - snapshot_version
  >= snapshot_every`). `nil` disables snapshotting.
- **SNAP-002** — `snapshot_version` (a *module-level* integer,
  separate from the per-snapshot `source_version`) marks the schema
  of the snapshotted data. The taker MUST stamp this version into
  the snapshot's metadata as `snapshot_module_version`. The reader
  MUST reject a snapshot whose metadata `snapshot_module_version`
  does not equal the currently configured value, and MUST fall back
  to full event replay.
- **SNAP-003** — Taking a snapshot is best-effort: a failed snapshot
  write MUST NOT fail the command that triggered it. The command's
  events are already durably appended; the snapshot is an
  optimisation.
- **SNAP-004** — The `source_version` recorded in a snapshot MUST
  equal the aggregate version at the moment the snapshot data was
  captured. On read, hydration resumes from `source_version + 1`.
- **SNAP-005** — `source_type` records the aggregate module name.
  It is informational; INV-SNAP-002 already specifies upsert
  semantics so the type field may be overwritten when the module
  evolves.

---

## Part C — Event handler (subscriber / projector)

Event handlers are the read-side of CQRS. They subscribe to a
stream (typically `:all`) and react to events. Projections (read
models) are the most common kind.

### Identity and subscription

- **HND-001** — An event handler has a name (`name:` option) and
  subscribes to either `:all` or a specific `stream_uuid`. The pair
  `(subscription_target, handler_name)` is the persistent
  subscription identity (INV-SUB-P-001).
- **HND-002** — Handler names are stable across deployments.
  Changing a handler's name creates a new subscription that starts
  from `start_from` (default `:origin`) and re-processes the
  history. This is a documented operational behaviour, not a bug.
- **HND-003** — `start_from` (one of `:origin`, `:current`, or an
  explicit `event_number`) takes effect only on first creation of
  the persistent subscription. Restarts and reconnects resume from
  `last_seen` (INV-SUB-P-021).

### Delivery and ack

- **HND-010** — The handler receives events in order, one at a time
  (single-event delivery) or in batches (`batch_size:` /
  `batch_timeout:`), and calls back into `handle(event, metadata)`
  (or `handle_batch(events)`).
- **HND-011** — A successful return (`:ok` or `{:ok,
  handler_state}`) MUST be followed by an ack to the subscription
  (`Subscription.ack_event(subscription, last_event)`). The ack
  advances the durable cursor (INV-SUB-P-032).
- **HND-012** — A handler MUST ack monotonically. Events with
  `event_number <= last_seen_event` are silently re-acked without
  re-invoking `handle/2`. (Commanded tracks `last_seen_event` in
  the handler's in-memory state, separately from the durable
  cursor, to absorb at-least-once redelivery.)
- **HND-013** — Returning `{:error, :already_seen_event}` from
  `handle/2` is equivalent to `:ok` plus skip — it acks the event
  without further processing. This is an opt-in mechanism for
  handlers that maintain their own idempotency outside of
  `last_seen_event`.

### Error handling

- **HND-020** — The handler module MAY define an `error/3` callback:
  `error({:error, reason}, failed_event_or_events, failure_context)`.
  Return values:
  - `{:retry, context}` / `{:retry, context_map}` — retry
    immediately with updated context.
  - `{:retry, delay_ms, context}` — retry after sleeping.
  - `:skip` — ack the event without re-invoking and move on.
  - `{:stop, reason}` — stop the handler process; supervisor
    decides restart.
- **HND-021** — Without an `error/3` callback, behaviour is set
  application-wide: `:stop` (default), `:backoff` (exponential up
  to 24h), or a custom module.
- **HND-022** — A `:retry` MUST NOT advance the cursor. A `:skip`
  MUST advance the cursor past the failed event. A `:stop` leaves
  the cursor where it is so that a restarted handler retries.
- **HND-023** — At-least-once delivery (INV-SUB-P-031) means
  handlers MUST be idempotent or use `last_seen_event` to absorb
  duplicates.

### State

- **HND-030** — Handlers MAY carry transient in-memory state passed
  between invocations as the second tuple element of `{:ok,
  handler_state}`. This state is **not persisted** by the
  framework; it lives only in the handler process.
- **HND-031** — Persistent projection state is the application's
  responsibility — the handler writes to its own tables. Atomicity
  between projection write and cursor advance is *not* provided by
  Commanded's adapter contract; in the reference adapter, the
  cursor advance is a separate SQL statement after the handler
  returns. Strong-consistency-on-dispatch (see Part E) is the
  intended mechanism for users who need to know the projection
  caught up.

### Upcasting

- **HND-040** — Before being delivered to `handle/2`, events MAY be
  passed through an `EventStore.Adapter.upcast/2` chain that
  rewrites old event shapes to new ones. This is a Commanded-level
  concern; the store contract does not see upcasted events.

### Partitioned consumers

- **HND-050** — When `concurrency:` > 1, multiple subscribers attach
  to the same persistent subscription (INV-SUB-P-040) and events
  are distributed by `partition_by` (INV-SUB-P-041). Order is
  preserved within a partition; cross-partition order is not.
  Deferred for `instructed` v1 (ML-0001).

---

## Part D — Process managers

A process manager is "an event handler with persistent state that
dispatches commands". It enables sagas — multi-aggregate workflows
expressed as event-driven state machines.

### Routing — `interested?/1` or `interested?/2`

- **PM-001** — For each event, the process manager module's
  `interested?` callback returns one of:
  - `false` — ignore the event; ack and continue.
  - `{:start, process_uuid}` / `{:start!, process_uuid}` — create a
    new process instance.
  - `{:continue, process_uuid}` / `{:continue!, process_uuid}` —
    deliver to an existing instance.
  - `{:stop, process_uuid}` — terminate the instance and delete its
    persistent state.
  - A *list* of `process_uuid`s, when one event fans out to
    multiple instances.
- **PM-002** — `{:start!, ...}` MUST fail if the instance already
  exists (error: `{:start!, :process_already_started}`).
  `{:continue!, ...}` MUST fail if it does not (error:
  `{:continue!, :process_not_started}`). The errors are routed
  through `error/3`.

### Per-instance event processing

For each event delivered to a process instance:

- **PM-010** — The instance's `handle(state, event, metadata)` is
  called and returns zero, one, or many commands.
- **PM-011** — Commands returned by `handle/3` MUST be dispatched
  *before* the process state is mutated by `apply/2`. (Commanded
  ordering: handle → dispatch all returned commands → apply →
  persist state → ack the event → run `after_command/2` for each
  dispatched command.)
- **PM-012** — Each dispatched command inherits
  `causation_id = event.event_id` and
  `correlation_id = event.correlation_id` from the triggering
  event. This is what makes a saga traceable end-to-end.
- **PM-013** — If `handle/3` raises or returns `{:error, ...}`, the
  `error/3` callback is invoked with the failure and the same
  retry/skip/stop options as event handlers (HND-020), plus:
  - `{:continue, commands, context}` — replace pending commands
    with the supplied list and continue dispatch.
  - `{:skip, :discard_pending}` — drop all remaining commands and
    ack.
  - `{:skip, :continue_pending}` — drop the failing command, keep
    the rest.
- **PM-014** — `apply(state, event)` mutates the in-memory process
  state. Like aggregate `apply/2`, it MUST NOT fail.

### State persistence

- **PM-020** — Process instance state is persisted **as a snapshot**
  in the event store (reusing the snapshots table). The snapshot's
  `source_uuid` is constructed as
  `"<process_manager_name>-<process_uuid>"`. `source_version` is
  the `event_number` of the last successfully handled event.
- **PM-021** — On instance startup, state is recovered by reading
  this snapshot. If absent, the instance starts with the default
  state from `new()`.
- **PM-022** — On `{:stop, process_uuid}`, the snapshot for that
  instance MUST be deleted (`delete_snapshot`). This is the
  cleanup path; absent this, terminated instances would haunt the
  snapshots table.
- **PM-023** — The order is significant: persist state → ack event.
  If the ack fails (or the process crashes after persist but before
  ack), the event will be redelivered, the instance will read its
  persisted state showing `last_seen_event = event_number`, and
  the duplicate will be detected and silently re-acked (PM-024).
- **PM-024** — The snapshot serves a second purpose: its
  `source_version` field IS the `last_seen_event` for that
  instance. Redelivery detection uses it.

### Compensation (sagas)

- **PM-030** — Commanded does **not** model compensating actions as
  a first-class concept. Compensation is whatever commands the
  process manager dispatches in response to failure events
  (e.g. on receiving a `FlightBookingFailed` event, dispatch
  `CancelHotelBooking`). The framework's contribution is the
  durable, ordered, at-least-once event-driven invocation; the
  *shape* of the saga is user code.
- **PM-031** — A failed command dispatch (i.e. the target
  aggregate returned an error) is surfaced to `error/3` as a
  command-failure rather than an event-failure, with the
  command-specific response variants from PM-013.

(See Phase 6 in `ROADMAP.md` and D-0001 in `decisions.md` for the
open `instructed` question: do we add first-class compensation?)

### Lifecycle

- **PM-040** **[beam-mechanism]** — Like aggregates, each process
  instance is a GenServer; multiple instances of the same process
  manager run side-by-side. `idle_timeout` shuts down idle
  instances. As with aggregates this is a hosting concern, not a
  semantic one.

---

## Part E — Strong-consistency on dispatch

This is the most BEAM-shaped guarantee in Commanded. It is the
reason teams choose Commanded over rolling their own — and it is
the place `instructed` will have to compromise most visibly.

### Per-handler consistency

- **CON-001** — Each event handler / process manager declares its
  consistency: `:eventual` (default) or `:strong`.
- **CON-002** — A handler declared `:strong` registers itself with
  the application's `Subscriptions` GenServer at startup.
- **CON-003** — On every successful `confirm_receipt` (handler) or
  `ack_event` (PM), the handler publishes
  `{:ack_event, name, stream_id, stream_version}` to a
  pubsub topic the `Subscriptions` GenServer is listening on.
  This GenServer maintains an ETS table of "which strongly
  consistent handlers have processed up to which
  `(stream_id, stream_version)`".

### Wait-on-dispatch

- **CON-010** — `Application.dispatch(command, consistency: :strong)`
  (or with an explicit handler list) waits, after a successful
  append, until every relevant strongly-consistent handler has
  acked at least up to the appended events'
  `(stream_id, stream_version)`. Then it returns `:ok` to the
  caller.
- **CON-011** — `consistency: [HandlerA, "HandlerB"]` waits only
  for the listed handlers. `consistency: :strong` waits for *all*
  strongly-consistent handlers registered with the application.
- **CON-012** — A `:consistency_timeout` (default configurable per
  application) bounds the wait. On timeout, dispatch returns
  `{:error, :consistency_timeout}`. The events are still durably
  appended; only the wait failed.
- **CON-013** **[beam-mechanism]** — The mechanism is *entirely*
  in-VM: pubsub broadcast on ack, an ETS table tracking handler
  positions, a `receive` that blocks the dispatching process. None
  of this exists across machines without cluster pubsub.

### Implications for `instructed`

D-0003 commits us to polling, not push. So our equivalent of
CON-010 is:

- Append the events; learn the assigned `(stream_id, stream_version)`
  range.
- Poll the subscription cursors for the requested handlers until
  each `last_seen >= our stream_version` (or `event_number`, for
  `:all`-scoped handlers).
- Return on success, time out otherwise.

This has higher latency than Commanded's same-VM pubsub but
identical semantics. The hard part is that the SDK must know
*which* subscriptions to poll. That is metadata work in Phase 4.

---

## Part F — Dispatch surface

The dispatching API around the aggregate process. Smaller than the
others but worth pinning down.

- **DSP-001** — `Application.dispatch(command, opts)` resolves the
  aggregate identity from the command (via a router-declared
  `identity:` function), routes to the aggregate process or starts
  it, and invokes the command handler.
- **DSP-002** — Options on dispatch: `correlation_id`,
  `causation_id`, `metadata`, `timeout`, `consistency`,
  `consistency_timeout`, `returning`, `retry_attempts`.
- **DSP-003** — `returning` controls the reply shape:
  - `false` (default) → `:ok | {:error, ...}`
  - `:aggregate_state` → `{:ok, aggregate_state}`
  - `:aggregate_version` → `{:ok, version}`
  - `:events` → `{:ok, events}`
  - `:execution_result` → `{:ok, %ExecutionResult{...}}` containing
    `aggregate_uuid`, `aggregate_state`, `aggregate_version`,
    `events`, `metadata`.
- **DSP-004** — `before_execute:` MAY be configured per command —
  a function called *after* hydration and *before* the command
  handler. Returning `{:error, reason}` cancels dispatch.
  Returning `:ok` proceeds.
- **DSP-005** — Middleware MAY wrap dispatch (`pipeline_before`,
  `pipeline_after`, `pipeline_after_failure`). This is purely a
  user-extension point on top of dispatch, not a semantic
  guarantee of CQRS/ES.

---

## Part G — Classification

Each guarantee is one of:

- **(I) Intrinsic to CQRS/ES** — must be reproduced or applications
  break.
- **(B) BEAM/OTP-specific optimisation** — provides
  ergonomics/perf, not semantics. `instructed` may drop or
  re-realise.
- **(C) Convenience** — useful but not load-bearing.

| ID    | Class | Notes |
|-------|-------|-------|
| AGG-001..003, 005..008 | I | Aggregate hydration & command semantics. |
| AGG-004, 011, 025, 030 | B | In-memory caching, GenServer serialisation, self-subscription, lifespan. |
| AGG-009, 010 | I | Optimistic locking and retry — semantic; mechanism may differ. |
| AGG-020, 021 | I | Causation/correlation propagation. |
| SNAP-001, 002 | C | Policy. Apps can implement themselves. |
| SNAP-003, 004 | I | Best-effort + version contract. |
| SNAP-005 | C | Informational. |
| HND-001..003 | I | Subscription identity & start_from. |
| HND-010..013 | I | Ordered delivery + ack semantics + idempotency hook. |
| HND-020..022 | C | Error-handling vocabulary. Apps can implement. |
| HND-023 | I | At-least-once is intrinsic. |
| HND-030 | C | Transient handler state. |
| HND-031 | I | "Cursor advance is application's atomicity problem unless you opt into strong consistency". |
| HND-040 | C | Upcasting is application concern. |
| HND-050 | B | Partitioned consumers (ML-0001). |
| PM-001, 002 | I | Routing semantics. |
| PM-010..014 | I | Per-instance handle/dispatch/apply/ack ordering. |
| PM-020..024 | I | State persistence model (reuses snapshots). |
| PM-030, 031 | I | (Or arguably C; see D-0001 — we want to revisit.) |
| PM-040 | B | Per-instance process. |
| CON-001..012 | I (semantic) / B (mechanism) | Strong consistency is semantically intrinsic; the push+pubsub mechanism is BEAM. |
| DSP-001..004 | I | Dispatch contract surface. |
| DSP-005 | C | Middleware. |

(Class assignments are the basis for Phase 4 decisions, not
predictions of what we will ultimately do.)

---

## Phase 3 status

Every Commanded-level guarantee referenced in Part G of
`invariants.md` has an entry here. No new open questions surfaced
beyond what is already noted inline (PM-030 / D-0001).

The output of Phase 2 (store-level invariants) and Phase 3
(layer-above guarantees) together specify what `instructed` must
provide. Phase 4 maps each to a Postgres-native realisation.
