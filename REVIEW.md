# CQRS/ES Feature Review: Instructed vs Commanded

> **Generated**: 2026-02-18  
> **Instructed version**: 1.0.0 (Gleam)  
> **Commanded reference**: latest main branch (Elixir)

This review systematically compares every feature, constraint, and guarantee between Instructed (this Gleam repo) and Commanded (the Elixir library it ports).

**Status indicators:**
- ✅ Feature present and equivalent
- ⚠️ Feature present but with gaps or differences
- ❌ Feature missing entirely
- 📝 Intentional design difference (not a problem)

---

## 1. Aggregates [⚠️]

### What Commanded Provides
- Aggregate is an Elixir module with `defstruct` for state, `execute/2` callback for commands, `apply/2` callback for events
- `execute/2` supports rich return types: single event, list of events, `{:ok, events}`, `:ok`, `nil`, `[]`, `{:error, reason}`, `%Multi{}`, or raising exceptions
- `apply/2` must never fail — used during state rebuilding
- Aggregate runs as a GenServer process (`Commanded.Aggregates.Aggregate`) with `restart: :temporary`
- State population via `AggregateStateBuilder.populate/1`: reads snapshot, then streams events in batches of 1,000
- Self-subscription to aggregate stream via `EventStore.subscribe/2` to catch externally appended events
- Validates expected event sequence (`stream_version == aggregate_version + 1`); throws on unexpected gaps
- Aggregate state is a struct; best practice docs advise keeping only data needed for command handling

### What Instructed Provides
- ✅ Aggregate is a record-of-functions (`Aggregate(state, command, event)`) with `execute`, `apply_event`, `empty_state` — idiomatic Gleam equivalent
- ⚠️ `execute` returns `Result(List(event), String)` — supports `Ok([])` for no-ops and `Ok(events)` for success, `Error(reason)` for failures. Does NOT support returning a `Multi` from execute (Multi is external, used within execute via `to_result()`)
- ✅ `apply_event` is a pure function that must never fail
- ✅ Aggregate server runs as an OTP Actor (`aggregate_server.gleam`) with cached state
- ✅ State population with snapshot support via `populate_from_event_store` — reads snapshot, then batches of 1,000
- ✅ Self-subscription to aggregate stream for external events (`ExternalEvent` message handler)
- ⚠️ Gap detection: on unexpected version, reloads full state from event store instead of throwing. This is actually more resilient than Commanded's behavior
- 📝 Record-of-functions instead of module+behaviour — idiomatic Gleam design choice

### Gaps & Issues
- **LOW**: `execute` cannot directly return a `Multi` struct — must call `multi.to_result()` inside execute. This is a minor ergonomic difference, not a correctness issue.
- **LOW**: No `{:ok, event}` or `nil` return variants — Gleam's type system makes these unnecessary (use `Ok([event])` or `Ok([])`)
- **LOW**: Exception handling differs — Gleam doesn't have exceptions; errors must be returned as `Error(reason)`. This is actually safer.

---

## 2. Command Routing & Dispatch [⚠️]

### What Commanded Provides
- Compile-time macro-based `Router` module with `identify` and `dispatch` macros
- `identify` macro: sets default identity field, prefix, per-aggregate; enforces one-per-aggregate-per-router
- `dispatch` macro: registers command → handler/aggregate mapping with options (timeout, lifespan, consistency, returning, etc.)
- `CompositeRouter`: combines multiple routers; detects duplicate command registrations at compile time
- Identity extraction via `ExtractAggregateIdentity` middleware: supports field name atoms, 1-arity functions, prefix (string or function)
- Identity converted to string via `String.Chars` protocol
- Duplicate command detection at compile time
- `Dispatcher.Payload` struct carries all dispatch metadata
- Task-based execution via `Task.Supervisor.async_nolink` with `Task.yield/shutdown` for timeout
- Retry on aggregate process death (`:aggregate_stopped`, `:remote_node_down`)
- Default timeout: 5,000ms; default retry attempts: 10

### What Instructed Provides
- ✅ Runtime `Router` record with `identity` function, `identity_prefix`, `middleware`, `retry_attempts`, `dispatch_timeout`
- ✅ Identity extraction via user-provided function (`fn(command) -> String`)
- ✅ Identity prefix support via `with_prefix`
- ✅ Validates identity is non-empty string
- ✅ Default timeout 5,000ms, default retry 10 attempts (matches Commanded)
- ✅ Middleware pipeline integration (`before_dispatch`, `after_dispatch`, `after_failure`)
- ✅ Strong consistency waiting via subscriptions integration
- ✅ Registry actor manages aggregate server processes per stream_id
- ❌ No compile-time dispatch — Gleam doesn't have macros
- ❌ No duplicate command detection (runtime router, single command type per router)
- ❌ No `returning` option on dispatch (always returns `DispatchResult` with state, version, events)
- ⚠️ No Task-based dispatch — command executed via `process.call` directly to aggregate server actor (blocking the caller)
- ⚠️ No retry on aggregate server death — if the actor stops during dispatch, the call times out

### Gaps & Issues
- **MEDIUM**: No `returning` option — dispatch always returns full `DispatchResult`. In Commanded, `returning: false` returns just `:ok` for performance. Instructed always materializes state + events. Not a correctness issue but impacts API compatibility.
- **LOW**: No compile-time dispatch registration — 📝 intentional Gleam design (no macros). Runtime config is equivalent.
- **MEDIUM**: No retry on aggregate process death. If the aggregate server stops (e.g., lifespan timeout) during a dispatch, the calling process gets a timeout rather than a retry. Commanded retries on `:aggregate_stopped` and `:remote_node_down`.
- **HIGH**: Registry actor is a simple `Dict` lookup without cleanup — dead aggregate servers remain in the registry. No process monitoring. If an aggregate server crashes, subsequent dispatches will fail because the registry returns the dead Subject. Should monitor aggregate server processes and remove on death.

---

## 3. Middleware Pipeline [⚠️]

### What Commanded Provides
- `Commanded.Middleware` behaviour with three callbacks: `before_dispatch/1`, `after_dispatch/1`, `after_failure/1`
- `Pipeline` struct with fields: application, command, command_uuid, consistency, identity, metadata, assigns, halted, response, plus causation/correlation IDs
- `Pipeline.chain/3` runs middleware; halting stops `before_dispatch` and `after_dispatch` but NOT `after_failure`
- `Pipeline.assign/3` for shared data (atom keys, any values)
- `Pipeline.assign_metadata/3` for event metadata
- `Pipeline.respond/2` with first-write-wins semantics
- Built-in middleware: `ExtractAggregateIdentity` (identity extraction + prefix + validation) and `ConsistencyGuarantee` (strong consistency waiting)
- User middleware runs BEFORE built-in middleware
- Execution order: user MW1 before → user MW2 before → ExtractIdentity before → ConsistencyGuarantee before → execute → ConsistencyGuarantee after → ExtractIdentity after → user MW2 after → user MW1 after

### What Instructed Provides
- ✅ `Middleware` record with `before_dispatch`, `after_dispatch`, `after_failure` functions
- ✅ `Pipeline` record with command, command_id, causation_id, correlation_id, metadata, assigns, halted, response, consistency, identity
- ✅ Halting stops `before_dispatch` and `after_dispatch` but not `after_failure` (matches Commanded)
- ✅ `assign/3`, `assign_metadata/3`, `halt/1`, `respond/2` (first-write-wins)
- ⚠️ No built-in middleware — identity extraction and consistency guarantee are handled inline in the router, not as composable middleware
- ⚠️ `assigns` uses `Dict(String, String)` — Commanded uses atom keys with any values. Gleam's type safety limits this.
- ⚠️ Middleware execution order is forward-only (fold left) — Commanded reverses for `after_dispatch`/`after_failure`. In Instructed, all three stages use the same order.

### Gaps & Issues
- **LOW**: No built-in `ExtractAggregateIdentity` middleware — 📝 handled inline in router. Functionally equivalent.
- **LOW**: No built-in `ConsistencyGuarantee` middleware — 📝 handled inline in router dispatch flow.
- **MEDIUM**: Middleware `after_dispatch`/`after_failure` execution order is not reversed. In Commanded, user MW1's `after_dispatch` runs LAST (onion model). In Instructed, user MW1's `after_dispatch` runs FIRST. This affects middleware that depends on ordering (e.g., timing middleware).
- **LOW**: `assigns` limited to `Dict(String, String)` — cannot store arbitrary typed values. This is a Gleam type system constraint.

---

## 4. Event Handlers [⚠️]

### What Commanded Provides
- Event handler is a GenServer wrapping a persistent subscription
- Config options: `name`, `consistency`, `start_from`, `subscribe_to`, `concurrency`, `batch_size`, `state`
- Singleton guarantee via event store subscription name uniqueness + registration
- `handle/2` receives domain event + enriched metadata map (atom keys for system, string keys for user)
- Return values: `:ok`, `{:ok, new_state}`, `{:error, :already_seen_event}`, `{:error, reason}`
- Catch-all default `handle/2` injected at compile time — unhandled events silently acked
- Error handling chain: per-handler `error/3` → app-level `on_event_handler_error` → default (stop)
- `FailureContext` with handler_state, context map (persists across retries), stacktrace
- Subscription retry with exponential backoff (1s–1min, jitter)
- `after_start/1` callback for post-subscription initialization
- `before_reset/0` callback for read model cleanup before replay
- Mix task `mix commanded.reset` for handler reset
- Telemetry spans for event handling
- State is transient (in-process, lost on restart)

### What Instructed Provides
- ✅ Event handler is an OTP Actor wrapping a persistent subscription
- ✅ Config: `name`, `consistency`, `start_from`, `stream_id` (AllStreams/SpecificStream), `initial_state`
- ✅ `handle_event` receives event data, full `RecordedEvent`, and handler state; returns `Result(handler_state, String)`
- ✅ Idempotency via `last_seen_event` guard (skips already-processed events)
- ✅ Error handling via optional `on_error` callback with Retry/RetryWithDelay/Skip/Stop strategies
- ✅ Event acknowledgment after successful processing
- ✅ Strong consistency acking via subscriptions integration
- ✅ Upcasting integration
- ✅ Telemetry events emitted for handle start/stop/exception
- ⚠️ No `after_start` callback
- ⚠️ No `before_reset` or reset mechanism
- ⚠️ No subscription retry with backoff — on SubscriptionAlreadyExists, deletes and recreates subscription
- ⚠️ No `concurrency` option (single handler only)
- ⚠️ No `batch_size` option (no batch processing)
- ❌ No `{:error, :already_seen_event}` special return — idempotency only via `last_seen_event` guard
- ⚠️ Error handling: when `on_error` is `None`, the default behavior acks the event and continues (`handle_error` in `event_handler.gleam` line ~290) — this **silently swallows errors**, losing events. Commanded's default is to STOP the handler.

### Gaps & Issues
- **CRITICAL**: Default error handling silently swallows errors. In `handle_error` when `on_error` is `None`: the handler acks the event and continues (`actor.continue(state)`). The comment says "Default: stop on error (matching Commanded's default)" but the actual code does NOT stop — it continues. Events are acknowledged and lost. Reference: `event_handler.gleam` `handle_error` function, `None ->` branch.
- **HIGH**: Subscription handling on restart: when SubscriptionAlreadyExists, handler deletes and recreates the subscription. For the in-memory adapter, this loses the subscription position, causing full replay. For production adapters, this could cause duplicate event processing if the adapter doesn't preserve position across delete+recreate.
- **MEDIUM**: No subscription retry with backoff. Commanded uses exponential backoff (1s–1min with jitter) for subscription failures. Instructed fails immediately on subscription error.
- **MEDIUM**: No `concurrency` option — only single-handler mode.
- **MEDIUM**: No `batch_size` / batch processing support.
- **LOW**: No `after_start` or `before_reset` lifecycle callbacks.
- **MEDIUM**: Retry logic in error handler only retries ONCE. If Retry or RetryWithDelay fails, the handler acks and continues (silently dropping the error). Commanded's retry is recursive — it keeps retrying as long as the error callback says to retry.

---

## 5. Projections [⚠️]

### What Commanded Provides
- Projections are event handlers with consistency: :strong for POST/Redirect/GET pattern
- `commanded_ecto_projections` library provides `project/2` and `project/3` macros wrapping Ecto.Multi for atomic updates
- Full reset support via Mix task + `before_reset/0` callback to truncate read model tables
- Projections can be rebuilt by deleting subscription and replaying from origin
- Strong consistency ensures read model is up-to-date when dispatch returns

### What Instructed Provides
- ✅ Dedicated `projection.gleam` module with `ProjectionConfig` and actor-based lifecycle
- ✅ `handle_event` callback receives event, recorded_event, and projection state
- ✅ `get_state` function to query current projection state
- ✅ Idempotency via `last_seen_event` guard
- ✅ Error handling via optional `on_error` callback with Skip/Retry/RetryWithDelay/Stop
- ⚠️ Projections are in-memory only — no integration with database (Ecto/Sqlight/Pog)
- ⚠️ No `consistency` option — projections cannot be configured for strong consistency
- ⚠️ No reset mechanism (no `before_reset`, no Mix task equivalent)
- ⚠️ Subscription handling: on SubscriptionAlreadyExists, deletes and recreates — same issue as event handlers

### Gaps & Issues
- **HIGH**: No strong consistency support for projections. Projections cannot register with the subscriptions actor, so dispatching with `consistency: Strong` does not wait for projections to update. This breaks the POST/Redirect/GET pattern.
- **MEDIUM**: In-memory projections only — no database persistence integration. State is lost on restart. Production use requires a database-backed projection pattern.
- **MEDIUM**: No reset/rebuild mechanism for projections.
- **MEDIUM**: Same subscription delete+recreate issue as event handlers on restart.
- **MEDIUM**: Error handling on retry failure: same as event handlers — retry only once, then ack and continue on second failure.

---

## 6. Process Managers [⚠️]

### What Commanded Provides
- Three-layer architecture: ProcessRouter (GenServer) → DynamicSupervisor → ProcessManagerInstance (GenServer per UUID)
- `interested?/1,2` with return types: `{:start, uuid}`, `{:start!, uuid}`, `{:continue, uuid}`, `{:continue!, uuid}`, `{:stop, uuid}`, `false`; supports list of UUIDs for multi-instance routing
- Execution order: handle → dispatch commands → apply → persist snapshot → ack → after_command
- `handle/2,3`, `apply/2,3`, `after_command/2,3`, `error/3` callbacks
- Error handling distinguishes event errors vs command dispatch errors with different strategies
- Command dispatch errors support: `{:continue, commands, context}`, `{:skip, :discard_pending}`, `{:skip, :continue_pending}`
- State persistence via snapshots after every event; snapshot source_version = event_number (global)
- Idempotency via last_seen_event from snapshot source_version
- Event timeout and idle timeout
- Strict routing validation: `:start!` fails if instance already exists; `:continue!` fails if instance doesn't exist
- Causation chain: commands dispatched with causation_id = event_id, correlation_id = event's correlation_id

### What Instructed Provides
- ✅ Single actor manages all instances in a Dict — simplified architecture
- ✅ `Interest` type with Start/StartStrict/StartMany/Continue/ContinueStrict/ContinueMany/Stop/StopMany/Skip
- ✅ Correct execution order: handle → dispatch → apply → persist snapshot → ack
- ✅ `handle` callback receives pm_state, event data, and full RecordedEvent
- ✅ `apply_event` callback for state mutation after command dispatch
- ✅ `after_command` callback with AfterContinue/AfterStop actions
- ✅ Separate error handling for event errors (`on_event_error`) and command errors (`on_command_error`)
- ✅ Command error strategies: CmdRetry, CmdRetryWithDelay, CmdSkip, CmdDiscardPending, CmdContinueWith, CmdStop
- ✅ State persistence via snapshots with event_number as source_version
- ✅ Idempotency via last_seen_event restored from snapshot
- ✅ Strict routing validation (StartStrict/ContinueStrict)
- ✅ Causation chain propagation: causation_id = event_id, correlation_id from event
- ✅ Strong consistency acking via subscriptions integration
- ✅ Fan-out routing with StartMany/ContinueMany/StopMany
- ⚠️ Single actor for all instances — no per-instance process isolation
- ⚠️ No event timeout or idle timeout
- ⚠️ Strict routing checks memory Dict only, not snapshot store — a restarted PM instance that was loaded from snapshot would not be detected by `instance_exists` until the snapshot is loaded

### Gaps & Issues
- **MEDIUM**: Single actor for all instances means a blocked command dispatch in one instance blocks event processing for ALL instances. Commanded's per-instance GenServer isolates this.
- **MEDIUM**: No event timeout — if a PM instance blocks (e.g., slow command dispatch), there's no mechanism to detect and recover.
- **MEDIUM**: No idle timeout — PM instances remain in memory forever. Commanded supports idle timeout to free memory.
- **MEDIUM**: Strict routing (`StartStrict`/`ContinueStrict`) only checks in-memory Dict, not snapshot store. If the PM restarts and instances aren't loaded yet, `StartStrict` would succeed even if the instance previously existed (because it's not in the Dict). Commanded checks `ProcessManagerInstance.new?/1` which looks at `last_seen_event` from the snapshot.
- **LOW**: No `apply/3` variant — apply only receives state and event, not enriched metadata.
- **LOW**: `handle_routing_error` function is a no-op for most error actions — Retry, RetryWithDelay, and Stop all just return `state` unchanged without actually performing the action.

---

## 7. Snapshots [⚠️]

### What Commanded Provides
- Per-aggregate snapshot configuration: `snapshot_every: N`, `snapshot_version: N`
- Snapshots stored via event store adapter (`record_snapshot/2`, `read_snapshot/2`, `delete_snapshot/2`)
- Version checking: `snapshot_module_version` in snapshot metadata; old snapshots invalidated when version changes
- Aggregate snapshots: stored after N events, using stream_version as source_version
- PM snapshots: stored after EVERY event, using event_number as source_version, deleted on stop
- Snapshot format goes through serializer (JSON by default with `Jason.Encoder`)
- Interaction with lifespan: snapshot taken before lifespan timeout applied

### What Instructed Provides
- ✅ `SnapshotConfig` with `snapshot_every` and `snapshot_version` fields
- ✅ `SnapshotData` record with source_uuid, source_version, source_type, data, created_at
- ✅ `snapshot_required` function checks threshold
- ✅ Aggregate snapshots: taken by aggregate_server after configured number of events
- ✅ PM snapshots: stored after every event with event_number as source_version
- ✅ PM snapshot deletion on stop
- ✅ `snapshot.coerce` for type bridging between aggregate state and event store types
- ⚠️ No snapshot version checking during read — `populate_from_event_store` reads the snapshot but does NOT check `snapshot_version` against the config. Stale snapshots from schema changes would be used silently, potentially causing incorrect state.
- ⚠️ Snapshot data is typed (`SnapshotData(data)`) with unsafe coercion via `snapshot.coerce` — works at Erlang runtime level but type-unsafe

### Gaps & Issues
- **HIGH**: No snapshot version validation on read. In `aggregate.gleam` `populate_from_event_store`, the snapshot is read and used without checking if its version matches `snapshot_config.snapshot_version`. If the aggregate schema changes and `snapshot_version` is incremented, old snapshots should be ignored and full replay triggered. Currently, stale snapshots are silently used, potentially producing incorrect aggregate state. Reference: `aggregate.gleam` lines ~105-115.
- **MEDIUM**: `snapshot.coerce` uses `unsafe_coerce` which bypasses type safety. If the snapshot data doesn't match the expected type, this will cause runtime errors (Erlang pattern match failures) without clear error messages.
- **LOW**: No serialization integration — snapshots store raw Gleam terms. Production adapters (postgres/sqlite) would need to handle serialization separately.

---

## 8. Event Store Interface [⚠️]

### What Commanded Provides
- `EventStore.Adapter` behaviour with full callback spec
- `append_to_stream/5` with `expected_version` semantics: `:any_version`, `:no_stream`, `:stream_exists`, non-negative integer
- `stream_forward/4` returns lazy stream (Enumerable) with batch size
- `subscribe/2` for transient subscriptions (no ack required)
- `subscribe_to/6` for persistent subscriptions with concurrency_limit, partition_by options
- `ack_event/3`, `unsubscribe/2`, `delete_subscription/3`
- `read_snapshot/2`, `record_snapshot/2`, `delete_snapshot/2`
- In-memory adapter: full OCC, serialization support, persistent subscription with back-pressure, partition routing
- Expected version: `:any_version`, `:no_stream`, `:stream_exists`, `0`, positive integer N

### What Instructed Provides
- ✅ `EventStore(event)` record of functions — idiomatic Gleam equivalent of the adapter behaviour
- ✅ `ExpectedVersion`: AnyVersion, NoStream, StreamExists, ExactVersion(Int)
- ✅ `append_to_stream` with OCC checking
- ✅ `read_stream_forward` with start version and batch size
- ✅ `subscribe` / `subscribe_to_stream` for transient subscriptions
- ✅ `subscribe_persistent` with stream, name, start_from, handler
- ✅ `ack_event`, `unsubscribe`, `delete_subscription`
- ✅ `read_snapshot`, `record_snapshot`, `delete_snapshot`
- ✅ `read_all_forward`, `get_latest_event_number` — additional convenience functions
- ✅ `reset` function for testing
- ✅ In-memory adapter with full OCC, persistent subscriptions with back-pressure (one-at-a-time delivery)
- ⚠️ No `concurrency_limit` or `partition_by` on persistent subscriptions
- ⚠️ No lazy stream — `read_stream_forward` returns a `List` not an Enumerable/Stream
- ⚠️ In-memory adapter: `subscribe_persistent` handler callback runs in the event store's process context, which could block the store if the handler is slow

### Gaps & Issues
- **LOW**: No lazy stream for `read_stream_forward` — returns materialized List. For very large streams, this could cause memory issues. The batched reading pattern mitigates this.
- **LOW**: No `concurrency_limit` / `partition_by` on subscriptions — 📝 these are advanced features, not needed for basic operation.
- **MEDIUM**: Persistent subscription handler callback runs synchronously in the event store actor's process. If the handler (which sends a message to another actor) is slow or the target actor's mailbox is full, this blocks the event store from processing other requests. Commanded's adapter sends events asynchronously via `send/2`.
- **LOW**: No serialization support in the in-memory adapter — events stored as raw Gleam terms. This is fine for testing.

---

## 9. Optimistic Concurrency Control [✅]

### What Commanded Provides
- `append_to_stream` checks `expected_version` against current stream version
- On `{:error, :wrong_expected_version}`, the aggregate rebuilds state from new events and retries the command
- Default 10 retry attempts; returns `{:error, :too_many_attempts}` when exhausted
- Retry reads only events since last known version (incremental, not full replay)
- OCC is the fundamental consistency mechanism — no distributed locks

### What Instructed Provides
- ✅ `ExactVersion(Int)` expected version on `append_to_stream` — in-memory adapter checks `current_version == v`
- ✅ On `WrongExpectedVersion`, aggregate server rebuilds from current version (incremental) and retries
- ✅ Default 10 retry attempts; returns `TooManyAttempts` when exhausted
- ✅ `rebuild_from_current_version` reads only events after current version (not full replay)
- ✅ Also handles `StreamAlreadyExists` as a version conflict (mapped to `WrongExpectedVersion`)

### Gaps & Issues
- No significant gaps. OCC is correctly implemented.

---

## 10. Command Serialization per Aggregate [✅]

### What Commanded Provides
- Each aggregate instance is a separate GenServer process
- GenServer mailbox guarantees FIFO ordering — commands to the same aggregate are serialized
- Commands to different aggregates execute concurrently (different processes)
- DynamicSupervisor manages aggregate processes; registration prevents duplicates

### What Instructed Provides
- ✅ Each aggregate instance runs as a separate OTP Actor
- ✅ Actor mailbox guarantees FIFO ordering — commands serialized per instance
- ✅ Different aggregate instances can run concurrently (separate actors)
- ✅ Registry actor manages aggregate servers per stream_id, preventing duplicates
- ⚠️ Registry actor is a single bottleneck for all get-or-start operations (serialized lookups)

### Gaps & Issues
- **LOW**: Registry actor serializes all get-or-start operations across all aggregate instances. Under high load, this could become a bottleneck. Commanded uses a DynamicSupervisor with Registration adapter which can be distributed. This is a scalability concern, not a correctness issue.

---

## 11. Event Ordering [⚠️]

### What Commanded Provides
- `event_number`: globally unique, monotonically incrementing across ALL streams (gapless)
- `stream_version`: per-stream sequential version starting at 1
- Event handlers receive events in `event_number` order via persistent subscription to `:all`
- Per-stream reads return events in `stream_version` order
- In-memory adapter: events stored in append order; `event_number` assigned sequentially

### What Instructed Provides
- ✅ `event_number`: globally incrementing integer assigned by in-memory adapter
- ✅ `stream_version`: per-stream sequential version starting at 1
- ✅ In-memory adapter stores events in append order
- ✅ Persistent subscriptions deliver events in order via one-at-a-time back-pressure model
- ⚠️ Event handlers and PMs subscribe to "$all" — events delivered in global `event_number` order

### Gaps & Issues
- **LOW**: Event ordering is correctly maintained in the in-memory adapter. Production adapters (postgres/sqlite) must maintain this invariant independently — this is an adapter-level concern.

---

## 12. Strong vs Eventual Consistency [⚠️]

### What Commanded Provides
- Handler-level: `consistency: :eventual` (default) or `consistency: :strong`
- Dispatch-level: `consistency: :eventual` (default), `:strong`, or `[Module1, "HandlerName"]` (selective)
- `ConsistencyGuarantee` middleware: blocks dispatch until all strong handlers have acked
- `Subscriptions` GenServer with ETS tracking: handler name + stream_id → version acked
- `wait_for/5` blocks caller, receives notification via PubSub broadcast
- Default timeout: 5 seconds (configurable)
- Strong consistency + concurrency > 1 → compile error
- Dispatcher PID excluded from wait_for to prevent deadlock
- Ack entries have TTL (1 hour default) with periodic purging

### What Instructed Provides
- ✅ `Consistency` type: `Eventual` | `Strong`
- ✅ Handler-level consistency config via `with_consistency`
- ✅ Subscriptions actor with registration, ack tracking, and wait_for
- ✅ Router integrates with subscriptions via `wait_for` after successful dispatch
- ✅ Event handlers ack subscriptions on successful processing when `consistency: Strong`
- ✅ Process managers ack subscriptions on successful processing when `consistency: Strong`
- ✅ `wait_for` blocks until all registered strong handlers have acked >= stream_version
- ✅ Default timeout matches dispatch_timeout (5000ms)
- ❌ No selective consistency (`[Module1, "HandlerName"]`) — only `:strong` (all) or `:eventual`
- ❌ No dispatcher PID exclusion — potential deadlock if a strong-consistency handler dispatches another command with strong consistency
- ⚠️ No TTL/purging of ack entries — subscriptions actor accumulates entries indefinitely
- ⚠️ Projections cannot participate in strong consistency (no subscriptions integration in projection.gleam)

### Gaps & Issues
- **HIGH**: No dispatcher PID exclusion from `wait_for`. If a strong-consistency handler dispatches another command with `consistency: Strong` inside its `handle_event` callback, the inner dispatch will wait for the same handler to ack. But the handler is blocked processing the current event. Result: **deadlock**. Commanded prevents this by storing `dispatcher_pid` in `before_dispatch` and passing `exclude: [dispatcher_pid]` to `wait_for`.
- **MEDIUM**: No selective consistency — cannot wait for specific handlers by name/module. Must wait for ALL registered strong handlers.
- **MEDIUM**: No TTL/purging in subscriptions actor — ack entries accumulate indefinitely in the Dict, causing memory growth over time.
- **MEDIUM**: Projections cannot register as strong-consistency handlers — `projection.gleam` has no `consistency` field or subscriptions integration.

---

## 13. Error Handling & Retry Strategies [⚠️]

### What Commanded Provides

#### Event Handlers
- `error/3` callback with `FailureContext` (handler_state, context map, stacktrace)
- Return values: `{:retry, context}`, `{:retry, delay, context}`, `:skip`, `{:stop, reason}`
- Context map persists across retries (user can track failure count)
- Application-level default: `:stop` (configurable to `:backoff` or custom module)
- Built-in backoff: `failures² × 1000 + jitter` ms, clamped to [1s, 24h]
- Recursive retry — keeps retrying as long as error callback returns retry

#### Process Managers
- Separate error paths for event handling vs command dispatch
- Event errors: retry/skip/stop
- Command dispatch errors: retry/skip/continue/stop plus `{:skip, :discard_pending}`, `{:skip, :continue_pending}`, `{:continue, commands, context}`
- `FailureContext` includes `pending_commands`, `process_manager_state`, `enriched_metadata`

#### Aggregates
- On wrong expected version: rebuild + retry (up to 10 attempts)
- On aggregate process death: retry dispatch
- Exceptions caught, returned as `{:error, error}` to caller

### What Instructed Provides
- ✅ `ErrorAction` type: Retry(context), RetryWithDelay(delay, context), Skip, Stop(reason)
- ✅ `PMCommandErrorAction` type: CmdRetry, CmdRetryWithDelay, CmdSkip, CmdDiscardPending, CmdContinueWith, CmdStop
- ✅ `FailureContext` record with context, handler_state, failure_count, last_error, stacktrace
- ✅ Aggregate OCC retry with rebuild (up to 10 attempts)
- ⚠️ Event handler retry only attempts ONCE — if the retry fails, event is acked and lost
- ⚠️ No recursive retry loop in event handlers — Commanded retries indefinitely until error callback says stop
- ⚠️ `FailureContext` is defined in `error.gleam` but never actually constructed or passed to error callbacks
- ❌ No application-level error handler configuration
- ❌ No built-in backoff strategy (exponential with jitter)
- ❌ No context map that persists across retries in event handlers — the handler state IS the context

### Gaps & Issues
- **CRITICAL**: Event handler retry is not recursive. In `event_handler.gleam` `handle_error`, when `on_error` returns `Retry(new_state)`, the handler tries once more. If the second attempt fails, the event is acked and the handler continues — **the event is silently lost**. Commanded retries in a recursive loop, calling `error/3` again on each failure, allowing infinite retries with backoff. This is a fundamental correctness issue for production use.
- **HIGH**: `FailureContext` is defined but never used. Error callbacks receive `(String, RecordedEvent(event), handler_state)` — not a `FailureContext`. There's no way for error handlers to track retry count across attempts or access a persistent context map.
- **HIGH**: No application-level error handler — every handler must define its own `on_error` or get the broken default (silent swallow).
- **MEDIUM**: No built-in exponential backoff strategy.

---

## 14. Causation & Correlation ID Tracking [✅]

### What Commanded Provides
- Every command gets a `command_uuid` (auto-generated)
- Events created by a command have `causation_id = command_uuid`
- `correlation_id` propagated from command to events
- Process managers: commands dispatched with `causation_id = event_id`, `correlation_id = event.correlation_id`
- Creates traceable chain: command → events → PM command → events → ...
- Metadata from source event propagated to downstream commands

### What Instructed Provides
- ✅ `dispatch` generates `command_id` via `uuid.v4_string()` and uses it as `causation_id` on events
- ✅ `correlation_id` generated and propagated through events
- ✅ Process manager dispatches commands with `causation_id = event_id`, `correlation_id = event.correlation_id`
- ✅ Full causation chain test (`causation_chain_test.gleam`, 391 lines)
- ✅ `dispatch_with_context` allows explicit causation/correlation IDs

### Gaps & Issues
- No significant gaps. Causation and correlation tracking is well-implemented and tested.

---

## 15. Idempotency [⚠️]

### What Commanded Provides
- Event handlers: `last_seen_event` guard (in-memory) + durable subscription position (event store checkpoint)
- `{:error, :already_seen_event}` return from `handle/2` — skip without calling `error/3`
- Process managers: `last_seen_event` restored from snapshot `source_version` on restart
- ProcessRouter-level deduplication (filters entire batches before routing to instances)
- Aggregate self-subscription: validates `stream_version == aggregate_version + 1`

### What Instructed Provides
- ✅ Event handlers: `last_seen_event` guard skips already-processed events
- ✅ Process managers: `last_seen_event` restored from snapshot source_version
- ✅ Per-instance idempotency in PMs (checks before processing)
- ✅ Aggregate self-subscription: checks `stream_version > aggregate_version`
- ✅ Projections: `last_seen_event` guard
- ⚠️ No `{:error, :already_seen_event}` special return — idempotency only via `last_seen_event` guard
- ⚠️ `last_seen_event` is transient (in-memory) for event handlers — lost on handler restart. Relies entirely on durable subscription checkpoint for restart idempotency
- ⚠️ Subscription delete+recreate on restart (SubscriptionAlreadyExists handling) may cause position loss in some adapters

### Gaps & Issues
- **HIGH**: Subscription position can be lost on handler restart. When a handler starts and the subscription already exists, Instructed deletes it and recreates (`event_handler.gleam` start function, `Error(error.SubscriptionAlreadyExists)` branch). For the in-memory adapter, this resets the checkpoint to `start_from`, causing full event replay and potential duplicate processing. For production adapters, behavior depends on whether delete+recreate preserves position.
- **MEDIUM**: No application-level idempotency guarantee beyond `last_seen_event`. Commanded's `{:error, :already_seen_event}` allows handlers to implement domain-level dedup. Instructed handlers would need to track this manually in handler state.

---

## 16. Multi Module [✅]

### What Commanded Provides
- `Multi.new(aggregate)` → `Multi.execute(multi, fn)` → `Multi.run(multi)` chain
- Named steps with 2-arity functions accessing intermediate state snapshots
- `Multi.reduce(multi, enumerable, fn)` for iterating over collections
- Nested Multi support (recursive `Multi.run/1`)
- Returned from `execute/2` — aggregate process detects `%Multi{}` and calls `Multi.run/1`
- Error in any step short-circuits entire chain via throw/catch
- On error: no events persisted, aggregate state unchanged

### What Instructed Provides
- ✅ `multi.new(state)` → `multi.execute(multi, fn)` → `multi.to_result()` chain
- ✅ `multi.apply(multi, apply_fn)` updates internal state between stages
- ✅ `multi.reduce(multi, items, execute_fn, apply_fn)` for collections
- ✅ Error short-circuits chain — on error, all events discarded
- ✅ `get_state`, `get_events`, `has_error` inspection functions
- ⚠️ No named steps — cannot access intermediate state by step name
- ⚠️ No nested Multi support — Multi returns `Result(List(event), String)`, not another Multi
- 📝 Multi is external to aggregate — used within `execute` via `multi.to_result()`, not returned directly

### Gaps & Issues
- **LOW**: No named steps — Commanded's named step feature (`Multi.execute(multi, :step_name, fn)`) with 2-arity access to `steps_map` is not available. The `multi.apply` function serves a similar purpose (updating state between stages).
- **LOW**: No nested Multi — cannot compose Multi chains recursively. Use flat composition instead.

---

## 17. Aggregate Lifespan Management [✅]

### What Commanded Provides
- `AggregateLifespan` behaviour with `after_event/1`, `after_command/1`, `after_error/1`
- Return types: timeout (integer ms), `:infinity`, `:hibernate`, `:stop`, `{:stop, reason}`
- `DefaultLifespan`: runs forever except stops on exceptions
- `after_event/1` called with the LAST event when command produces events
- `after_command/1` called when command produces NO events
- Applied via GenServer reply tuples with timeout parameter
- Invalid lifespan returns logged as warnings, default to `:infinity`
- Snapshot taken before lifespan timeout applied

### What Instructed Provides
- ✅ `Lifespan` record with `after_command`, `after_error`, `after_event` functions
- ✅ `LifespanDecision`: KeepRunning, Stop, StopAfter(ms), Hibernate (falls back to KeepRunning)
- ✅ `always_running()`, `new_idle(ms)`, `stop_after_command()` convenience constructors
- ✅ Timer-based idle timeout via `process.send_after` with timer cancellation on new commands
- ✅ `after_event` called for externally-applied events (self-subscription)
- ⚠️ `after_command` called regardless of whether events were produced (Commanded distinguishes: `after_event` for events, `after_command` for no events)
- ⚠️ Hibernate falls back to KeepRunning — Erlang-level hibernation not exposed through Gleam's actor API
- ⚠️ Snapshot is NOT deferred for lifespan timeout — snapshot taken before lifespan decision. This matches Commanded's behavior.

### Gaps & Issues
- **LOW**: `after_command` vs `after_event` semantics differ slightly. Commanded calls `after_event(last_event)` when events are produced, `after_command(command)` when no events. Instructed calls `after_command(state, command)` always. This is a minor semantic difference.
- **LOW**: Hibernate not supported — 📝 Gleam's actor abstraction doesn't expose Erlang hibernation. Falls back to KeepRunning.

---

## 18. Composite Router [❌]

### What Commanded Provides
- `CompositeRouter` combines multiple routers into one
- Detects duplicate command registrations across child routers at compile time
- Dispatches by pattern-matching command struct to originating child router
- Can nest: composite routers can include other composite routers
- `Commanded.Application` itself uses `CompositeRouter` internally

### What Instructed Provides
- ❌ No composite router concept
- 📝 Single router per application — runtime configuration rather than compile-time composition

### Gaps & Issues
- **LOW**: No composite router — 📝 Gleam doesn't have macros for compile-time routing. The runtime router pattern handles single-aggregate-type routing. For multi-aggregate applications, users would need to build their own dispatch layer or use the Application module with multiple routers. This is a design limitation, not a bug.

---

## 19. Application Supervision Tree [⚠️]

### What Commanded Provides
- `Application` is an OTP Supervisor starting all infrastructure:
  - Event store adapter children
  - PubSub adapter children
  - Registration adapter children
  - Task.Supervisor for command dispatch
  - Aggregates.Supervisor (DynamicSupervisor)
  - Subscriptions.Registry
  - Subscriptions GenServer
- `runtime_config/4` merges compile-time, app env, start_link opts, and `init/1` callback
- Dynamic named applications for multi-tenancy
- `hibernate_after` option for memory optimization

### What Instructed Provides
- ✅ `Application` struct grouping event store, router, and optional subscriptions
- ✅ `start` function returns an Application struct
- ✅ Helper functions: `dispatch`, `start_event_handler`, `start_projection`, `start_process_manager`
- ✅ Multi-tenancy via separate Application instances with separate event stores
- 📝 Application is a plain struct, NOT an OTP supervisor — supervision is external
- ⚠️ No automatic infrastructure startup — event store must be started separately
- ⚠️ No DynamicSupervisor for aggregates — the router's registry actor manages aggregate servers
- ⚠️ No supervision of event handlers, projections, or process managers — they are started but not supervised
- ⚠️ No Task.Supervisor for command dispatch isolation

### Gaps & Issues
- **HIGH**: No supervision tree. Event handlers, projections, and process managers are started as bare actors without supervision. If any crashes, it is not restarted. Commanded supervises all components under `Application.Supervisor` with `one_for_one` strategy. Users must build their own supervision tree using `gleam/otp/static_supervisor`.
- **HIGH**: Aggregate server processes are not supervised. The registry actor holds references but doesn't monitor them. If an aggregate server crashes, the registry still holds the dead Subject, causing subsequent dispatches to fail with timeout.
- **MEDIUM**: No automatic infrastructure wiring — users must manually start event store, subscriptions, create router, create application, and start handlers. Commanded does this automatically in the supervision tree.

---

## 20. Event Upcasting [✅]

### What Commanded Provides
- `Upcaster` protocol with `upcast(event, metadata)` callback
- Fallback to `Any` implementation (identity, no-op)
- Applied at read time: aggregate state rebuild, event handler delivery, PM delivery
- Metadata includes all system fields + user metadata
- Chaining: manual (call other upcasters inside your implementation)

### What Instructed Provides
- ✅ `Upcaster(event)` record with `upcast` function taking `RecordedEvent(event)` and returning `RecordedEvent(event)`
- ✅ `identity()` — no-op upcaster
- ✅ `apply` and `apply_all` for single/batch upcasting
- ✅ `chain(first, second)` and `chain_all(list)` for composing upcasters
- ✅ Applied in aggregate server (state rebuild via `effective_event_store` wrapper)
- ✅ Applied in event handlers (before delivery via handler callback wrapper)
- ✅ Applied in process managers (before delivery via handler callback wrapper)
- ✅ Tests cover basic upcasting, chaining, and identity

### Gaps & Issues
- No significant gaps. Upcasting is well-implemented with explicit chaining support (better than Commanded's manual chaining).

---

## 21. Telemetry & Observability [⚠️]

### What Commanded Provides
- Full `:telemetry.span/3` integration with `:start`, `:stop`, `:exception` suffixes
- Events for: command dispatch, aggregate execute, aggregate populate, event handler, batch handler, process manager, event store operations
- Rich metadata: application, handler info, events, errors, stacktraces, durations
- Standard Erlang `:telemetry` library — pluggable handlers, metrics, tracing

### What Instructed Provides
- ✅ `TelemetryEvent` union type for all instrumentation events
- ✅ Events for: CommandDispatchStart/Stop/Exception, AggregateExecuteStart/Stop/Exception, EventHandleStart/Stop/Exception, ProcessManagerHandleStart/Stop/Exception
- ✅ Gleam-first handler via `set_handler/1` (registers a callback function)
- ✅ Optional Erlang `:telemetry` emission via FFI (`instructed_telemetry_ffi`)
- ✅ Convenience emit helpers for all event types
- ✅ Duration measurement via monotonic time
- ⚠️ No aggregate populate telemetry (state rebuild from events)
- ⚠️ No event store operation telemetry
- ⚠️ Less metadata than Commanded — no aggregate_state, handler_state, full event list in telemetry

### Gaps & Issues
- **LOW**: No aggregate populate (state rebuild) telemetry — useful for diagnosing slow startups.
- **LOW**: No event store operation telemetry — useful for diagnosing adapter performance.
- **LOW**: Telemetry metadata is simpler than Commanded's — missing aggregate state, events list, handler state in event metadata. Sufficient for basic observability.

---

## 22. Batch Processing & Concurrency in Handlers [❌]

### What Commanded Provides
- `batch_size` option activates batch mode with `handle_batch/1` callback
- `concurrency` option creates multiple handler processes under a `Handler.Supervisor`
- `partition_by` callback for consistent event routing to concurrent workers
- Batch acknowledgment: only last event in batch is acked (implicit ack of all preceding)
- Compile-time validation: cannot have both `handle/2` and `handle_batch/1`; cannot use `batch_size` with `concurrency`; cannot use `concurrency > 1` with `:strong` consistency
- Batch error handling: `error/3` with `failed_event: nil`, `:skip` skips entire batch

### What Instructed Provides
- ❌ No `batch_size` option or `handle_batch` callback
- ❌ No `concurrency` option or multi-handler supervisor
- ❌ No `partition_by` callback
- Events processed one at a time only

### Gaps & Issues
- **MEDIUM**: No batch processing — for high-throughput systems, batch processing significantly improves performance by reducing per-event overhead (ack roundtrips, DB transactions).
- **MEDIUM**: No concurrent handlers — cannot scale event processing across multiple processes for a single handler.
- **LOW**: No partition_by — 📝 requires concurrency support first.

---

# Summary

## Feature Parity Scorecard

| # | Feature | Status | Severity of Gaps |
|---|---------|--------|-----------------|
| 1 | Aggregates | ⚠️ | LOW |
| 2 | Command Routing & Dispatch | ⚠️ | HIGH (registry cleanup) |
| 3 | Middleware Pipeline | ⚠️ | MEDIUM (ordering) |
| 4 | Event Handlers | ⚠️ | CRITICAL (silent error swallow) |
| 5 | Projections | ⚠️ | HIGH (no strong consistency) |
| 6 | Process Managers | ⚠️ | MEDIUM (single actor) |
| 7 | Snapshots | ⚠️ | HIGH (no version check) |
| 8 | Event Store Interface | ⚠️ | LOW |
| 9 | Optimistic Concurrency Control | ✅ | — |
| 10 | Command Serialization per Aggregate | ✅ | LOW |
| 11 | Event Ordering | ⚠️ | LOW |
| 12 | Strong vs Eventual Consistency | ⚠️ | HIGH (deadlock risk) |
| 13 | Error Handling & Retry Strategies | ⚠️ | CRITICAL (single retry) |
| 14 | Causation & Correlation ID Tracking | ✅ | — |
| 15 | Idempotency | ⚠️ | HIGH (subscription position loss) |
| 16 | Multi Module | ✅ | LOW |
| 17 | Aggregate Lifespan Management | ✅ | LOW |
| 18 | Composite Router | ❌ | LOW |
| 19 | Application Supervision Tree | ⚠️ | HIGH (no supervision) |
| 20 | Event Upcasting | ✅ | — |
| 21 | Telemetry & Observability | ⚠️ | LOW |
| 22 | Batch Processing & Concurrency | ❌ | MEDIUM |

## Critical Issues (Must Fix for Production Use)

### 1. CRITICAL: Event handler default error handling silently swallows errors
**Location**: `event_handler.gleam`, `handle_error` function, `None ->` branch  
**Problem**: When no `on_error` callback is configured, the handler acks the event and continues processing. Events that fail are silently lost.  
**Expected**: Stop the handler (matching Commanded's default `ErrorHandler.stop_on_error`).  
**Fix**: Change `None -> { ack_event(...); actor.continue(state) }` to `None -> { actor.stop() }` (or at minimum, don't ack the event).

### 2. CRITICAL: Event handler retry is not recursive
**Location**: `event_handler.gleam`, `handle_error` function, `Retry(new_state) ->` and `RetryWithDelay(delay_ms, new_state) ->` branches  
**Problem**: On retry failure, the handler acks the event and continues — the event is lost. Should re-call `error/3` (recursive retry loop).  
**Fix**: Implement recursive retry: on retry failure, call the `on_error` callback again with incremented failure count, allowing the callback to decide whether to retry again, skip, or stop.

### 3. HIGH: No snapshot version validation
**Location**: `aggregate.gleam`, `populate_from_event_store` function  
**Problem**: Snapshot is read and used without checking `snapshot_version` against config. Schema changes invalidate snapshot data but stale snapshots are used, producing incorrect aggregate state.  
**Fix**: Compare `snapshot_config.snapshot_version` against stored snapshot metadata, discard if mismatched.

### 4. HIGH: Registry doesn't monitor aggregate server processes
**Location**: `router.gleam`, `handle_registry_message` function  
**Problem**: Dead aggregate servers remain in the registry Dict. Subsequent dispatches get a dead Subject and time out.  
**Fix**: Monitor started aggregate server processes (via `process.monitor`) and remove from Dict on death.

### 5. HIGH: Strong consistency deadlock risk
**Location**: `router.gleam`, `dispatch_through_server` function / `subscriptions.gleam`  
**Problem**: No dispatcher PID exclusion in `wait_for`. Recursive strong-consistency dispatch from within a handler causes deadlock.  
**Fix**: Pass the dispatcher PID to `wait_for` and exclude it from the set of handlers being waited on.

### 6. HIGH: Subscription position loss on handler restart
**Location**: `event_handler.gleam`, `start` function, `Error(error.SubscriptionAlreadyExists)` branch  
**Problem**: Deletes and recreates subscription on restart, potentially losing checkpoint position.  
**Fix**: Instead of delete+recreate, reconnect to the existing subscription. The event store adapter should support re-subscribing to an existing persistent subscription.

## Recommended Priority Order

1. **Fix #1 and #2** (error handling) — these cause silent data loss in production
2. **Fix #4** (registry monitoring) — causes cascading failures after any aggregate crash
3. **Fix #6** (subscription restart) — causes duplicate event processing on handler restart
4. **Fix #3** (snapshot version) — causes incorrect aggregate state after schema changes
5. **Fix #5** (deadlock) — affects systems using strong consistency with nested dispatch
6. Add proper supervision tree support (section 19) — required for production resilience
7. Add projection strong consistency (section 5) — required for POST/Redirect/GET pattern
8. Implement recursive retry in error handlers (section 13) — required for production error recovery
9. Add TTL purging to subscriptions actor (section 12) — prevents memory leaks
10. Add process monitoring to PM strict routing (section 6) — correctness improvement

