# Instructed vs Commanded: CQRS/ES Feature & Guarantee Review

This document compares the Instructed framework (Gleam) against the Commanded framework (Elixir) across all major features, constraints, and guarantees essential for a robust CQRS/ES system.

**Legend:**
- ✅ Feature present and equivalent
- ⚠️ Feature present but with gaps or differences
- ❌ Feature missing entirely
- 📝 Design difference (not necessarily a problem)

---

## 1. Aggregates ⚠️

### What Commanded Provides
- **Behaviour-based**: `execute/2` and `apply/2` callbacks on aggregate module
- **GenServer process per instance**: Each aggregate instance is a separate OTP process, started on-demand under a DynamicSupervisor with `restart: :temporary`
- **Rich return types from execute**: Single event, list, `{:ok, event}`, `:ok`, `nil`, `[]`, `{:error, reason}`, `%Multi{}` (for multi-step operations)
- **Separate command handler**: Optional `Commanded.Commands.Handler` behaviour for separating command handling from the aggregate
- **State rebuilding**: Events read in batches of 1,000 via `stream_forward`; applied sequentially
- **Aggregate subscribes to its own stream**: Catches externally appended events and stays in sync
- **`apply/2` must never fail**: Critical invariant — used during replay

### What Instructed Provides
- ✅ **Record-of-functions pattern**: `Aggregate(empty_state, execute, apply_event)` — idiomatic Gleam
- ✅ **execute returns `Result(List(event), String)`**: Strongly typed, covers success-with-events and errors
- ✅ **apply_event is `(state, event) -> state`**: Pure, infallible — matches Commanded's requirement
- ✅ **`rebuild_state`**: Simple `list.fold` over events
- 📝 **No separate command handler**: In Gleam, the function-record approach means the user wires execute however they like — this is a design simplification, not a gap

### Gaps & Issues
- ❌ **No aggregate-as-process model in core aggregate module**: The `aggregate.gleam` is just a data structure + functions. The `aggregate_server.gleam` adds the GenServer wrapper, but this is separate and the router (`router.gleam`) does NOT use the aggregate server — it loads state fresh from the event store on every dispatch
- ❌ **No batched event reading**: `read_stream_forward` reads all events at once (no pagination/streaming)
- ❌ **No aggregate self-subscription**: Aggregate doesn't subscribe to its own stream for external event sync
- ❌ **No Multi support**: Cannot do multi-step command execution with intermediate state application
- ⚠️ **Router loads state fresh every time**: `load_aggregate_state` in `router.gleam` reads the entire stream on every command dispatch — no caching, no aggregate process. This is a significant performance concern for aggregates with many events

---

## 2. Command Routing & Dispatch ⚠️

### What Commanded Provides
- **Compile-time macro-based router**: `dispatch [OpenAccount, CloseAccount], to: BankAccount, identity: :account_number`
- **`identify` macro**: Sets default identity field and prefix per aggregate
- **Duplicate detection**: Same command registered twice raises `ArgumentError` at compile time
- **Dispatch options**: `timeout` (default 5s), `consistency` (:eventual/:strong), `returning` (:aggregate_state/:events/:execution_result), `lifespan`, `metadata`, `causation_id`, `correlation_id`
- **Task-based execution**: Commands dispatched via `Task.Supervisor.async_nolink` with yield/shutdown timeout handling
- **Default retry attempts**: 10 on version conflicts
- **Identity extraction middleware**: `ExtractAggregateIdentity` middleware extracts UUID, validates it's non-nil/non-empty, converts via `String.Chars`

### What Instructed Provides
- ✅ **Runtime router**: `Router` record with `identity` function, `identity_prefix`, `middleware` list
- ✅ **Identity extraction**: Via user-provided function `fn(command) -> String`
- ✅ **Prefix support**: `with_prefix` for stream ID construction
- ✅ **Retry on version conflict**: Retries on `VersionConflict` error
- ✅ **Causation/correlation/metadata support**: Via `dispatch_with_context`
- ✅ **Middleware pipeline**: Before/after dispatch, after failure

### Gaps & Issues
- ❌ **No compile-time command registration**: All routing is runtime — no compile-time duplicate detection, no exhaustiveness checking. (📝 This is a reasonable Gleam design choice — Gleam doesn't have macros)
- ❌ **No dispatch timeout**: No configurable command execution timeout
- ❌ **No `returning` option**: Always returns `DispatchResult` with state, version, and events
- ❌ **No `consistency` option on dispatch**: No strong/eventual consistency choice
- ❌ **No task-based isolation**: Command execution is synchronous in the caller's process (via router) or in the aggregate server's GenServer process. No timeout/kill protection
- ⚠️ **Default retries: 3 vs 10**: Lower retry count may lead to more `TooManyAttempts` errors under contention
- ⚠️ **No identity validation**: No check for nil/empty aggregate identity — could silently create events in empty-string streams

---

## 3. Middleware Pipeline ⚠️

### What Commanded Provides
- **Three callbacks**: `before_dispatch/1`, `after_dispatch/1`, `after_failure/1` — all receive/return `%Pipeline{}` struct
- **Pipeline struct**: Contains `command`, `command_uuid`, `causation_id`, `correlation_id`, `metadata`, `assigns`, `halted`, `response`, `consistency`, `identity`, `identity_prefix`
- **Halting**: `before_dispatch` and `after_dispatch` stop on halt; `after_failure` always runs through all middleware
- **Chain order**: User middleware runs first in `before_dispatch`, reversed for `after_dispatch`/`after_failure`
- **Built-in middleware**: `ExtractAggregateIdentity` (identity validation) and `ConsistencyGuarantee` (strong consistency blocking) are automatically appended
- **Assigns**: Arbitrary key-value store for passing data between middleware stages and to the response
- **Pipeline assigns include**: `:aggregate_uuid`, `:aggregate_version`, `:events`, `:aggregate_state`, `:error`, `:error_reason`

### What Instructed Provides
- ✅ **Same three callbacks**: `before_dispatch`, `after_dispatch`, `after_failure`
- ✅ **Pipeline struct**: Contains `command`, `command_id`, `causation_id`, `correlation_id`, `metadata`, `assigns`, `halted`, `response`
- ✅ **Halting support**: `before_dispatch` and `after_dispatch` skip on halt; `after_failure` always runs all
- ✅ **`assign`, `halt`, `respond` helpers**
- ✅ **`run_before_dispatch`, `run_after_dispatch`, `run_after_failure`**: Chain execution functions

### Gaps & Issues
- ❌ **No built-in middleware**: No `ExtractAggregateIdentity` or `ConsistencyGuarantee` middleware provided out of the box
- ❌ **No `consistency` field on Pipeline**: Cannot pass consistency choice through middleware
- ⚠️ **Assigns are `Dict(String, String)`**: Only string values — Commanded uses `map()` with atom keys and any values (aggregate state, event lists, etc.). This limits middleware's ability to inspect execution results
- ⚠️ **Middleware chain order**: Instructed runs in list order for all three stages (no reversal for `after_dispatch`/`after_failure`). Commanded reverses for `after_dispatch`/`after_failure` to create proper nesting (first in, last out). Actually, looking more carefully at the Commanded code — it appears `after_dispatch` and `after_failure` also run in the same order. The key difference is that Commanded's default middleware (identity + consistency) is appended last.
- ⚠️ **Pipeline response discarded**: In `router.gleam`, the pipeline result after middleware is assigned to `_` — middleware responses aren't actually used to influence the dispatch result

---

## 4. Event Handlers ⚠️

### What Commanded Provides
- **GenServer-based**: Each handler is a supervised GenServer process
- **Persistent subscription**: Subscribes via event store adapter with durable position tracking
- **Singleton guarantee**: Only one instance runs across the cluster (enforced by event store subscription mechanism)
- **Configuration options**: `name` (required, unique, immutable), `consistency`, `start_from` (:origin/:current/integer), `subscribe_to` (:all/stream-name), `concurrency`, `batch_size`, `state`
- **`handle/2` callback**: Receives event data + enriched metadata map
- **Return values**: `:ok`, `{:ok, new_state}`, `{:error, reason}`
- **Error handling**: `error/3` callback with retry/skip/stop strategies, exponential backoff built-in
- **Idempotency**: `last_seen_event` tracking in process memory + durable subscription position
- **Handler state**: Transient in-memory state threaded through `handle/2` calls
- **Subscription resilience**: Exponential backoff on subscription failure (1s–60s jitter)
- **Reset mechanism**: `before_reset/0` callback, subscription deletion, replay from start_from
- **Telemetry events**: start/stop/exception for each event processed
- **`after_start/1` callback**: Post-startup hook

### What Instructed Provides
- ✅ **Actor-based**: Uses Gleam's `actor` (OTP GenServer equivalent)
- ✅ **Handler function**: `handle_event(event, recorded_event, handler_state) -> Result(handler_state, String)`
- ✅ **Handler state**: Threaded through handler calls
- ✅ **Stream selection**: `AllStreams` or `SpecificStream(String)`
- ✅ **Persistent subscription**: Uses `subscribe_persistent` on the event store

### Gaps & Issues
- ❌ **No `error/3` callback**: Errors from `handle_event` are silently swallowed (`Error(_reason) -> actor.continue(state)`) — the handler continues with unchanged state, losing the event
- ❌ **No retry strategy**: No retry, skip, or backoff mechanism on handler failure
- ❌ **No subscription backoff**: If subscription fails, no retry mechanism
- ❌ **No `start_from` configuration**: Always deletes and recreates subscription from `Origin` — no `:current` or specific event number option
- ❌ **No `consistency` option**: No strong/eventual consistency choice
- ❌ **No concurrency/partitioning**: Single handler instance only
- ❌ **No batch processing**: No `batch_size` or `handle_batch` support
- ❌ **No event acknowledgment**: Events are not acknowledged to the event store after processing — subscription position not updated
- ❌ **No idempotency tracking**: No `last_seen_event` guard against duplicate processing
- ❌ **No reset mechanism**: No way to reset and replay
- ❌ **No telemetry/observability**
- ❌ **No `after_start` callback**
- ⚠️ **Subscription recreation on start**: Deletes existing subscription and re-creates from Origin — this means every handler restart replays ALL events, which is both a correctness issue (duplicate processing without idempotency) and a performance issue
- ⚠️ **No name uniqueness enforcement**: Handler name uniqueness is not validated

---

## 5. Projections ⚠️

### What Commanded Provides
- **Projections ARE event handlers**: In Commanded, projections are built using the standard event handler mechanism — there's no separate projection module in core. The `commanded_ecto_projections` library provides `Commanded.Projections.Ecto` for Ecto-based projections
- **All handler features apply**: Consistency, error handling, retry, idempotency, subscription management
- **Ecto projections**: `project` macro with `Ecto.Multi` for transactional operations — subscription position updated atomically with the projection (exactly-once at projection level)
- **Strong consistency recommended**: For POST/Redirect/GET patterns
- **Queryable**: Via standard database queries on the projected tables
- **Reset/rebuild**: Via handler reset mechanism (truncate + replay)

### What Instructed Provides
- ✅ **Separate projection module**: `projection.gleam` with `ProjectionConfig`, `start`, `get_state`
- ✅ **In-memory state**: Projection state held in actor process memory, queryable via `get_state`
- ✅ **Event handling**: `handle_event(event, recorded_event, projection_state) -> Result(projection_state, String)`
- ✅ **Persistent subscription**: Uses `subscribe_persistent` on the event store

### Gaps & Issues
- ❌ **No persistence of projection state**: Projection state is only in-memory — lost on process restart. This is fundamentally different from Commanded's Ecto projections which persist to database
- ❌ **No atomic position tracking**: Subscription position and projection state are not updated atomically — on crash between processing and ack, events may be replayed (though ack isn't called anyway)
- ❌ **All event handler gaps apply**: Same issues as Section 4 — no error handling, no retry, no idempotency, subscription recreated from Origin on restart
- ❌ **No consistency option**: Cannot request strong consistency for read-after-write patterns
- ❌ **No reset/rebuild mechanism**
- ⚠️ **`get_state` is synchronous call**: Blocking call into the actor — OK for simple use cases but doesn't support complex query patterns (filtering, pagination, etc.)
- 📝 **Design difference**: Instructed's in-memory projection is useful for simple cases. For production, users would need to build their own persistence (like the example-todo does with its projection handlers). This is analogous to how Commanded's core doesn't include Ecto projections — they're in a separate package

---

## 6. Process Managers ⚠️

### What Commanded Provides
- **Three-layer process hierarchy**: ProcessRouter (event subscription) → DynamicSupervisor → ProcessManagerInstance (one per process_uuid)
- **`interested?/1,2` routing**: Returns `{:start, uuid}`, `{:start!, uuid}`, `{:continue, uuid}`, `{:continue!, uuid}`, `{:stop, uuid}`, or `false`. Supports multi-instance routing (list of UUIDs)
- **`handle/2,3`**: Returns commands to dispatch (with metadata propagation)
- **`apply/2,3`**: State mutation from events
- **`after_command/2,3`**: Post-command hook returning `:continue` or `:stop`
- **State persistence**: PM state saved as snapshots after each event — survives restarts
- **Error handling**: `error/3` callback with retry/skip/stop/continue strategies for both event handling errors AND command dispatch errors
- **Pending commands**: On command dispatch failure, remaining pending commands are tracked in `FailureContext`
- **Idempotency**: `last_seen_event` tracking from snapshot; already-seen events are skipped
- **Event timeout**: Configurable timeout for event processing
- **Idle timeout**: Configurable inactivity timeout
- **Causation/correlation chain**: Automatically propagated from triggering event to dispatched commands

### What Instructed Provides
- ✅ **Single-actor router**: `PMRouterState` with `instances` dict tracking per-instance state in-memory
- ✅ **Interest routing**: `Start(uuid)`, `Continue(uuid)`, `Stop(uuid)`, `Skip`
- ✅ **Event handling**: `handle(event, recorded_event, pm_state) -> Result(List(command), String)`
- ✅ **State mutation**: `apply_event(pm_state, event) -> pm_state`
- ✅ **Command dispatch**: Via user-provided `dispatch_command` function
- ✅ **Persistent subscription**: Uses event store persistent subscription

### Gaps & Issues
- ❌ **No state persistence**: PM instance state is only in-memory (`Dict(String, pm_state)`) — lost on restart. Commanded persists state as snapshots after each event
- ❌ **No `{:start!, uuid}` / `{:continue!, uuid}` strict routing**: No validation of instance existence
- ❌ **No multi-instance routing**: Cannot route one event to multiple PM instances (list of UUIDs)
- ❌ **No `after_command` hook**: No post-command decision point
- ❌ **No error handling/retry**: Errors from `handle` are silently swallowed (`Error(_) -> actor.continue(state)`). Command dispatch errors are also ignored (`let _ = state.config.dispatch_command(cmd)`)
- ❌ **No pending commands tracking**: If a command fails mid-dispatch, remaining commands are lost
- ❌ **No idempotency**: No `last_seen_event` tracking — on restart, all events replayed without dedup
- ❌ **No event timeout or idle timeout**
- ❌ **No causation/correlation propagation**: `dispatch_command` is a bare function — doesn't receive or propagate causation_id/correlation_id from the triggering event
- ⚠️ **Single-process model**: All PM instances share one actor process — contention issue. Commanded has separate GenServer per instance for isolation and parallelism
- ⚠️ **State mutation order**: Instructed calls `apply_event` BEFORE `handle` for `Start` interest but AFTER `handle` for `Continue` — inconsistent. Commanded calls `handle` first, then `apply` after successful command dispatch
- ⚠️ **Stop doesn't apply event**: On `Stop`, the event is not applied to state before handling — the PM handles the event with potentially stale state

---

## 7. Snapshots ⚠️

### What Commanded Provides
- **Aggregate snapshots**: Configured per-aggregate with `snapshot_every: N` and `snapshot_version: N`
- **Automatic triggering**: After every N events, snapshot is taken asynchronously (via self-send)
- **State rebuilding with snapshots**: `AggregateStateBuilder.populate/1` reads snapshot first, then replays only events after snapshot version
- **Snapshot versioning**: Incrementing `snapshot_version` invalidates old snapshots (for schema migration)
- **Snapshot-lifespan interaction**: Snapshot taken before lifespan timeout is applied
- **Process manager snapshots**: PM state auto-persisted as snapshot after each event — enables restart recovery
- **Snapshot operations**: read, record (upsert), delete — all in EventStore adapter

### What Instructed Provides
- ✅ **SnapshotData type**: `SnapshotData(source_uuid, source_version, source_type, data, created_at)`
- ✅ **SnapshotConfig type**: `snapshot_every` and `snapshot_version` fields
- ✅ **Router has `snapshot_every` config**: `with_snapshot_every` setter on Router
- ✅ **EventStore snapshot operations**: `read_snapshot`, `record_snapshot`, `delete_snapshot` in the interface
- ✅ **Adapter implementations**: Both PostgreSQL and SQLite adapters implement snapshot operations with upsert

### Gaps & Issues
- ❌ **Snapshots are never actually used**: Despite having the types and config, NO code in `router.gleam` or `aggregate_server.gleam` reads or writes snapshots. The `snapshot_every` config on Router is set but never checked
- ❌ **No snapshot-based state rebuilding**: `load_aggregate_state` always reads entire stream — never checks for snapshots
- ❌ **No PM state persistence via snapshots**: Process manager state is in-memory only
- ❌ **No snapshot versioning logic**: `snapshot_version` field exists but no validation against stored snapshots
- 📝 **Infrastructure is in place**: The types, configs, and adapter implementations exist — the wiring to actually use them is missing

---

## 8. Event Store Interface ⚠️

### What Commanded Provides
- **Behaviour-based adapter**: `Commanded.EventStore.Adapter` with well-defined callbacks
- **`append_to_stream/5`**: With `expected_version` (`:any_version`, `:no_stream`, `:stream_exists`, `non_neg_integer`)
- **`stream_forward/4`**: Paginated reading with batch size parameter
- **Transient subscriptions**: `subscribe/2` — fire-and-forget notifications
- **Persistent subscriptions**: `subscribe_to/6` — with name, start_from, acknowledgment, concurrency_limit, partition_by
- **`ack_event/3`**: Acknowledge event processing
- **`unsubscribe/2`**, **`delete_subscription/3`**: Subscription lifecycle
- **Snapshot operations**: read, record, delete
- **Adapter metadata**: Opaque `adapter_meta` map passed to all calls, containing adapter-specific state
- **Serialization**: Configured per adapter, events serialized/deserialized transparently
- **`RecordedEvent`**: Rich struct with event_id, event_number (global), stream_id, stream_version, causation_id, correlation_id, event_type, data, metadata, created_at

### What Instructed Provides
- ✅ **Record-of-functions adapter**: `EventStore(event)` — idiomatic Gleam alternative to behaviour
- ✅ **`append_to_stream`**: With `ExpectedVersion` (AnyVersion, NoStream, StreamExists, ExactVersion(Int))
- ✅ **`read_stream_forward`**: Stream reading from a version
- ✅ **Transient subscriptions**: `subscribe` (all streams), `subscribe_to_stream` (specific stream)
- ✅ **Persistent subscriptions**: `subscribe_persistent` with stream, name, start_from, handler
- ✅ **`ack_event`**: Acknowledge event processing
- ✅ **`unsubscribe`**, **`delete_subscription`**: Subscription lifecycle
- ✅ **Snapshot operations**: read, record, delete
- ✅ **`read_all_forward`**: Read all events across streams (Commanded uses `stream_forward` with `:all`)
- ✅ **`get_latest_event_number`**: Query latest global event number
- ✅ **`RecordedEvent`**: With event_id, event_number, stream_id, stream_version, causation_id, correlation_id, data, metadata, created_at
- ✅ **Three adapter implementations**: In-memory, PostgreSQL, SQLite

### Gaps & Issues
- ❌ **No paginated reading**: `read_stream_forward` returns all events at once — no batch size parameter. For aggregates with thousands of events, this loads everything into memory
- ❌ **No `event_type` field on RecordedEvent**: Commanded stores the event type string (module name) for deserialization. Instructed's in-memory store doesn't need it (events stay as typed values), but the PostgreSQL/SQLite adapters store and discard the event_type during read
- ❌ **No concurrency_limit or partition_by on persistent subscriptions**: Subscription options are minimal
- ❌ **No `reset!` on adapter**: In-memory adapter has `reset` but it's exposed as part of the EventStore interface rather than being adapter-specific
- ⚠️ **Subscription handler is push-based callback**: In Commanded, the event store sends `{:events, events}` messages to a subscriber process. In Instructed, the subscriber provides a callback function that is called directly (often within the event store actor's context) — this means subscription handlers execute inside the event store process, blocking it
- ⚠️ **PostgreSQL adapter has race condition in append**: `get_stream_version` and `INSERT` are not atomic — another process could insert between the version check and the insert, causing duplicate stream_versions. The UNIQUE constraint catches this, but the error is returned as `StorageError` not `VersionConflict`
- ⚠️ **In-memory adapter `ack_event` is a no-op**: It doesn't actually track the acknowledged position, making persistent subscription position tracking meaningless

---

## 9. Optimistic Concurrency Control ⚠️

### What Commanded Provides
- **Expected version on append**: `:any_version`, `:no_stream`, `:stream_exists`, or exact integer
- **`{:error, :wrong_expected_version}`**: Returned on version mismatch
- **Automatic retry in aggregate process**: On `:wrong_expected_version`, aggregate rebuilds state from events and re-executes the command (up to `retry_attempts`, default 10)
- **In-memory adapter**: Validates expected version against stream length before appending
- **PostgreSQL adapter (eventstore)**: Database-level enforcement via unique constraints or stream version tracking

### What Instructed Provides
- ✅ **Expected version types**: `AnyVersion`, `NoStream`, `StreamExists`, `ExactVersion(Int)` — matches Commanded
- ✅ **VersionConflict error**: Returned on mismatch
- ✅ **Retry in router**: On `VersionConflict`, `dispatch_to_aggregate` retries with decremented attempts (default 3)
- ✅ **In-memory adapter**: Validates expected version correctly
- ✅ **SQLite adapter**: UNIQUE(stream_id, stream_version) constraint catches conflicts
- ✅ **PostgreSQL adapter**: UNIQUE(stream_id, stream_version) constraint

### Gaps & Issues
- ⚠️ **Router retries re-read entire stream**: On conflict, `dispatch_to_aggregate` is called recursively, which calls `load_aggregate_state` which reads ALL events again. This is correct but expensive — Commanded's aggregate process only reads events since last known version
- ⚠️ **Aggregate server doesn't retry**: `aggregate_server.gleam` returns `WrongExpectedVersion` immediately on conflict — no automatic retry. Only `router.gleam` retries
- ⚠️ **PostgreSQL adapter version check is non-atomic**: The version check (`get_stream_version`) and insert are separate queries without a transaction. Race condition: two concurrent appenders could both read the same version, both pass the check, but only one insert succeeds. The UNIQUE constraint catches this, but the error surfaces as `StorageError` instead of `VersionConflict`
- ⚠️ **Lower retry count**: 3 vs Commanded's 10 — more likely to exhaust retries under contention
- ✅ **SQLite is safe**: All operations serialized through actor, so version check + insert are effectively atomic

---

## 10. Command Serialization per Aggregate ❌

### What Commanded Provides
- **GenServer per aggregate instance**: Each aggregate is a separate process identified by `{application, aggregate_module, aggregate_uuid}`
- **Natural serialization**: GenServer mailbox ensures commands to the same aggregate are processed one-at-a-time
- **Concurrent different aggregates**: Different aggregate instances run in different processes — full parallelism
- **DynamicSupervisor**: Aggregate processes started on-demand, tracked by Registration adapter

### What Instructed Provides
- ⚠️ **`aggregate_server.gleam` exists**: Provides a GenServer-like actor per aggregate instance with serialized command execution
- ❌ **Router doesn't use it**: `router.gleam` does NOT use `aggregate_server` — it loads state and executes commands synchronously in the caller's process
- ❌ **No aggregate process registry**: No way to look up an existing aggregate process by stream ID
- ❌ **No DynamicSupervisor**: No supervisor for aggregate processes

### Gaps & Issues
- ❌ **CRITICAL: No command serialization in the router path**: When using `router.dispatch`, two concurrent callers dispatching commands to the same aggregate will BOTH read the same state, BOTH execute the command, and one will get a version conflict. While the retry mechanism handles this, it's wasteful and doesn't guarantee ordering
- ⚠️ **aggregate_server provides serialization but is disconnected**: The `aggregate_server.gleam` module correctly serializes commands via an actor, but the main `router.gleam` dispatch path doesn't use it. Users would need to manually create and manage aggregate server instances
- ⚠️ **Application module dispatches through router**: `application.gleam`'s `dispatch` function delegates to `router.dispatch`, inheriting its lack of serialization
- 📝 **Commanded's design**: The aggregate process acts as both a command serializer AND a state cache — this is fundamental to the CQRS/ES architecture. Without it, every dispatch reads all events and concurrent commands create unnecessary conflicts

---

## 11. Event Ordering ⚠️

### What Commanded Provides
- **Global monotonic event_number**: Every event gets a globally unique, monotonically incrementing number — guarantees total ordering across all streams
- **Per-stream stream_version**: Sequential within each stream
- **Subscription delivers in order**: Persistent subscriptions deliver events in event_number order
- **Handler processes one-at-a-time**: GenServer ensures sequential processing within each handler
- **Concurrency with partitioning**: `concurrency > 1` with `partition_by` ensures same-partition events are processed in order by the same handler instance
- **Aggregate events applied in stream_version order**: State rebuilding follows stream ordering

### What Instructed Provides
- ✅ **Global event_number**: In-memory, SQLite (AUTOINCREMENT), and PostgreSQL (BIGSERIAL) all produce monotonic global event numbers
- ✅ **Per-stream stream_version**: Sequential within each stream
- ✅ **Handler is actor-based**: Single actor process ensures sequential processing
- ✅ **Events stored and read in order**: SQL adapters use ORDER BY; in-memory uses append

### Gaps & Issues
- ⚠️ **Subscription callback executes in event store process**: In the in-memory adapter, persistent subscription handlers are called directly from the `handle_message` function (during `Append` processing). This means the handler callback runs inside the event store actor, blocking all other event store operations until it returns
- ⚠️ **PostgreSQL adapter notification order**: The notifier actor sends events via transient subscription callbacks — but the persistent subscription handler is registered as a transient subscriber after initial replay. If events arrive between initial replay and transient subscription registration, they could be missed
- ⚠️ **No partition_by support**: Cannot partition events across concurrent handlers for ordered parallel processing
- 📝 **Event ordering within a batch**: When multiple events are appended in one call, they get sequential event_numbers and stream_versions — this is correct in all three adapters

---

## 12. Strong vs Eventual Consistency ❌

### What Commanded Provides
- **Per-dispatch consistency choice**: `consistency: :eventual` (default) or `:strong`
- **Per-handler consistency config**: Handlers declare `consistency: :strong` or `:eventual`
- **ConsistencyGuarantee middleware**: Blocks dispatch until all strong-consistency handlers have processed the events
- **Subscriptions registry (ETS)**: Tracks handler acknowledgments per stream/version
- **PubSub for ack broadcasting**: Handlers broadcast acknowledgments via PubSub; Subscriptions GenServer aggregates them
- **Configurable timeout**: `dispatch_consistency_timeout` (default 5s) — returns `{:error, :consistency_timeout}` on timeout
- **Selective waiting**: Can wait for specific handlers by name
- **Strong consistency incompatible with concurrency > 1**: Enforced at compile/start time

### What Instructed Provides
- ❌ **No consistency model at all**: No `:strong`/`:eventual` option anywhere
- ❌ **No Subscriptions registry**: No tracking of handler acknowledgments
- ❌ **No ConsistencyGuarantee middleware**: No waiting mechanism
- ❌ **No PubSub system**: No internal pub/sub for coordination

### Impact
This is a significant gap for production CQRS/ES systems. Without strong consistency support:
- **POST/Redirect/GET fails**: After dispatching a command, a redirect to a read page may show stale data
- **No read-after-write guarantee**: Users cannot be sure their changes are reflected immediately
- **Workaround**: Users must implement their own polling/waiting mechanisms or accept eventual consistency everywhere

---

## 13. Error Handling & Retry Strategies ❌

### What Commanded Provides

**Event Handler Errors:**
- `error/3` callback: `(error, failed_event, FailureContext) -> {:retry, ctx} | {:retry, delay, ctx} | :skip | {:stop, reason}`
- `FailureContext`: carries `context` map (threaded across retries), `handler_state`, `metadata`, `stacktrace`
- Built-in exponential backoff: `delay = max(1s, min(24h, failures² × 1000 + rand(0..1000)))`
- Application-level default: `on_event_handler_error: :stop | :backoff | MyModule`

**Process Manager Errors:**
- `error/3` callback for both event handling AND command dispatch failures
- Command dispatch errors: `{:retry, ctx} | {:retry, delay, ctx} | :skip | {:skip, :continue_pending} | {:skip, :discard_pending} | {:continue, new_commands, ctx} | {:stop, reason}`
- `FailureContext`: includes `pending_commands`, `process_manager_state`

**Aggregate/Dispatch Errors:**
- Version conflict: automatic retry with state rebuild (up to 10 attempts)
- Aggregate process death: automatic retry (starts new process)
- Remote node down: automatic retry
- Execution timeout: `{:error, :aggregate_execution_timeout}`
- Exceptions in command handler: rescued, returned as `{:error, error}`

### What Instructed Provides
- ✅ **Version conflict retry in router**: Retries on `VersionConflict` (up to 3 attempts)
- ⚠️ **Aggregate errors surface**: Command execution errors bubble up as `AggregateError(reason)`

### Gaps & Issues
- ❌ **No error callback on event handlers**: Errors silently swallowed — handler continues with unchanged state
- ❌ **No error callback on process managers**: Errors silently swallowed
- ❌ **No retry/skip/backoff strategies**: No retry mechanism for event processing failures
- ❌ **No FailureContext**: No context tracking across retry attempts
- ❌ **No exponential backoff**: No built-in backoff
- ❌ **No exception handling in handlers**: If handler callback raises, the entire actor crashes
- ❌ **No application-level error config**: No `on_event_handler_error` equivalent
- ❌ **No aggregate process death recovery**: No retry on process crash during dispatch
- ❌ **No execution timeout**: No configurable timeout on command execution

---

## 14. Causation & Correlation ID Tracking ⚠️

### What Commanded Provides
- **Automatic command UUID**: Every dispatched command gets a UUID (`command_uuid`), auto-generated if not provided
- **`causation_id`**: The command's UUID becomes the `causation_id` of all events it produces
- **`correlation_id`**: Set at dispatch time, carried through all events; auto-generated if not provided
- **Process manager chain**: When PM dispatches commands from an event, `causation_id` = source event_id, `correlation_id` = source event's correlation_id
- **Full causal chain**: Event → Command → new Events, all sharing correlation_id and linked via causation_id

### What Instructed Provides
- ✅ **EventData has causation_id and correlation_id**: `Option(String)` fields
- ✅ **RecordedEvent has causation_id and correlation_id**: Persisted and readable
- ✅ **dispatch_with_context**: Allows setting causation_id, correlation_id, and metadata
- ✅ **Router propagates to events**: `EventData` is created with pipeline's causation/correlation IDs
- ✅ **CommandContext**: Carries causation_id and correlation_id

### Gaps & Issues
- ❌ **No automatic command UUID generation for causation**: In `dispatch`, `command_id` is generated but it's not automatically set as the `causation_id` on produced events. The `causation_id` comes from `pipeline.causation_id` which is `None` by default in `dispatch` (only set via `dispatch_with_context`)
- ❌ **Process manager doesn't propagate**: `dispatch_command` is a bare `fn(command) -> Result(Nil, String)` with no way to pass causation/correlation IDs from the triggering event
- ❌ **No auto-generated correlation_id**: If not explicitly provided, correlation_id is `None` — no automatic chain generation
- ⚠️ **Event handlers receive RecordedEvent**: The causation/correlation data is available on RecordedEvent but there's no mechanism to propagate it when dispatching new commands from handlers

---

## 15. Idempotency ❌

### What Commanded Provides
- **Handler-level `last_seen_event`**: Process-memory guard — events with `event_number <= last_seen_event` are automatically skipped
- **Durable subscription position**: Event store tracks last acknowledged event persistently — on restart, subscription resumes from last ack'd position
- **`{:error, :already_seen_event}`**: Handler can return this to skip without error
- **Process manager idempotency**: `last_seen_event` from snapshot — on restart, PM skips events already processed

### What Instructed Provides
- ❌ **No `last_seen_event` tracking**: No in-process guard against duplicate events
- ❌ **No durable subscription position**: In-memory adapter's `ack_event` is a no-op. PostgreSQL/SQLite adapters update the position but handlers never call `ack_event`
- ❌ **Subscription deleted on restart**: Event handlers and process managers delete and recreate subscriptions from Origin on startup, causing full replay without dedup

### Impact
- **Event handlers will process events multiple times** on restart — projections will apply the same changes again (corrupt state)
- **Process managers will dispatch duplicate commands** on restart — may create duplicate aggregates or duplicate side effects
- **No protection against at-least-once delivery duplicates** — even within a single process lifetime, the in-memory adapter doesn't track what was acknowledged

---

## 16. Multi Module ❌

### What Commanded Provides
- **`Commanded.Aggregate.Multi`**: Generates multiple events from a single command where later events depend on intermediate state
- **Pipeline API**: `Multi.new(aggregate) |> Multi.execute(fn) |> Multi.execute(fn)` — each step receives updated aggregate state
- **Named steps**: Steps can be named; subsequent 2-arity functions receive a map of named step results
- **`Multi.reduce`**: Iterate over enumerables, applying events after each item
- **Nested Multi**: Multi structs can return other Multi structs — recursive execution
- **Atomic**: All events from a Multi are persisted together; any error discards all changes

### What Instructed Provides
- ❌ **No Multi module**: Not implemented

### Impact
- Users cannot implement multi-step command logic where later decisions depend on intermediate state
- Workaround: Users must compute all events upfront or return intermediate state from the execute function, but this loses the composable pipeline pattern
- Not critical for simple use cases but important for complex domain logic

---

## 17. Aggregate Lifespan Management ❌

### What Commanded Provides
- **`AggregateLifespan` behaviour**: `after_event/1`, `after_command/1`, `after_error/1` callbacks
- **Return values**: timeout (ms), `:infinity`, `:hibernate`, `:stop`, `{:stop, reason}`
- **`DefaultLifespan`**: Infinite timeout normally, stops on exceptions
- **Per-command configuration**: `lifespan: MyLifespan` in dispatch registration
- **GenServer timeout mechanism**: Applied via reply/noreply tuples
- **Memory management**: `:hibernate` reduces memory for idle aggregates

### What Instructed Provides
- ❌ **No lifespan management**: No timeout, hibernate, or stop mechanism
- ❌ **No aggregate process (in router path)**: Since the router doesn't use aggregate processes, there's nothing to manage the lifespan of
- ⚠️ **aggregate_server has no lifespan**: The `aggregate_server.gleam` actor runs indefinitely with no timeout/shutdown mechanism

### Impact
- Memory leak risk: If using aggregate_server directly, processes accumulate without cleanup
- No way to automatically clean up idle aggregate processes
- No hibernation for memory optimization

---

## 18. Composite Router ❌

### What Commanded Provides
- **`CompositeRouter`**: Combines multiple routers into one dispatch point
- **Compile-time duplicate detection**: Detects duplicate command registrations across child routers
- **Nesting**: Composite routers can include other composite routers
- **Application integration**: `Commanded.Application` itself uses `CompositeRouter` internally via the `router` macro

### What Instructed Provides
- ❌ **No composite router**: Not implemented
- 📝 **Runtime routers**: Since Instructed uses runtime function-record routers, composition could be achieved by matching commands and delegating, but there's no built-in abstraction for it

### Impact
- Low impact for simple applications with one aggregate type
- For larger applications with many aggregates, users must manually route commands to the correct router
- The `Application` module's `dispatch` function requires passing the router explicitly

---

## 19. Application Supervision Tree ❌

### What Commanded Provides
- **Full supervision tree**: Application Supervisor → [EventStore children, PubSub children, Registry children, Task.Supervisor, Aggregates.Supervisor (DynamicSupervisor), Subscriptions.Registry, Subscriptions]
- **Pluggable adapters**: Event store, PubSub, and Registration adapters each provide child specs for the supervision tree
- **Dynamic named applications**: Multiple application instances with isolated state (`name: :tenant1`)
- **Process registration**: Local (Elixir Registry) or Global (`:global`) adapters for process discovery
- **Task.Supervisor**: For isolated command execution
- **Aggregates.Supervisor (DynamicSupervisor)**: On-demand aggregate process lifecycle
- **Supervised event handlers/PMs**: Users add them to their own supervision tree

### What Instructed Provides
- ⚠️ **Minimal Application module**: `application.gleam` wraps an actor that holds the event store reference
- ✅ **`start` creates an actor**: Application actor can be started
- ✅ **`dispatch` delegates to router**: Convenience function
- ✅ **`start_projection`**: Starts a projection within the application context

### Gaps & Issues
- ❌ **No supervision tree**: The Application actor is a single GenServer, not a supervisor. No child process management
- ❌ **No DynamicSupervisor for aggregates**: No on-demand aggregate process lifecycle
- ❌ **No Registration adapter**: No process registry for looking up aggregate instances
- ❌ **No PubSub system**: No internal pub/sub for consistency coordination
- ❌ **No Task.Supervisor**: No isolated command execution
- ❌ **No multi-tenant support**: No dynamic named applications
- ❌ **No supervised handler/PM startup**: Users must manually start and manage handler/PM processes
- ⚠️ **Application actor is trivial**: Only handles `GetEventStore` message — essentially a wrapper around a reference. No coordination, no lifecycle management

---

## 20. Event Upcasting ❌

### What Commanded Provides
- **`Commanded.Event.Upcaster` protocol**: `upcast(event, metadata) -> struct()` — runtime transformation of historical events
- **Default `Any` implementation**: Pass-through for events without upcasters
- **Applied before consumers**: Upcasting happens before events reach aggregates, handlers, and process managers
- **Non-destructive**: Events in the store are never modified — transformation is runtime-only
- **Chained**: Multiple upcasts can be composed (A→B, B→C)
- **Type replacement**: Can replace event type entirely (e.g., `%OldEvent{}` → `%NewEvent{}`)

### What Instructed Provides
- ❌ **No upcasting mechanism**: Not implemented
- 📝 **Gleam's type system**: Strongly typed events mean schema evolution requires careful handling. Without upcasting, changing event schemas requires migration or versioned deserialization logic in the event store adapter's deserialize function

### Impact
- Schema evolution is harder: Users must handle all historical event formats in their deserialize functions
- No clean separation between storage format and runtime format
- For long-lived systems, this becomes increasingly painful as event schemas evolve

---

## 21. Telemetry & Observability ❌

### What Commanded Provides
- **Telemetry events for aggregate execution**: `[:commanded, :aggregate, :execute, :start/:stop/:exception]`
- **Telemetry events for aggregate population**: `[:commanded, :aggregate, :populate, :start/:stop]` with event count
- **Telemetry events for dispatch**: `[:commanded, :application, :dispatch, :start/:stop]`
- **Telemetry events for event handling**: `[:commanded, :event, :handle, :start/:stop/:exception]`
- **Telemetry events for batch processing**: `[:commanded, :event, :batch, :start/:stop/:exception]`
- **Telemetry events for process managers**: `[:commanded, :process_manager, :handle, :start/:stop/:exception]`
- **Rich metadata**: application, aggregate_uuid, handler_name, events, errors, stacktraces
- **Logger middleware**: Built-in logging middleware with duration formatting

### What Instructed Provides
- ❌ **No telemetry**: No instrumentation events
- ❌ **No logging**: No built-in logging middleware
- ❌ **No duration tracking**: No performance measurement

### Impact
- No visibility into system behaviour in production
- No ability to set up alerts, dashboards, or tracing
- Debugging production issues requires adding manual logging

---

## 22. Batch Processing & Concurrency in Handlers ❌

### What Commanded Provides
- **Batch processing**: `batch_size: N` configuration, `handle_batch/1` callback receiving list of `{event, metadata}` tuples
- **Batch acknowledgment**: Only last event in batch acknowledged — all-or-nothing
- **Concurrency**: `concurrency: N` starts N handler processes under a handler supervisor
- **Partitioning**: `partition_by/2` callback assigns events to partitions — same partition processed in order by same handler
- **Mutual exclusion**: `concurrency` and `batch_size` are mutually exclusive (raises `ArgumentError`)
- **Strong consistency guard**: `concurrency > 1` requires `consistency: :eventual`

### What Instructed Provides
- ❌ **No batch processing**: Events processed one at a time only
- ❌ **No concurrency**: Single handler instance only
- ❌ **No partitioning**: No `partition_by` mechanism

### Impact
- Performance limitation for high-throughput event streams
- No way to parallelize event processing while maintaining per-partition ordering
- Acceptable for low-to-medium throughput systems

---

## Summary

### Feature Parity Scorecard

| # | Feature/Guarantee | Status | Severity |
|---|---|---|---|
| 1 | Aggregates | ⚠️ | Medium — core types work but no process model |
| 2 | Command Routing | ⚠️ | Medium — functional but missing options |
| 3 | Middleware | ⚠️ | Low — works but limited assigns |
| 4 | Event Handlers | ⚠️ | **HIGH** — missing error handling, idempotency |
| 5 | Projections | ⚠️ | Medium — in-memory only, all handler gaps |
| 6 | Process Managers | ⚠️ | **HIGH** — no persistence, no error handling |
| 7 | Snapshots | ⚠️ | Medium — types exist, never used |
| 8 | Event Store Interface | ⚠️ | Medium — functional with caveats |
| 9 | Optimistic Concurrency | ⚠️ | Low — works, PG adapter has race |
| 10 | Command Serialization | ❌ | **CRITICAL** — no per-aggregate serialization in router |
| 11 | Event Ordering | ⚠️ | Medium — works but handlers block event store |
| 12 | Strong/Eventual Consistency | ❌ | **HIGH** — no consistency model |
| 13 | Error Handling & Retry | ❌ | **CRITICAL** — errors silently swallowed |
| 14 | Causation/Correlation | ⚠️ | Medium — types exist, chain breaks in PMs |
| 15 | Idempotency | ❌ | **CRITICAL** — duplicate processing on restart |
| 16 | Multi Module | ❌ | Low — advanced feature |
| 17 | Aggregate Lifespan | ❌ | Low — no process model |
| 18 | Composite Router | ❌ | Low — convenience feature |
| 19 | Supervision Tree | ❌ | **HIGH** — no process management |
| 20 | Event Upcasting | ❌ | Medium — schema evolution |
| 21 | Telemetry | ❌ | Medium — observability |
| 22 | Batch/Concurrency | ❌ | Low — performance optimization |

### Critical Issues (Must Fix for Production Use)

1. **No command serialization per aggregate (§10)**: The router loads state and executes commands without any process-level serialization. Concurrent commands to the same aggregate will cause unnecessary version conflicts and potential data inconsistency. The `aggregate_server.gleam` exists but isn't wired into the dispatch path.

2. **Errors silently swallowed (§13)**: Event handlers and process managers catch errors and continue as if nothing happened. This means failed projections silently have missing data, and failed process manager commands are lost.

3. **No idempotency / duplicate processing (§15)**: On handler restart, subscriptions are deleted and recreated from Origin, replaying ALL events without deduplication. Projections will apply the same events twice, corrupting state.

4. **Subscription handlers block event store (§11)**: In the in-memory adapter, subscription callbacks execute inside the event store actor, blocking all other operations.

5. **PostgreSQL adapter race condition (§9)**: Version check and insert are not atomic — concurrent appends can bypass optimistic concurrency control.

### Recommended Priority Order for Fixes

1. Wire `aggregate_server` into router for per-aggregate command serialization
2. Add event acknowledgment and idempotency tracking to handlers/PMs
3. Add error handling callbacks (or at minimum, don't silently swallow errors)
4. Move subscription callbacks to run outside the event store process
5. Wrap PostgreSQL version check + insert in a transaction
6. Implement snapshot-based state rebuilding (infrastructure already exists)
7. Add strong consistency support
8. Add error retry/backoff strategies
