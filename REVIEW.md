# CQRS/ES Feature Review: Instructed vs Commanded

> **Generated**: 2026-02-19
> **Instructed version**: 1.0.0 (Gleam)
> **Commanded version**: latest main branch (Elixir)

This document systematically compares every feature, constraint, and guarantee between Instructed (Gleam CQRS/ES framework) and Commanded (the Elixir reference implementation it ports).

**Status indicators:**
- ✅ Feature present and equivalent
- ⚠️ Feature present but with gaps or differences
- ❌ Feature missing entirely
- 📝 Intentional design difference (not a problem)

---

## Table of Contents

### Core Features
1. [Aggregates](#1-aggregates-)
2. [Command Routing & Dispatch](#2-command-routing--dispatch-)
3. [Middleware Pipeline](#3-middleware-pipeline-)
4. [Event Handlers](#4-event-handlers-)
5. [Projections](#5-projections-)
6. [Process Managers](#6-process-managers-)
7. [Snapshots](#7-snapshots-)
8. [Event Store Interface](#8-event-store-interface-)

### CQRS/ES Guarantees & Constraints
9. [Optimistic Concurrency Control](#9-optimistic-concurrency-control-)
10. [Command Serialization per Aggregate](#10-command-serialization-per-aggregate-)
11. [Event Ordering](#11-event-ordering-)
12. [Strong vs Eventual Consistency](#12-strong-vs-eventual-consistency-)
13. [Error Handling & Retry Strategies](#13-error-handling--retry-strategies-)
14. [Causation & Correlation ID Tracking](#14-causation--correlation-id-tracking-)
15. [Idempotency](#15-idempotency-)

### Advanced Features
16. [Multi Module](#16-multi-module-)
17. [Aggregate Lifespan Management](#17-aggregate-lifespan-management-)
18. [Composite Router](#18-composite-router-)
19. [Application Supervision Tree](#19-application-supervision-tree-)
20. [Event Upcasting](#20-event-upcasting-)
21. [Telemetry & Observability](#21-telemetry--observability-)
22. [Batch Processing & Concurrency in Handlers](#22-batch-processing--concurrency-in-handlers-)

### Summary
- [Feature Parity Scorecard](#feature-parity-scorecard)
- [Critical Issues](#critical-issues)
- [Recommended Priority Order](#recommended-priority-order)

---

## 1. Aggregates [⚠️]

### What Commanded Provides
- Aggregate defined as a module with `execute/2` and `apply/2` callbacks (behaviour)
- `execute/2` accepts many return types: single event, list, `{:ok, event}`, `{:ok, [events]}`, `:ok`, `nil`, `[]`, `{:ok, []}`, `{:error, reason}`, `%Multi{}`, or raised exceptions (caught)
- `apply/2` must never fail — events are facts
- State is an Elixir struct, created via `struct(module)` for the empty state
- State rebuilding via `AggregateStateBuilder.populate/1` with snapshot integration and batched reading (1,000 events per batch)
- Aggregate process is a GenServer with `restart: :temporary` under a DynamicSupervisor
- External event handling via self-subscription to the aggregate's stream
- Lazy state loading via `handle_continue(:populate_aggregate_state)` on first access

### What Instructed Provides
- ✅ Aggregate defined as a record of functions (`Aggregate(state, command, event)` in `aggregate.gleam`)
- ✅ `execute` returns `Result(List(event), String)` — covers success with events, empty list (no-op), and errors
- ✅ `apply_event` takes state + event, returns state (infallible by convention)
- ✅ State rebuilding via `populate_from_event_store` with snapshot support and batched reading (1,000 default)
- ✅ `rebuild_from_version` for incremental state rebuild (used during retry)
- ✅ Aggregate server as OTP Actor (`aggregate_server.gleam`) with lazy loading on first command
- ✅ Self-subscription to own stream for external events (`ExternalEvent` message handling)
- 📝 Functions-as-records instead of behaviours — idiomatic Gleam, not a gap
- ⚠️ `execute` returns `String` errors only — Commanded allows any term as error reason
- ⚠️ No support for returning a single event (must always return a list) — minor ergonomic difference
- ⚠️ No exception catching in `execute` — Commanded rescues exceptions and returns them as errors

### Gaps & Issues
- **MEDIUM** — Error type is `String` only (`Result(List(event), String)`). Commanded allows `{:error, any_term}`. This limits structured error reporting from aggregates. Design choice but reduces expressiveness.
- **LOW** — No exception rescue in `execute`. In Gleam, panics crash the process. Commanded catches exceptions and returns them as `{:error, error}`. The Gleam approach is arguably more correct (let it crash), but differs from Commanded's behavior where the aggregate survives handler exceptions.
- **LOW** — External event handling in `aggregate_server.gleam` checks for sequential version (`stream_version == aggregate_version + 1`) and reloads on gap. Commanded throws `{:error, :unexpected_event_received}` and stops. Instructed's approach (reload) is actually more resilient.

---

## 2. Command Routing & Dispatch [⚠️]

### What Commanded Provides
- Compile-time macro-based router with `identify` and `dispatch` macros
- `identify` macro: sets default identity field/function per aggregate with optional prefix
- `dispatch` macro: registers command→aggregate mapping with many options (identity, prefix, function, timeout, lifespan, consistency, before_execute)
- Identity extraction via `ExtractAggregateIdentity` middleware
- Custom identity types via `String.Chars` protocol
- Duplicate command detection at compile time (raises `ArgumentError`)
- Per-command `identity:` override
- `before_execute` hook on handler module
- Default timeout: 5,000ms
- Default retry: 10 attempts
- `returning` option: `:aggregate_state`, `:aggregate_version`, `:events`, `:execution_result`
- Task-based execution via `Task.Supervisor.async_nolink` for isolation
- Retry on aggregate process death (lifespan timeout during dispatch) and remote node down

### What Instructed Provides
- ✅ Runtime router configuration (`Router` record in `router.gleam`) with `new()`, builder pattern
- ✅ Identity extraction via `identity` function on router
- ✅ Identity prefix via `with_prefix`
- ✅ Empty identity validation
- ✅ Middleware pipeline execution (before_dispatch, after_dispatch, after_failure)
- ✅ Registry for aggregate server processes (per stream_id)
- ✅ Default timeout: 5,000ms (matches Commanded)
- ✅ Default retry: 10 attempts (matches Commanded)
- ✅ Dispatch with context (causation_id, correlation_id, metadata)
- ✅ Strong consistency wait integration via subscriptions actor
- 📝 Runtime configuration instead of compile-time macros — Gleam doesn't have macros
- ⚠️ No `returning` option support — `CommandContext` defines the types but they're not wired into dispatch
- ⚠️ No duplicate command detection — router handles one aggregate type; multiple routers are separate
- ❌ No `before_execute` hook
- ❌ No Task-based execution isolation — commands execute directly via `process.call`
- ❌ No retry on aggregate process death during dispatch

### Gaps & Issues
- **MEDIUM** — `returning` option is defined in `command_context.gleam` (`Returning` type with 5 variants) but never used in the actual dispatch pipeline. `DispatchResult` always returns full state, version, and events. The type exists but the feature is inert.
- **MEDIUM** — No Task-based isolation. In Commanded, command execution runs in a separate task, so if the aggregate process crashes, the dispatcher isn't killed. In Instructed, `process.call` means the caller is blocked and may be affected by aggregate crashes.
- **LOW** — No `before_execute` hook. Minor convenience feature — can be replaced by middleware.
- **LOW** — No retry on process death during dispatch. If the aggregate server dies from a lifespan timeout while a command is in-flight, Instructed will return an error rather than retrying with a fresh process.

---

## 3. Middleware Pipeline [✅]

### What Commanded Provides
- `Commanded.Middleware` behaviour with `before_dispatch/1`, `after_dispatch/1`, `after_failure/1`
- `Pipeline` struct with command, UUIDs, metadata, assigns, halted flag, response, consistency, identity
- `halt/1` stops `before_dispatch` and `after_dispatch` chains; `after_failure` always runs all middleware
- `assign/3` for shared data between middleware stages
- `assign_metadata/3` for event metadata
- `respond/2` first-write-wins response setting
- Built-in: `ExtractAggregateIdentity`, `ConsistencyGuarantee`, `Logger`
- Middleware order: user middleware first, built-in appended last
- `after_dispatch` runs in reverse order of `before_dispatch`

### What Instructed Provides
- ✅ `Middleware` record with `before_dispatch`, `after_dispatch`, `after_failure` functions
- ✅ `Pipeline` struct with command, UUIDs, metadata, assigns, halted flag, response, consistency, identity
- ✅ `halt/1` stops `before_dispatch` and `after_dispatch`; `after_failure` always runs
- ✅ `assign/3`, `assign_metadata/3`, `respond/2`, `with_consistency/2`
- ✅ `run_before_dispatch`, `run_after_dispatch`, `run_after_failure` properly implement chain semantics
- 📝 No built-in `ExtractAggregateIdentity` middleware — identity extraction is done directly in `router.gleam`
- 📝 No built-in `Logger` middleware — user can add their own
- ⚠️ `after_dispatch`/`after_failure` run in same order as `before_dispatch` (not reversed)

### Gaps & Issues
- **LOW** — Middleware execution order: Commanded runs `after_dispatch` in reverse order (innermost first). Instructed runs all stages in the same order (leftmost first). This is a minor semantic difference that could matter if middleware has ordering dependencies.
- **LOW** — `assigns` uses `Dict(String, String)` in Instructed vs `%{atom => any}` in Commanded. Less flexible but more type-safe — a design trade-off.

---

## 4. Event Handlers [⚠️]

### What Commanded Provides
- GenServer-based handler with persistent subscription to event store
- Configuration: name (required), consistency, start_from, subscribe_to, concurrency, batch_size, state
- Singleton guarantee via subscription name uniqueness + process registration
- `handle/2` callback receives event + enriched metadata map (includes handler state under `:state` key)
- Return values: `:ok`, `{:ok, new_state}`, `{:error, :already_seen_event}`, `{:error, reason}`
- Default catch-all `handle/2` injected at compile time (unhandled events silently acked)
- `error/3` callback with `FailureContext` — retry/skip/stop strategies
- Application-level error handler fallback (`:stop`, `:backoff`, or custom module)
- Exponential backoff: `failures² × 1000 + jitter` ms, clamped [1s, 24h]
- `after_start/1` callback (post-subscription initialization)
- Handler state: transient in-process, passed via metadata, updated via `{:ok, new_state}`
- Subscription backoff on connection failure (1s min → 1min max, jitter)
- `last_seen_event` idempotency guard
- `before_reset/0` + mix task for handler reset
- Trap exits for graceful shutdown

### What Instructed Provides
- ✅ Actor-based handler with persistent subscription (`event_handler.gleam`)
- ✅ Configuration: name, handle_event, on_error, initial_state, stream_id, start_from, consistency, subscriptions, upcaster
- ✅ `handle_event` receives event data, RecordedEvent, and handler_state — returns `Result(handler_state, String)`
- ✅ `last_seen_event` idempotency guard (event_number comparison)
- ✅ Event acknowledgment after successful processing
- ✅ Strong consistency acking via subscriptions actor
- ✅ Telemetry emission (start/stop/exception)
- ✅ Error handling via `on_error` callback with Retry/RetryWithDelay/Skip/Stop strategies
- ⚠️ Error handling only retries once — Commanded retries recursively until skip/stop
- ⚠️ Default error behavior: silently continues (`ack_event` + `actor.continue`) — Commanded stops by default
- ⚠️ No `after_start` callback
- ⚠️ No subscription reconnection backoff
- ❌ No application-level error handler fallback
- ❌ No exponential backoff built-in
- ❌ No handler reset mechanism (no `before_reset`, no mix task equivalent)
- ❌ No `{:error, :already_seen_event}` return to skip from within handler

### Gaps & Issues
- **HIGH** — Default error behavior is wrong. In `handle_error` (`event_handler.gleam`), when `on_error` is `None`, the code acks the event and continues: `ack_event(state, recorded_event); actor.continue(state)`. Commanded's default is to **stop** the handler. Silently swallowing errors means events can be lost without any indication. This is in the comment: "Default: stop on error (matching Commanded's default)" but the code does the opposite.
- **HIGH** — Retry is single-shot only. When `on_error` returns `Retry(new_state)`, the code retries once. If the retry also fails, it acks and continues (swallowing the error). Commanded retries recursively until the error handler returns `:skip` or `{:stop, reason}`.
- **MEDIUM** — No subscription reconnection backoff. If the event store subscription fails, the handler fails to start. Commanded backs off with jitter (1s→1min) and retries silently.
- **MEDIUM** — On restart, subscription is deleted and recreated (`SubscriptionAlreadyExists` handler). This loses the subscription position in the in-memory adapter. The comment acknowledges this: "For in-memory adapter, this loses position (acceptable for testing)". For production, the PostgreSQL adapter preserves position via the database, but the delete+recreate pattern is still concerning.
- **LOW** — No `after_start` callback for post-subscription initialization.

---

## 5. Projections [⚠️]

### What Commanded Provides
- Projections are event handlers with strong consistency for POST/Redirect/GET pattern
- `commanded-ecto-projections` library provides `project/2` and `project/3` macros wrapping Ecto.Multi
- Atomic read model updates within a database transaction
- `before_reset/0` callback for truncating read model before replay
- Strong consistency option guarantees read model is up-to-date when dispatch returns
- Any storage backend can be used

### What Instructed Provides
- ✅ Dedicated `projection.gleam` module with `ProjectionConfig` and `start/2`
- ✅ `handle_event` callback receives event, RecordedEvent, and projection state
- ✅ `get_state` for querying current projection state
- ✅ `on_error` callback with Skip/Retry/RetryWithDelay/Stop strategies
- ✅ `last_seen_event` idempotency guard
- ✅ Event acknowledgment
- ⚠️ Projections are in-memory state only — no built-in persistence to a database
- ⚠️ Same delete+recreate subscription issue as event handlers
- ❌ No Ecto/database integration for atomic read model updates
- ❌ No `before_reset` callback
- ❌ No strong consistency integration (projections don't register with subscriptions actor)

### Gaps & Issues
- **MEDIUM** — Projections are in-memory only. The `get_state` function returns in-process state, which is useful for testing but not for production read models that need persistence. Users must implement their own database writes inside `handle_event`.
- **MEDIUM** — No strong consistency support. Projections don't register with the subscriptions actor, so dispatch with `consistency: Strong` won't wait for projections to complete. Only event handlers and process managers participate in strong consistency.
- **LOW** — No reset mechanism for rebuilding projections from scratch.

---

## 6. Process Managers [⚠️]

### What Commanded Provides
- Three-layer hierarchy: ProcessRouter → DynamicSupervisor → ProcessManagerInstance
- `interested?/1,2` with Start/Start!/Continue/Continue!/Stop/false routing
- Multi-instance routing (list of UUIDs)
- `handle/2,3` returns commands to dispatch
- `apply/2,3` mutates state AFTER command dispatch
- `after_command/2,3` for stopping instance after specific commands
- `error/3` with event errors vs command dispatch errors (different strategies)
- Command dispatch errors support `{:continue, commands, context}`, `{:skip, :discard_pending}`, `{:skip, :continue_pending}`
- State persistence via snapshots (write after every event, read on startup, delete on stop)
- Idempotency via `last_seen_event` from snapshot `source_version`
- `event_timeout` for stuck instances
- `idle_timeout` for inactive instances
- Acknowledgment tracking: all instances must ack before event is confirmed
- Strict routing validation (`:start!` checks process doesn't exist, `:continue!` checks it does)
- Process identity helper (`identity/0` via process dictionary)

### What Instructed Provides
- ✅ Single actor managing all instances in a Dict (`process_manager.gleam`)
- ✅ `interested` with Start/StartStrict/StartMany/Continue/ContinueStrict/ContinueMany/Stop/StopMany/Skip
- ✅ `handle` returns commands to dispatch
- ✅ `apply_event` mutates state AFTER command dispatch (correct order)
- ✅ `after_command` callback with AfterContinue/AfterStop
- ✅ `on_event_error` with Retry/RetryWithDelay/Skip/Stop strategies
- ✅ `on_command_error` with CmdRetry/CmdRetryWithDelay/CmdSkip/CmdDiscardPending/CmdContinueWith/CmdStop
- ✅ State persistence via snapshots (write after every event, read on startup, delete on stop)
- ✅ Per-instance idempotency via `last_seen_event` from snapshot `source_version`
- ✅ Causation chain propagation (causation_id = event_id, correlation_id from event)
- ✅ Strong consistency acking
- 📝 Single actor instead of three-layer hierarchy — trades per-instance parallelism for simplicity
- ⚠️ Strict routing uses in-memory `instance_exists` check, not snapshot-based
- ❌ No `event_timeout` for stuck processing
- ❌ No `idle_timeout` for inactive instances
- ❌ No acknowledgment tracking for multi-instance events (all instances processed serially)
- ❌ No `handle/3` variant with enriched metadata

### Gaps & Issues
- **MEDIUM** — Strict routing (`StartStrict`/`ContinueStrict`) only checks the in-memory `instances` dict, not the snapshot store. If a PM instance was active in a previous session (snapshot exists) but not loaded in memory, `ContinueStrict` will incorrectly fail. Commanded checks `ProcessManagerInstance.new?/1` which loads state from snapshot.
- **MEDIUM** — No timeout mechanisms. Both `event_timeout` (for stuck instance processing) and `idle_timeout` (for memory management) are missing. Long-running command dispatches or inactive instances will accumulate without cleanup.
- **MEDIUM** — Single-actor architecture means all instances are processed serially. If a PM handles high event throughput with many instances, this becomes a bottleneck. Commanded's per-instance GenServer allows parallel processing.
- **LOW** — No enriched metadata variant for `handle` callback. Commanded's `handle/3` receives a metadata map with system fields. Instructed passes the raw `RecordedEvent` as the third argument, which provides the same data but in a different format.

---

## 7. Snapshots [⚠️]

### What Commanded Provides
- **Aggregate snapshots**: Configurable via `snapshot_every: N` and `snapshot_version: N`
- Version checking: old snapshots with wrong `snapshot_module_version` are ignored (forces full replay)
- Snapshot taken asynchronously via self-message after command execution
- Lifespan timeout deferred during snapshot
- Serializer integration (JSON by default)
- **PM snapshots**: Written after every event, read on startup, deleted on stop
- PM `source_uuid` format: `"\"pm_name\"-\"pm_uuid\""`
- PM `source_version` = `event_number` (global)

### What Instructed Provides
- ✅ `SnapshotConfig` with `snapshot_every` and `snapshot_version` fields
- ✅ `SnapshotData` record with source_uuid, source_version, source_type, data, created_at
- ✅ `snapshot_required` check based on events since last snapshot
- ✅ Snapshot reading during `populate_from_event_store` with `snapshot.coerce` for type bridging
- ✅ Snapshot writing in `maybe_take_snapshot` after command execution
- ✅ PM snapshots: written after every event, read on startup, deleted on stop
- ✅ PM `source_uuid` format: `"pm-" <> name <> "-" <> uuid`
- ⚠️ No `snapshot_version` checking — old snapshots are always used regardless of version mismatch
- ⚠️ Snapshot taken synchronously (not via self-message)

### Gaps & Issues
- **HIGH** — `snapshot_version` is stored in `SnapshotConfig` but never checked during snapshot reading. In `populate_from_event_store` (`aggregate.gleam`), the snapshot is read and used unconditionally — there's no comparison of `snapshot_version` from config against any field in the stored snapshot. If an aggregate's state type changes and you increment `snapshot_version`, old snapshots will still be loaded, potentially causing type mismatch crashes or silent data corruption.
- **LOW** — Synchronous snapshot writing. Commanded sends `{:take_snapshot, lifespan_timeout}` to self and takes the snapshot asynchronously, deferring the lifespan timeout. Instructed takes snapshots inline, which blocks the aggregate server briefly. For production workloads this could matter.

---

## 8. Event Store Interface [✅]

### What Commanded Provides
- `EventStore.Adapter` behaviour with typed callbacks
- `append_to_stream/5` with expected_version semantics (any_version, no_stream, stream_exists, exact version)
- `stream_forward/4` returning lazy Enumerable
- `subscribe/2` (transient) and `subscribe_to/6` (persistent)
- `ack_event/3` for backpressure
- `unsubscribe/2` (pause) and `delete_subscription/3` (delete permanently)
- `read_snapshot/2`, `record_snapshot/2`, `delete_snapshot/2`
- In-memory adapter for testing, PostgreSQL adapter for production (via `commanded-eventstore-adapter`)

### What Instructed Provides
- ✅ `EventStore(event)` record of functions with all core operations
- ✅ `append_to_stream` with `ExpectedVersion` (AnyVersion, NoStream, StreamExists, ExactVersion)
- ✅ `read_stream_forward` with batch size
- ✅ `subscribe` (transient all), `subscribe_to_stream` (transient per-stream)
- ✅ `subscribe_persistent` with StartFrom (Origin, Current, FromEventNumber)
- ✅ `ack_event` for backpressure
- ✅ `unsubscribe` and `delete_subscription`
- ✅ `read_snapshot`, `record_snapshot`, `delete_snapshot`
- ✅ `read_all_forward` and `get_latest_event_number` (extra utilities)
- ✅ `reset` (testing utility)
- ✅ In-memory adapter (`in_memory_event_store.gleam`) — full implementation
- ✅ PostgreSQL adapter (`instructed_postgres.gleam`) with transactional OCC, poll-based subscriptions, gap detection
- ✅ SQLite adapter (`instructed_sqlite.gleam`)
- 📝 Record-of-functions instead of behaviour — idiomatic Gleam

### Gaps & Issues
- **LOW** — `read_stream_forward` returns `Result(List(...), ...)` instead of a lazy stream. For very large streams, this loads all events in a batch into memory at once. The batched reading in `aggregate.gleam` mitigates this with the 1,000-event batch size.
- **LOW** — `subscribe_persistent` takes a callback function rather than delivering `{:events, events}` messages to a PID. Both patterns work, but the callback approach requires the callback to be non-blocking (documented correctly).

---

## 9. Optimistic Concurrency Control [✅]

### What Commanded Provides
- `expected_version` parameter on `append_to_stream`
- Returns `{:error, :wrong_expected_version}` on conflict
- Aggregate GenServer rebuilds state from new events and retries command
- Default 10 retry attempts
- Incremental rebuild (read only events since last known version)

### What Instructed Provides
- ✅ `ExactVersion(Int)` expected version on append
- ✅ `VersionConflict` error from event store
- ✅ `execute_with_retry` in aggregate_server with configurable attempts (default 10)
- ✅ `rebuild_from_current_version` for incremental rebuild (not full replay)
- ✅ `WrongExpectedVersion` mapped to `VersionConflict` in `execute_once`
- ✅ `TooManyAttempts` error after exhausting retries

### Gaps & Issues
None. OCC is fully implemented and matches Commanded's behavior.

---

## 10. Command Serialization per Aggregate [✅]

### What Commanded Provides
- GenServer process per aggregate instance
- Commands serialized via GenServer mailbox (one at a time)
- Different aggregates execute in parallel

### What Instructed Provides
- ✅ OTP Actor per aggregate instance (via registry in `router.gleam`)
- ✅ Commands serialized via actor mailbox (`process.call` to `ServerMessage.Execute`)
- ✅ Different aggregates get different server processes (registry keyed by stream_id)
- ✅ Registry actor manages lifecycle of aggregate servers

### Gaps & Issues
None. Command serialization is correctly implemented via the actor model.

---

## 11. Event Ordering [⚠️]

### What Commanded Provides
- `event_number`: globally unique, monotonically incrementing (BIGSERIAL in PostgreSQL)
- `stream_version`: per-stream sequential version
- Handler delivery in `event_number` order (persistent subscription guarantees)
- In-memory adapter: events stored in append order, delivered in order
- Subscription callbacks receive events in order, one at a time (with ack-based backpressure)

### What Instructed Provides
- ✅ `event_number` and `stream_version` on `RecordedEvent`
- ✅ In-memory adapter stores events in append order
- ✅ Persistent subscriptions deliver one event at a time with ack-based backpressure
- ✅ PostgreSQL adapter uses poll-based subscriptions with gap detection for correct ordering under concurrent writes
- ⚠️ In-memory adapter `event_number` assigned in the event store actor — serialized, so always sequential
- ⚠️ PostgreSQL adapter handles BIGSERIAL gaps from rolled-back transactions (gap_retries mechanism)

### Gaps & Issues
- **LOW** — PostgreSQL gap detection waits up to 10 retries × poll interval before skipping permanent gaps. This is a reasonable approach but could introduce up to 10 seconds of delivery latency when a transaction rolls back. The max_gap_retries=10 is configurable only at compile time in the poller.

---

## 12. Strong vs Eventual Consistency [⚠️]

### What Commanded Provides
- Handler-level: `consistency: :strong` registers handler for consistency tracking
- Dispatch-level: `consistency: :strong` blocks until all strong handlers ack
- Selective consistency: `consistency: [Module1, Module2]` waits for specific handlers
- `ConsistencyGuarantee` middleware handles blocking
- Default timeout: 5s (configurable via `:dispatch_consistency_timeout`)
- Dispatcher PID excluded from wait_for to prevent deadlock
- ETS-based tracking with TTL purging (1 hour default)
- PubSub broadcast for cross-process notification

### What Instructed Provides
- ✅ `Consistency` type with `Eventual` and `Strong` variants
- ✅ Handler-level consistency via `with_consistency` configuration
- ✅ Subscriptions actor for tracking (registers handlers, accepts acks, unblocks waiters)
- ✅ Router integration: dispatch blocks on `Strong` consistency + subscriptions actor
- ✅ `wait_for` with timeout → returns `ConsistencyTimeout` error
- ✅ Handlers ack subscriptions actor after successful processing
- ⚠️ No selective consistency (can't wait for specific handlers by name/module)
- ❌ No dispatcher PID exclusion from wait_for (potential deadlock)
- ❌ No TTL purging of ack entries (memory leak over time)

### Gaps & Issues
- **HIGH** — No dispatcher PID exclusion. In Commanded, when a strong-consistency handler dispatches another command inside `handle/2`, the `ConsistencyGuarantee` middleware excludes the dispatching handler's PID from the wait list. Without this, if a strong handler triggers a command that also requires strong consistency, it creates a deadlock — the dispatch waits for the handler, but the handler can't ack until the dispatch completes.
- **MEDIUM** — No selective consistency. Commanded allows `consistency: [MyProjector, "OtherHandler"]` to wait for specific handlers. Instructed only supports `Strong` (all) or `Eventual` (none).
- **MEDIUM** — No TTL purging. The subscriptions actor accumulates ack entries in the `acked` dict forever. In a long-running system with many streams, this will grow unbounded. Commanded purges entries older than 1 hour.

---

## 13. Error Handling & Retry Strategies [⚠️]

### What Commanded Provides
- **Handler errors**: `error/3` callback with `FailureContext` (includes context map, handler_state, stacktrace, metadata)
- Retry recursively until skip/stop
- Application-level fallback: `:stop` (default), `:backoff`, or custom module
- Built-in exponential backoff: `failures² × 1000 + jitter`, clamped [1s, 24h]
- `:skip` acknowledges and moves on
- `{:stop, reason}` stops the handler GenServer
- Context map persists across retries
- **PM event errors**: same `error/3` but only retry/skip/stop (no pending command management)
- **PM command errors**: `error/3` with additional strategies: `{:continue, commands, context}`, `{:skip, :continue_pending}`, `{:skip, :discard_pending}`
- FailureContext includes `pending_commands` for command errors

### What Instructed Provides
- ✅ `ErrorAction` type: Retry/RetryWithDelay/Skip/Stop
- ✅ `FailureContext` type with context, handler_state, failure_count, last_error, stacktrace
- ✅ Handler `on_error` callback with error strategies
- ✅ PM `on_event_error` with error strategies
- ✅ PM `on_command_error` with CmdRetry/CmdRetryWithDelay/CmdSkip/CmdDiscardPending/CmdContinueWith/CmdStop
- ⚠️ Handler retry is single-shot (retry once, then ack+continue on second failure)
- ⚠️ Default handler error behavior silently swallows errors (should stop)
- ❌ No application-level error handler fallback
- ❌ No built-in exponential backoff
- ❌ No recursive retry in handlers
- ❌ No context map that persists across retries (FailureContext exists but isn't threaded through recursive retries)

### Gaps & Issues
- **CRITICAL** — Default handler error behavior is wrong. When `on_error` is `None` in `event_handler.gleam`, the handler acks the event and continues (`actor.continue(state)`). This means errors are silently swallowed and events are lost. Commanded's default stops the handler, making the failure visible. See `handle_error` function at line ~262 of `event_handler.gleam`.
- **HIGH** — Single-shot retry. When retry also fails, the code acks and continues. This means transient failures that need 2+ retries will silently lose events. Commanded retries recursively (limited only by the error handler's decisions).
- **MEDIUM** — No exponential backoff built-in. Users must implement their own delay calculation in the `on_error` callback. Commanded provides `ErrorHandler.backoff/4` out of the box.

---

## 14. Causation & Correlation ID Tracking [✅]

### What Commanded Provides
- Command dispatch generates `command_uuid` → becomes `causation_id` on events
- `correlation_id` propagated from command to events
- PM command dispatch: `causation_id = event_id`, `correlation_id` from source event
- Full chain: User command → event (causation=command_uuid) → PM → command (causation=event_id) → event (causation=command_uuid)

### What Instructed Provides
- ✅ `dispatch` generates `command_id` and `correlation_id` via `uuid.v4_string()`
- ✅ `command_id` becomes `causation_id` on events (set in `dispatch_with_context`)
- ✅ `causation_id` and `correlation_id` flow through pipeline and into `EventData`
- ✅ PM dispatch propagates: `causation_id = recorded_event.event_id`, `correlation_id = recorded_event.correlation_id`
- ✅ Tested in `causation_chain_test.gleam` (391 lines of chain validation tests)

### Gaps & Issues
None. Causation and correlation chain propagation is correctly implemented and well-tested.

---

## 15. Idempotency [⚠️]

### What Commanded Provides
- Handler: `last_seen_event` field (event_number) — in-memory guard
- Durable subscription position tracked by event store adapter
- Handler can return `{:error, :already_seen_event}` for application-level idempotency
- PM: `last_seen_event` from snapshot's `source_version` (restored on restart)
- ProcessRouter: own `last_seen_event` for batch-level filtering
- Subscription ack advances durable checkpoint

### What Instructed Provides
- ✅ Handler: `last_seen_event` in-memory guard (event_number comparison)
- ✅ PM: per-instance `last_seen_event` from snapshot `source_version`
- ✅ Subscription ack via `ack_event` on event store
- ✅ Projection: `last_seen_event` guard
- ⚠️ Handler `last_seen_event` is only in-memory — lost on restart
- ❌ No `{:error, :already_seen_event}` return value support from handlers

### Gaps & Issues
- **MEDIUM** — Handler `last_seen_event` is only in-memory. On handler restart, it's reset to `None`. The subscription position (persisted by the event store) provides the durable guard, but in the gap between subscription creation (with `start_from`) and first ack, events could be redelivered. The delete+recreate subscription pattern in the handler's `start` function makes this worse — it resets the subscription position.
- **LOW** — No `{:error, :already_seen_event}` for application-level idempotency from within the handler callback. Users must handle this entirely within their `handle_event` function.

---

## 16. Multi Module [✅]

### What Commanded Provides
- `Commanded.Aggregate.Multi` for multi-step command execution
- `new/1`, `execute/2,3` (with optional named steps), `reduce/3,4`
- Named steps: 2-arity execute functions receive a map of post-step aggregate states
- `run/1`: executes all steps, returns `{aggregate_state, events}` or `{:error, reason}`
- Error short-circuits the chain (via throw/catch)
- Nested Multi support (recursive `run/1`)
- Events applied between steps to update aggregate state

### What Instructed Provides
- ✅ `Multi(state, event)` opaque type in `multi.gleam`
- ✅ `new/1`, `execute/2`, `apply/2`, `reduce/4`, `to_result/1`
- ✅ Error short-circuits (preserved in `Option(String)`)
- ✅ `apply` folds accumulated events through apply_fn to update state between stages
- ✅ `get_state/1`, `get_events/1`, `has_error/1` introspection
- ✅ Well-tested in `multi_test.gleam` (243 lines)
- 📝 No named steps — Gleam doesn't need them (closures capture variables directly)
- 📝 Explicit `apply` call between stages instead of automatic — more explicit, arguably better
- ❌ No nested Multi support (returning a Multi from execute is not handled)

### Gaps & Issues
- **LOW** — No nested Multi support. Commanded allows an execute function to return a `%Multi{}`, which is then recursively executed. This is a niche feature. In Instructed, you'd compose by chaining `execute` calls on the same Multi.
- **LOW** — No named steps. In Commanded, named steps allow later stages to access the aggregate state at intermediate points via a map. In Gleam, closures can capture variables, making this less necessary.

---

## 17. Aggregate Lifespan Management [✅]

### What Commanded Provides
- `AggregateLifespan` behaviour with `after_event/1`, `after_command/1`, `after_error/1`
- Return values: timeout integer, `:infinity`, `:hibernate`, `:stop`, `{:stop, reason}`
- `DefaultLifespan`: `:infinity` always, except stops on exceptions
- `after_event` called with **last** event when command produces events
- `after_command` called when command produces **no** events
- Hibernate: reduces memory via process hibernation
- Timeout via GenServer `:timeout` mechanism

### What Instructed Provides
- ✅ `Lifespan` record with `after_command`, `after_error`, `after_event` functions
- ✅ `LifespanDecision`: KeepRunning, Stop, StopAfter(ms), Hibernate
- ✅ `always_running()`, `new_idle(ms)`, `stop_after_command()` convenience constructors
- ✅ Timer-based StopAfter with cancellation on new commands
- ✅ `after_event` for externally applied events
- ✅ `after_error` called after command errors
- ✅ Well-tested in `lifespan_test.gleam` (235 lines)
- ⚠️ `Hibernate` falls back to `KeepRunning` (Gleam actors don't support Erlang hibernation)
- ⚠️ `after_command` receives `(state, command)` — called after ALL commands, not just no-event commands
- 📝 `after_event` receives `(state, event)` — slightly different signature from Commanded's `after_event(event)`

### Gaps & Issues
- **LOW** — `Hibernate` is a no-op. Documented as falling back to `KeepRunning`. Gleam's actor abstraction doesn't expose Erlang's process hibernation. This is an acceptable design limitation.
- **LOW** — `after_command` is always called after successful commands (with events). Commanded calls `after_event` when events are produced and `after_command` only when no events are produced. This is a semantic difference but unlikely to cause issues in practice.

---

## 18. Composite Router [❌]

### What Commanded Provides
- `CompositeRouter` macro combines multiple routers into one
- Compile-time duplicate command detection across child routers
- Supports nesting (composite of composites)
- Application itself uses CompositeRouter internally
- Pattern-matched dispatch delegation

### What Instructed Provides
- ❌ No CompositeRouter equivalent
- Each Router handles one aggregate type
- Users must dispatch to the correct router manually

### Gaps & Issues
- **LOW** — No CompositeRouter. This is primarily a convenience feature. In Instructed, users can create an application-level dispatch function that pattern-matches commands and delegates to the appropriate router. The lack of compile-time duplicate detection is a minor downside.

---

## 19. Application Supervision Tree [⚠️]

### What Commanded Provides
- `Commanded.Application` is an OTP Supervisor module
- Full supervision tree: EventStore adapters, PubSub, Registry, Task.Supervisor, Aggregate.Supervisor, Subscriptions.Registry, Subscriptions
- Dynamic named applications for multi-tenancy
- Config priority: compile-time < Application.get_env < start_link opts < init/1 callback
- All infrastructure started automatically under supervision

### What Instructed Provides
- ✅ `Application` module as a plain struct (not an OTP process)
- ✅ Groups event store, router, and subscriptions actor
- ✅ Convenience functions: `dispatch`, `start_event_handler`, `start_projection`, `start_process_manager`
- ✅ Automatic subscriptions actor injection into router and handlers
- ✅ Multi-tenancy via multiple Application instances with different event stores
- 📝 User manages supervision externally — documented approach using `gleam/otp/static_supervisor`
- ⚠️ No automatic supervision tree — user must wire components manually
- ❌ No Registration adapter abstraction (no LocalRegistry/GlobalRegistry)
- ❌ No Task.Supervisor for command execution isolation
- ❌ No DynamicSupervisor for aggregate processes (plain Dict-based registry instead)

### Gaps & Issues
- **MEDIUM** — No process registration. Commanded uses Registration adapters (LocalRegistry via Elixir's Registry, GlobalRegistry via `:global`) for singleton guarantee and process lookup. Instructed uses a plain actor-based registry dict. This means: (a) no cluster-wide singleton guarantee for aggregate processes, (b) if the registry actor dies, all aggregate server references are lost.
- **MEDIUM** — No supervision of aggregate servers. Aggregate server processes are started via the registry actor but not supervised. If an aggregate server crashes, it's silently lost from the registry dict. Commanded uses DynamicSupervisor with `:temporary` restart to at least track child processes.
- **LOW** — No automatic supervision tree setup. Users must manually start the event store, create routers, start handlers, etc. This is more explicit but more work than Commanded's `use Commanded.Application`.

---

## 20. Event Upcasting [✅]

### What Commanded Provides
- `Upcaster` protocol with `upcast/2` callback (receives event + metadata)
- Default no-op implementation for `Any`
- Applied at read time (aggregate rebuild, handler delivery)
- Supports changing event type (returning a different struct)
- Manual chaining (no automatic chain-through)
- Pure functions (no side effects)

### What Instructed Provides
- ✅ `Upcaster(event)` record with `upcast` function
- ✅ `identity()` — no-op upcaster
- ✅ `apply/2`, `apply_all/2` for single and list application
- ✅ `chain/2`, `chain_all/1` for composing upcasters
- ✅ Applied in aggregate server (wraps event store reads via `apply_upcasting_to_store`)
- ✅ Applied in event handlers (before delivery via `upcast.apply`)
- ✅ Applied in process managers (before delivery)
- ✅ Well-tested in `upcast_test.gleam` (208 lines)
- 📝 Function-based instead of protocol-based — more explicit composition

### Gaps & Issues
- **LOW** — Upcaster receives `RecordedEvent(event)` instead of `(event, metadata)`. Provides the same information (metadata is in the RecordedEvent) but different API shape.
- Instructed's `chain/2` is actually an improvement over Commanded, which requires manual chaining.

---

## 21. Telemetry & Observability [✅]

### What Commanded Provides
- Erlang `:telemetry` library integration
- Events: `[:commanded, :aggregate, :execute, :start/:stop/:exception]`
- Events: `[:commanded, :application, :dispatch, :start/:stop]`
- Events: `[:commanded, :event, :handle, :start/:stop/:exception]`
- Events: `[:commanded, :process_manager, :handle, :start/:stop/:exception]`
- Events: `[:commanded, :event_store, :*, :start/:stop/:exception]` (all store operations)
- Measurements: system_time, duration
- Rich metadata: application, handler_name, aggregate_state, recorded_event, etc.

### What Instructed Provides
- ✅ `TelemetryEvent` sum type with all major event categories
- ✅ Command dispatch: Start/Stop/Exception
- ✅ Aggregate execute: Start/Stop/Exception
- ✅ Event handler: Start/Stop/Exception
- ✅ Process manager: Start/Stop/Exception
- ✅ `emit/1` function with Erlang `:telemetry` integration (graceful no-op if not available)
- ✅ `set_handler/1` for pure-Gleam telemetry consumption
- ✅ Convenience helpers: `dispatch_start`, `aggregate_start`, `event_handle_start`, `pm_handle_start`, etc.
- ✅ Well-tested in `telemetry_test.gleam` (332 lines)
- ⚠️ No event store operation telemetry

### Gaps & Issues
- **LOW** — No event store operation telemetry. Commanded wraps every event store call in a telemetry span. Instructed only instruments the higher-level operations (dispatch, aggregate execute, handler, PM).

---

## 22. Batch Processing & Concurrency in Handlers [❌]

### What Commanded Provides
- `batch_size: N` → activates `handle_batch/1` callback
- Receives list of `{event, metadata}` tuples
- Ack only the last event in batch
- Mutual exclusion: can't have both `handle/2` and `handle_batch/1`
- `concurrency: N` → starts N handler processes under a Supervisor
- `partition_by/2` callback for consistent event routing (`:erlang.phash2`)
- Constraints: `concurrency + batch_size` not allowed; `concurrency + :strong` not allowed
- Each concurrent process shares the same subscription name

### What Instructed Provides
- ❌ No batch processing support
- ❌ No concurrent handler support
- ❌ No `partition_by` callback
- Events are always processed one at a time by a single handler process

### Gaps & Issues
- **LOW** — No batch processing. This is a performance optimization for high-throughput scenarios. Single-event processing is correct and sufficient for most use cases.
- **LOW** — No concurrent handlers. This is an advanced feature for scaling event processing across multiple BEAM processes. The single-handler model is simpler and correct.
- **LOW** — No partition_by. Without concurrent handlers, partitioning is unnecessary.

---

## Feature Parity Scorecard

| # | Feature | Status | Severity of Gaps |
|---|---------|--------|-----------------|
| 1 | Aggregates | ⚠️ | MEDIUM |
| 2 | Command Routing & Dispatch | ⚠️ | MEDIUM |
| 3 | Middleware Pipeline | ✅ | LOW |
| 4 | Event Handlers | ⚠️ | HIGH |
| 5 | Projections | ⚠️ | MEDIUM |
| 6 | Process Managers | ⚠️ | MEDIUM |
| 7 | Snapshots | ⚠️ | HIGH |
| 8 | Event Store Interface | ✅ | LOW |
| 9 | Optimistic Concurrency Control | ✅ | — |
| 10 | Command Serialization per Aggregate | ✅ | — |
| 11 | Event Ordering | ⚠️ | LOW |
| 12 | Strong vs Eventual Consistency | ⚠️ | HIGH |
| 13 | Error Handling & Retry Strategies | ⚠️ | CRITICAL |
| 14 | Causation & Correlation ID Tracking | ✅ | — |
| 15 | Idempotency | ⚠️ | MEDIUM |
| 16 | Multi Module | ✅ | LOW |
| 17 | Aggregate Lifespan Management | ✅ | LOW |
| 18 | Composite Router | ❌ | LOW |
| 19 | Application Supervision Tree | ⚠️ | MEDIUM |
| 20 | Event Upcasting | ✅ | — |
| 21 | Telemetry & Observability | ✅ | LOW |
| 22 | Batch Processing & Concurrency | ❌ | LOW |

---

## Critical Issues

Issues that **must be fixed** before production use:

1. **Default handler error behavior silently swallows errors** (§13, §4)
   - Location: `event_handler.gleam`, `handle_error` function, `None` branch
   - Impact: Events are lost without any indication when `on_error` is not configured
   - Fix: Change default to `actor.stop()` to match Commanded's `:stop` default
   - Severity: **CRITICAL**

2. **Single-shot retry in handlers** (§13, §4)
   - Location: `event_handler.gleam`, `handle_error` function, `Retry(new_state)` and `RetryWithDelay` branches
   - Impact: If retry also fails, error is silently swallowed (ack + continue)
   - Fix: Implement recursive retry loop that calls `on_error` again on retry failure
   - Severity: **HIGH**

3. **Snapshot version not checked during aggregate state loading** (§7)
   - Location: `aggregate.gleam`, `populate_from_event_store` function
   - Impact: Changed aggregate state types will deserialize incorrectly from old snapshots
   - Fix: Compare `snapshot_config.snapshot_version` against a version field in stored snapshot data
   - Severity: **HIGH**

4. **No dispatcher PID exclusion in strong consistency wait** (§12)
   - Location: `subscriptions.gleam`, `is_waiter_satisfied` function
   - Impact: Deadlock when strong-consistency handler dispatches a strong-consistency command
   - Fix: Pass dispatcher identity to `wait_for` and exclude from satisfaction check
   - Severity: **HIGH**

---

## Recommended Priority Order

1. **Fix default handler error behavior** — Change `None` branch in `handle_error` to stop the handler (CRITICAL, ~5 min fix)
2. **Implement recursive retry** — Make handler retry call `on_error` again on repeated failure instead of silently continuing (HIGH, ~30 min)
3. **Add snapshot version checking** — Compare stored snapshot version against config during load (HIGH, ~20 min)
4. **Add dispatcher exclusion to consistency wait** — Prevent strong-consistency deadlock (HIGH, ~30 min)
5. **Add TTL purging to subscriptions actor** — Prevent unbounded memory growth (MEDIUM, ~1 hour)
6. **Fix strict routing in process managers** — Check snapshot store, not just in-memory dict (MEDIUM, ~30 min)
7. **Add subscription reconnection backoff** — Handle transient event store failures gracefully (MEDIUM, ~1 hour)
8. **Add selective consistency support** — Wait for specific handlers by name (MEDIUM, ~2 hours)
9. **Add PM timeout mechanisms** — event_timeout and idle_timeout (MEDIUM, ~2 hours)
10. **Add process supervision** — Supervise aggregate servers and handle crashes (MEDIUM, ~4 hours)
