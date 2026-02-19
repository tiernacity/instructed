# CQRS/ES Correctness & Feature Review: Instructed vs Commanded (v2)

> **Date**: 2026-02-19
> **Methodology**: Parallel sub-agent research (Commanded source analysis, CQRS/ES theory) followed by parallel tier-based review (correctness invariants, core features, advanced features). All claims cite specific source files and functions. Test coverage is primary evidence.

## Executive Summary

Instructed is a well-designed CQRS/ES framework for Gleam that achieves strong feature parity with Commanded for core functionality. Its type-safe approach (three type parameters on aggregates, typed telemetry events, explicit error types) represents a genuine improvement over Commanded's dynamic Elixir model.

**Verdicts by tier:**
- **Tier 1 (Correctness)**: 4/6 correct or mostly correct, 1 broken (process lifecycle), 1 mostly correct with caveats
- **Tier 2 (Feature Parity)**: 5/6 at parity or near-parity, 1 with notable architectural differences
- **Tier 3 (Advanced)**: 4/6 present with reasonable coverage, 2 with significant gaps

**Top 3 Critical Issues:**
1. **Registry never cleans up dead aggregate servers** — after lifespan timeout or crash, the aggregate becomes permanently unreachable (`router.gleam`, `RegistryState`)
2. **Event handler/PM delete-recreate subscription on restart loses position** — replays all events from Origin instead of resuming (`event_handler.gleam`, `process_manager.gleam`, `start` functions)
3. **`register_pid` uses spawner's PID, not handler actor's PID** — deadlock prevention for strong consistency doesn't work correctly (`event_handler.gleam`, `process_manager.gleam`)

---

## Tier 1: Correctness Invariants

## T1.1. Event Append Atomicity and OCC [⚠️ MOSTLY CORRECT]

### Required Invariant
All events from a single command must be persisted atomically (all or none). Concurrent writes to the same stream must be detected via optimistic concurrency control, with one rejected.

### How Commanded Enforces It
`EventStore.append_to_stream/4` passes the full event list in a single call. The InMemory adapter handles everything in one `GenServer.call`. PostgreSQL adapters use database transactions. On `{:error, :wrong_expected_version}`, the aggregate rebuilds state and retries up to `retry_attempts` (default 10).

### How Instructed Implements It
- **In-memory**: `handle_append` in `in_memory_event_store.gleam` runs entirely within a single actor message handler. Version check, event creation, stream update, and subscriber notification happen atomically before the reply is sent.
- **PostgreSQL**: `append_to_stream` in `instructed_postgres.gleam` wraps the version check and INSERT in a `pog.transaction`. The `UNIQUE(stream_id, stream_version)` constraint provides defense-in-depth.
- **OCC retry**: `execute_with_retry` in `aggregate_server.gleam` detects `WrongExpectedVersion`, calls `rebuild_from_current_version` (incremental — only reads events after current version), and retries up to `remaining_attempts` (default 10).
- **State rollback**: In `execute_once`, the aggregate's state is only updated in the `Ok(new_version)` branch. On error, the original state is returned unchanged.

### Test Coverage
- `event_store_test.gleam`: `version_conflict_test`, `exact_version_test`, `any_version_test`, `no_stream_conflict_test`, `stream_exists_test`, `append_multiple_events_test`
- `aggregate_server_test.gleam`: OCC retry tested indirectly through aggregate server command sequences
- **Not tested**: PostgreSQL adapter transaction behavior (no integration tests visible); explicit OCC retry-exhaust scenario

### Verdict & Issues
- **MEDIUM: Event type always empty string.** In `execute_once` (`aggregate_server.gleam`), every `EventData` is created with `event_type: ""`. Events persisted through the aggregate server have no type metadata, undermining event store querying and debugging. The PostgreSQL adapter's `event_type` callback is never used when going through the aggregate server path.

---

## T1.2. Command Serialization Per Aggregate [✅ CORRECT]

### Required Invariant
At most one command executes against a given aggregate instance at any time. Commands to the same aggregate are serialized.

### How Commanded Enforces It
Each aggregate instance is a `GenServer` process. Command execution is a synchronous `GenServer.call`. The process is registered by name via `Commanded.Registration`, ensuring a single process per aggregate identity.

### How Instructed Implements It
- **Per-aggregate actors**: `aggregate_server.gleam` runs as an OTP actor. Each aggregate instance gets its own actor process, processing one `Execute` message at a time.
- **Registry**: `RegistryState` in `router.gleam` maps `stream_id → Subject`. The registry is itself an actor, making lookup-or-create atomic.
- **Synchronous dispatch**: `router.dispatch` uses `process.call` (synchronous OTP call), blocking until the aggregate server replies.

### Test Coverage
- `aggregate_server_test.gleam`: Sequential command execution, state caching
- `router_test.gleam`: `dispatch_sequence_test`, `dispatch_multiple_aggregates_test`

### Verdict & Issues
- **MEDIUM: Registry never removes dead servers.** If an aggregate server stops (lifespan timeout, error), the stale `Subject` remains in the registry dict. Subsequent commands timeout instead of starting a fresh server. See T1.6 for full analysis.

---

## T1.3. At-Least-Once Delivery and Subscription Protocol [⚠️ MOSTLY CORRECT]

### Required Invariant
Every persisted event must eventually be delivered to every active subscriber at least once. Subscribers track checkpoints; on restart, delivery resumes from the last checkpoint.

### How Commanded Enforces It
Persistent subscriptions use event-store-managed checkpoints. Events are delivered one at a time; the next event is only sent after `ack_event`. Handlers monitor their subscription process; if it dies, the handler stops and restarts (re-subscribing from checkpoint).

### How Instructed Implements It
- **Backpressure**: `PersistentSub` in `in_memory_event_store.gleam` tracks `in_flight: Option(RecordedEvent)`. `maybe_deliver_next` only delivers when `in_flight` is `None`.
- **Checkpoint**: `handle_ack_event` sets `checkpoint: event.event_number`. Un-acked events are redelivered on re-subscribe.
- **PostgreSQL**: `SubscriptionPoller` uses cursor-based polling with gap detection (`take_contiguous_prefix`, `max_gap_retries`). Checkpoint stored durably in `event_store_subscriptions`.

### Test Coverage
- `event_store_test.gleam`: `persistent_subscription_delivers_events_test`, `persistent_subscription_backpressure_test`, `persistent_subscription_from_current_test`, `persistent_subscription_ack_tracks_position_test`

### Verdict & Issues
- **HIGH: Handler/PM delete-recreate subscription on restart loses position.** In `event_handler.gleam` and `process_manager.gleam`, `start` handles `SubscriptionAlreadyExists` by deleting and recreating:
  ```gleam
  Error(error.SubscriptionAlreadyExists) -> {
    let _ = event_store.delete_subscription(stream, config.name)
    case event_store.subscribe_persistent(...) {
  ```
  This deletes the checkpoint. On recreate, it starts from `config.start_from` (default `Origin`). With both in-memory and PostgreSQL adapters, the database row is deleted, losing the last acked position. All events replay from the beginning. The comment acknowledges this for in-memory but the same code runs for PostgreSQL.
  
  **Root cause**: The handler can't re-attach to an existing subscription to update its handler callback. Instead it tears down and rebuilds.

---

## T1.4. Idempotency [⚠️ MOSTLY CORRECT]

### Required Invariant
Handlers must tolerate receiving the same event more than once. Duplicate delivery is expected during restarts and catch-up.

### How Commanded Enforces It
- **Handlers**: `last_seen_event` (in-memory) skips events with `event_number <= last_seen_event`. Not persisted — relies on subscription checkpoint for restart boundary.
- **PMs**: `last_seen_event` persisted in snapshot `source_version`. Survives restarts via `event_already_seen?`.

### How Instructed Implements It
- **Event handlers** (`event_handler.gleam`): `last_seen_event: Option(Int)` checked in `handle_actor_message`. Events ≤ last_seen are acked and skipped.
- **Process managers** (`process_manager.gleam`): Per-instance `last_seen_event` in `PMInstance`. Restored from snapshot `source_version` via `get_or_load_instance`. Guard in `start_or_continue_instance`.
- **PM snapshot ordering**: dispatch → apply → save snapshot (with `last_seen_event`) → ack. Crash between save and ack is safe — snapshot filters duplicate on restart.

### Test Coverage
- Event handler idempotency tested indirectly through handler tests
- PM instance idempotency tested via `start_strict`/`continue_strict` tests and snapshot reload

### Verdict & Issues
- **HIGH: Event handler `last_seen_event` not persisted, combined with subscription replay.** On handler restart, `last_seen_event` resets to `None`. Combined with T1.3 Issue (subscription replays from Origin), the handler reprocesses all historical events. The idempotency guard only works within a single handler lifetime. PM idempotency correctly survives restarts via snapshots.
- **LOW: No idempotency for dispatched commands.** Neither aggregate server nor router checks for duplicate commands. If a PM re-dispatches due to restart, the aggregate executes it again. Same limitation as Commanded.

---

## T1.5. Error Handling and Failure Recovery [⚠️ MOSTLY CORRECT]

### Required Invariant
Errors must be handled predictably. Default behavior should be safe (stop and redeliver, not silently skip). Error callbacks must support retry, skip, and stop strategies.

### How Commanded Enforces It
- Handler default: `:stop` (handler stops, event redelivered on restart)
- PM event error default: `{:stop, reason}` (PM instance stops)
- PM command error default: stop with error reason
- Recursive retry via `error/3` callback

### How Instructed Implements It
- **Event handler default**: `on_error: None` → `actor.stop()` without acking. Correct — matches Commanded.
- **Handler error strategies**: `Retry`, `RetryWithDelay`, `Skip`, `Stop` — all implemented with recursive retry on re-failure.
- **PM event error default**: `on_event_error: None` → returns `state` unchanged, silently skipping. **Different from Commanded** which stops.
- **PM command error**: `CmdRetry`, `CmdRetryWithDelay`, `CmdSkip`, `CmdDiscardPending`, `CmdContinueWith`, `CmdStop` — all implemented.

### Test Coverage
- `event_handler_test.gleam`: `handler_stops_on_unhandled_error_test`, `handler_stops_on_explicit_stop_test`, `handler_continues_on_skip_test`, `handler_retries_multiple_times_then_skips_test`, `handler_retries_then_stops_test`, `handler_retry_with_delay_recursive_test`
- `process_manager_test.gleam`: `pm_event_error_callback_test`, `pm_command_error_skip_continues_test`, `pm_command_error_stop_test`

### Verdict & Issues
- **HIGH: PM default event error silently skips.** When no `on_event_error` is configured in `process_manager.gleam`, `handle_event_error` returns `state` unchanged. The event is acked (in `route_event`). In Commanded, the default is to stop the PM instance. This can lead to silently lost events.
- **MEDIUM: Recursive retry has unbounded depth.** In `event_handler.gleam`, `handle_error` calls itself recursively. If `on_error` always returns `Retry(...)`, this causes infinite recursion / stack overflow. Same pattern exists in PM `handle_event_error`. Commanded has the same design — it relies on user callbacks to eventually stop/skip.
- **LOW: `Stop` acks event before stopping.** In `handle_error`, `error.Stop` calls `ack_event` then `actor.stop()`. The event is consumed even though handling failed. This matches Commanded's explicit-stop behavior but may surprise users.

---

## T1.6. Process Lifecycle and Supervision [❌ BROKEN]

### Required Invariant
Processes must be properly supervised. Dead processes must be detected and cleaned up. The system must recover from process crashes without manual intervention.

### How Commanded Enforces It
- Aggregates: `restart: :temporary` — started on demand, not auto-restarted. Registration detects `:aggregate_stopped`.
- Handlers: `restart: :permanent` — auto-restarted by supervisor.
- PM Router: Monitors PM instances; abnormal exit cascades to router restart.

### How Instructed Implements It
- **Aggregate server**: OTP Actor started by registry on demand.
- **Registry**: `Dict(String, Subject)` in a single actor. No monitoring of servers.
- **Event handlers/PMs**: Started via `start` functions returning `Subject`. No supervision wiring.

### Test Coverage
- Lifespan tested in `lifespan_test.gleam` (decisions work within a single lifecycle)
- No tests for: registry stale entries, process crash recovery, supervision, `SetSelf` race

### Verdict & Issues
- **CRITICAL: Registry never cleans up dead aggregate servers.** In `router.gleam`, `RegistryState` holds `servers: Dict(String, Subject(...))`. When an aggregate server stops (lifespan timeout, error, `actor.stop()`), the registry is never notified. The stale `Subject` remains. Next command → `process.call` to dead process → timeout (5000ms) → `Error(Timeout)`. The aggregate is permanently unreachable until application restart. This breaks lifespan management as a feature.
- **HIGH: No monitoring of handlers or PMs.** No supervisor auto-restarts crashed handlers. No child specs provided. User must wrap in supervision manually, but the API doesn't facilitate this (no `child_spec` equivalent).
- **HIGH: Aggregate server self-subscription leak.** `aggregate_server.start` creates a transient subscription with a closure capturing the server's `Subject`. If the server dies, the subscription remains in the event store, calling `process.send` on a dead Subject (no-op in Erlang). The subscription entry is never cleaned up. Memory leak grows linearly with aggregate server churn.
- **MEDIUM: `SetSelf` / `SetSubscriptionInfo` race condition.** Setup messages are sent asynchronously after `start`. If a command/event arrives before setup completes, `self_subject` is `None` (lifespan timer not scheduled) or `event_store`/`subscription` is `None` (ack silently skipped).
- **HIGH: `register_pid` uses spawner's PID, not handler actor's PID.** In `event_handler.start` and `process_manager.start`:
  ```gleam
  subscriptions.register_pid(subs, process.self(), config.name)
  ```
  `process.self()` is the calling process, not the handler actor. If the handler actor later dispatches a strong-consistency command, `wait_for` captures the handler actor's PID, which doesn't match the registered (spawner) PID. Deadlock prevention fails.

---

## Cross-Cutting: Causation/Correlation Chain [✅ CORRECT]

The causation chain is correctly implemented end-to-end:
1. `router.dispatch` (`router.gleam`) auto-generates `command_id` and `correlation_id`, sets `causation_id = command_id`
2. `aggregate_server.execute_once` (`aggregate_server.gleam`) stamps `causation_id` and `correlation_id` onto every `EventData`
3. `process_manager.process_instance_event` (`process_manager.gleam`) sets `causation_id = Some(recorded_event.event_id)` and preserves `correlation_id = recorded_event.correlation_id`

Chain: Command₁ → Event₁ (causation=cmd₁) → PM dispatches Command₂ (causation=event₁_id, correlation=original) → Event₂ (causation=cmd₂)

Tested thoroughly in `causation_chain_test.gleam` with 6 test cases covering explicit context, auto-generation, correlation preservation, PM causation propagation, full chain, and event ID uniqueness.

---

## Tier 2: Core Feature Parity

## T2.7. Aggregates [✅ AT PARITY]

### Commanded Reference
Elixir module with `execute/2` and `apply/2` callbacks. Supports separate command handler modules (`dispatch ... to: Handler, aggregate: Aggregate`), `before_execute` hooks, and multiple return types from execute (event, list, `{:ok, events}`, `:ok`, `nil`, `{:error, reason}`, `%Multi{}`).

### Instructed Implementation
- ✅ `Aggregate(state, command, event)` record with `execute`, `apply_event`, `empty_state` — fully typed
- ✅ State rebuild via `populate_from_event_store` with snapshot support and batched reading (1000 batch size)
- ✅ Incremental rebuild on version conflict (`rebuild_from_version`)
- ✅ Upcasting during rebuild via `apply_upcasting_to_store` wrapper
- ❌ No separate command handler module
- ❌ No `before_execute` hook
- 📝 Execute returns `Result(List(event), String)` — simplified but complete; `Ok([])` = no-op

### Test Coverage
`aggregate_test.gleam`: 10 tests covering empty state, execute, apply, rebuild, event store integration, version rebuild. `aggregate_server_test.gleam`: 8+ tests covering server lifecycle, caching, snapshot integration.

### Gaps & Issues
- **LOW**: No separate command handler module — Gleam's record-of-functions approach makes this unnecessary in practice.
- **LOW**: No `before_execute` hook — can be implemented in middleware or the execute function itself.

---

## T2.8. Command Dispatch Pipeline [⚠️ PARTIAL]

### Commanded Reference
`Router` with compile-time `dispatch` macro supporting per-command configuration (handler, aggregate, function, identity, timeout, lifespan, consistency). `CompositeRouter` for multi-aggregate routing. `Dispatcher` handles task isolation, retry, and `ExecutionResult` returning options.

### Instructed Implementation
- ✅ Router with aggregate server registry, identity extraction, prefix, middleware pipeline
- ✅ Middleware: `before_dispatch`, `after_dispatch`, `after_failure` with halt semantics
- ✅ Default timeout 5000ms, default retry 10, configurable
- ✅ Identity validation (empty check before dispatch)
- ✅ Telemetry integration (start/stop/exception events)
- ❌ No composite router — one aggregate per router
- ❌ No per-command dispatch configuration (lifespan, timeout, consistency per command type)
- ❌ `returning` option defined in `CommandContext` but not wired into router
- 📝 Always returns full `DispatchResult` (simpler than Commanded's selective return)

### Test Coverage
`router_test.gleam`: 7 tests covering dispatch, sequences, errors, prefix, multiple aggregates, middleware.

### Gaps & Issues
- **MEDIUM**: No composite router — multi-aggregate apps need multiple routers and manual coordination.
- **LOW**: No per-command dispatch options — timeout/consistency/lifespan are router-level only.

---

## T2.9. Event Handlers [✅ AT PARITY]

### Commanded Reference
GenServer with persistent subscription. Configuration: `name`, `consistency`, `start_from`, `subscribe_to`, `concurrency`, `batch_size`, `state`. Callbacks: `handle/2`, `handle_batch/1`, `error/3`, `init/1`, `after_start/1`, `before_reset/0`, `partition_by/2`.

### Instructed Implementation
- ✅ OTP Actor with persistent subscription, `name`, `start_from`, `stream_id` (AllStreams/SpecificStream)
- ✅ `handle_event(event, recorded_event, state)` — receives full recorded event as metadata
- ✅ Error handling: `Retry`, `RetryWithDelay`, `Skip`, `Stop` with recursive retry
- ✅ Idempotency guard via `last_seen_event`
- ✅ Strong consistency support with subscriptions actor integration
- ✅ Upcaster support per-handler
- ❌ No `concurrency` / `partition_by` (single-threaded)
- ❌ No `handle_batch` / `batch_size`
- ❌ No `init/1`, `after_start/1`, `before_reset/0` lifecycle hooks

### Test Coverage
`event_handler_test.gleam`: 6 tests covering stop-on-error, explicit stop, skip, recursive retry (multiple times then skip, retry then stop), retry with delay.

### Gaps & Issues
- **MEDIUM**: No concurrency/batching — advanced features for high-throughput scenarios.
- **LOW**: No lifecycle hooks — can be handled externally.

---

## T2.10. Process Managers [✅ AT PARITY]

### Commanded Reference
Three-layer architecture: ProcessRouter → DynamicSupervisor → ProcessManagerInstance. Callbacks: `interested?/1,2`, `handle/2,3`, `apply/2,3`, `after_command/2,3`, `error/3`. Per-instance processes with snapshot persistence. Causation chain propagation.

### Instructed Implementation
- ✅ Full routing: `Start`, `StartStrict`, `StartMany`, `Continue`, `ContinueStrict`, `ContinueMany`, `Stop`, `StopMany`, `Skip`
- ✅ Correct handle/apply order: handle → dispatch → apply → persist
- ✅ Per-instance snapshots with `last_seen_event` in `source_version`
- ✅ Causation chain: `causation_id = event_id`, `correlation_id` preserved
- ✅ `after_command` with `AfterContinue` / `AfterStop`
- ✅ Command error handling: `CmdRetry`, `CmdRetryWithDelay`, `CmdSkip`, `CmdDiscardPending`, `CmdContinueWith`, `CmdStop`
- ✅ Strong consistency support
- ⚠️ Single actor architecture (all instances in one Dict) — trades per-instance parallelism for simplicity
- ⚠️ Default event error silently skips (Commanded stops)
- ❌ No `event_timeout` or `idle_timeout`
- ❌ No `identity/0` function for reading instance UUID within callbacks

### Test Coverage
`process_manager_test.gleam`: 14 tests covering happy path, skip, stop, fan-out (StartMany, StopMany, ContinueMany), strict routing, after_command, event errors, command errors (skip, stop), causation propagation, state updates.

### Gaps & Issues
- **HIGH**: Default PM event error silently skips instead of stopping (behavioral divergence from Commanded).
- **MEDIUM**: Single-actor architecture serializes all PM instances — may become a bottleneck.
- **LOW**: No event/idle timeouts.

---

## T2.11. Strong vs Eventual Consistency [✅ AT PARITY]

### Commanded Reference
`ConsistencyGuarantee` middleware calls `Subscriptions.wait_for/5` after dispatch. GenServer + ETS tracks handler acks. PID-based dispatcher exclusion prevents deadlock. Default timeout 5000ms. Selective consistency via handler name lists.

### Instructed Implementation
- ✅ `subscriptions.gleam`: Actor-based tracking with dict state
- ✅ Handler/PM registration with consistency level
- ✅ `ack_event` after successful processing
- ✅ `wait_for` blocks until all strong handlers ack ≥ version
- ✅ `wait_for_handlers` for selective consistency
- ✅ Auto-exclusion via PID lookup (prevents deadlock)
- ✅ Ack TTL purge (1-hour, 5-minute cycle) prevents memory leaks
- ✅ No strong handlers → immediate Ok

### Test Coverage
`subscriptions_test.gleam`: 13 tests covering start, no-handlers, eventual-only, acked-before-wait, higher-version, async ack, timeout, partial ack, all-acked, stream isolation, handler integration, eventual-no-ack, application integration.

### Gaps & Issues
- **HIGH**: `register_pid` bug (see T1.6) — spawner PID registered instead of handler actor PID, breaking deadlock prevention.
- **LOW**: Single-node only — no distributed strong consistency.

---

## T2.12. Snapshots [✅ AT PARITY]

### Commanded Reference
Configurable per-aggregate (`snapshot_every`, `snapshot_version`). Async snapshot via `send(self(), {:take_snapshot})`. Version validation rejects stale snapshots. PM snapshots taken automatically after every event.

### Instructed Implementation
- ✅ `SnapshotConfig(snapshot_every, snapshot_version)` per aggregate server
- ✅ `maybe_take_snapshot` called synchronously after events appended (fault-tolerant — failure doesn't fail the command)
- ✅ Version encoded in `source_type` as `"aggregate:vN"`, validated on read
- ✅ Version mismatch → full event replay
- ✅ PM snapshots after every event processing with `event_number` as `source_version`
- ✅ PM snapshot deleted on Stop/AfterStop
- ✅ `snapshot.coerce` bridges type gap between aggregate state and event store
- 📝 Synchronous snapshot (vs Commanded's async) — simpler, no snapshot-deferred lifespan timeout needed

### Test Coverage
`aggregate_server_test.gleam`: `snapshot_taken_after_n_events_test`, `snapshot_version_match_uses_snapshot_test`, `snapshot_version_mismatch_forces_replay_test`, `snapshot_encode_decode_version_test`

### Gaps & Issues
- **LOW**: PM snapshots have no version checking (always `source_type: "process_manager"` without version encoding).

---

## Tier 3: Advanced & Convenience Features

## T3.13. Multi Module [⚠️ PARTIAL]

### Commanded Reference
`Multi.new(aggregate)` with `execute/2,3` (named steps), `reduce/3,4`, nested Multi support, auto-apply between steps, `run/1` returns `{aggregate, events}`.

### Instructed Implementation
- ✅ `multi.new(state)`, `multi.execute(multi, fn)`, `multi.to_result()`
- ✅ `multi.reduce(multi, items, execute_fn, apply_fn)` with per-item execute+apply
- ✅ Error short-circuit and atomic discard
- ✅ `get_state`, `get_events`, `has_error` introspection helpers
- ❌ No named steps with state map
- ❌ No auto-apply between steps (user must call `multi.apply` explicitly)
- ❌ No nested Multi support

### Assessment
Core Multi workflow works. Named steps and nested Multi are power-user features. The explicit `apply` call is more verbose but transparent. **Severity: LOW** — gaps are convenience, not correctness.

---

## T3.14. Aggregate Lifespan Management [⚠️ PARTIAL]

### Commanded Reference
`AggregateLifespan` behaviour: `after_event/1`, `after_command/1`, `after_error/1`. Returns timeout/`:infinity`/`:hibernate`/`:stop`/`{:stop, reason}`. `DefaultLifespan` stops on exceptions. Per-command router config. Snapshot-deferred timeout.

### Instructed Implementation
- ✅ `Lifespan` record: `after_command(state, cmd)`, `after_error(state, cmd, reason)`, `after_event(state, event)`
- ✅ `KeepRunning`, `Stop`, `StopAfter(ms)`, `Hibernate` (falls back to KeepRunning)
- ✅ Convenience: `always_running()`, `new_idle(ms)`, `stop_after_command()`
- ✅ Callbacks receive full aggregate state (richer context than Commanded)
- ❌ No `{:stop, reason}` — only normal stop
- ❌ No true Erlang hibernate support
- ❌ No per-command lifespan in router
- ❌ No snapshot-deferred timeout

### Assessment
Lifespan works correctly within a single lifecycle but is undermined by T1.6 (registry doesn't clean up dead servers). **Severity: HIGH** when combined with registry issue — **the feature is broken in practice**.

---

## T3.15. Event Upcasting [✅ PRESENT]

### Commanded Reference
Protocol-based `Upcaster` with automatic dispatch by struct type. `upcast(event, metadata)`. Fallback to `Any`. Applied everywhere events are read.

### Instructed Implementation
- ✅ `Upcaster(event)` record with `upcast: fn(RecordedEvent) -> RecordedEvent`
- ✅ `chain/2`, `chain_all/1` for composable multi-version transforms
- ✅ Applied to aggregate server (rebuild + external events), event handlers, process managers
- ✅ Per-component configuration via `with_upcaster`
- ✅ Full `RecordedEvent` access (can modify event_type, metadata, data)
- 📝 Explicit wiring (vs Commanded's automatic protocol dispatch)

### Assessment
Feature-complete with compositional advantages over Commanded. **Severity: None** — no gaps.

---

## T3.16. Telemetry & Observability [✅ PRESENT]

### Commanded Reference
Erlang `:telemetry` with `[:commanded, :subsystem, :action, :start/:stop/:exception]` events. Rich metadata including aggregate state, execution context, stacktraces. Covers dispatch, aggregate, handler, PM.

### Instructed Implementation
- ✅ Typed `TelemetryEvent` union (12 variants) with start/stop/exception pattern
- ✅ Erlang `:telemetry` integration (optional, graceful fallback)
- ✅ Pure-Gleam `set_handler/1` callback for testing/simple logging
- ✅ Covers: command dispatch, aggregate execution, event handler, process manager
- ⚠️ Lighter metadata (no aggregate state, no execution context, no stacktrace — error string only)

### Assessment
Type-safe telemetry with convenient Gleam-first API. Metadata is lighter than Commanded but sufficient for most observability needs. **Severity: LOW**.

---

## T3.17. Projections [✅ PRESENT]

### Commanded Reference
No separate projection module — projections are event handlers. `commanded_ecto_projections` adds database-backed projections.

### Instructed Implementation
- ✅ Dedicated `projection` module with in-memory queryable state via `get_state/2`
- ✅ Full handler guarantees: ack, idempotency, error handling, start_from
- ✅ Strong consistency support
- ❌ In-memory only — no database-backed projection equivalent

### Assessment
First-class projection abstraction is a convenience win. In-memory limitation is acceptable for v1. **Severity: LOW**.

---

## T3.18. Missing Features [VARIES]

| Feature | Commanded | Instructed | Severity |
|---------|-----------|------------|----------|
| Composite Router | ✅ | ❌ | MEDIUM |
| Batch processing (`handle_batch`) | ✅ | ❌ | LOW |
| Handler concurrency (`concurrency > 1`) | ✅ | ❌ | MEDIUM |
| PubSub (distributed) | ✅ | ❌ | LOW (expected) |
| Subscription reset | ✅ | ❌ | LOW |
| Serialization / TypeProvider | ✅ | ❌ | MEDIUM (needed for production adapters) |
| `before_execute` hook | ✅ | ❌ | LOW |
| `returning` option | ✅ | ❌ | LOW |
| `aggregate_state/3` query | ✅ | ❌ | LOW |
| PM `event_timeout` / `idle_timeout` | ✅ | ❌ | LOW |
| Application-level error config | ✅ | ❌ | LOW |
| OTP Supervision tree | ✅ | ❌ | HIGH (user must manage) |

---

## Summary

### Correctness Scorecard

| # | Invariant | Verdict | Critical Issues |
|---|-----------|---------|-----------------|
| 1 | Event append atomicity and OCC | ⚠️ MOSTLY CORRECT | Event type always empty string |
| 2 | Command serialization per aggregate | ✅ CORRECT | Registry stale entries (liveness) |
| 3 | At-least-once delivery | ⚠️ MOSTLY CORRECT | Delete-recreate loses subscription position |
| 4 | Idempotency | ⚠️ MOSTLY CORRECT | Handler idempotency lost on restart |
| 5 | Error handling and recovery | ⚠️ MOSTLY CORRECT | PM default silently skips; unbounded retry |
| 6 | Process lifecycle and supervision | ❌ BROKEN | Registry stale entries; no monitoring; PID registration bug |

### Feature Parity Scorecard

| # | Feature | Status | Key Gap |
|---|---------|--------|---------|
| 7 | Aggregates | ✅ AT PARITY | No separate handler module |
| 8 | Command dispatch pipeline | ⚠️ PARTIAL | No composite router |
| 9 | Event handlers | ✅ AT PARITY | No concurrency/batching |
| 10 | Process managers | ✅ AT PARITY | Single-actor architecture |
| 11 | Strong/eventual consistency | ✅ AT PARITY | PID registration bug |
| 12 | Snapshots | ✅ AT PARITY | No PM snapshot versioning |
| 13 | Multi module | ⚠️ PARTIAL | No named steps/nested Multi |
| 14 | Aggregate lifespan | ⚠️ PARTIAL | Broken by registry issue |
| 15 | Event upcasting | ✅ PRESENT | None |
| 16 | Telemetry | ✅ PRESENT | Lighter metadata |
| 17 | Projections | ✅ PRESENT | In-memory only |
| 18 | Missing features | N/A | Composite router, serialization |

### Critical Issues (must-fix for production)

1. **Registry never cleans up dead aggregate servers** (`router.gleam`, `RegistryState`). The registry must monitor aggregate server processes and remove entries when they die. Without this fix, lifespan management is broken and any aggregate server crash makes the aggregate permanently unreachable.

2. **Handler/PM delete-recreate subscription loses checkpoint** (`event_handler.gleam:start`, `process_manager.gleam:start`). Instead of deleting and recreating, the subscription system should support re-attaching a new handler callback to an existing subscription. Alternatively, the in-memory store should preserve the checkpoint across delete+recreate.

3. **`register_pid` uses spawner PID instead of handler actor PID** (`event_handler.gleam:start`, `process_manager.gleam:start`). The `subscriptions.register_pid` call should happen inside the actor's init (e.g., as an initial message handler), not in the spawning function.

### Recommended Improvements (by priority)

**Correctness fixes (P0):**
1. Add process monitoring in registry — detect dead servers, remove stale entries
2. Fix subscription re-attachment to preserve checkpoints on restart
3. Fix `register_pid` to capture handler actor PID, not spawner PID
4. Change PM default event error from silent skip to stop (match Commanded)

**Liveness fixes (P1):**
5. Set `event_type` from aggregate/event metadata instead of empty string
6. Clean up transient subscriptions when aggregate servers die
7. Add bounded retry depth (max retry count) as a safety valve

**Feature additions (P2):**
8. Composite router for multi-aggregate applications
9. Add supervision support (child specs, start functions compatible with `static_supervisor`)
10. Serialization framework for production PostgreSQL/SQLite adapters

**Nice-to-have (P3):**
11. Handler concurrency and partitioning
12. Batch event processing
13. Named Multi steps
14. PM event/idle timeouts
15. Distributed strong consistency

### Test Coverage Gaps

| Area | What's Missing |
|------|---------------|
| Registry stale entries | No test for command dispatch after aggregate server stops |
| Subscription resume | No test for handler restart preserving position |
| Handler restart idempotency | No test for reprocessing after restart |
| PostgreSQL adapter | No integration tests visible |
| Unbounded retry | No test for recursive retry depth |
| `SetSelf`/`SetSubscriptionInfo` race | No test for early message arrival |
| PID registration correctness | No test verifying deadlock prevention works |
| Lifespan + registry interaction | No test for command after lifespan-triggered stop |
