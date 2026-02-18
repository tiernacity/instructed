# Instructed Feature Parity with Commanded

Iterative module-by-module implementation to bring Instructed (Gleam CQRS/ES) to feature parity with Commanded (Elixir).

## Goals
- Achieve feature parity with Commanded across all 21 modules ✅
- Each module must pass self-review (no ⚠️ or ❌ findings) ✅
- Architecture reviews after each module completion ✅
- All 20 key invariants maintained ✅

## Checklist
- [x] 1. Event Store Interface & In-Memory Adapter
- [x] 2. Event Types & Recorded Events
- [x] 3. Error Types
- [x] 4. Aggregate Core (types + state rebuilding)
- [x] 5. Aggregate Server (GenServer process per instance)
- [x] 6. Snapshot Types & Integration
- [x] 7. Command Context & Execution
- [x] 8. Middleware Pipeline
- [x] 9. Command Router (wiring aggregate server, identity, retry)
- [x] 10. Event Handler (lifecycle, subscriptions, error handling, idempotency)
- [x] 11. Projection (builds on event handler patterns)
- [x] 12. Process Manager (routing, state persistence, error handling, command dispatch)
- [x] 13. Application & Supervision Tree
- [x] 14. Causation & Correlation Chain
- [x] 15. Strong vs Eventual Consistency
- [x] 16. PostgreSQL Adapter
- [x] 17. SQLite Adapter
- [x] 18. Multi Module
- [x] 19. Aggregate Lifespan
- [x] 20. Event Upcasting
- [x] 21. Telemetry & Observability

## Current State — COMPLETE ✅

- **Build**: clean — `instructed`, `instructed-sqlite`, `instructed-postgres` all compile with no errors/warnings
- **Tests**: 151 passed (instructed) + 18 passed (instructed-sqlite) = **169 total**; postgres tests need live DB
- **Last commit**: `a4f1964` — Modules 17-21: SQLite, Multi, Lifespan, Upcast, Telemetry
- **Source files**: 20 modules in `instructed/src/instructed/` + 1 Erlang FFI
- **Test files**: 14 test files in `instructed/test/`

## Module Summary

### M17: SQLite Adapter (`instructed-sqlite/`)
- Full `EventStore(event)` record of functions via `sqlight`
- Schema: WAL-mode SQLite with events, snapshots, subscriptions tables
- UNIQUE(stream_id, stream_version) constraint for optimistic concurrency
- Persistent subscriptions with position tracking (ack_event updates DB)
- Transient all-stream and per-stream subscriptions
- Snapshot upsert with ON CONFLICT DO UPDATE
- 18 passing tests

### M18: Multi Module (`instructed/multi.gleam`)
- Gleam equivalent of `Commanded.Aggregates.Multi`
- Pipeline: `new → execute → apply → reduce → to_result`
- Short-circuits on first error — all accumulated events discarded (Invariant 15)
- Helpers: `get_state`, `get_events`, `has_error`

### M19: Aggregate Lifespan (`instructed/lifespan.gleam`)
- `LifespanDecision`: `KeepRunning`, `Stop`, `StopAfter(ms)`, `Hibernate`
- Three callbacks: `after_command`, `after_error`, `after_event`
- Wired into `aggregate_server` with `LifespanTimeout` message + timer scheduling
- Helpers: `always_running()`, `new_idle(ms)`, `stop_after_command()`

### M20: Event Upcasting (`instructed/upcast.gleam`)
- `Upcaster(event)` record with a single `upcast` function
- `apply`, `apply_all`, `chain`, `chain_all` helpers
- `identity()` placeholder upcaster
- Wired into `aggregate_server` (reading events during state rebuild)
- Wired into `event_handler` (before delivering events to handler callbacks)

### M21: Telemetry & Observability (`instructed/telemetry.gleam`)
- `TelemetryEvent` type covering all key instrumentation points:
  - `CommandDispatch{Start,Stop,Exception}`
  - `AggregateExecute{Start,Stop,Exception}`
  - `EventHandle{Start,Stop,Exception}`
  - `ProcessManagerHandle{Start,Stop,Exception}`
- Erlang FFI (`instructed_telemetry_ffi.erl`):
  - Gleam handler via `persistent_term` — zero-cost when no handler registered
  - Optional `:telemetry.execute/3` emission with graceful `catch` fallback
  - `erlang:monotonic_time(nanosecond)` for high-resolution durations
- Convenience helpers: `dispatch_start/2`, `dispatch_stop/4`, `aggregate_start/1`, etc.
- Wired into `router.dispatch_with_context` (CommandDispatch events)
- Wired into `event_handler` primary handle_event path (EventHandle events)
- 13 passing telemetry tests

## Architecture Integration Map (all ✅)

- [x] Event Store → Event Handler: subscription callbacks in handler's process
- [x] Event Store → Aggregate Server: append returns version; incremental reads
- [x] Aggregate Server → Router: dispatches through server; state cached in process
- [x] Router → Middleware: pipeline result influences dispatch
- [x] Router → Telemetry: dispatch start/stop/exception emitted
- [x] Event Handler → Subscription tracking: ack called after processing
- [x] Event Handler → Telemetry: handle start/stop/exception emitted
- [x] Process Manager → Router: dispatch_command with causation_id from event
- [x] Process Manager → Event Store: state saved as snapshot; loaded on restart
- [x] Application → All: event_store threaded to all start_* helpers
- [x] Consistency → Subscriptions: strong handlers register; dispatch waits (M15)
- [x] Aggregate Server → Snapshots: snapshot loaded during rebuild; written after N events
- [x] Aggregate Server → Lifespan: stop/hibernate/timeout after command/event/error
- [x] Aggregate Server → Upcaster: applied to events during state rebuild
- [x] Event Handler → Upcaster: applied to events before delivery to handler callback
- [x] Multi → Aggregate Execute: composable multi-step command execution (Invariant 15)

## Key Invariants — All 20 Maintained ✅

1. Commands to the same aggregate instance are serialized (via actor) ✅
2. `apply_event` must never fail — used during replay ✅
3. Events appended atomically with expected version ✅
4. Version conflict → rebuild from new events only, retry (up to 10) ✅
5. Event handlers are singletons — one instance per handler name ✅
6. Handler subscription name must never change between releases ✅ (documented)
7. `start_from` only applies on FIRST subscription creation ✅
8. Event handler errors invoke error callback — never silently swallowed ✅
9. PM state persisted (snapshot) after each handled event ✅
10. PM dispatches with causation_id = source event_id, correlation_id preserved ✅
11. Strong consistency blocks dispatch until all strong handlers acked ✅
12. Subscription callbacks deliver events to subscriber's process ✅
13. Aggregate stream prefix must never change ✅ (documented)
14. Snapshot version incremented when aggregate struct changes ✅ (documented)
15. Multi errors discard all events — atomic ✅
16. Default retry attempts: 10 for version conflicts ✅
17. Default dispatch timeout: 5 seconds ✅
18. Event batch read size: 1,000 when rebuilding state ✅
19. Aggregate processes are temporary — started on demand ✅
20. Handler `last_seen_event` provides idempotency guard ✅

## Verification Commands

```bash
cd /workspace/instructed && gleam build   # clean
cd /workspace/instructed && gleam test    # 151 passed
cd /workspace/instructed-sqlite && gleam build && gleam test  # 18 passed
cd /workspace/instructed-postgres && gleam build  # clean (needs live DB for tests)
```
