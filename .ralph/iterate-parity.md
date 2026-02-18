# Instructed Feature Parity with Commanded

Iterative module-by-module implementation to bring Instructed (Gleam CQRS/ES) to feature parity with Commanded (Elixir).

## Goals
- Achieve feature parity with Commanded across all 21 modules
- Each module must pass self-review (no ⚠️ or ❌ findings)
- Architecture reviews after each module completion
- All 20 key invariants maintained

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
- [ ] 15. Strong vs Eventual Consistency
- [ ] 16. PostgreSQL Adapter
- [ ] 17. SQLite Adapter
- [ ] 18. Multi Module
- [ ] 19. Aggregate Lifespan
- [ ] 20. Event Upcasting
- [ ] 21. Telemetry & Observability

## Current State
- **Build**: clean (`gleam build` — no errors, no warnings)
- **Tests**: 89 passed, no failures
- **Last commit**: `4d3f4f3` — Module 14: Causation & Correlation Chain
- **Source files**: 16 modules in `instructed/src/instructed/`
- **Test files**: 9 test files in `instructed/test/`

## Verification Commands
```bash
cd /workspace/instructed && gleam build   # should be clean
cd /workspace/instructed && gleam test    # should be 89+ passed
```

---

## Module 15 Research (next to implement)

### What Commanded does (already researched)

Commanded's strong consistency system spans several components:

**`Commanded.Subscriptions`** (GenServer + ETS):
- Tracks which named handlers have processed which `(stream_id, stream_version)` pairs
- `ack_event(application, name, :strong, event)` — broadcasts via PubSub when a strong-consistency handler acks
- `wait_for(application, stream_uuid, stream_version, opts, timeout)` — blocks the caller's process until all strong handlers have acked; returns `:ok` or `{:error, :timeout}`
- Default timeout: 5 seconds (matches Invariant 17)

**`Commanded.Subscriptions.Registry`** (tracks registered handlers):
- `register(application, name, module, pid, consistency)` — registers a handler as `:strong` or `:eventual`
- `all(application)` — returns all registered handlers as `{name, module, pid}` tuples

**`Commanded.Dispatcher`** (router.ex / dispatcher.ex):
- After successful command dispatch (events appended to store), calls `Subscriptions.wait_for/5` if consistency is `:strong`
- The wait covers ALL registered strong handlers for that event

**`Commanded.Middleware.ConsistencyGuarantee`**:
- A built-in middleware that handles the blocking wait
- Integrated into the pipeline after successful dispatch

**Event handlers and process managers**:
- When configured with `consistency: :strong`, they call `Subscriptions.ack_event` after processing
- This triggers the Subscriptions GenServer to notify any waiting dispatchers

### What Instructed currently has

In `middleware.gleam`:
```gleam
pub type Consistency {
  Eventual   // default
  Strong
}
```

The `Pipeline` has a `consistency` field and `with_consistency` function. But **there is NO actual blocking logic** — the consistency field exists in the type but is completely inert. Nothing reads it after dispatch.

In `router.gleam`: the pipeline's consistency is passed through but never acted on.

In `event_handler.gleam`: has a `consistency: Consistency` config field, but it is never read or used.

### What needs to be implemented

#### New file: `instructed/src/instructed/subscriptions.gleam`

An actor that tracks strong-consistency handler acknowledgments and unblocks waiting dispatchers.

```gleam
// Key types
pub type SubscriptionEntry {
  SubscriptionEntry(name: String, consistency: Consistency)
}

// Internal state
type SubState {
  SubState(
    // registered handlers: name → consistency
    handlers: Dict(String, Consistency),
    // acked: {name, stream_id} → last stream_version acked
    acked: Dict(#(String, String), Int),
    // waiters: processes blocked on a particular (stream_id, stream_version)
    waiters: List(Waiter),
  )
}

type Waiter {
  Waiter(
    reply_to: Subject(Result(Nil, Nil)),
    stream_id: String,
    stream_version: Int,
    // which handler names to wait for (None = all strong handlers)
    wait_for: Option(List(String)),
  )
}

// Messages
pub type SubMessage {
  Register(name: String, consistency: Consistency)
  AckEvent(name: String, stream_id: String, stream_version: Int)
  WaitFor(
    stream_id: String,
    stream_version: Int,
    wait_for: Option(List(String)),
    reply_to: Subject(Result(Nil, Nil)),
  )
}
```

#### Changes to `router.gleam`

After successful command execution (events appended), if `pipeline.consistency == Strong`:
1. Call `subscriptions.wait_for(subs_subject, stream_id, stream_version, None, 5000)`
2. Return `Error(error.ConsistencyTimeout)` if it times out

Router config needs an `Option(Subject(SubMessage))` for the subscriptions actor.

#### Changes to `event_handler.gleam`

After successfully processing an event AND the handler is configured `consistency: Strong`:
1. Call `subscriptions.ack_event(subs_subject, config.name, event.stream_id, event.stream_version)`

Handler config/start needs an `Option(Subject(SubMessage))` for the subscriptions actor.

#### Changes to `process_manager.gleam`

Same as event_handler — strong-consistency PMs ack after processing.

#### Changes to `application.gleam`

- Optionally start a Subscriptions actor as part of `application.start()`
- Thread the subscriptions subject through to router, handlers, PMs

### Implementation approach (Gleam idiom)

Unlike Commanded's PubSub-based broadcast (for distributed cluster support), Instructed can use a simpler single-node actor. The Subscriptions actor:
- Receives `AckEvent` messages from handlers
- Receives `WaitFor` messages from dispatchers (with a reply Subject)
- When an ack arrives that satisfies all waiting dispatchers for that version, sends `Ok(Nil)` to each waiter's reply Subject
- Dispatcher calls `process.receive(reply_subject, timeout_ms)` to block

### Key invariants for Module 15

- **Invariant 11**: Strong consistency blocks dispatch until all strong handlers have acked
- **Invariant 17**: Default dispatch timeout: 5 seconds → `ConsistencyTimeout` error
- Strong vs Eventual is opt-in: default is Eventual (non-blocking)
- Per-handler consistency is configured at start time (not per-dispatch)

---

## Architecture Integration Points (already checked OK through Module 14)

- [x] Event Store → Event Handler: subscription callbacks in handler's process
- [x] Event Store → Aggregate Server: append returns version; incremental reads
- [x] Aggregate Server → Router: dispatches through server; state cached
- [x] Router → Middleware: pipeline result influences dispatch
- [x] Event Handler → Subscription tracking: ack called after processing
- [x] Process Manager → Router: dispatch_command with causation_id from event
- [x] Process Manager → Event Store: state saved as snapshot; loaded on restart
- [x] Application → All: event_store threaded to all start_* helpers
- [ ] Consistency → Subscriptions: (Module 15) strong handlers register; dispatch waits
- [x] Aggregate Server → Snapshots: snapshot loaded during rebuild; written after N events

## Key Invariants Reference

1. Commands to the same aggregate instance are serialized (via actor)
2. `apply_event` must never fail — used during replay ✅
3. Events appended atomically with expected version ✅
4. Version conflict → rebuild from new events only, retry (up to 10) ✅
5. Event handlers are singletons — one instance per handler name ✅
6. Handler subscription name must never change between releases ✅ (documented)
7. `start_from` only applies on FIRST subscription creation ✅
8. Event handler errors invoke error callback — never silently swallowed ✅
9. PM state persisted (snapshot) after each handled event ✅ (fixed in M12)
10. PM dispatches with causation_id = source event_id, correlation_id preserved ✅
11. Strong consistency blocks dispatch until all strong handlers acked — TODO (M15)
12. Subscription callbacks deliver events to subscriber's process ✅
13. Aggregate stream prefix must never change ✅ (documented)
14. Snapshot version incremented when aggregate struct changes ✅ (documented)
15. Multi errors discard all events — atomic (TODO M18)
16. Default retry attempts: 10 for version conflicts ✅
17. Default dispatch timeout: 5 seconds ✅
18. Event batch read size: 1,000 when rebuilding state ✅
19. Aggregate processes are temporary — started on demand ✅
20. Handler `last_seen_event` provides idempotency guard ✅

## Notes on Remaining Modules

### M16: PostgreSQL Adapter
- `instructed-postgres/` directory exists with partial implementation
- Compare against `/tmp/commanded/lib/commanded/event_store/adapters/` (if cloned)
- Must implement full `EventStore(event)` record of functions

### M17: SQLite Adapter  
- `instructed-sqlite/` directory exists with partial implementation

### M18: Multi Module
- Gleam equivalent of `Commanded.Aggregates.Multi`
- Allows aggregate `execute` to return multiple event streams atomically
- Need new `instructed/multi.gleam` module

### M19: Aggregate Lifespan
- Commanded's `AggregateLifespan` behaviour: `after_event`, `after_command`, `after_error`
- Returns `:stop | :hibernate | {:stop_timeout, ms}`
- Need to add lifespan config to `AggregateConfig` and check in `aggregate_server.gleam`

### M20: Event Upcasting
- Transforms old event versions to current schema on read
- Need `instructed/upcast.gleam` with an `upcast` function record
- Wire into `aggregate_server` state rebuild and `event_handler` delivery

### M21: Telemetry & Observability
- Gleam doesn't have `:telemetry` library natively
- Document telemetry events; optionally wire to Erlang `:telemetry` via FFI
