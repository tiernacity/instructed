# Instructed — Design & Commanded Comparison

This document explains the architecture of Instructed, the design decisions made in adapting Commanded to Gleam, and where the two frameworks differ in approach, capability, or idiom.

---

## Contents

1. [What Instructed Is](#1-what-instructed-is)
2. [Architecture Overview](#2-architecture-overview)
3. [Core Design Decisions](#3-core-design-decisions)
4. [Module-by-Module Comparison](#4-module-by-module-comparison)
5. [The 20 Key Invariants](#5-the-20-key-invariants)
6. [Telemetry & Observability](#6-telemetry--observability)
7. [Known Differences & Limitations](#7-known-differences--limitations)
8. [Event Store Adapters](#8-event-store-adapters)

---

## 1. What Instructed Is

Instructed is a **CQRS/ES framework for Gleam**, ported from the Elixir [Commanded](https://github.com/commanded/commanded) library. It provides the primitives for building event-sourced systems: aggregates, command routing, event handlers, projections, process managers, and pluggable event store adapters.

### Packages

| Package | Purpose |
|---------|---------|
| `instructed` | Core framework — aggregates, router, handlers, middleware, consistency |
| `instructed-sqlite` | SQLite event store adapter (via `sqlight`) |
| `instructed-postgres` | PostgreSQL event store adapter (via `pog`) |

### Status

151 core tests, 18 SQLite adapter tests, all passing. PostgreSQL adapter requires a live database.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Your Application                        │
│                                                             │
│   Commands → Router → Middleware Pipeline                   │
│                    ↓                                        │
│              Aggregate Server (per instance)                │
│                    ↓                                        │
│              Aggregate.execute / apply_event                │
│                    ↓                                        │
│              EventStore.append_to_stream                    │
│                    ↓                                        │
│         ┌──────────┴──────────┐                            │
│   Event Handlers         Process Managers                   │
│   (projections,          (sagas — dispatch                  │
│    read models,           new commands from                 │
│    side effects)          event sequences)                  │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Command dispatch**: `router.dispatch(router, command)` extracts the aggregate identity, runs middleware, and routes to the aggregate server for that instance.
2. **Aggregate server**: A long-lived OTP actor per aggregate instance. Serializes commands, caches state in process memory, loads snapshots on start, writes snapshots after N events.
3. **Execute + apply**: The aggregate's `execute` function runs the business rule and returns events (or an error). `apply_event` folds each event into state — pure and infallible.
4. **Append**: Events are written to the event store with an expected version for optimistic concurrency.
5. **Subscriptions**: Event handlers and process managers subscribe to streams. On each appended batch, they receive events, process them, and acknowledge position.
6. **Strong consistency** (optional): If a command is dispatched with `consistency: Strong`, the router blocks until all registered strong-consistency handlers have acked the events.

---

## 3. Core Design Decisions

### 3.1 Record-of-functions instead of behaviours

**Commanded** uses Elixir behaviours (compile-time callbacks defined with `@callback` and `@impl`):

```elixir
defmodule BankAccount do
  @behaviour Commanded.Aggregates.Aggregate

  def execute(%BankAccount{}, %OpenAccount{} = cmd), do: %AccountOpened{...}
  def apply(%BankAccount{} = acct, %AccountOpened{} = evt), do: %{acct | status: :open}
end
```

**Instructed** uses records-of-functions — a single value that bundles the callbacks:

```gleam
pub fn bank_account() -> Aggregate(State, Command, Event) {
  Aggregate(
    empty_state: BankAccount(status: Closed, balance: 0),
    execute: fn(state, cmd) { ... },
    apply_event: fn(state, event) { ... },
  )
}
```

**Why**: Gleam has no macros or behaviours. Record-of-functions is the idiomatic Gleam way to express a "protocol" or "typeclass". It keeps everything first-class and composable — aggregates, event handlers, and process managers are all ordinary values you can pass around and test in isolation.

**Trade-off**: You lose compile-time exhaustiveness checking on the callbacks (Elixir's `@impl` warns on missing callbacks). In Gleam, if you forget a field in the record literal, the compiler catches it with a type error — different mechanism, same result.

### 3.2 Runtime router instead of compile-time macro

**Commanded** uses compile-time macros to register command-to-aggregate mappings:

```elixir
defmodule MyRouter do
  use Commanded.Commands.Router

  dispatch [OpenAccount, CloseAccount],
    to: BankAccount,
    identity: :account_number
end
```

Duplicate registrations are detected at compile time. The resulting module is a struct with built-in dispatch logic.

**Instructed** uses a runtime `Router` record:

```gleam
let my_router = router.new(
  aggregate: bank_account(),
  event_store: store,
  identity: fn(cmd) { cmd.account_number },
)
|> router.with_prefix("bank-account-")
|> router.with_retry_attempts(10)
```

**Why**: Gleam has no macros. Runtime configuration is explicit and testable. You can create multiple routers for the same aggregate type with different configurations, compose them, or swap them in tests.

**Trade-off**: No compile-time duplicate detection (two routers can handle the same command type without warning). No global registry — each router is an independent value.

### 3.3 Functional aggregate server instead of GenServer behaviour

**Commanded** uses Elixir's `GenServer` behaviour with `handle_call`, `handle_cast`, etc.

**Instructed** uses `gleam_otp`'s `actor` module, which provides the same OTP guarantees with a functional API:

```gleam
actor.new(initial_state)
|> actor.on_message(handle_message)
|> actor.start()
```

The `handle_message` function is a pure function `(State, Message) -> actor.Next(State, Message)`. This is Gleam's idiomatic equivalent of a GenServer.

### 3.4 Consistency without PubSub

**Commanded** implements strong consistency via PubSub:
- Each strong-consistency handler broadcasts acknowledgments over PubSub
- A `Commanded.Subscriptions` GenServer aggregates acks in ETS
- The dispatcher blocks by calling `Subscriptions.wait_for/5`
- PubSub enables this to work across a distributed cluster

**Instructed** implements single-node consistency via a direct actor:
- A `Subscriptions` actor (`subscriptions.gleam`) maintains handler registrations and ack state
- Handlers call `subscriptions.ack_event/4` directly after processing
- Dispatchers call `subscriptions.wait_for/4` which sends a `Subject` reply to the subscriptions actor
- When all strong handlers have acked, the actor sends `Ok(Nil)` to all waiting reply subjects
- Timeout returns `Error(Nil)` → `Error(ConsistencyTimeout)` at the router level

**Why**: Gleam's `gleam_otp` actor model makes direct message passing natural. No PubSub infrastructure needed for a single-node deployment. For multi-node, this would need to be extended — but that's out of scope for the current framework.

### 3.5 Event store as a record-of-functions

**Commanded** uses an `Adapter` behaviour implemented by separate modules (EventStore, InMemory, PostgreSQL adapters all implement the same behaviour).

**Instructed** uses an `EventStore(event)` record:

```gleam
pub type EventStore(event) {
  EventStore(
    append_to_stream: fn(String, ExpectedVersion, List(EventData(event))) -> Result(Int, EventStoreError),
    read_stream_forward: fn(String, Int, Int) -> Result(List(RecordedEvent(event)), EventStoreError),
    subscribe: fn(fn(RecordedEvent(event)) -> Nil) -> Result(Subscription, EventStoreError),
    // ... etc
  )
}
```

All three adapters (in-memory, SQLite, PostgreSQL) produce an `EventStore(event)` value. The framework only depends on this type — adapters are completely interchangeable.

---

## 4. Module-by-Module Comparison

### Aggregates

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Definition | Module with `@behaviour` callbacks | `Aggregate(state, command, event)` record |
| `execute` return | Many forms: event, list, `{:ok, event}`, `:ok`, `nil`, `%Multi{}` | `Result(List(event), String)` |
| `apply` return | Updated struct | Updated state (pure) |
| State rebuild | Batch reads of 1,000; subscribes to own stream | Batch reads of 1,000; snapshot load then incremental |
| Process per instance | Yes — DynamicSupervisor | Yes — registry actor per router |
| Snapshot | Async after N events | After N events (configurable) |

### Command Router

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Registration | Compile-time macro | Runtime record |
| Duplicate detection | Compile-time error | None (runtime) |
| Identity extraction | `:identity` option + middleware | `fn(command) -> String` |
| Retry on conflict | 10 attempts (default) | 10 attempts (default) |
| Dispatch timeout | 5 seconds (default) | 5 seconds (default) |
| Consistency option | Per-dispatch: `:strong` / `:eventual` | Per-dispatch: `Strong` / `Eventual` |
| Composite router | `CompositeRouter` module | Not implemented — compose manually |

### Middleware

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Callbacks | `before_dispatch`, `after_dispatch`, `after_failure` | Same three callbacks |
| Pipeline struct | `%Pipeline{}` with many fields, assigns as map | `Pipeline(command)` with typed fields, assigns as `Dict(String, String)` |
| Built-in middleware | `ExtractAggregateIdentity`, `ConsistencyGuarantee` | None built-in; identity validated in router |
| Halting | `halt/1` function | `halt(pipeline)` function |
| Assigns | Any Elixir term | `String` values only |

**Gap**: Assigns being `Dict(String, String)` limits middleware from passing complex values (like aggregate state or event lists) between stages. This is a Gleam type safety trade-off — a `Dict(String, Dynamic)` could be added if richer assigns are needed.

### Event Handlers

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Implementation | GenServer behaviour | `actor` with `EventHandlerConfig` record |
| Singleton | Enforced by event store subscription name | Enforced by subscription name uniqueness |
| `start_from` | `:origin`, `:current`, or event number | `Origin`, `Current`, `FromEventNumber(n)` |
| Idempotency | `last_seen_event` in-process guard | `last_seen_event` in actor state |
| Error handling | `error/3` callback with retry/skip/stop/backoff | `on_error` callback with `Retry`, `RetryWithDelay`, `Skip`, `Stop` |
| Resumption | From last acked position | From last acked position (persistent subscription) |
| Concurrency | `concurrency: N` with partitioning | Single actor only |
| Batch processing | `batch_size` + `handle_batch` | Not implemented |
| Consistency | `consistency: :strong` — registers with Subscriptions | `consistency: Strong` — registers with Subscriptions actor |
| Upcasting | Applied before handler receives event | Applied before handler receives event |

### Projections

**Commanded** does not have a separate projection module in core — projections are event handlers, often using `commanded_ecto_projections` for database-backed, transactionally-safe state.

**Instructed** has a `Projection` module as a thin wrapper over the event handler pattern, storing state in process memory. This is suitable for in-memory read models. For persistent projections (database-backed), users would write their own event handler that writes to a database.

### Process Managers

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Routing | `interested?/1,2` returning `{:start, uuid}` etc | `interest` function returning `Start(uuid)`, `Continue(uuid)`, `Stop(uuid)`, `Skip` |
| Multi-instance | Single event → multiple UUIDs | One UUID per event |
| State | Per-instance, persisted as snapshot | Per-instance, persisted as snapshot |
| Error handling | `error/3` callback | `on_error` callback |
| Causation chain | Automatic from triggering event | Propagated via `dispatch_command` context |
| Strict routing | `{:start!, uuid}` / `{:continue!, uuid}` | `StartStrict`, `ContinueStrict` variants |

### Consistency

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Mechanism | PubSub + ETS subscriptions registry | Direct actor messaging |
| Cluster support | Yes (via PubSub adapter) | No — single node only |
| Handler registration | At handler start time | At handler start time |
| Timeout | Configurable, default 5s | Configurable, default 5s |
| Error | `{:error, :consistency_timeout}` | `Error(ConsistencyTimeout)` |

### Multi (composable execute)

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| API | `Multi.new/1 \|> Multi.execute/2 \|> Multi.execute/2` | `multi.new \|> multi.execute \|> multi.apply \|> multi.to_result` |
| Named steps | Yes — 2-arity functions receive named step results | No |
| Nested Multi | Yes — Multi can return Multi | No |
| `reduce` | Yes | Yes — `multi.reduce` |
| Atomicity | All events persisted together or none | Error → all events discarded (Invariant 15) |

### Aggregate Lifespan

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Callbacks | `after_event/1`, `after_command/1`, `after_error/1` | `after_event`, `after_command`, `after_error` |
| Return values | `timeout \| :infinity \| :hibernate \| :stop` | `KeepRunning \| Stop \| StopAfter(ms) \| Hibernate` |
| Hibernate | Full Erlang process hibernation | Falls back to `KeepRunning` (OTP actor limitation) |
| Per-command config | `:lifespan` in dispatch registration | Set on `AggregateConfig` |

**Note on Hibernate**: Erlang process hibernation (GC + suspend) isn't directly exposed through `gleam_otp`'s actor abstraction. The `Hibernate` variant is accepted for API compatibility but behaves as `KeepRunning`. A low-level FFI could implement true hibernation if needed.

### Event Upcasting

| Aspect | Commanded | Instructed |
|--------|-----------|------------|
| Mechanism | `Commanded.Event.Upcaster` protocol | `Upcaster(event)` record |
| Application point | Before aggregates, handlers, PMs receive events | Before aggregate state rebuild; before handler delivery |
| Chaining | Elixir protocol dispatch | `upcast.chain(first, second)` / `upcast.chain_all(list)` |
| Default | `Any` pass-through implementation | `upcast.identity()` |

---

## 5. The 20 Key Invariants

These invariants are maintained by the framework and should hold regardless of which event store adapter is used.

| # | Invariant | Mechanism |
|---|-----------|-----------|
| 1 | Commands to the same aggregate are serialized | Aggregate server actor mailbox |
| 2 | `apply_event` must never fail | Pure function, no Result return type |
| 3 | Events appended atomically with expected version | EventStore `ExpectedVersion` + DB constraints |
| 4 | Version conflict → rebuild and retry (up to 10) | Aggregate server retry loop |
| 5 | Event handlers are singletons | Subscription name uniqueness in event store |
| 6 | Handler subscription name must never change | Documentation; name is subscription key |
| 7 | `start_from` only applies on first subscription creation | Persistent subscription creation check |
| 8 | Handler errors invoke error callback — never silently swallowed | `on_error` callback required; default stops |
| 9 | PM state persisted after each handled event | Snapshot written after each PM event |
| 10 | PM dispatches with causation_id = source event_id | `dispatch_command` propagates causation |
| 11 | Strong consistency blocks dispatch until all strong handlers acked | Subscriptions actor + wait_for |
| 12 | Subscription callbacks deliver events to subscriber's process | Handler actor receives events |
| 13 | Aggregate stream prefix must never change | Documentation; prefix is part of stream key |
| 14 | Snapshot version incremented when aggregate struct changes | Documentation; validates on load |
| 15 | Multi errors discard all events — atomic | `multi.to_result` returns Error, discards events |
| 16 | Default retry attempts: 10 for version conflicts | `Router` default: `retry_attempts: 10` |
| 17 | Default dispatch timeout: 5 seconds | `Router` default: `dispatch_timeout: 5000` |
| 18 | Event batch read size: 1,000 when rebuilding state | Aggregate server reads 1,000 at a time |
| 19 | Aggregate processes are temporary — started on demand | Registry starts actors on `GetOrStart` |
| 20 | Handler `last_seen_event` provides idempotency guard | Actor state tracks last processed event number |

---

## 6. Telemetry & Observability

### Current Design

Instructed has a two-layer telemetry system in `instructed/telemetry.gleam`:

**Layer 1 — Gleam callback** (primary, zero-dependency):

```gleam
import instructed/telemetry

// Register a handler — runs synchronously in the emitting process
telemetry.set_handler(fn(event) {
  case event {
    telemetry.CommandDispatchStop(command_id:, duration_ns:, event_count:, ..) ->
      io.println("dispatched in " <> int.to_string(duration_ns / 1_000_000) <> "ms")
    _ -> Nil
  }
})

// Clear when done
telemetry.clear_handler()
```

**Layer 2 — Erlang `:telemetry`** (optional, graceful fallback):

If the Erlang `telemetry` package (v1.3.0 on hex.pm) is present in the release, Instructed will also call `telemetry:execute/3` for each event. If `:telemetry` is absent, the call is wrapped in `catch` and silently skipped.

### Instrumented Points

| Event | When emitted |
|-------|-------------|
| `CommandDispatchStart` | Before middleware runs |
| `CommandDispatchStop` | After events appended + consistency resolved |
| `CommandDispatchException` | When dispatch returns an error |
| `AggregateExecuteStart` | Before aggregate server executes command |
| `AggregateExecuteStop` | After events appended to store |
| `AggregateExecuteException` | On command execution error |
| `EventHandleStart` | Before handler callback called |
| `EventHandleStop` | After handler callback returns `Ok` |
| `EventHandleException` | After handler callback returns `Error` |
| `ProcessManagerHandleStart` | Before PM handler called |
| `ProcessManagerHandleStop` | After PM handler + command dispatch |
| `ProcessManagerHandleException` | On PM handling error |

### Making It More Pluggable

The current Gleam callback slot is global and single (`persistent_term` key). For production use, you'd want:

**Multiple named handlers** (like `:telemetry.attach/4`):

```gleam
// Not yet implemented — would look like:
telemetry.attach("my-logger", fn(event) { log(event) })
telemetry.attach("my-metrics", fn(event) { record_metric(event) })
telemetry.detach("my-logger")
```

This is straightforward to implement using a `Dict(String, fn(TelemetryEvent) -> Nil)` in persistent_term rather than a single value. PRs welcome.

### Consuming via Erlang `:telemetry`

The `:telemetry` package is the standard BEAM ecosystem telemetry hub. No Gleam wrapper currently exists on hex.pm, but it's usable via Erlang FFI:

**Adding `:telemetry` as a dependency** — in your project's `gleam.toml`, add to `[erlang-extra-packages]` (or use a manifest), then write an FFI file:

```erlang
% my_app_telemetry_ffi.erl
-module(my_app_telemetry_ffi).
-export([attach/0]).

attach() ->
  telemetry:attach_many(
    <<"my-app-instrumented">>,
    [
      [instructed, command, dispatch, stop],
      [instructed, command, dispatch, exception],
      [instructed, event, handle, stop]
    ],
    fun handle_event/4,
    nil
  ).

handle_event([instructed, command, dispatch, stop], Measurements, Meta, _Config) ->
  Duration = maps:get(duration, Measurements, 0),
  StreamId = maps:get(aggregate_stream_id, Meta, <<>>),
  io:format("Command dispatched to ~s in ~bus~n", [StreamId, Duration div 1000]);
handle_event(_, _, _, _) ->
  ok.
```

### Consuming via OpenTelemetry

`opentelemetry_telemetry` (on hex.pm) is a bridge that automatically converts `:telemetry` events into OpenTelemetry spans. Since Instructed emits to `:telemetry`, OTel tracing works without any additional code in Instructed itself:

```
Instructed → :telemetry.execute/3 → opentelemetry_telemetry bridge → OTel spans → Jaeger / Honeycomb / etc
```

The `instructed-postgres` package already depends on `opentelemetry_api` for its own span instrumentation. Client code can reuse the same OTel setup.

### Consuming via telemetry_metrics / Prometheus / StatsD

The Elixir ecosystem has well-established tooling:

- `telemetry_metrics` — defines metric aggregation rules over `:telemetry` events
- `telemetry_metrics_prometheus_core` — exports `telemetry_metrics` definitions as Prometheus metrics
- `telemetry_metrics_statsd` — exports to StatsD

Because Instructed emits standard `:telemetry` events with standard measurement keys (`duration`, `system_time`, `event_count`), these libraries work as-is.

### No Native Gleam Telemetry Packages

As of early 2026, there are no Gleam-native telemetry or metrics packages on hex.pm. The options are:

1. **Use the Gleam callback** (`telemetry.set_handler`) — pure Gleam, no deps, bridge to anything in the callback body
2. **Use `:telemetry` via FFI** — the standard BEAM hub; all Elixir/Erlang tooling works
3. **Use OpenTelemetry** — `opentelemetry_api` is accessible via FFI, `instructed-postgres` already shows how

The Gleam callback approach is recommended for simple use cases (logging, testing). The `:telemetry` path is recommended for production systems that want Prometheus, StatsD, or OTel integration without writing custom aggregation logic.

---

## 7. Known Differences & Limitations

### Not Implemented

| Feature | Commanded | Instructed | Notes |
|---------|-----------|------------|-------|
| Composite router | `CompositeRouter` module | Manual | Low priority — runtime routers compose via pattern matching |
| Batch event processing | `batch_size` + `handle_batch` | Single events only | Medium priority for high-throughput handlers |
| Handler concurrency | `concurrency: N` + `partition_by` | Single actor only | Medium priority |
| Distributed consistency | PubSub-based across nodes | Single-node actor | Requires distributed infrastructure |
| True process hibernation | `:hibernate` in lifespan | Falls back to KeepRunning | OTP actor limitation |
| Multi named steps | 2-arity `execute` with step names | Unnamed steps | Low priority |
| Multiple named telemetry handlers | `telemetry:attach/4` | Single global callback | PRs welcome |

### Design Differences

**Assigns are `Dict(String, String)`**: Commanded's pipeline assigns can hold any Elixir term (aggregate state, event lists, etc.). Instructed's `Dict(String, String)` is more restrictive. This was a deliberate choice to keep assigns serializable, but it limits middleware from passing complex values. A `Dict(String, Dynamic)` with codec support would be the upgrade path.

**No compile-time registration**: Commanded's macro-based router detects duplicate command registrations at compile time. Instructed's runtime router does not. Discipline is on the user.

**Single-node consistency**: Instructed's `Subscriptions` actor works within a single BEAM node. Commanded's PubSub mechanism can span a cluster. For multi-node deployments, Instructed's consistency model would need a distributed store (e.g., pg2 groups or a distributed cache) as the ack coordinator.

**Projection persistence**: Instructed's `Projection` module is in-memory only. Commanded (with `commanded_ecto_projections`) writes projection state to a database transactionally with the subscription position. For persistent projections in Instructed, write a custom event handler that updates your database directly.

### PostgreSQL Adapter Caveats

The PostgreSQL adapter (`instructed-postgres`) has one known issue: the version check and the `INSERT` are not in a transaction. Two concurrent appenders to the same stream could both pass the version check but only one insert would succeed (caught by the `UNIQUE(stream_id, stream_version)` constraint). The error currently surfaces as `StorageError` rather than `VersionConflict`. Fix: wrap `get_stream_version + INSERT` in a `BEGIN / COMMIT` block.

The SQLite adapter does not have this issue — all operations are serialized through the actor's mailbox.

---

## 8. Event Store Adapters

All three adapters implement the same `EventStore(event)` record interface. They are interchangeable — switch by passing a different value to your router/handlers.

### In-Memory (`instructed/in_memory_event_store`)

```gleam
let assert Ok(subject) = in_memory_event_store.start()
let store = in_memory_event_store.to_event_store(subject)
```

- All state in process memory — lost on stop
- All operations serialized through the actor mailbox (no races)
- Ideal for tests and examples
- `reset()` clears all events and subscriptions

### SQLite (`instructed-sqlite`)

```gleam
let config = instructed_sqlite.SqliteConfig(
  db_path: "events.db",           // or ":memory:" for tests
  serialize: json.to_string ∘ encode,
  deserialize: fn(s) { decode(s) },
  event_type: fn(event) { event_type_name(event) },
)
let assert Ok(subject) = instructed_sqlite.start(config)
let store = instructed_sqlite.to_event_store(subject)
```

- WAL mode for concurrent reads
- `UNIQUE(stream_id, stream_version)` enforces optimistic concurrency at DB level
- Snapshot upsert via `ON CONFLICT DO UPDATE`
- All writes serialized through the OTP actor — no race conditions
- `:memory:` path creates an in-memory SQLite DB (useful for isolated tests)

### PostgreSQL (`instructed-postgres`)

```gleam
let config = instructed_postgres.PgConfig(
  connection: pog.default_config() |> pog.host("localhost") |> ...,
  pool_size: 10,
  serialize: ...,
  deserialize: ...,
  event_type: ...,
)
let assert Ok(subject) = instructed_postgres.start(config)
let store = instructed_postgres.to_event_store(subject)
```

- Uses `pog` (Gleam PostgreSQL driver)
- BIGSERIAL for global event numbers
- `UNIQUE(stream_id, stream_version)` enforced at DB level
- Notification-based live subscriptions via `LISTEN/NOTIFY`
- **Known issue**: version check and INSERT not in a transaction (see §7)

### Implementing Your Own Adapter

Construct an `EventStore(event)` record directly:

```gleam
import instructed/event_store.{EventStore}

pub fn my_adapter() -> EventStore(MyEvent) {
  EventStore(
    append_to_stream: fn(stream_id, expected_version, events) { ... },
    read_stream_forward: fn(stream_id, start_version, count) { ... },
    subscribe: fn(handler) { ... },
    subscribe_to_stream: fn(stream_id, handler) { ... },
    subscribe_persistent: fn(stream, name, start_from, handler) { ... },
    ack_event: fn(sub, event) { ... },
    unsubscribe: fn(sub) { ... },
    delete_subscription: fn(stream, name) { ... },
    read_snapshot: fn(source_uuid) { ... },
    record_snapshot: fn(snapshot) { ... },
    delete_snapshot: fn(source_uuid) { ... },
    reset: fn() { ... },
    read_all_forward: fn(start_number) { ... },
    get_latest_event_number: fn() { ... },
  )
}
```

All functions must be thread-safe. The in-memory and SQLite adapters achieve this via an OTP actor. The PostgreSQL adapter uses connection pooling with database-level constraints.
