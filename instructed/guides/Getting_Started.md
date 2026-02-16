# Getting Started

Instructed is a CQRS/ES framework for Gleam. This guide walks you through setting up your first event-sourced application.

## Prerequisites

- Gleam 1.0 or later
- Erlang/OTP 26 or later

## Installation

Add `instructed` to your `gleam.toml` dependencies:

```toml
[dependencies]
instructed = { path = "../instructed" }
```

For PostgreSQL persistence:

```toml
[dependencies]
instructed_postgres = { path = "../instructed_postgres" }
```

## Your First Application

### Step 1: Define Domain Types

In Instructed, everything is strongly typed. Define your aggregate state, commands, and events as Gleam types:

```gleam
// State
type Counter {
  Counter(id: String, count: Int)
}

// Commands
type CounterCommand {
  CreateCounter(id: String)
  Increment(id: String)
  Decrement(id: String)
}

// Events
type CounterEvent {
  CounterCreated(id: String)
  CounterIncremented(id: String)
  CounterDecremented(id: String)
}
```

### Step 2: Define the Aggregate

An aggregate is created using `aggregate.new/3`:

```gleam
import instructed/aggregate

let counter = aggregate.new(
  empty_state: fn() { Counter("", 0) },
  execute: fn(state, cmd) {
    case cmd {
      CreateCounter(id) ->
        case state.id == "" {
          True -> Ok([CounterCreated(id)])
          False -> Error("Already exists")
        }
      Increment(id) ->
        case state.id != "" {
          True -> Ok([CounterIncremented(id)])
          False -> Error("Not created")
        }
      Decrement(id) ->
        case state.id != "" && state.count > 0 {
          True -> Ok([CounterDecremented(id)])
          False -> Error("Cannot decrement")
        }
    }
  },
  apply_event: fn(state, event) {
    case event {
      CounterCreated(id) -> Counter(id, 0)
      CounterIncremented(_) -> Counter(..state, count: state.count + 1)
      CounterDecremented(_) -> Counter(..state, count: state.count - 1)
    }
  },
)
```

### Step 3: Set Up Event Store

For testing, use the in-memory event store:

```gleam
import instructed/in_memory_event_store

let assert Ok(store_subject) = in_memory_event_store.start()
let store = in_memory_event_store.to_event_store(store_subject)
```

### Step 4: Create a Router

```gleam
import instructed/router

let counter_router = router.new(
  aggregate: counter,
  event_store: store,
  identity: fn(cmd) {
    case cmd {
      CreateCounter(id) -> id
      Increment(id) -> id
      Decrement(id) -> id
    }
  },
)
```

### Step 5: Dispatch Commands

```gleam
let assert Ok(result) = router.dispatch(counter_router, CreateCounter("c1"))
// result.aggregate_state == Counter("c1", 0)

let assert Ok(result) = router.dispatch(counter_router, Increment("c1"))
// result.aggregate_state == Counter("c1", 1)
```

## Next Steps

- [Aggregates](Aggregates.md) - Deep dive into aggregate design
- [Commands](Commands.md) - Command routing and dispatch
- [Events](Events.md) - Event handling and projections
- [Middleware](Middleware.md) - Command validation and authorization
