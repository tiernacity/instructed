# Supervision

Instructed leverages Gleam's OTP integration for fault-tolerant supervision of event-sourced processes.

## Application Supervisor

The `application` module provides a simple way to manage your CQRS components:

```gleam
import instructed/application as app
import instructed/in_memory_event_store

let assert Ok(store_subject) = in_memory_event_store.start()
let store = in_memory_event_store.to_event_store(store_subject)

let assert Ok(application) = app.start(app.new(store))
```

## OTP Actors

All Instructed processes are OTP-compatible actors:

- **Event Store**: Runs as an actor (in-memory) or connection pool (PostgreSQL)
- **Aggregate Server**: Each aggregate instance is an actor
- **Event Handlers**: Each handler runs as an actor
- **Process Managers**: Each process manager runs as an actor
- **Projections**: Each projection runs as an actor

## Using with gleam_otp Supervisors

For production, wire Instructed into your supervision tree:

```gleam
import gleam/otp/static_supervisor as supervisor

pub fn start_supervisor() {
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(supervised_event_store())
  |> supervisor.add(supervised_app())
  |> supervisor.start
}
```

## Aggregate Server Lifecycle

Aggregate servers are started on demand when commands are dispatched:

```gleam
import instructed/aggregate_server

let config = aggregate_server.Config(
  aggregate: my_aggregate,
  event_store: store,
  stream_id: "entity-123",
)

let assert Ok(server) = aggregate_server.start(config)
```

The server will:
1. Start and load state from the event store
2. Cache state in memory
3. Handle commands sequentially
4. Persist events atomically

## Fault Tolerance

- If an aggregate server crashes, it can be restarted and will reload state from the event store
- Event handlers can be restarted and will resume from their last acknowledged position
- The event store is the source of truth - all state can be rebuilt
