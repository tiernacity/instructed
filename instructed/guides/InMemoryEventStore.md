# In-Memory Event Store

The in-memory event store is included with Instructed for development and testing. It stores all events in memory using an OTP Actor process.

## Usage

```gleam
import instructed/in_memory_event_store

// Start the event store actor
let assert Ok(subject) = in_memory_event_store.start()

// Create an EventStore interface
let store = in_memory_event_store.to_event_store(subject)
```

## Features

- **No serialization needed** - events are stored as native Gleam values
- **Full EventStore interface** - supports all operations
- **Transient and persistent subscriptions**
- **Snapshot support**
- **Reset capability** for test isolation

## Limitations

- Events are lost when the process stops
- No persistence across restarts
- Designed for testing only

## For Production

Use the [PostgreSQL event store](../instructed_postgres/) for production:

```gleam
import instructed_postgres

let config = instructed_postgres.PgConfig(
  db: db_connection,
  serialize: serialize_fn,
  deserialize: deserialize_fn,
  event_type: event_type_fn,
)

let store = instructed_postgres.new(config)
```
