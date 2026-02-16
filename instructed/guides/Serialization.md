# Serialization

When using a persistent event store (like PostgreSQL), events need to be serialized to and from a storage format. Instructed uses simple function-based serialization.

## JSON Serialization

The PostgreSQL event store requires serialize/deserialize functions:

```gleam
import gleam/json
import gleam/dynamic/decode

type MyEvent {
  UserCreated(name: String, email: String)
  UserUpdated(name: String)
  UserDeleted
}

fn serialize_event(event: MyEvent) -> String {
  case event {
    UserCreated(name, email) ->
      json.object([
        #("type", json.string("UserCreated")),
        #("name", json.string(name)),
        #("email", json.string(email)),
      ])
      |> json.to_string

    UserUpdated(name) ->
      json.object([
        #("type", json.string("UserUpdated")),
        #("name", json.string(name)),
      ])
      |> json.to_string

    UserDeleted ->
      json.object([#("type", json.string("UserDeleted"))])
      |> json.to_string
  }
}

fn deserialize_event(json_str: String) -> Result(MyEvent, String) {
  let type_decoder = decode.at(["type"], decode.string)
  case json.parse(json_str, type_decoder) {
    Ok("UserCreated") -> {
      let decoder = {
        use name <- decode.field("name", decode.string)
        use email <- decode.field("email", decode.string)
        decode.success(UserCreated(name, email))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode UserCreated")
      }
    }
    Ok("UserUpdated") -> {
      let decoder = {
        use name <- decode.field("name", decode.string)
        decode.success(UserUpdated(name))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode UserUpdated")
      }
    }
    Ok("UserDeleted") -> Ok(UserDeleted)
    _ -> Error("Unknown event type")
  }
}

fn event_type_name(event: MyEvent) -> String {
  case event {
    UserCreated(..) -> "UserCreated"
    UserUpdated(..) -> "UserUpdated"
    UserDeleted -> "UserDeleted"
  }
}
```

## Using with PostgreSQL Store

```gleam
import instructed_postgres

let config = instructed_postgres.PgConfig(
  db: db_connection,
  serialize: serialize_event,
  deserialize: deserialize_event,
  event_type: event_type_name,
)

let store = instructed_postgres.new(config)
```

## In-Memory Event Store

The in-memory event store doesn't need serialization - it stores Gleam values directly:

```gleam
import instructed/in_memory_event_store

let assert Ok(subject) = in_memory_event_store.start()
let store = in_memory_event_store.to_event_store(subject)
// No serialization needed!
```
