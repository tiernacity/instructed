//// Conformance test runner for the PostgreSQL event store adapter.

import gleam/erlang/process
import gleam/json
import gleam/string
import instructed/conformance/append_events
import instructed/conformance/concurrency
import instructed/conformance/snapshot
import instructed/conformance/subscription
import instructed/conformance/test_event.{type TestEvent, Created, Deleted, Updated}
import instructed/conformance/transient_subscription
import instructed/event_store.{type EventStore}
import instructed_postgres
import pog

fn serialize_event(event: TestEvent) -> String {
  case event {
    Created(name) ->
      json.object([
        #("type", json.string("Created")),
        #("name", json.string(name)),
      ])
      |> json.to_string
    Updated(name) ->
      json.object([
        #("type", json.string("Updated")),
        #("name", json.string(name)),
      ])
      |> json.to_string
    Deleted ->
      json.object([#("type", json.string("Deleted"))])
      |> json.to_string
  }
}

fn deserialize_event(json_str: String) -> Result(TestEvent, String) {
  case string.contains(json_str, "\"Created\"") {
    True -> {
      case string.split(json_str, "\"name\":\"") {
        [_, rest] ->
          case string.split(rest, "\"") {
            [name, ..] -> Ok(Created(name))
            _ -> Ok(Created(""))
          }
        _ -> Ok(Created(""))
      }
    }
    False ->
      case string.contains(json_str, "\"Updated\"") {
        True -> {
          case string.split(json_str, "\"name\":\"") {
            [_, rest] ->
              case string.split(rest, "\"") {
                [name, ..] -> Ok(Updated(name))
                _ -> Ok(Updated(""))
              }
            _ -> Ok(Updated(""))
          }
        }
        False ->
          case string.contains(json_str, "\"Deleted\"") {
            True -> Ok(Deleted)
            False -> Error("Unknown event type")
          }
      }
  }
}

fn event_type_name(event: TestEvent) -> String {
  case event {
    Created(_) -> "Created"
    Updated(_) -> "Updated"
    Deleted -> "Deleted"
  }
}

/// Create a factory function that uses a shared pool.
/// Each call to the returned factory resets the store data (not the schema).
fn make_pg_factory() -> fn() -> EventStore(TestEvent) {
  let pool_name = process.new_name(prefix: "conformance_pool")
  let assert Ok(url_config) =
    pog.url_config(pool_name, "postgresql://postgres:postgres@db:5432/app")
  let small_pool = pog.pool_size(url_config, 5)
  let assert Ok(started) = pog.start(small_pool)
  let db = started.data

  let assert Ok(Nil) = instructed_postgres.drop_schema(db)
  let assert Ok(Nil) = instructed_postgres.create_schema(db)

  fn() -> EventStore(TestEvent) {
    // Reset data between sub-tests, reuse same pool + schema
    let pg_config =
      instructed_postgres.PgConfig(
        db: db,
        serialize: serialize_event,
        deserialize: deserialize_event,
        event_type: event_type_name,
      )

    let store = instructed_postgres.new(pg_config)
    let assert Ok(Nil) = store.reset()
    // Reset the event_number sequence too
    let assert Ok(_) =
      pog.query("ALTER SEQUENCE event_store_events_event_number_seq RESTART WITH 1")
      |> pog.execute(db)
    store
  }
}

pub fn append_conformance_test() -> Nil {
  append_events.run_all(make_pg_factory())
}

pub fn snapshot_conformance_test() -> Nil {
  snapshot.run_all(make_pg_factory())
}

pub fn subscription_conformance_test() -> Nil {
  // The poller now provides backpressure — run full suite
  subscription.run_all(make_pg_factory())
}

pub fn transient_subscription_conformance_test() -> Nil {
  transient_subscription.run_all(make_pg_factory())
}

pub fn concurrency_different_streams_test() -> Nil {
  let factory = make_pg_factory()
  concurrency.test_concurrent_appends_different_streams(factory)
}

pub fn concurrency_same_stream_occ_test() -> Nil {
  let factory = make_pg_factory()
  concurrency.test_concurrent_appends_same_stream_occ(factory)
}

pub fn concurrency_subscription_ordering_test() -> Nil {
  let factory = make_pg_factory()
  concurrency.test_concurrent_appends_subscription_ordering(factory)
}
