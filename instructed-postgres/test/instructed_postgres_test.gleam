import gleam/dict
import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should
import instructed/event.{type EventData, type RecordedEvent, EventData}
import instructed/event_store.{AnyVersion, ExactVersion, NoStream, Origin}
import instructed/snapshot.{SnapshotData}
import instructed_postgres
import pog

pub fn main() -> Nil {
  gleeunit.main()
}

// --- Test event type ---

type TestEvent {
  Created(name: String)
  Updated(name: String)
  Deleted
}

fn serialize_event(event: TestEvent) -> String {
  case event {
    Created(name) ->
      json.object([#("type", json.string("Created")), #("name", json.string(name))])
      |> json.to_string
    Updated(name) ->
      json.object([#("type", json.string("Updated")), #("name", json.string(name))])
      |> json.to_string
    Deleted ->
      json.object([#("type", json.string("Deleted"))])
      |> json.to_string
  }
}

fn deserialize_event(json_str: String) -> Result(TestEvent, String) {
  case string.contains(json_str, "\"Created\"") {
    True -> Ok(Created(extract_name(json_str)))
    False ->
      case string.contains(json_str, "\"Updated\"") {
        True -> Ok(Updated(extract_name(json_str)))
        False ->
          case string.contains(json_str, "\"Deleted\"") {
            True -> Ok(Deleted)
            False -> Error("Unknown event type")
          }
      }
  }
}

fn extract_name(json_str: String) -> String {
  case string.split(json_str, "\"name\":\"") {
    [_, rest] ->
      case string.split(rest, "\"") {
        [name, ..] -> name
        _ -> ""
      }
    _ -> ""
  }
}

fn event_type_name(event: TestEvent) -> String {
  case event {
    Created(_) -> "Created"
    Updated(_) -> "Updated"
    Deleted -> "Deleted"
  }
}

fn get_db() -> pog.Connection {
  let pool_name = process.new_name(prefix: "test_pool")
  let assert Ok(config) =
    pog.url_config(pool_name, "postgresql://postgres:postgres@db:5432/app")
  let assert Ok(started) = pog.start(config)
  started.data
}

fn setup() {
  let db = get_db()
  let assert Ok(Nil) = instructed_postgres.drop_schema(db)
  let assert Ok(Nil) = instructed_postgres.create_schema(db)

  let config =
    instructed_postgres.PgConfig(
      db: db,
      serialize: serialize_event,
      deserialize: deserialize_event,
      event_type: event_type_name,
    )

  instructed_postgres.new(config)
}

// --- Helper: EventData with event_type ---

fn evt(data: TestEvent) -> EventData(TestEvent) {
  EventData(
    data: data,
    event_type: event_type_name(data),
    causation_id: None,
    correlation_id: None,
    metadata: dict.new(),
  )
}

fn evt_meta(
  data: TestEvent,
  causation: String,
  correlation: String,
  meta: dict.Dict(String, String),
) -> EventData(TestEvent) {
  EventData(
    data: data,
    event_type: event_type_name(data),
    causation_id: Some(causation),
    correlation_id: Some(correlation),
    metadata: meta,
  )
}

// --- Tests ---

pub fn pg_append_and_read_test() {
  let store = setup()

  let result = store.append_to_stream("user-1", NoStream, [evt(Created("Alice"))])
  should.be_ok(result)
  let assert Ok(version) = result
  should.equal(version, 1)

  let assert Ok(recorded) = store.read_stream_forward("user-1", 1, 1000)
  should.equal(list.length(recorded), 1)

  let assert [first] = recorded
  should.equal(first.data, Created("Alice"))
  should.equal(first.stream_id, "user-1")
  should.equal(first.stream_version, 1)
  should.equal(first.event_type, "Created")
}

pub fn pg_version_conflict_test() {
  let store = setup()

  let assert Ok(_) = store.append_to_stream("user-vc", NoStream, [evt(Created("Bob"))])

  let result = store.append_to_stream("user-vc", ExactVersion(5), [evt(Updated("Robert"))])
  should.be_error(result)
}

pub fn pg_exact_version_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("user-ev", NoStream, [evt(Created("Charlie"))])

  let assert Ok(v) =
    store.append_to_stream("user-ev", ExactVersion(1), [evt(Updated("Chuck"))])
  should.equal(v, 2)
}

pub fn pg_any_version_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("user-av", AnyVersion, [evt(Created("Dave"))])

  let assert Ok(v) =
    store.append_to_stream("user-av", AnyVersion, [evt(Updated("David"))])
  should.equal(v, 2)
}

pub fn pg_read_nonexistent_test() {
  let store = setup()
  let result = store.read_stream_forward("nonexistent", 1, 1000)
  should.be_error(result)
}

pub fn pg_read_count_limit_test() {
  let store = setup()

  // Append 5 events
  let assert Ok(_) =
    store.append_to_stream("paged-stream", NoStream, [
      evt(Created("a")),
      evt(Updated("b")),
      evt(Created("c")),
      evt(Updated("d")),
      evt(Created("e")),
    ])

  // Read only 3
  let assert Ok(batch) = store.read_stream_forward("paged-stream", 1, 3)
  should.equal(list.length(batch), 3)

  // Read from offset 4
  let assert Ok(rest) = store.read_stream_forward("paged-stream", 4, 1000)
  should.equal(list.length(rest), 2)
}

pub fn pg_read_all_forward_test() {
  let store = setup()

  let assert Ok(_) = store.append_to_stream("sa", NoStream, [evt(Created("A"))])
  let assert Ok(_) = store.append_to_stream("sb", NoStream, [evt(Created("B"))])

  let assert Ok(all) = store.read_all_forward(1)
  should.equal(list.length(all), 2)
}

pub fn pg_snapshot_test() {
  let store = setup()

  // Read non-existent
  let result = store.read_snapshot("src-1")
  should.be_error(result)

  // Record
  let snap =
    SnapshotData(
      source_uuid: "src-1",
      source_version: 5,
      source_type: "test",
      data: Created("snapshot"),
      created_at: 12_345,
    )
  let assert Ok(Nil) = store.record_snapshot(snap)

  // Read
  let assert Ok(read_snap) = store.read_snapshot("src-1")
  should.equal(read_snap.source_uuid, "src-1")
  should.equal(read_snap.source_version, 5)
  should.equal(read_snap.data, Created("snapshot"))

  // Delete
  let assert Ok(Nil) = store.delete_snapshot("src-1")
  should.be_error(store.read_snapshot("src-1"))
}

pub fn pg_reset_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("reset-stream", NoStream, [evt(Created("Reset"))])

  let assert Ok(Nil) = store.reset()
  should.be_error(store.read_stream_forward("reset-stream", 1, 1000))
}

pub fn pg_latest_event_number_test() {
  let store = setup()

  let assert Ok(None) = store.get_latest_event_number()

  let assert Ok(_) =
    store.append_to_stream("num-stream", NoStream, [
      evt(Created("Num")),
      evt(Updated("Number")),
    ])

  let assert Ok(Some(n)) = store.get_latest_event_number()
  should.equal(n, 2)
}

pub fn pg_metadata_test() {
  let store = setup()

  let meta = dict.from_list([#("user", "admin"), #("ip", "127.0.0.1")])
  let assert Ok(_) =
    store.append_to_stream("meta-stream", NoStream, [
      evt_meta(Created("Meta"), "cause-1", "corr-1", meta),
    ])

  let assert Ok([ev]) = store.read_stream_forward("meta-stream", 1, 1000)
  should.equal(ev.causation_id, Some("cause-1"))
  should.equal(ev.correlation_id, Some("corr-1"))
  should.equal(dict.get(ev.metadata, "user"), Ok("admin"))
  should.equal(dict.get(ev.metadata, "ip"), Ok("127.0.0.1"))
}

pub fn pg_persistent_subscription_test() {
  let store = setup()

  // Start a persistent subscription
  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev.data)
  }

  let assert Ok(_sub) =
    store.subscribe_persistent("sub-stream", "test-sub", Origin, handler)

  // Append events
  let assert Ok(_) =
    store.append_to_stream("sub-stream", NoStream, [
      evt(Created("sub-event")),
    ])

  // Event should be delivered
  let assert Ok(data) = process.receive(received, 2000)
  should.equal(data, Created("sub-event"))
}

pub fn pg_subscription_duplicate_test() {
  let store = setup()

  let noop = fn(_ev: RecordedEvent(TestEvent)) { Nil }

  let assert Ok(_) =
    store.subscribe_persistent("dup-stream", "dup-sub", Origin, noop)

  // Second subscribe with same name → SubscriptionAlreadyExists
  let result =
    store.subscribe_persistent("dup-stream", "dup-sub", Origin, noop)
  should.be_error(result)
}

pub fn pg_delete_subscription_test() {
  let store = setup()

  let noop = fn(_ev: RecordedEvent(TestEvent)) { Nil }

  let assert Ok(_) =
    store.subscribe_persistent("del-stream", "del-sub", Origin, noop)

  let assert Ok(Nil) = store.delete_subscription("del-stream", "del-sub")

  // After delete, can create again
  let assert Ok(_) =
    store.subscribe_persistent("del-stream", "del-sub", Origin, noop)
  Nil
}
