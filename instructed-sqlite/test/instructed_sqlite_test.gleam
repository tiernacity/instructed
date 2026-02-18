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
import instructed_sqlite

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
            False -> Error("Unknown event type: " <> json_str)
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

// Each test uses an in-memory SQLite database (:memory:).
// Since each connection is independent, tests are isolated.
fn setup() {
  let config =
    instructed_sqlite.SqliteConfig(
      db_path: ":memory:",
      serialize: serialize_event,
      deserialize: deserialize_event,
      event_type: event_type_name,
    )
  let assert Ok(subject) = instructed_sqlite.start(config)
  instructed_sqlite.to_event_store(subject)
}

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

pub fn sqlite_append_and_read_test() {
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

pub fn sqlite_version_conflict_test() {
  let store = setup()

  let assert Ok(_) = store.append_to_stream("user-vc", NoStream, [evt(Created("Bob"))])

  let result =
    store.append_to_stream("user-vc", ExactVersion(5), [evt(Updated("Robert"))])
  should.be_error(result)
}

pub fn sqlite_exact_version_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("user-ev", NoStream, [evt(Created("Charlie"))])

  let assert Ok(v) =
    store.append_to_stream("user-ev", ExactVersion(1), [evt(Updated("Chuck"))])
  should.equal(v, 2)
}

pub fn sqlite_any_version_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("user-av", AnyVersion, [evt(Created("Dave"))])

  let assert Ok(v) =
    store.append_to_stream("user-av", AnyVersion, [evt(Updated("David"))])
  should.equal(v, 2)
}

pub fn sqlite_read_nonexistent_test() {
  let store = setup()
  let result = store.read_stream_forward("nonexistent", 1, 1000)
  should.be_error(result)
}

pub fn sqlite_read_count_limit_test() {
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

pub fn sqlite_read_all_forward_test() {
  let store = setup()

  let assert Ok(_) = store.append_to_stream("sa", NoStream, [evt(Created("A"))])
  let assert Ok(_) = store.append_to_stream("sb", NoStream, [evt(Created("B"))])

  let assert Ok(all) = store.read_all_forward(1)
  should.equal(list.length(all), 2)
}

pub fn sqlite_snapshot_test() {
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

pub fn sqlite_reset_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("reset-stream", NoStream, [evt(Created("Reset"))])

  let assert Ok(Nil) = store.reset()
  should.be_error(store.read_stream_forward("reset-stream", 1, 1000))
}

pub fn sqlite_latest_event_number_test() {
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

pub fn sqlite_metadata_test() {
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

pub fn sqlite_persistent_subscription_test() {
  let store = setup()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev.data)
  }

  let assert Ok(_sub) =
    store.subscribe_persistent("sub-stream", "test-sub", Origin, handler)

  let assert Ok(_) =
    store.append_to_stream("sub-stream", NoStream, [evt(Created("sub-event"))])

  // Event should be delivered synchronously (same process — SQLite adapter
  // notifies during append inside the actor, which calls handler inline)
  let assert Ok(data) = process.receive(received, 2000)
  should.equal(data, Created("sub-event"))
}

pub fn sqlite_subscription_duplicate_test() {
  let store = setup()

  let noop = fn(_ev: RecordedEvent(TestEvent)) { Nil }

  let assert Ok(_) =
    store.subscribe_persistent("dup-stream", "dup-sub", Origin, noop)

  // Second subscribe with same name → SubscriptionAlreadyExists
  let result = store.subscribe_persistent("dup-stream", "dup-sub", Origin, noop)
  should.be_error(result)
}

pub fn sqlite_delete_subscription_test() {
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

pub fn sqlite_multiple_streams_test() {
  let store = setup()

  let assert Ok(_) = store.append_to_stream("stream-a", NoStream, [evt(Created("A1"))])
  let assert Ok(_) = store.append_to_stream("stream-b", NoStream, [evt(Created("B1"))])
  let assert Ok(_) = store.append_to_stream("stream-a", ExactVersion(1), [evt(Updated("A2"))])

  let assert Ok(a_events) = store.read_stream_forward("stream-a", 1, 1000)
  should.equal(list.length(a_events), 2)

  let assert Ok(b_events) = store.read_stream_forward("stream-b", 1, 1000)
  should.equal(list.length(b_events), 1)
}

pub fn sqlite_snapshot_upsert_test() {
  let store = setup()

  let snap1 =
    SnapshotData(
      source_uuid: "upsert-1",
      source_version: 1,
      source_type: "test",
      data: Created("v1"),
      created_at: 1000,
    )
  let assert Ok(Nil) = store.record_snapshot(snap1)

  // Upsert with new version
  let snap2 =
    SnapshotData(
      source_uuid: "upsert-1",
      source_version: 5,
      source_type: "test",
      data: Updated("v5"),
      created_at: 5000,
    )
  let assert Ok(Nil) = store.record_snapshot(snap2)

  let assert Ok(read) = store.read_snapshot("upsert-1")
  should.equal(read.source_version, 5)
  should.equal(read.data, Updated("v5"))
}

pub fn sqlite_transient_subscribe_test() {
  let store = setup()

  let received = process.new_subject()
  let assert Ok(_sub) =
    store.subscribe(fn(ev: RecordedEvent(TestEvent)) {
      process.send(received, ev.data)
    })

  let assert Ok(_) =
    store.append_to_stream("tsub-stream", NoStream, [evt(Created("transient"))])

  let assert Ok(data) = process.receive(received, 1000)
  should.equal(data, Created("transient"))
}

pub fn sqlite_stream_subscribe_test() {
  let store = setup()

  let received = process.new_subject()
  let assert Ok(_sub) =
    store.subscribe_to_stream("specific-stream", fn(ev: RecordedEvent(TestEvent)) {
      process.send(received, ev.data)
    })

  // Append to a different stream - should NOT trigger handler
  let assert Ok(_) =
    store.append_to_stream("other-stream", NoStream, [evt(Created("other"))])

  // No message for the other stream
  let no_msg = process.receive(received, 100)
  should.be_error(no_msg)

  // Append to the subscribed stream - should trigger handler
  let assert Ok(_) =
    store.append_to_stream("specific-stream", NoStream, [evt(Created("specific"))])

  let assert Ok(data) = process.receive(received, 1000)
  should.equal(data, Created("specific"))
}
