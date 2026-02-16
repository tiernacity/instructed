import gleam/dict
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should
import instructed/event.{EventData}
import instructed/event_store.{AnyVersion, ExactVersion, NoStream}
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
    True -> {
      let name = extract_name(json_str)
      Ok(Created(name))
    }
    False ->
      case string.contains(json_str, "\"Updated\"") {
        True -> {
          let name = extract_name(json_str)
          Ok(Updated(name))
        }
        False ->
          case string.contains(json_str, "\"Deleted\"") {
            True -> Ok(Deleted)
            False -> Error("Unknown event type")
          }
      }
  }
}

fn extract_name(json_str: String) -> String {
  // Simple JSON name extraction
  case string.split(json_str, "\"name\":\"") {
    [_, rest] ->
      case string.split(rest, "\"") {
        [name, ..] -> name
        _ -> ""
      }
    _ -> ""
  }
}

fn event_type(event: TestEvent) -> String {
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

import gleam/erlang/process

fn setup() {
  let db = get_db()
  let assert Ok(Nil) = instructed_postgres.drop_schema(db)
  let assert Ok(Nil) = instructed_postgres.create_schema(db)

  let config =
    instructed_postgres.PgConfig(
      db: db,
      serialize: serialize_event,
      deserialize: deserialize_event,
      event_type: event_type,
    )

  instructed_postgres.new(config)
}

// --- Tests ---

pub fn pg_append_and_read_test() {
  let store = setup()

  let events = [
    EventData(
      data: Created("Alice"),
      causation_id: None,
      correlation_id: None,
      metadata: dict.new(),
    ),
  ]

  let result = store.append_to_stream("user-1", NoStream, events)
  should.be_ok(result)
  let assert Ok(version) = result
  should.equal(version, 1)

  let read_result = store.read_stream_forward("user-1", 1)
  should.be_ok(read_result)
  let assert Ok(recorded) = read_result
  should.equal(list.length(recorded), 1)

  let assert [first] = recorded
  should.equal(first.data, Created("Alice"))
  should.equal(first.stream_id, "user-1")
  should.equal(first.stream_version, 1)
}

pub fn pg_version_conflict_test() {
  let store = setup()

  let events = [
    EventData(
      data: Created("Bob"),
      causation_id: None,
      correlation_id: None,
      metadata: dict.new(),
    ),
  ]

  let assert Ok(_) = store.append_to_stream("user-vc", NoStream, events)

  let more = [
    EventData(
      data: Updated("Robert"),
      causation_id: None,
      correlation_id: None,
      metadata: dict.new(),
    ),
  ]
  let result = store.append_to_stream("user-vc", ExactVersion(5), more)
  should.be_error(result)
}

pub fn pg_exact_version_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("user-ev", NoStream, [
      EventData(
        data: Created("Charlie"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  let assert Ok(v) =
    store.append_to_stream("user-ev", ExactVersion(1), [
      EventData(
        data: Updated("Chuck"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])
  should.equal(v, 2)
}

pub fn pg_any_version_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("user-av", AnyVersion, [
      EventData(
        data: Created("Dave"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  let assert Ok(v) =
    store.append_to_stream("user-av", AnyVersion, [
      EventData(
        data: Updated("David"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])
  should.equal(v, 2)
}

pub fn pg_read_nonexistent_test() {
  let store = setup()
  let result = store.read_stream_forward("nonexistent", 1)
  should.be_error(result)
}

pub fn pg_read_all_forward_test() {
  let store = setup()

  let assert Ok(_) =
    store.append_to_stream("sa", NoStream, [
      EventData(
        data: Created("A"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])
  let assert Ok(_) =
    store.append_to_stream("sb", NoStream, [
      EventData(
        data: Created("B"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

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
    store.append_to_stream("reset-stream", NoStream, [
      EventData(
        data: Created("Reset"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  let assert Ok(Nil) = store.reset()
  should.be_error(store.read_stream_forward("reset-stream", 1))
}

pub fn pg_latest_event_number_test() {
  let store = setup()

  let assert Ok(None) = store.get_latest_event_number()

  let assert Ok(_) =
    store.append_to_stream("num-stream", NoStream, [
      EventData(
        data: Created("Num"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
      EventData(
        data: Updated("Number"),
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  let assert Ok(Some(n)) = store.get_latest_event_number()
  should.equal(n, 2)
}

pub fn pg_metadata_test() {
  let store = setup()

  let metadata = dict.from_list([#("user", "admin"), #("ip", "127.0.0.1")])
  let assert Ok(_) =
    store.append_to_stream("meta-stream", NoStream, [
      EventData(
        data: Created("Meta"),
        causation_id: Some("cause-1"),
        correlation_id: Some("corr-1"),
        metadata: metadata,
      ),
    ])

  let assert Ok([evt]) = store.read_stream_forward("meta-stream", 1)
  should.equal(evt.causation_id, Some("cause-1"))
  should.equal(evt.correlation_id, Some("corr-1"))
  should.equal(dict.get(evt.metadata, "user"), Ok("admin"))
  should.equal(dict.get(evt.metadata, "ip"), Ok("127.0.0.1"))
}


