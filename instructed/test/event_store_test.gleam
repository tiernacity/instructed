import gleam/dict
import gleam/list
import gleam/option.{None}
import gleeunit/should
import instructed/event.{type EventData, EventData}
import instructed/event_store.{AnyVersion, ExactVersion, NoStream, StreamExists}
import instructed/in_memory_event_store
import instructed/snapshot

// --- Test event type ---

type TestEvent {
  UserCreated(name: String)
  UserRenamed(new_name: String)
}

fn start_store() {
  let assert Ok(subject) = in_memory_event_store.start()
  in_memory_event_store.to_event_store(subject)
}

fn make_event_data(event: TestEvent) -> EventData(TestEvent) {
  EventData(data: event, causation_id: None, correlation_id: None, metadata: dict.new())
}

// --- Tests ---

pub fn append_and_read_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Alice"))]

  let result = store.append_to_stream("user-1", NoStream, events)
  should.be_ok(result)
  let assert Ok(version) = result
  should.equal(version, 1)

  let read_result = store.read_stream_forward("user-1", 1)
  should.be_ok(read_result)
  let assert Ok(recorded) = read_result
  should.equal(list.length(recorded), 1)

  let assert [first] = recorded
  should.equal(first.data, UserCreated("Alice"))
  should.equal(first.stream_id, "user-1")
  should.equal(first.stream_version, 1)
}

pub fn append_multiple_events_test() {
  let store = start_store()
  let events = [
    make_event_data(UserCreated("Bob")),
    make_event_data(UserRenamed("Robert")),
  ]

  let assert Ok(version) = store.append_to_stream("user-2", NoStream, events)
  should.equal(version, 2)

  let assert Ok(recorded) = store.read_stream_forward("user-2", 1)
  should.equal(list.length(recorded), 2)

  let assert [first, second] = recorded
  should.equal(first.stream_version, 1)
  should.equal(second.stream_version, 2)
  should.equal(first.data, UserCreated("Bob"))
  should.equal(second.data, UserRenamed("Robert"))
}

pub fn version_conflict_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Alice"))]

  let assert Ok(_) = store.append_to_stream("user-3", NoStream, events)

  // Try to append with wrong version
  let more_events = [make_event_data(UserRenamed("Alicia"))]
  let result = store.append_to_stream("user-3", ExactVersion(5), more_events)
  should.be_error(result)
}

pub fn exact_version_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Charlie"))]

  let assert Ok(_) = store.append_to_stream("user-4", NoStream, events)

  // Append with correct expected version
  let more = [make_event_data(UserRenamed("Chuck"))]
  let result = store.append_to_stream("user-4", ExactVersion(1), more)
  should.be_ok(result)
  let assert Ok(version) = result
  should.equal(version, 2)
}

pub fn any_version_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Dave"))]

  let assert Ok(_) = store.append_to_stream("user-5", AnyVersion, events)
  let more = [make_event_data(UserRenamed("David"))]
  let assert Ok(version) = store.append_to_stream("user-5", AnyVersion, more)
  should.equal(version, 2)
}

pub fn no_stream_conflict_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Eve"))]
  let assert Ok(_) = store.append_to_stream("user-6", NoStream, events)

  // Second append with NoStream should fail
  let more = [make_event_data(UserRenamed("Eva"))]
  let result = store.append_to_stream("user-6", NoStream, more)
  should.be_error(result)
}

pub fn stream_exists_test() {
  let store = start_store()

  // StreamExists on non-existent stream should fail
  let events = [make_event_data(UserCreated("Frank"))]
  let result = store.append_to_stream("user-7", StreamExists, events)
  should.be_error(result)
}

pub fn read_nonexistent_stream_test() {
  let store = start_store()
  let result = store.read_stream_forward("nonexistent", 1)
  should.be_error(result)
}

pub fn read_from_version_test() {
  let store = start_store()
  let events = [
    make_event_data(UserCreated("Grace")),
    make_event_data(UserRenamed("Gracie")),
    make_event_data(UserRenamed("Grace H.")),
  ]
  let assert Ok(_) = store.append_to_stream("user-8", NoStream, events)

  // Read from version 2 onwards
  let assert Ok(recorded) = store.read_stream_forward("user-8", 2)
  should.equal(list.length(recorded), 2)
  let assert [first, _second] = recorded
  should.equal(first.stream_version, 2)
}

pub fn read_all_forward_test() {
  let store = start_store()
  let assert Ok(_) =
    store.append_to_stream("stream-a", NoStream, [
      make_event_data(UserCreated("A")),
    ])
  let assert Ok(_) =
    store.append_to_stream("stream-b", NoStream, [
      make_event_data(UserCreated("B")),
    ])

  let assert Ok(all) = store.read_all_forward(1)
  should.equal(list.length(all), 2)
}

pub fn snapshot_test() {
  let store = start_store()

  // Read non-existent snapshot
  let result = store.read_snapshot("source-1")
  should.be_error(result)

  // Record snapshot
  let snap =
    snapshot.SnapshotData(
      source_uuid: "source-1",
      source_version: 5,
      source_type: "test",
      data: UserCreated("snapshot"),
      created_at: 12_345,
    )
  let assert Ok(Nil) = store.record_snapshot(snap)

  // Read snapshot
  let assert Ok(read_snap) = store.read_snapshot("source-1")
  should.equal(read_snap.source_uuid, "source-1")
  should.equal(read_snap.source_version, 5)

  // Delete snapshot
  let assert Ok(Nil) = store.delete_snapshot("source-1")
  let result = store.read_snapshot("source-1")
  should.be_error(result)
}

pub fn reset_test() {
  let store = start_store()
  let assert Ok(_) =
    store.append_to_stream("stream-reset", NoStream, [
      make_event_data(UserCreated("Reset")),
    ])

  let assert Ok(Nil) = store.reset()

  // Stream should be gone
  let result = store.read_stream_forward("stream-reset", 1)
  should.be_error(result)
}

pub fn get_latest_event_number_test() {
  let store = start_store()

  // No events yet
  let assert Ok(None) = store.get_latest_event_number()

  // Add events
  let assert Ok(_) =
    store.append_to_stream("stream-num", NoStream, [
      make_event_data(UserCreated("Num")),
      make_event_data(UserRenamed("Number")),
    ])

  let assert Ok(option.Some(num)) = store.get_latest_event_number()
  should.equal(num, 2)
}
