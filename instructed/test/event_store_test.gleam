import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import instructed/error
import instructed/event.{type EventData, EventData}
import instructed/event_store.{
  AnyVersion, ExactVersion, NoStream, Origin, StreamExists,
}
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
  EventData(data: event, event_type: "", causation_id: None, correlation_id: None, metadata: dict.new())
}

// --- Basic Append & Read Tests ---

pub fn append_and_read_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Alice"))]

  let result = store.append_to_stream("user-1", NoStream, events)
  should.be_ok(result)
  let assert Ok(version) = result
  should.equal(version, 1)

  let read_result = store.read_stream_forward("user-1", 1, 1000)
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

  let assert Ok(recorded) = store.read_stream_forward("user-2", 1, 1000)
  should.equal(list.length(recorded), 2)

  let assert [first, second] = recorded
  should.equal(first.stream_version, 1)
  should.equal(second.stream_version, 2)
  should.equal(first.data, UserCreated("Bob"))
  should.equal(second.data, UserRenamed("Robert"))
}

// --- Expected Version Tests ---

pub fn version_conflict_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Alice"))]

  let assert Ok(_) = store.append_to_stream("user-3", NoStream, events)

  // Try to append with wrong version
  let more_events = [make_event_data(UserRenamed("Alicia"))]
  let result = store.append_to_stream("user-3", ExactVersion(5), more_events)
  should.be_error(result)
  let assert Error(error.VersionConflict) = result
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

  // Second append with NoStream should fail with StreamAlreadyExists
  let more = [make_event_data(UserRenamed("Eva"))]
  let result = store.append_to_stream("user-6", NoStream, more)
  should.be_error(result)
  let assert Error(error.StreamAlreadyExists) = result
}

pub fn stream_exists_test() {
  let store = start_store()

  // StreamExists on non-existent stream should fail with StreamNotFound
  let events = [make_event_data(UserCreated("Frank"))]
  let result = store.append_to_stream("user-7", StreamExists, events)
  should.be_error(result)
  let assert Error(error.StreamNotFound) = result
}

pub fn stream_exists_success_test() {
  let store = start_store()
  let events = [make_event_data(UserCreated("Grace"))]
  let assert Ok(_) = store.append_to_stream("user-se", NoStream, events)

  // StreamExists on existing stream should succeed
  let more = [make_event_data(UserRenamed("Gracie"))]
  let result = store.append_to_stream("user-se", StreamExists, more)
  should.be_ok(result)
  let assert Ok(version) = result
  should.equal(version, 2)
}

// --- Read Tests ---

pub fn read_nonexistent_stream_test() {
  let store = start_store()
  let result = store.read_stream_forward("nonexistent", 1, 1000)
  should.be_error(result)
  let assert Error(error.StreamNotFound) = result
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
  let assert Ok(recorded) = store.read_stream_forward("user-8", 2, 1000)
  should.equal(list.length(recorded), 2)
  let assert [first, _second] = recorded
  should.equal(first.stream_version, 2)
}

pub fn read_batch_size_test() {
  let store = start_store()
  let events = [
    make_event_data(UserCreated("Batch1")),
    make_event_data(UserRenamed("Batch2")),
    make_event_data(UserRenamed("Batch3")),
    make_event_data(UserRenamed("Batch4")),
    make_event_data(UserRenamed("Batch5")),
  ]
  let assert Ok(_) = store.append_to_stream("batch-stream", NoStream, events)

  // Read with batch_size of 2 - should only get first 2
  let assert Ok(recorded) = store.read_stream_forward("batch-stream", 1, 2)
  should.equal(list.length(recorded), 2)
  let assert [first, second] = recorded
  should.equal(first.stream_version, 1)
  should.equal(second.stream_version, 2)

  // Read next batch starting from version 3
  let assert Ok(recorded2) = store.read_stream_forward("batch-stream", 3, 2)
  should.equal(list.length(recorded2), 2)
  let assert [third, fourth] = recorded2
  should.equal(third.stream_version, 3)
  should.equal(fourth.stream_version, 4)

  // Read final batch
  let assert Ok(recorded3) = store.read_stream_forward("batch-stream", 5, 2)
  should.equal(list.length(recorded3), 1)
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

// --- Global Event Number Tests ---

pub fn global_event_numbers_are_monotonic_test() {
  let store = start_store()
  let assert Ok(_) =
    store.append_to_stream("s1", NoStream, [
      make_event_data(UserCreated("A")),
      make_event_data(UserRenamed("B")),
    ])
  let assert Ok(_) =
    store.append_to_stream("s2", NoStream, [
      make_event_data(UserCreated("C")),
    ])

  let assert Ok(all) = store.read_all_forward(1)
  should.equal(list.length(all), 3)
  let assert [e1, e2, e3] = all
  should.equal(e1.event_number, 1)
  should.equal(e2.event_number, 2)
  should.equal(e3.event_number, 3)
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

  let assert Ok(Some(num)) = store.get_latest_event_number()
  should.equal(num, 2)
}

// --- Snapshot Tests ---

pub fn snapshot_test() {
  let store = start_store()

  // Read non-existent snapshot
  let result = store.read_snapshot("source-1")
  should.be_error(result)
  let assert Error(error.SnapshotNotFound) = result

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

pub fn snapshot_upsert_test() {
  let store = start_store()

  // Record initial snapshot
  let snap1 =
    snapshot.SnapshotData(
      source_uuid: "source-u",
      source_version: 1,
      source_type: "test",
      data: UserCreated("v1"),
      created_at: 100,
    )
  let assert Ok(Nil) = store.record_snapshot(snap1)

  // Upsert with new version
  let snap2 =
    snapshot.SnapshotData(
      source_uuid: "source-u",
      source_version: 5,
      source_type: "test",
      data: UserCreated("v5"),
      created_at: 200,
    )
  let assert Ok(Nil) = store.record_snapshot(snap2)

  // Should get the latest snapshot
  let assert Ok(read_snap) = store.read_snapshot("source-u")
  should.equal(read_snap.source_version, 5)
}

// --- Reset Test ---

pub fn reset_test() {
  let store = start_store()
  let assert Ok(_) =
    store.append_to_stream("stream-reset", NoStream, [
      make_event_data(UserCreated("Reset")),
    ])

  let assert Ok(Nil) = store.reset()

  // Stream should be gone
  let result = store.read_stream_forward("stream-reset", 1, 1000)
  should.be_error(result)
}

// --- Persistent Subscription Tests ---

pub fn persistent_subscription_delivers_events_test() {
  let store = start_store()

  // Append some events first
  let assert Ok(_) =
    store.append_to_stream("sub-stream", NoStream, [
      make_event_data(UserCreated("Sub1")),
      make_event_data(UserRenamed("Sub1-renamed")),
    ])

  // Create a subscriber process that collects events
  let test_subject = process.new_subject()

  let handler = fn(event: event.RecordedEvent(TestEvent)) {
    process.send(test_subject, event)
  }

  // Subscribe from origin - should get historical events
  let assert Ok(subscription) =
    store.subscribe_persistent(
      "sub-stream",
      "test-sub",
      Origin,
      handler,
    )

  // First event should be delivered immediately
  let assert Ok(evt1) = process.receive(test_subject, 1000)
  should.equal(evt1.data, UserCreated("Sub1"))
  should.equal(evt1.stream_version, 1)

  // Ack first event to get next one
  let assert Ok(Nil) = store.ack_event(subscription, evt1)

  // Second event should now be delivered
  let assert Ok(evt2) = process.receive(test_subject, 1000)
  should.equal(evt2.data, UserRenamed("Sub1-renamed"))
  should.equal(evt2.stream_version, 2)

  let assert Ok(Nil) = store.ack_event(subscription, evt2)
}

pub fn persistent_subscription_backpressure_test() {
  let store = start_store()

  let test_subject = process.new_subject()

  let handler = fn(event: event.RecordedEvent(TestEvent)) {
    process.send(test_subject, event)
  }

  // Subscribe to all streams
  let assert Ok(subscription) =
    store.subscribe_persistent(
      "$all",
      "backpressure-sub",
      Origin,
      handler,
    )

  // Append events
  let assert Ok(_) =
    store.append_to_stream("bp-stream", NoStream, [
      make_event_data(UserCreated("BP1")),
      make_event_data(UserRenamed("BP2")),
    ])

  // Should get first event
  let assert Ok(evt1) = process.receive(test_subject, 1000)
  should.equal(evt1.data, UserCreated("BP1"))

  // Without acking, second event should NOT be delivered
  let result = process.receive(test_subject, 100)
  should.be_error(result)

  // After acking, second event should be delivered
  let assert Ok(Nil) = store.ack_event(subscription, evt1)
  let assert Ok(evt2) = process.receive(test_subject, 1000)
  should.equal(evt2.data, UserRenamed("BP2"))

  let assert Ok(Nil) = store.ack_event(subscription, evt2)
}

pub fn persistent_subscription_from_current_test() {
  let store = start_store()

  // Append some events BEFORE subscribing
  let assert Ok(_) =
    store.append_to_stream("cur-stream", NoStream, [
      make_event_data(UserCreated("Old")),
    ])

  let test_subject = process.new_subject()
  let handler = fn(event: event.RecordedEvent(TestEvent)) {
    process.send(test_subject, event)
  }

  // Subscribe from Current - should NOT get old events
  let assert Ok(subscription) =
    store.subscribe_persistent(
      "$all",
      "current-sub",
      event_store.Current,
      handler,
    )

  // No events should be delivered
  let result = process.receive(test_subject, 100)
  should.be_error(result)

  // Append new events AFTER subscribing
  let assert Ok(_) =
    store.append_to_stream("cur-stream", ExactVersion(1), [
      make_event_data(UserRenamed("New")),
    ])

  // New event should be delivered
  let assert Ok(evt) = process.receive(test_subject, 1000)
  should.equal(evt.data, UserRenamed("New"))

  let assert Ok(Nil) = store.ack_event(subscription, evt)
}

pub fn persistent_subscription_idempotent_reconnect_test() {
  let store = start_store()
  let handler = fn(_event: event.RecordedEvent(TestEvent)) { Nil }

  let assert Ok(_) =
    store.subscribe_persistent("$all", "dup-sub", Origin, handler)

  // Second subscription with same name should succeed (idempotent reconnect, Fix 3).
  // The adapter preserves the checkpoint position and updates the handler.
  let result =
    store.subscribe_persistent("$all", "dup-sub", Origin, handler)
  should.be_ok(result)
}

pub fn delete_subscription_test() {
  let store = start_store()
  let handler = fn(_event: event.RecordedEvent(TestEvent)) { Nil }

  let assert Ok(_) =
    store.subscribe_persistent("$all", "del-sub", Origin, handler)

  // Delete should succeed
  let assert Ok(Nil) = store.delete_subscription("$all", "del-sub")

  // Delete non-existent should fail
  let result = store.delete_subscription("$all", "del-sub")
  should.be_error(result)
  let assert Error(error.SubscriptionNotFound) = result
}

pub fn persistent_subscription_ack_tracks_position_test() {
  let store = start_store()

  // Append events
  let assert Ok(_) =
    store.append_to_stream("pos-stream", NoStream, [
      make_event_data(UserCreated("P1")),
      make_event_data(UserRenamed("P2")),
      make_event_data(UserRenamed("P3")),
    ])

  let test_subject = process.new_subject()
  let handler = fn(event: event.RecordedEvent(TestEvent)) {
    process.send(test_subject, event)
  }

  let assert Ok(subscription) =
    store.subscribe_persistent(
      "pos-stream",
      "pos-sub",
      Origin,
      handler,
    )

  // Process and ack first two events
  let assert Ok(evt1) = process.receive(test_subject, 1000)
  should.equal(evt1.data, UserCreated("P1"))
  let assert Ok(Nil) = store.ack_event(subscription, evt1)

  let assert Ok(evt2) = process.receive(test_subject, 1000)
  should.equal(evt2.data, UserRenamed("P2"))
  let assert Ok(Nil) = store.ack_event(subscription, evt2)

  let assert Ok(evt3) = process.receive(test_subject, 1000)
  should.equal(evt3.data, UserRenamed("P3"))
  let assert Ok(Nil) = store.ack_event(subscription, evt3)
}

pub fn persistent_subscription_new_events_after_subscribe_test() {
  let store = start_store()

  let test_subject = process.new_subject()
  let handler = fn(event: event.RecordedEvent(TestEvent)) {
    process.send(test_subject, event)
  }

  let assert Ok(subscription) =
    store.subscribe_persistent(
      "$all",
      "new-events-sub",
      Origin,
      handler,
    )

  // Append events AFTER subscribing
  let assert Ok(_) =
    store.append_to_stream("new-stream", NoStream, [
      make_event_data(UserCreated("New1")),
    ])

  // Event should be delivered
  let assert Ok(evt) = process.receive(test_subject, 1000)
  should.equal(evt.data, UserCreated("New1"))
  should.equal(evt.event_number, 1)

  let assert Ok(Nil) = store.ack_event(subscription, evt)

  // Append more
  let assert Ok(_) =
    store.append_to_stream("new-stream", ExactVersion(1), [
      make_event_data(UserRenamed("New2")),
    ])

  let assert Ok(evt2) = process.receive(test_subject, 1000)
  should.equal(evt2.data, UserRenamed("New2"))
  should.equal(evt2.event_number, 2)

  let assert Ok(Nil) = store.ack_event(subscription, evt2)
}

// --- Causation/Correlation ID Tests ---

pub fn causation_correlation_preserved_test() {
  let store = start_store()

  let event_data =
    EventData(
      data: UserCreated("Causal"),
      event_type: "UserCreated",
      causation_id: Some("cmd-123"),
      correlation_id: Some("corr-456"),
      metadata: dict.from_list([#("key", "value")]),
    )

  let assert Ok(_) =
    store.append_to_stream("causal-stream", NoStream, [event_data])

  let assert Ok([recorded]) =
    store.read_stream_forward("causal-stream", 1, 1000)

  should.equal(recorded.causation_id, Some("cmd-123"))
  should.equal(recorded.correlation_id, Some("corr-456"))
  should.equal(recorded.metadata, dict.from_list([#("key", "value")]))
}

// --- Event ID Uniqueness Test ---

pub fn event_type_preserved_test() {
  let store = start_store()

  let event_data =
    EventData(
      data: UserCreated("TypeTest"),
      event_type: "UserCreated",
      causation_id: None,
      correlation_id: None,
      metadata: dict.new(),
    )

  let assert Ok(_) =
    store.append_to_stream("type-stream", NoStream, [event_data])

  let assert Ok([recorded]) =
    store.read_stream_forward("type-stream", 1, 1000)

  should.equal(recorded.event_type, "UserCreated")
}

pub fn event_ids_are_unique_test() {
  let store = start_store()

  let assert Ok(_) =
    store.append_to_stream("uid-stream", NoStream, [
      make_event_data(UserCreated("A")),
      make_event_data(UserRenamed("B")),
    ])

  let assert Ok([e1, e2]) = store.read_stream_forward("uid-stream", 1, 1000)
  should.not_equal(e1.event_id, e2.event_id)
}
