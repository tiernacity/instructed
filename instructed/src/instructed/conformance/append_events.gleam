//// Append events conformance tests.
////
//// Tests that an EventStore correctly implements append_to_stream,
//// read_stream_forward, and read_all_forward with proper version
//// checking and event data preservation.

import instructed/conformance/test_event.{Created, Deleted, Updated, evt, evt_with_metadata}
import gleam/dict
import gleam/list
import gleam/option.{Some}
import instructed/conformance/assertions as should
import instructed/error
import instructed/event_store.{
  type EventStore, AnyVersion, ExactVersion, NoStream, StreamExists,
}

/// Run all append conformance tests.
pub fn run_all(factory: fn() -> EventStore(test_event.TestEvent)) -> Nil {
  test_append_exact_version(factory)
  test_append_any_version(factory)
  test_append_no_stream_succeeds_on_new(factory)
  test_append_no_stream_fails_on_existing(factory)
  test_append_stream_exists_succeeds_on_existing(factory)
  test_append_stream_exists_fails_on_new(factory)
  test_wrong_version_returns_version_conflict(factory)
  test_read_nonexistent_stream(factory)
  test_read_events_correct_data(factory)
  test_read_events_metadata(factory)
  test_read_batched(factory)
  test_read_single_stream_isolation(factory)
  test_read_all_forward_ordering(factory)
  test_append_multiple_events_atomically(factory)
  test_event_numbers_are_monotonic(factory)
}

fn test_append_exact_version(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(v1) =
    store.append_to_stream("ev-stream", NoStream, [evt(Created("Alice"))])
  should.equal(v1, 1)

  let assert Ok(v2) =
    store.append_to_stream("ev-stream", ExactVersion(1), [
      evt(Updated("Alicia")),
    ])
  should.equal(v2, 2)

  let assert Ok(v3) =
    store.append_to_stream("ev-stream", ExactVersion(2), [evt(Deleted)])
  should.equal(v3, 3)
  Nil
}

fn test_append_any_version(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(v1) =
    store.append_to_stream("av-stream", AnyVersion, [evt(Created("Bob"))])
  should.equal(v1, 1)

  let assert Ok(v2) =
    store.append_to_stream("av-stream", AnyVersion, [evt(Updated("Robert"))])
  should.equal(v2, 2)
  Nil
}

fn test_append_no_stream_succeeds_on_new(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(v) =
    store.append_to_stream("ns-stream", NoStream, [evt(Created("Charlie"))])
  should.equal(v, 1)
  Nil
}

fn test_append_no_stream_fails_on_existing(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("ns-fail", NoStream, [evt(Created("Dave"))])

  let result =
    store.append_to_stream("ns-fail", NoStream, [evt(Updated("David"))])
  should.be_error(result)
  Nil
}

fn test_append_stream_exists_succeeds_on_existing(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("se-stream", NoStream, [evt(Created("Eve"))])

  let assert Ok(v) =
    store.append_to_stream("se-stream", StreamExists, [evt(Updated("Eva"))])
  should.equal(v, 2)
  Nil
}

fn test_append_stream_exists_fails_on_new(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let result =
    store.append_to_stream("se-fail", StreamExists, [evt(Created("Frank"))])
  should.be_error(result)
  Nil
}

fn test_wrong_version_returns_version_conflict(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("vc-stream", NoStream, [evt(Created("Grace"))])

  let result =
    store.append_to_stream("vc-stream", ExactVersion(99), [
      evt(Updated("Gracie")),
    ])
  let assert Error(error.VersionConflict) = result
  Nil
}

fn test_read_nonexistent_stream(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let result = store.read_stream_forward("nonexistent", 1, 1000)
  should.be_error(result)
  Nil
}

fn test_read_events_correct_data(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("rd-stream", NoStream, [
      evt(Created("Heidi")),
      evt(Updated("Heidi2")),
    ])

  let assert Ok(events) = store.read_stream_forward("rd-stream", 1, 1000)
  should.equal(list.length(events), 2)

  let assert [e1, e2] = events
  should.equal(e1.data, Created("Heidi"))
  should.equal(e1.stream_id, "rd-stream")
  should.equal(e1.stream_version, 1)
  should.equal(e1.event_type, "Created")

  should.equal(e2.data, Updated("Heidi2"))
  should.equal(e2.stream_version, 2)
  should.equal(e2.event_type, "Updated")
  Nil
}

fn test_read_events_metadata(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let meta = dict.from_list([#("user", "admin"), #("ip", "127.0.0.1")])
  let assert Ok(_) =
    store.append_to_stream("meta-stream", NoStream, [
      evt_with_metadata(Created("Meta"), Some("cause-1"), Some("corr-1"), meta),
    ])

  let assert Ok([ev]) = store.read_stream_forward("meta-stream", 1, 1000)
  should.equal(ev.causation_id, Some("cause-1"))
  should.equal(ev.correlation_id, Some("corr-1"))
  should.equal(dict.get(ev.metadata, "user"), Ok("admin"))
  should.equal(dict.get(ev.metadata, "ip"), Ok("127.0.0.1"))
  Nil
}

fn test_read_batched(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("batch-stream", NoStream, [
      evt(Created("a")),
      evt(Updated("b")),
      evt(Created("c")),
      evt(Updated("d")),
      evt(Created("e")),
    ])

  // Read only 3
  let assert Ok(batch) = store.read_stream_forward("batch-stream", 1, 3)
  should.equal(list.length(batch), 3)

  // Read from offset 4
  let assert Ok(rest) = store.read_stream_forward("batch-stream", 4, 1000)
  should.equal(list.length(rest), 2)
  Nil
}

fn test_read_single_stream_isolation(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("iso-a", NoStream, [evt(Created("A"))])
  let assert Ok(_) =
    store.append_to_stream("iso-b", NoStream, [evt(Created("B"))])

  let assert Ok(events_a) = store.read_stream_forward("iso-a", 1, 1000)
  should.equal(list.length(events_a), 1)
  let assert [ea] = events_a
  should.equal(ea.data, Created("A"))
  Nil
}

fn test_read_all_forward_ordering(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("all-a", NoStream, [evt(Created("A"))])
  let assert Ok(_) =
    store.append_to_stream("all-b", NoStream, [evt(Created("B"))])
  let assert Ok(_) =
    store.append_to_stream("all-a", ExactVersion(1), [evt(Updated("A2"))])

  let assert Ok(all) = store.read_all_forward(1)
  should.equal(list.length(all), 3)

  // Verify ordering by event_number
  let assert [e1, e2, e3] = all
  should.be_true(e1.event_number < e2.event_number)
  should.be_true(e2.event_number < e3.event_number)
  Nil
}

fn test_append_multiple_events_atomically(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(v) =
    store.append_to_stream("multi-stream", NoStream, [
      evt(Created("X")),
      evt(Updated("Y")),
      evt(Deleted),
    ])
  should.equal(v, 3)

  let assert Ok(events) = store.read_stream_forward("multi-stream", 1, 1000)
  should.equal(list.length(events), 3)
  let assert [e1, e2, e3] = events
  should.equal(e1.stream_version, 1)
  should.equal(e2.stream_version, 2)
  should.equal(e3.stream_version, 3)
  Nil
}

fn test_event_numbers_are_monotonic(
  factory: fn() -> EventStore(test_event.TestEvent),
) -> Nil {
  let store = factory()
  let assert Ok(_) =
    store.append_to_stream("mono-a", NoStream, [
      evt(Created("A")),
      evt(Updated("B")),
    ])
  let assert Ok(_) =
    store.append_to_stream("mono-b", NoStream, [evt(Created("C"))])

  let assert Ok(all) = store.read_all_forward(1)
  should.equal(list.length(all), 3)
  let assert [e1, e2, e3] = all
  should.equal(e1.event_number, 1)
  should.equal(e2.event_number, 2)
  should.equal(e3.event_number, 3)
  Nil
}
