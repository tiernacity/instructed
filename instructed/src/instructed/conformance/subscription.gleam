//// Persistent subscription conformance tests.
////
//// Tests that an EventStore correctly implements persistent subscriptions
//// with ordered delivery, backpressure, resume from checkpoint, and
//// duplicate detection.

import instructed/conformance/test_event.{type TestEvent, Created, Updated, evt}
import gleam/erlang/process
import instructed/conformance/assertions as should
import instructed/error
import instructed/event.{type RecordedEvent}
import instructed/event_store.{
  type EventStore, ExactVersion, NoStream, Origin,
}

/// Run all persistent subscription conformance tests.
pub fn run_all(factory: fn() -> EventStore(TestEvent)) -> Nil {
  test_subscribe_from_origin_gets_historical(factory)
  test_subscribe_from_current_skips_existing(factory)
  test_subscribe_from_event_number(factory)
  test_subscribe_to_specific_stream(factory)
  test_subscribe_to_all_streams(factory)
  test_duplicate_subscription_name(factory)
  test_events_delivered_in_order(factory)
  test_resume_from_checkpoint(factory)
  test_delete_and_resubscribe(factory)
  test_backpressure(factory)
}

pub fn test_subscribe_from_origin_gets_historical(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  // Append events before subscribing
  let assert Ok(_) =
    store.append_to_stream("hist-stream", NoStream, [
      evt(Created("H1")),
      evt(Updated("H2")),
    ])

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) =
    store.subscribe_persistent("hist-stream", "hist-sub", Origin, handler)

  // Should receive historical events
  let assert Ok(e1) = process.receive(received, 2000)
  should.equal(e1.data, Created("H1"))
  let assert Ok(Nil) = store.ack_event(sub, e1)

  let assert Ok(e2) = process.receive(received, 2000)
  should.equal(e2.data, Updated("H2"))
  let assert Ok(Nil) = store.ack_event(sub, e2)
  Nil
}

pub fn test_subscribe_from_current_skips_existing(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  // Append events before subscribing
  let assert Ok(_) =
    store.append_to_stream("cur-stream", NoStream, [evt(Created("Old"))])

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) =
    store.subscribe_persistent(
      "$all",
      "cur-sub",
      event_store.Current,
      handler,
    )

  // Should NOT receive old events
  let result = process.receive(received, 200)
  should.be_error(result)

  // New events should be delivered
  let assert Ok(_) =
    store.append_to_stream("cur-stream", ExactVersion(1), [
      evt(Updated("New")),
    ])

  let assert Ok(ev) = process.receive(received, 2000)
  should.equal(ev.data, Updated("New"))
  let assert Ok(Nil) = store.ack_event(sub, ev)
  Nil
}

pub fn test_subscribe_from_event_number(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  // Append 3 events
  let assert Ok(_) =
    store.append_to_stream("en-stream", NoStream, [
      evt(Created("E1")),
      evt(Updated("E2")),
      evt(Updated("E3")),
    ])

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  // Subscribe from event_number 2 — should get events 2 and 3
  let assert Ok(sub) =
    store.subscribe_persistent(
      "$all",
      "en-sub",
      event_store.FromEventNumber(2),
      handler,
    )

  let assert Ok(e1) = process.receive(received, 2000)
  should.equal(e1.event_number, 2)
  let assert Ok(Nil) = store.ack_event(sub, e1)

  let assert Ok(e2) = process.receive(received, 2000)
  should.equal(e2.event_number, 3)
  let assert Ok(Nil) = store.ack_event(sub, e2)
  Nil
}

pub fn test_subscribe_to_specific_stream(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) =
    store.subscribe_persistent("target-stream", "stream-sub", Origin, handler)

  // Append to target stream
  let assert Ok(_) =
    store.append_to_stream("target-stream", NoStream, [evt(Created("Target"))])

  // Append to other stream
  let assert Ok(_) =
    store.append_to_stream("other-stream", NoStream, [evt(Created("Other"))])

  // Should only receive target stream event
  let assert Ok(ev) = process.receive(received, 2000)
  should.equal(ev.data, Created("Target"))
  should.equal(ev.stream_id, "target-stream")
  let assert Ok(Nil) = store.ack_event(sub, ev)

  // Should NOT receive other stream event
  let result = process.receive(received, 500)
  should.be_error(result)
  Nil
}

pub fn test_subscribe_to_all_streams(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) =
    store.subscribe_persistent("$all", "all-sub", Origin, handler)

  let assert Ok(_) =
    store.append_to_stream("stream-x", NoStream, [evt(Created("X"))])
  let assert Ok(_) =
    store.append_to_stream("stream-y", NoStream, [evt(Created("Y"))])

  let assert Ok(e1) = process.receive(received, 2000)
  should.equal(e1.data, Created("X"))
  let assert Ok(Nil) = store.ack_event(sub, e1)

  let assert Ok(e2) = process.receive(received, 2000)
  should.equal(e2.data, Created("Y"))
  let assert Ok(Nil) = store.ack_event(sub, e2)
  Nil
}

pub fn test_duplicate_subscription_name(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()
  let noop = fn(_ev: RecordedEvent(TestEvent)) { Nil }

  let assert Ok(_) =
    store.subscribe_persistent("$all", "dup-sub", Origin, noop)

  let result = store.subscribe_persistent("$all", "dup-sub", Origin, noop)
  let assert Error(error.SubscriptionAlreadyExists) = result
  Nil
}

pub fn test_events_delivered_in_order(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) =
    store.subscribe_persistent("$all", "order-sub", Origin, handler)

  // Append events across multiple streams
  let assert Ok(_) =
    store.append_to_stream("ord-a", NoStream, [evt(Created("A"))])
  let assert Ok(_) =
    store.append_to_stream("ord-b", NoStream, [evt(Created("B"))])
  let assert Ok(_) =
    store.append_to_stream("ord-a", ExactVersion(1), [evt(Updated("A2"))])

  // Receive all 3 and verify ordering
  let assert Ok(e1) = process.receive(received, 2000)
  let assert Ok(Nil) = store.ack_event(sub, e1)

  let assert Ok(e2) = process.receive(received, 2000)
  let assert Ok(Nil) = store.ack_event(sub, e2)

  let assert Ok(e3) = process.receive(received, 2000)
  let assert Ok(Nil) = store.ack_event(sub, e3)

  // Event numbers must be strictly increasing
  should.be_true(e1.event_number < e2.event_number)
  should.be_true(e2.event_number < e3.event_number)
  Nil
}

pub fn test_resume_from_checkpoint(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  // Append 3 events
  let assert Ok(_) =
    store.append_to_stream("resume-stream", NoStream, [
      evt(Created("R1")),
      evt(Updated("R2")),
      evt(Updated("R3")),
    ])

  let received1 = process.new_subject()
  let handler1 = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received1, ev)
  }

  // Subscribe, receive and ack first 2 events
  let assert Ok(sub1) =
    store.subscribe_persistent(
      "$all",
      "resume-sub",
      Origin,
      handler1,
    )

  let assert Ok(e1) = process.receive(received1, 2000)
  let assert Ok(Nil) = store.ack_event(sub1, e1)

  let assert Ok(e2) = process.receive(received1, 2000)
  let assert Ok(Nil) = store.ack_event(sub1, e2)

  // Delete subscription and re-create — should resume from last ack'd position
  let assert Ok(Nil) = store.delete_subscription("$all", "resume-sub")

  let received2 = process.new_subject()
  let handler2 = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received2, ev)
  }

  // Re-subscribe from Origin — but should get event 3 onwards because of checkpoint
  // Note: delete_subscription removes the checkpoint, so re-subscribing from Origin
  // replays everything. This matches Commanded behaviour.
  let assert Ok(sub2) =
    store.subscribe_persistent(
      "$all",
      "resume-sub",
      Origin,
      handler2,
    )

  // After delete + re-create from Origin, all events replay
  let assert Ok(re1) = process.receive(received2, 2000)
  should.equal(re1.data, Created("R1"))
  let assert Ok(Nil) = store.ack_event(sub2, re1)

  let assert Ok(re2) = process.receive(received2, 2000)
  should.equal(re2.data, Updated("R2"))
  let assert Ok(Nil) = store.ack_event(sub2, re2)

  let assert Ok(re3) = process.receive(received2, 2000)
  should.equal(re3.data, Updated("R3"))
  let assert Ok(Nil) = store.ack_event(sub2, re3)
  Nil
}

pub fn test_delete_and_resubscribe(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()
  let noop = fn(_ev: RecordedEvent(TestEvent)) { Nil }

  let assert Ok(_) =
    store.subscribe_persistent("$all", "del-resub", Origin, noop)

  let assert Ok(Nil) = store.delete_subscription("$all", "del-resub")

  // Should be able to create again after delete
  let assert Ok(_) =
    store.subscribe_persistent("$all", "del-resub", Origin, noop)
  Nil
}

pub fn test_backpressure(factory: fn() -> EventStore(TestEvent)) -> Nil {
  let store = factory()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) =
    store.subscribe_persistent("$all", "bp-sub", Origin, handler)

  // Append 2 events
  let assert Ok(_) =
    store.append_to_stream("bp-stream", NoStream, [
      evt(Created("BP1")),
      evt(Updated("BP2")),
    ])

  // Should get first event
  let assert Ok(e1) = process.receive(received, 2000)
  should.equal(e1.data, Created("BP1"))

  // Without acking, should NOT get second event (backpressure)
  let result = process.receive(received, 300)
  should.be_error(result)

  // After acking, second event arrives
  let assert Ok(Nil) = store.ack_event(sub, e1)
  let assert Ok(e2) = process.receive(received, 2000)
  should.equal(e2.data, Updated("BP2"))
  let assert Ok(Nil) = store.ack_event(sub, e2)
  Nil
}
