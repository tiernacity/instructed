//// Transient subscription conformance tests.
////
//// Tests that an EventStore correctly implements transient (non-persistent)
//// subscriptions for all streams and specific streams.

import instructed/conformance/test_event.{type TestEvent, Created, Updated, evt}
import gleam/erlang/process
import instructed/conformance/assertions as should
import instructed/event.{type RecordedEvent}
import instructed/event_store.{type EventStore, ExactVersion, NoStream}

/// Run all transient subscription conformance tests.
pub fn run_all(factory: fn() -> EventStore(TestEvent)) -> Nil {
  test_subscribe_all_receives_all(factory)
  test_subscribe_stream_receives_only_that_stream(factory)
  test_unsubscribe_stops_delivery(factory)
}

fn test_subscribe_all_receives_all(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(_sub) = store.subscribe(handler)

  let assert Ok(_) =
    store.append_to_stream("trans-a", NoStream, [evt(Created("A"))])
  let assert Ok(_) =
    store.append_to_stream("trans-b", NoStream, [evt(Created("B"))])

  let assert Ok(e1) = process.receive(received, 2000)
  should.equal(e1.data, Created("A"))

  let assert Ok(e2) = process.receive(received, 2000)
  should.equal(e2.data, Created("B"))
  Nil
}

fn test_subscribe_stream_receives_only_that_stream(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(_sub) = store.subscribe_to_stream("target-ts", handler)

  let assert Ok(_) =
    store.append_to_stream("target-ts", NoStream, [evt(Created("Target"))])
  let assert Ok(_) =
    store.append_to_stream("other-ts", NoStream, [evt(Created("Other"))])

  let assert Ok(ev) = process.receive(received, 2000)
  should.equal(ev.data, Created("Target"))

  // Should NOT receive other stream event
  let result = process.receive(received, 500)
  should.be_error(result)
  Nil
}

fn test_unsubscribe_stops_delivery(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) = store.subscribe(handler)

  // Receives event before unsub
  let assert Ok(_) =
    store.append_to_stream("unsub-stream", NoStream, [evt(Created("Before"))])
  let assert Ok(_ev) = process.receive(received, 2000)

  // Unsubscribe
  let assert Ok(Nil) = store.unsubscribe(sub)

  // Should NOT receive after unsub
  let assert Ok(_) =
    store.append_to_stream("unsub-stream", ExactVersion(1), [
      evt(Updated("After")),
    ])
  let result = process.receive(received, 500)
  should.be_error(result)
  Nil
}
