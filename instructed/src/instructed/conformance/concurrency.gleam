//// Concurrency conformance tests.
////
//// Tests that an EventStore handles concurrent operations correctly:
//// - Concurrent appends to different streams all succeed
//// - Concurrent appends to same stream with ExactVersion produce VersionConflict
//// - Concurrent appends + persistent subscription delivers all events in order

import instructed/conformance/test_event.{type TestEvent, Created, evt}
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/set
import instructed/conformance/assertions as should
import instructed/error
import instructed/event.{type RecordedEvent}
import instructed/event_store.{
  type EventStore, ExactVersion, NoStream, Origin,
}

/// Generate a list of integers from 1 to n.
fn range(n: Int) -> List(Int) {
  do_range(n, [])
}

fn do_range(n: Int, acc: List(Int)) -> List(Int) {
  case n < 1 {
    True -> acc
    False -> do_range(n - 1, [n, ..acc])
  }
}

/// Run all concurrency conformance tests.
pub fn run_all(factory: fn() -> EventStore(TestEvent)) -> Nil {
  test_concurrent_appends_different_streams(factory)
  test_concurrent_appends_same_stream_occ(factory)
  test_concurrent_appends_subscription_ordering(factory)
}

pub fn test_concurrent_appends_different_streams(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()
  let n = 50

  let results_subject = process.new_subject()

  range(n)
  |> list.each(fn(i) {
    process.spawn(fn() {
      let stream_id = "conc-stream-" <> int.to_string(i)
      let result =
        store.append_to_stream(stream_id, NoStream, [
          evt(Created("Event-" <> int.to_string(i))),
        ])
      process.send(results_subject, result)
    })
  })

  // Collect all results — all should succeed
  let results =
    range(n)
    |> list.map(fn(_) {
      let assert Ok(result) = process.receive(results_subject, 5000)
      result
    })

  list.each(results, fn(r) { should.be_ok(r) })

  // All events should be readable
  let assert Ok(all) = store.read_all_forward(1)
  should.equal(list.length(all), n)
  Nil
}

pub fn test_concurrent_appends_same_stream_occ(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()

  // Create the stream first
  let assert Ok(_) =
    store.append_to_stream("occ-stream", NoStream, [evt(Created("Initial"))])

  let n = 20
  let results_subject = process.new_subject()

  // Spawn N tasks all trying to append at ExactVersion(1)
  range(n)
  |> list.each(fn(i) {
    process.spawn(fn() {
      let result =
        store.append_to_stream("occ-stream", ExactVersion(1), [
          evt(Created("Concurrent-" <> int.to_string(i))),
        ])
      process.send(results_subject, result)
    })
  })

  // Collect results
  let results =
    range(n)
    |> list.map(fn(_) {
      let assert Ok(result) = process.receive(results_subject, 5000)
      result
    })

  // Exactly one should succeed, rest should get VersionConflict
  let successes = list.filter(results, fn(r) { r == Ok(2) })
  let conflicts =
    list.filter(results, fn(r) { r == Error(error.VersionConflict) })

  should.equal(list.length(successes), 1)
  should.equal(list.length(conflicts), n - 1)
  Nil
}

pub fn test_concurrent_appends_subscription_ordering(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()
  let n = 100

  // Set up persistent subscription before appending
  let received = process.new_subject()
  let handler = fn(ev: RecordedEvent(TestEvent)) {
    process.send(received, ev)
  }

  let assert Ok(sub) =
    store.subscribe_persistent("$all", "conc-order-sub", Origin, handler)

  // Spawn N tasks, each appending to a unique stream
  let done_subject = process.new_subject()
  range(n)
  |> list.each(fn(i) {
    process.spawn(fn() {
      let stream_id = "conc-ord-" <> int.to_string(i)
      let assert Ok(_) =
        store.append_to_stream(stream_id, NoStream, [
          evt(Created("E-" <> int.to_string(i))),
        ])
      process.send(done_subject, Nil)
    })
  })

  // Wait for all appends to complete
  range(n)
  |> list.each(fn(_) {
    let assert Ok(Nil) = process.receive(done_subject, 5000)
  })

  // Receive all N events from subscription, acking each
  let events =
    range(n)
    |> list.map(fn(_) {
      let assert Ok(ev) = process.receive(received, 5000)
      let assert Ok(Nil) = store.ack_event(sub, ev)
      ev
    })

  // Verify: exactly N events received
  should.equal(list.length(events), n)

  // Verify: all event numbers are unique
  let event_numbers = list.map(events, fn(e) { e.event_number })
  let unique_numbers = set.from_list(event_numbers)
  should.equal(set.size(unique_numbers), n)

  // Verify: event numbers are strictly increasing (ordered delivery)
  let is_ordered =
    list.window_by_2(event_numbers)
    |> list.all(fn(pair) { pair.0 < pair.1 })
  should.be_true(is_ordered)
  Nil
}
