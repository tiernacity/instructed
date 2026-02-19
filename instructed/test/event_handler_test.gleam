import gleam/dict
import gleam/erlang/process
import gleam/option.{None}
import gleeunit/should
import instructed/error
import instructed/event.{EventData}
import instructed/event_handler
import instructed/event_store.{NoStream}
import instructed/in_memory_event_store

// ---------------------------------------------------------------------------
// Test domain
// ---------------------------------------------------------------------------

type TestEvent {
  TestOccurred(id: String)
  TestFailed(reason: String)
}

fn make_store() {
  let assert Ok(sub) = in_memory_event_store.start()
  in_memory_event_store.to_event_store(sub)
}

fn append_event(store: event_store.EventStore(TestEvent), stream, event) {
  let assert Ok(_) =
    store.append_to_stream(stream, NoStream, [
      EventData(
        data: event,
        event_type: "TestEvent",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])
  Nil
}

// ---------------------------------------------------------------------------
// Fix 1: Default handler stops on unhandled error
// ---------------------------------------------------------------------------

pub fn handler_stops_on_unhandled_error_test() {
  let store = make_store()

  // Handler that always fails, with NO on_error callback
  let config =
    event_handler.new(
      name: "stop_on_error_handler",
      handle_event: fn(_event, _recorded, _state) {
        Error("something went wrong")
      },
      initial_state: Nil,
    )

  let assert Ok(handler_subject) = event_handler.start(config, store)

  // Get the PID to check if it's alive later
  let assert Ok(pid) = process.subject_owner(handler_subject)
  should.equal(process.is_alive(pid), True)

  // Append an event that will cause the handler to fail
  append_event(store, "test-stream-1", TestOccurred("1"))

  // Give the handler time to process and stop
  process.sleep(100)

  // The handler should have stopped (not silently continued)
  should.equal(process.is_alive(pid), False)
}

// ---------------------------------------------------------------------------
// Fix 1: Handler with on_error=Stop also stops
// ---------------------------------------------------------------------------

pub fn handler_stops_on_explicit_stop_test() {
  let store = make_store()

  let config =
    event_handler.new(
      name: "explicit_stop_handler",
      handle_event: fn(_event, _recorded, _state) {
        Error("fail")
      },
      initial_state: Nil,
    )
    |> event_handler.with_error_handler(fn(_reason, _event, _state) {
      error.Stop("stopping intentionally")
    })

  let assert Ok(handler_subject) = event_handler.start(config, store)
  let assert Ok(pid) = process.subject_owner(handler_subject)

  append_event(store, "test-stream-2", TestOccurred("2"))
  process.sleep(100)

  should.equal(process.is_alive(pid), False)
}

// ---------------------------------------------------------------------------
// Fix 1: Handler with on_error=Skip continues
// ---------------------------------------------------------------------------

pub fn handler_continues_on_skip_test() {
  let store = make_store()
  let received = process.new_subject()

  let config =
    event_handler.new(
      name: "skip_handler",
      handle_event: fn(event, _recorded, _state) {
        case event {
          TestFailed(_) -> Error("fail")
          TestOccurred(id) -> {
            process.send(received, id)
            Ok(Nil)
          }
        }
      },
      initial_state: Nil,
    )
    |> event_handler.with_error_handler(fn(_reason, _event, _state) {
      error.Skip
    })

  let assert Ok(handler_subject) = event_handler.start(config, store)
  let assert Ok(pid) = process.subject_owner(handler_subject)

  // Append a failing event then a succeeding one
  let assert Ok(_) =
    store.append_to_stream("test-stream-3", NoStream, [
      EventData(
        data: TestFailed("bad"),
        event_type: "TestEvent",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(50)

  // Handler should still be alive after skipping
  should.equal(process.is_alive(pid), True)
}
