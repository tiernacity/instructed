import gleam/dict
import gleam/erlang/process
import gleam/option.{None}
import gleeunit/should
import instructed/error
import instructed/event.{EventData}
import instructed/event_store.{NoStream}
import instructed/in_memory_event_store
import instructed/projection

// --- Test domain ---

type TestEvent {
  ItemAdded(name: String, quantity: Int)
  ItemRemoved(name: String)
}

type Inventory =
  dict.Dict(String, Int)

fn start_projection() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let proj_config =
    projection.new(
      name: "inventory_projection",
      initial_state: dict.new(),
      handle_event: fn(event, _recorded, state: Inventory) {
        case event {
          ItemAdded(name, qty) -> {
            let current = case dict.get(state, name) {
              Ok(v) -> v
              Error(_) -> 0
            }
            Ok(dict.insert(state, name, current + qty))
          }
          ItemRemoved(name) -> Ok(dict.delete(state, name))
        }
      },
    )

  let assert Ok(proj) = projection.start(proj_config, store)
  #(store, proj)
}

// --- Tests ---

pub fn empty_projection_test() {
  let #(_store, proj) = start_projection()
  let state = projection.get_state(proj, 5000)
  should.equal(dict.size(state), 0)
}

pub fn projection_processes_events_test() {
  let #(store, proj) = start_projection()

  // Add some events
  let events = [
    EventData(
      data: ItemAdded("Widget", 10),
      event_type: "",
      causation_id: None,
      correlation_id: None,
      metadata: dict.new(),
    ),
    EventData(
      data: ItemAdded("Gadget", 5),
      event_type: "",
      causation_id: None,
      correlation_id: None,
      metadata: dict.new(),
    ),
  ]

  let assert Ok(_) = store.append_to_stream("items", NoStream, events)

  // Give projection time to process events
  process.sleep(100)

  let state = projection.get_state(proj, 5000)
  should.equal(dict.get(state, "Widget"), Ok(10))
  should.equal(dict.get(state, "Gadget"), Ok(5))
}

pub fn projection_removes_items_test() {
  let #(store, proj) = start_projection()

  let assert Ok(_) =
    store.append_to_stream("items2", NoStream, [
      EventData(
        data: ItemAdded("Thing", 3),
      event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(100)

  let assert Ok(_) =
    store.append_to_stream("items2", event_store.ExactVersion(1), [
      EventData(
        data: ItemRemoved("Thing"),
      event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(100)

  let state = projection.get_state(proj, 5000)
  should.equal(dict.has_key(state, "Thing"), False)
}

pub fn projection_multiple_streams_test() {
  let #(store, proj) = start_projection()

  let assert Ok(_) =
    store.append_to_stream("stream-x", NoStream, [
      EventData(
        data: ItemAdded("X", 1),
      event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  let assert Ok(_) =
    store.append_to_stream("stream-y", NoStream, [
      EventData(
        data: ItemAdded("Y", 2),
      event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(100)

  let state = projection.get_state(proj, 5000)
  should.equal(dict.size(state), 2)
  should.equal(dict.get(state, "X"), Ok(1))
  should.equal(dict.get(state, "Y"), Ok(2))
}

pub fn projection_accumulates_test() {
  let #(store, proj) = start_projection()

  let assert Ok(_) =
    store.append_to_stream("acc-stream", NoStream, [
      EventData(
        data: ItemAdded("Counter", 5),
      event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(50)

  let assert Ok(_) =
    store.append_to_stream("acc-stream", event_store.ExactVersion(1), [
      EventData(
        data: ItemAdded("Counter", 3),
      event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(100)

  let state = projection.get_state(proj, 5000)
  should.equal(dict.get(state, "Counter"), Ok(8))
}

// ---------------------------------------------------------------------------
// Fix 3: Projection stops on unhandled error (no on_error callback)
// ---------------------------------------------------------------------------

pub fn projection_stops_on_unhandled_error_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let proj_config =
    projection.new(
      name: "stop_on_error_proj",
      initial_state: 0,
      handle_event: fn(_event, _recorded, _state) { Error("fail") },
    )

  let assert Ok(proj) = projection.start(proj_config, store)
  let assert Ok(pid) = process.subject_owner(proj)
  should.equal(process.is_alive(pid), True)

  let assert Ok(_) =
    store.append_to_stream("proj-err-stream", NoStream, [
      EventData(
        data: ItemAdded("X", 1),
        event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(100)
  should.equal(process.is_alive(pid), False)
}

// ---------------------------------------------------------------------------
// Fix 3: Projection recursive retry then skip
// ---------------------------------------------------------------------------

pub fn projection_recursive_retry_then_skip_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)
  let attempt_counter = process.new_subject()

  let proj_config =
    projection.new(
      name: "retry_proj",
      initial_state: 0,
      handle_event: fn(_event, _recorded, state: Int) {
        case state >= 2 {
          True -> Ok(state)
          False -> Error("not ready")
        }
      },
    )
    |> projection.with_error_handler(fn(_reason, _event, state: Int) {
      let new_state = state + 1
      process.send(attempt_counter, new_state)
      case new_state >= 2 {
        True -> error.Skip
        False -> error.Retry(new_state)
      }
    })

  let assert Ok(proj) = projection.start(proj_config, store)
  let assert Ok(pid) = process.subject_owner(proj)

  let assert Ok(_) =
    store.append_to_stream("proj-retry-stream", NoStream, [
      EventData(
        data: ItemAdded("Y", 1),
        event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(100)

  // Should have called on_error twice (recursive retry)
  let assert Ok(1) = process.receive(attempt_counter, 100)
  let assert Ok(2) = process.receive(attempt_counter, 100)

  // Projection should still be alive after skip
  should.equal(process.is_alive(pid), True)
}

// ---------------------------------------------------------------------------
// Fix 3: Projection recursive retry then stop
// ---------------------------------------------------------------------------

pub fn projection_recursive_retry_then_stop_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)
  let attempt_counter = process.new_subject()

  let proj_config =
    projection.new(
      name: "retry_stop_proj",
      initial_state: 0,
      handle_event: fn(_event, _recorded, _state: Int) {
        Error("always fails")
      },
    )
    |> projection.with_error_handler(fn(_reason, _event, state: Int) {
      let new_state = state + 1
      process.send(attempt_counter, new_state)
      case new_state >= 2 {
        True -> error.Stop("giving up")
        False -> error.Retry(new_state)
      }
    })

  let assert Ok(proj) = projection.start(proj_config, store)
  let assert Ok(pid) = process.subject_owner(proj)

  let assert Ok(_) =
    store.append_to_stream("proj-retry-stop-stream", NoStream, [
      EventData(
        data: ItemAdded("Z", 1),
        event_type: "",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  process.sleep(100)

  let assert Ok(1) = process.receive(attempt_counter, 100)
  let assert Ok(2) = process.receive(attempt_counter, 100)

  should.equal(process.is_alive(pid), False)
}
