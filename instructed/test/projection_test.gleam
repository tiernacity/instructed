import gleam/dict
import gleam/erlang/process
import gleam/option.{None}
import gleeunit/should
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


