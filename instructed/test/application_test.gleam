import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/option.{None, Some}
import gleeunit/should
import instructed/aggregate
import instructed/application
import instructed/error
import instructed/event.{EventData}
import instructed/event_handler
import instructed/event_store.{AnyVersion}
import instructed/in_memory_event_store
import instructed/process_manager.{Skip, Start}
import instructed/projection
import instructed/router

// ---------------------------------------------------------------------------
// Test domain
// ---------------------------------------------------------------------------

type CounterState {
  Counter(id: String, count: Int)
}

type CounterCommand {
  CreateCounter(id: String)
  Increment(id: String)
}

type CounterEvent {
  CounterCreated(id: String)
  CounterIncremented(id: String)
}

fn counter_aggregate() {
  aggregate.new(
    empty_state: fn() { Counter("", 0) },
    execute: fn(state, cmd) {
      case cmd {
        CreateCounter(id) ->
          case state.id == "" {
            True -> Ok([CounterCreated(id)])
            False -> Error("already exists")
          }
        Increment(id) ->
          case state.id != "" {
            True -> Ok([CounterIncremented(id)])
            False -> Error("not created")
          }
      }
    },
    apply_event: fn(state, event) {
      case event {
        CounterCreated(id) -> Counter(id, 0)
        CounterIncremented(_) -> Counter(..state, count: state.count + 1)
      }
    },
  )
}

fn make_store() {
  let assert Ok(sub) = in_memory_event_store.start()
  in_memory_event_store.to_event_store(sub)
}

fn make_router(store) {
  router.new(
    aggregate: counter_aggregate(),
    event_store: store,
    identity: fn(cmd) {
      case cmd {
        CreateCounter(id) -> id
        Increment(id) -> id
      }
    },
  )
}

// ---------------------------------------------------------------------------
// Test 1: Application starts without errors
// ---------------------------------------------------------------------------

pub fn application_starts_test() {
  let store = make_store()
  let assert Ok(_app) = application.new(store) |> application.start()
  should.equal(True, True)
}

// ---------------------------------------------------------------------------
// Test 2: Event store accessor returns the configured store
// ---------------------------------------------------------------------------

pub fn application_event_store_accessor_test() {
  let store = make_store()
  let assert Ok(app) = application.new(store) |> application.start()

  // We can use the event store through the app
  let es = application.event_store(app)
  let assert Ok(_) =
    es.append_to_stream("test", AnyVersion, [
      EventData(
        data: CounterCreated("test"),
        event_type: "CounterCreated",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])
  should.equal(True, True)
}

// ---------------------------------------------------------------------------
// Test 3: Dispatch fails with no router configured
// ---------------------------------------------------------------------------

pub fn application_dispatch_no_router_test() {
  let store = make_store()
  let assert Ok(app) = application.new(store) |> application.start()

  let result = application.dispatch(app, CreateCounter("x"))
  should.be_error(result)
  let assert Error(err) = result
  should.equal(err, error.AggregateStartError("No router configured"))
}

// ---------------------------------------------------------------------------
// Test 4: Dispatch with router creates an aggregate
// ---------------------------------------------------------------------------

pub fn application_dispatch_with_router_test() {
  let store = make_store()
  let r = make_router(store)
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()

  let assert Ok(_result) = application.dispatch(app, CreateCounter("counter-1"))
  should.equal(True, True)
}

// ---------------------------------------------------------------------------
// Test 5: Dispatch with context propagates causation_id
// ---------------------------------------------------------------------------

pub fn application_dispatch_with_context_test() {
  let store = make_store()
  let r = make_router(store)
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()

  let assert Ok(_) =
    application.dispatch_with_context(
      app,
      CreateCounter("counter-2"),
      "cmd-id-1",
      Some("cause-1"),
      Some("correlation-1"),
      dict.new(),
    )

  // Read back the events to verify causation_id was propagated
  let assert Ok(events) = application.read_stream(app, "counter-2")
  let assert [ev] = events
  should.equal(ev.causation_id, Some("cause-1"))
  should.equal(ev.correlation_id, Some("correlation-1"))
}

// ---------------------------------------------------------------------------
// Test 6: read_stream returns events from the event store
// ---------------------------------------------------------------------------

pub fn application_read_stream_test() {
  let store = make_store()
  let r = make_router(store)
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()

  let assert Ok(_) = application.dispatch(app, CreateCounter("counter-3"))
  let assert Ok(_) = application.dispatch(app, Increment("counter-3"))

  let assert Ok(events) = application.read_stream(app, "counter-3")
  should.equal(2, list.length(events))
}

import gleam/list

// ---------------------------------------------------------------------------
// Test 7: start_event_handler wires handler to application's event store
// ---------------------------------------------------------------------------

pub fn application_start_event_handler_test() {
  let store = make_store()
  let r = make_router(store)
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()

  let event_subject: Subject(CounterEvent) = process.new_subject()

  let handler_config =
    event_handler.new(
      name: "app_test_handler",
      handle_event: fn(event, _recorded, _state) {
        process.send(event_subject, event)
        Ok(Nil)
      },
      initial_state: Nil,
    )

  let assert Ok(_handler) = application.start_event_handler(app, handler_config)
  process.sleep(20)

  // Dispatch creates events that the handler should receive
  let assert Ok(_) = application.dispatch(app, CreateCounter("counter-4"))
  process.sleep(50)

  let assert Ok(ev) = process.receive(event_subject, 100)
  should.equal(ev, CounterCreated("counter-4"))
}

// ---------------------------------------------------------------------------
// Test 8: start_projection wires projection to application's event store
// ---------------------------------------------------------------------------

pub fn application_start_projection_test() {
  let store = make_store()
  let r = make_router(store)
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()

  let proj_config =
    projection.new(
      name: "app_test_projection",
      initial_state: 0,
      handle_event: fn(event, _recorded, count: Int) {
        case event {
          CounterCreated(_) -> Ok(count + 1)
          _ -> Ok(count)
        }
      },
    )

  let assert Ok(proj) = application.start_projection(app, proj_config)
  process.sleep(20)

  let assert Ok(_) = application.dispatch(app, CreateCounter("counter-5"))
  process.sleep(50)

  let state = projection.get_state(proj, 500)
  should.equal(state, 1)
}

// ---------------------------------------------------------------------------
// Test 9: start_process_manager wires PM to application's event store
// ---------------------------------------------------------------------------

pub fn application_start_process_manager_test() {
  let store = make_store()
  let r = make_router(store)
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()

  let dispatched_subject: Subject(String) = process.new_subject()

  let pm_config =
    process_manager.new(
      name: "app_test_pm",
      interested: fn(event) {
        case event {
          CounterCreated(id) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          CounterCreated(id) -> {
            process.send(dispatched_subject, "created-" <> id)
            Ok([])
          }
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: Nil,
      dispatch_command: fn(_cmd, _c, _r) { Ok(Nil) },
    )

  let assert Ok(_pm) = application.start_process_manager(app, pm_config)
  process.sleep(20)

  let assert Ok(_) = application.dispatch(app, CreateCounter("counter-6"))
  process.sleep(50)

  let assert Ok(msg) = process.receive(dispatched_subject, 100)
  should.equal(msg, "created-counter-6")
}

// ---------------------------------------------------------------------------
// Test 10: read_stream_from supports pagination
// ---------------------------------------------------------------------------

pub fn application_read_stream_from_test() {
  let store = make_store()
  let r = make_router(store)
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()

  let assert Ok(_) = application.dispatch(app, CreateCounter("counter-7"))
  let assert Ok(_) = application.dispatch(app, Increment("counter-7"))

  // Read from version 2 (only the second event)
  let assert Ok(events) =
    application.read_stream_from(app, "counter-7", 2, 100)
  should.equal(list.length(events), 1)
  let assert [ev] = events
  should.equal(ev.data, CounterIncremented("counter-7"))
}
