import gleeunit/should
import instructed/aggregate
import instructed/in_memory_event_store
import instructed/middleware
import instructed/router

// --- Test domain ---

type Counter {
  Counter(id: String, count: Int)
}

type CounterCommand {
  CreateCounter(id: String)
  IncrementCounter(id: String)
  DecrementCounter(id: String)
  AddToCounter(id: String, amount: Int)
}

type CounterEvent {
  CounterCreated(id: String)
  CounterIncremented(id: String)
  CounterDecremented(id: String)
  CounterAdded(id: String, amount: Int)
}

fn counter_aggregate() -> aggregate.Aggregate(Counter, CounterCommand, CounterEvent) {
  aggregate.new(
    empty_state: fn() { Counter("", 0) },
    execute: fn(state, cmd) {
      case cmd {
        CreateCounter(id) ->
          case state.id == "" {
            True -> Ok([CounterCreated(id)])
            False -> Error("Counter already exists")
          }
        IncrementCounter(id) ->
          case state.id != "" {
            True -> Ok([CounterIncremented(id)])
            False -> Error("Counter not created")
          }
        DecrementCounter(id) ->
          case state.id != "" && state.count > 0 {
            True -> Ok([CounterDecremented(id)])
            False -> Error("Cannot decrement")
          }
        AddToCounter(id, amount) ->
          case state.id != "" && amount > 0 {
            True -> Ok([CounterAdded(id, amount)])
            False -> Error("Invalid add")
          }
      }
    },
    apply_event: fn(state, event) {
      case event {
        CounterCreated(id) -> Counter(id, 0)
        CounterIncremented(_) -> Counter(..state, count: state.count + 1)
        CounterDecremented(_) -> Counter(..state, count: state.count - 1)
        CounterAdded(_, amount) -> Counter(..state, count: state.count + amount)
      }
    },
  )
}

fn make_router() {
  let assert Ok(subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(subject)

  router.new(
    aggregate: counter_aggregate(),
    event_store: store,
    identity: fn(cmd) {
      case cmd {
        CreateCounter(id) -> id
        IncrementCounter(id) -> id
        DecrementCounter(id) -> id
        AddToCounter(id, _) -> id
      }
    },
  )
}

// --- Tests ---

pub fn dispatch_create_test() {
  let r = make_router()
  let result = router.dispatch(r, CreateCounter("c1"))
  should.be_ok(result)

  let assert Ok(dispatch_result) = result
  should.equal(dispatch_result.aggregate_version, 1)
  should.equal(dispatch_result.events, [CounterCreated("c1")])
  should.equal(dispatch_result.aggregate_state, Counter("c1", 0))
}

pub fn dispatch_sequence_test() {
  let r = make_router()
  let assert Ok(_) = router.dispatch(r, CreateCounter("c2"))
  let assert Ok(result) = router.dispatch(r, IncrementCounter("c2"))
  should.equal(result.aggregate_state.count, 1)
  should.equal(result.aggregate_version, 2)

  let assert Ok(result2) = router.dispatch(r, IncrementCounter("c2"))
  should.equal(result2.aggregate_state.count, 2)
  should.equal(result2.aggregate_version, 3)
}

pub fn dispatch_error_test() {
  let r = make_router()
  // Increment without creating should fail
  let result = router.dispatch(r, IncrementCounter("c3"))
  should.be_error(result)
}

pub fn dispatch_with_prefix_test() {
  let assert Ok(subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(subject)

  let r =
    router.new(
      aggregate: counter_aggregate(),
      event_store: store,
      identity: fn(cmd) {
        case cmd {
          CreateCounter(id) -> id
          IncrementCounter(id) -> id
          DecrementCounter(id) -> id
          AddToCounter(id, _) -> id
        }
      },
    )
    |> router.with_prefix("counter-")

  let assert Ok(_) = router.dispatch(r, CreateCounter("c4"))

  // Verify events are stored with prefix
  let assert Ok(events) = store.read_stream_forward("counter-c4", 1, 1000)
  should.equal(events |> list.length, 1)
}

pub fn dispatch_multiple_aggregates_test() {
  let r = make_router()
  let assert Ok(_) = router.dispatch(r, CreateCounter("a"))
  let assert Ok(_) = router.dispatch(r, CreateCounter("b"))
  let assert Ok(_) = router.dispatch(r, IncrementCounter("a"))

  let assert Ok(result_a) = router.dispatch(r, IncrementCounter("a"))
  should.equal(result_a.aggregate_state.count, 2)

  let assert Ok(result_b) = router.dispatch(r, IncrementCounter("b"))
  should.equal(result_b.aggregate_state.count, 1)
}

pub fn dispatch_domain_error_test() {
  let r = make_router()
  let assert Ok(_) = router.dispatch(r, CreateCounter("c5"))

  // Can't decrement from 0
  let result = router.dispatch(r, DecrementCounter("c5"))
  should.be_error(result)
}

pub fn middleware_test() {
  let assert Ok(subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(subject)

  // Create a halting middleware for specific commands
  let halt_mw =
    middleware.new(
      before_dispatch: fn(pipeline) {
        case pipeline.command {
          DecrementCounter(_) -> middleware.halt(pipeline)
          _ -> pipeline
        }
      },
      after_dispatch: fn(pipeline) { pipeline },
      after_failure: fn(pipeline) { pipeline },
    )

  let r =
    router.new(
      aggregate: counter_aggregate(),
      event_store: store,
      identity: fn(cmd) {
        case cmd {
          CreateCounter(id) -> id
          IncrementCounter(id) -> id
          DecrementCounter(id) -> id
          AddToCounter(id, _) -> id
        }
      },
    )
    |> router.with_middleware(halt_mw)

  let assert Ok(_) = router.dispatch(r, CreateCounter("c6"))

  // Decrement should be halted by middleware
  let result = router.dispatch(r, DecrementCounter("c6"))
  should.be_error(result)
}

import gleam/list
