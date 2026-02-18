//// End-to-end tests for the Causation & Correlation Chain (Module 14).
////
//// These tests verify that causation_id and correlation_id are correctly
//// propagated through the full command → events → PM → commands → events chain.
////
//// ## Commanded Invariant (Invariant 10):
////
//// - `causation_id` = UUID of the thing that CAUSED this event/command
////   - For events: the command_id that triggered them
////   - For PM-dispatched commands: the event_id that triggered the PM
////   - For PM-produced events: the PM command_id
////
//// - `correlation_id` = UUID that correlates the ENTIRE saga/chain
////   - Generated on first command dispatch
////   - Preserved unchanged throughout the chain
////
//// ## Chain example:
////
////   [Command] dispatch("CreateOrder")
////     → correlation_id: "corr-1", causation_id: "cmd-1"
////   [Events] OrderCreated
////     → correlation_id: "corr-1", causation_id: "cmd-1"
////   [PM] receives OrderCreated, dispatches "ChargeCard"
////     → causation_id: "event-id-of-OrderCreated", correlation_id: "corr-1"
////   [Events] CardCharged
////     → causation_id: "cmd-id-of-ChargeCard", correlation_id: "corr-1"

import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import instructed/aggregate
import instructed/application
import instructed/event.{type RecordedEvent}
import instructed/in_memory_event_store
import instructed/process_manager.{Skip, Start}
import instructed/router

// ---------------------------------------------------------------------------
// Test domain: Order processing saga
// ---------------------------------------------------------------------------

type OrderEvent {
  OrderCreated(order_id: String, amount: Int)
  PaymentProcessed(order_id: String, amount: Int)
}

type OrderCommand {
  CreateOrder(order_id: String, amount: Int)
  ProcessPayment(order_id: String, amount: Int)
}

type OrderState {
  OrderState(id: String, amount: Int, paid: Bool)
}

fn order_aggregate() {
  aggregate.new(
    empty_state: fn() { OrderState("", 0, False) },
    execute: fn(state, cmd) {
      case cmd {
        CreateOrder(id, amount) ->
          case state.id == "" {
            True -> Ok([OrderCreated(id, amount)])
            False -> Error("already exists")
          }
        ProcessPayment(id, amount) ->
          case state.id != "" {
            True -> Ok([PaymentProcessed(id, amount)])
            False -> Error("not created")
          }
      }
    },
    apply_event: fn(state, event) {
      case event {
        OrderCreated(id, amt) -> OrderState(id: id, amount: amt, paid: False)
        PaymentProcessed(_, _) -> OrderState(..state, paid: True)
      }
    },
  )
}

fn setup() {
  let assert Ok(sub) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(sub)
  let r =
    router.new(
      aggregate: order_aggregate(),
      event_store: store,
      identity: fn(cmd) {
        case cmd {
          CreateOrder(id, _) -> id
          ProcessPayment(id, _) -> id
        }
      },
    )
  let assert Ok(app) =
    application.new(store) |> application.with_router(r) |> application.start()
  #(app, r, store)
}

fn collect(subject: Subject(a), n: Int) -> List(a) {
  collect_loop(subject, n, [])
}

fn collect_loop(subject: Subject(a), remaining: Int, acc: List(a)) -> List(a) {
  case remaining {
    0 -> list.reverse(acc)
    _ ->
      case process.receive(subject, 200) {
        Ok(msg) -> collect_loop(subject, remaining - 1, [msg, ..acc])
        Error(Nil) -> list.reverse(acc)
      }
  }
}

// ---------------------------------------------------------------------------
// Test 1: Events carry causation_id from the command that produced them
// ---------------------------------------------------------------------------

pub fn events_carry_command_causation_id_test() {
  let #(app, _r, _store) = setup()

  // Dispatch with explicit causation/correlation
  let assert Ok(_) =
    application.dispatch_with_context(
      app,
      CreateOrder("order-1", 100),
      "cmd-uuid-1",
      Some("cause-0"),
      Some("corr-1"),
      dict.new(),
    )

  let assert Ok(events) = application.read_stream(app, "order-1")
  let assert [ev] = events

  // causation_id of the event = command_uuid
  should.equal(ev.causation_id, Some("cause-0"))
  // correlation_id is preserved from dispatch options
  should.equal(ev.correlation_id, Some("corr-1"))
}

// ---------------------------------------------------------------------------
// Test 2: router.dispatch auto-generates causation_id = command_id
// ---------------------------------------------------------------------------

pub fn auto_dispatch_sets_causation_id_test() {
  let #(app, _r, _store) = setup()

  let assert Ok(_) = application.dispatch(app, CreateOrder("order-2", 200))

  let assert Ok(events) = application.read_stream(app, "order-2")
  let assert [ev] = events

  // Both should be Some(uuid) — automatically generated
  should.not_equal(ev.causation_id, None)
  should.not_equal(ev.correlation_id, None)
}

// ---------------------------------------------------------------------------
// Test 3: Multiple commands preserve the correlation_id chain
// ---------------------------------------------------------------------------

pub fn correlation_id_preserved_across_commands_test() {
  let #(app, _r, _store) = setup()
  let correlation_id = "corr-global-chain"

  let assert Ok(_) =
    application.dispatch_with_context(
      app,
      CreateOrder("order-3", 300),
      "cmd-1",
      None,
      Some(correlation_id),
      dict.new(),
    )

  let assert Ok(_) =
    application.dispatch_with_context(
      app,
      ProcessPayment("order-3", 300),
      "cmd-2",
      None,
      Some(correlation_id),
      dict.new(),
    )

  let assert Ok(events) = application.read_stream(app, "order-3")
  should.equal(list.length(events), 2)

  // All events in the chain share the same correlation_id
  list.each(events, fn(ev) {
    should.equal(ev.correlation_id, Some(correlation_id))
  })
}

// ---------------------------------------------------------------------------
// Test 4: PM dispatches commands with causation_id = source event_id
// ---------------------------------------------------------------------------

pub fn pm_sets_causation_id_from_event_id_test() {
  let #(app, r, _store) = setup()

  // Capture what causation_id the PM sends when dispatching
  let pm_causation_subject: Subject(option.Option(String)) =
    process.new_subject()

  // The PM's dispatch_command should receive causation_id = event_id
  let pm_dispatch = fn(
    cmd: OrderCommand,
    causation: option.Option(String),
    correlation: option.Option(String),
  ) {
    process.send(pm_causation_subject, causation)
    // Forward to actual router with the causation/correlation chain
    router.dispatch_with_context(
      r,
      cmd,
      "pm-cmd-uuid",
      causation,
      correlation,
      dict.new(),
    )
    |> result_to_nil
  }

  let pm_config =
    process_manager.new(
      name: "chain_pm_4",
      interested: fn(event) {
        case event {
          OrderCreated(id, _) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderCreated(id, amount) -> Ok([ProcessPayment(id, amount)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: Nil,
      dispatch_command: pm_dispatch,
    )

  let assert Ok(_pm) = application.start_process_manager(app, pm_config)
  process.sleep(20)

  // Dispatch the initial command
  let assert Ok(_) =
    application.dispatch_with_context(
      app,
      CreateOrder("order-4", 400),
      "cmd-initial-4",
      None,
      Some("corr-chain-4"),
      dict.new(),
    )

  process.sleep(100)

  // The PM should have dispatched with causation_id = event_id of OrderCreated
  let causations = collect(pm_causation_subject, 3)
  let assert [pm_causation] = causations
  should.not_equal(pm_causation, None)

  // The causation_id should be the event_id of the OrderCreated event
  let assert Ok(events) = application.read_stream(app, "order-4")
  let order_created_events =
    list.filter(events, fn(ev) {
      case ev.data {
        OrderCreated(_, _) -> True
        _ -> False
      }
    })
  let assert [created_ev] = order_created_events
  // PM's causation_id should equal the OrderCreated event_id
  should.equal(pm_causation, Some(created_ev.event_id))
}

// ---------------------------------------------------------------------------
// Test 5: Full chain — correlation_id preserved through PM dispatch
// ---------------------------------------------------------------------------

pub fn full_chain_correlation_id_preserved_test() {
  let #(app, r, _store) = setup()

  let pm_correlation_subject: Subject(option.Option(String)) =
    process.new_subject()

  let pm_dispatch = fn(
    cmd: OrderCommand,
    causation: option.Option(String),
    correlation: option.Option(String),
  ) {
    process.send(pm_correlation_subject, correlation)
    router.dispatch_with_context(r, cmd, "pm-cmd-5", causation, correlation, dict.new())
    |> result_to_nil
  }

  let pm_config =
    process_manager.new(
      name: "chain_pm_5",
      interested: fn(event) {
        case event {
          OrderCreated(id, _) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderCreated(id, amount) -> Ok([ProcessPayment(id, amount)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: Nil,
      dispatch_command: pm_dispatch,
    )

  let assert Ok(_pm) = application.start_process_manager(app, pm_config)
  process.sleep(20)

  let original_correlation_id = "corr-preserved-5"

  let assert Ok(_) =
    application.dispatch_with_context(
      app,
      CreateOrder("order-5", 500),
      "cmd-5",
      None,
      Some(original_correlation_id),
      dict.new(),
    )

  process.sleep(100)

  // PM should receive the original correlation_id
  let correlations = collect(pm_correlation_subject, 3)
  let assert [pm_correlation] = correlations
  should.equal(pm_correlation, Some(original_correlation_id))

  // The PaymentProcessed event should also carry the original correlation_id
  let assert Ok(events) = application.read_stream(app, "order-5")
  let payment_events =
    list.filter(events, fn(ev) {
      case ev.data {
        PaymentProcessed(_, _) -> True
        _ -> False
      }
    })
  let assert [payment_ev] = payment_events
  should.equal(payment_ev.correlation_id, Some(original_correlation_id))
}

// ---------------------------------------------------------------------------
// Test 6: event_id field uniquely identifies each event
// ---------------------------------------------------------------------------

pub fn each_event_has_unique_event_id_test() {
  let #(app, _r, _store) = setup()

  let assert Ok(_) = application.dispatch(app, CreateOrder("order-6", 600))
  let assert Ok(_) =
    application.dispatch(app, ProcessPayment("order-6", 600))

  let assert Ok(events) = application.read_stream(app, "order-6")
  should.equal(list.length(events), 2)

  let event_ids = list.map(events, fn(ev: RecordedEvent(OrderEvent)) { ev.event_id })
  let assert [id1, id2] = event_ids

  // Each event has a unique ID
  should.not_equal(id1, id2)
  should.not_equal(id1, "")
  should.not_equal(id2, "")
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn result_to_nil(r: Result(a, b)) -> Result(Nil, String) {
  case r {
    Ok(_) -> Ok(Nil)
    Error(_) -> Error("dispatch failed")
  }
}
