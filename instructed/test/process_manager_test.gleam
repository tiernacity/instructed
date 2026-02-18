import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/option.{None}
import gleeunit/should
import instructed/error
import instructed/event.{EventData}
import instructed/event_store.{AnyVersion}
import instructed/in_memory_event_store
import instructed/process_manager.{
  AfterContinue, AfterStop, CmdSkip, CmdStop, Continue, ContinueMany,
  ContinueStrict, Skip, Start, StartMany, StartStrict, Stop, StopMany,
}

// ---------------------------------------------------------------------------
// Test domain
// ---------------------------------------------------------------------------

type OrderEvent {
  OrderPlaced(order_id: String, amount: Int)
  OrderApproved(order_id: String)
  OrderCancelled(order_id: String)
  PaymentRequested(order_id: String, amount: Int)
}

type FulfillmentCommand {
  ChargeCard(order_id: String, amount: Int)
  ShipOrder(order_id: String)
  RefundCard(order_id: String)
}

type OrderPMState {
  OrderPMState(order_id: String, amount: Int, approved: Bool)
}

fn initial_state() -> OrderPMState {
  OrderPMState(order_id: "", amount: 0, approved: False)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup_store() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  in_memory_event_store.to_event_store(store_subject)
}

fn make_event_data(event: OrderEvent) -> event.EventData(OrderEvent) {
  EventData(
    data: event,
    event_type: "test",
    causation_id: None,
    correlation_id: None,
    metadata: dict.new(),
  )
}

fn append_event(store: event_store.EventStore(OrderEvent), ev: OrderEvent) {
  let assert Ok(_) =
    store.append_to_stream("$orders", AnyVersion, [make_event_data(ev)])
  Nil
}

/// Collect up to `n` messages from a subject with a per-message timeout.
fn collect_messages(
  subject: Subject(a),
  count: Int,
  timeout_ms: Int,
) -> List(a) {
  collect_loop(subject, count, timeout_ms, [])
}

fn collect_loop(
  subject: Subject(a),
  remaining: Int,
  timeout_ms: Int,
  acc: List(a),
) -> List(a) {
  case remaining {
    0 -> list.reverse(acc)
    _ ->
      case process.receive(subject, timeout_ms) {
        Ok(msg) -> collect_loop(subject, remaining - 1, timeout_ms, [msg, ..acc])
        Error(Nil) -> list.reverse(acc)
      }
  }
}

import gleam/list

fn make_capturing_dispatcher(subject: Subject(FulfillmentCommand)) {
  fn(cmd: FulfillmentCommand, _causation: option.Option(String), _correlation: option.Option(String)) {
    process.send(subject, cmd)
    Ok(Nil)
  }
}

// ---------------------------------------------------------------------------
// Test 1: Happy path — event handled, commands dispatched
// ---------------------------------------------------------------------------

pub fn pm_handles_event_and_dispatches_commands_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()

  let config =
    process_manager.new(
      name: "order_pm_1",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          OrderApproved(id) -> Continue(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderPlaced(id, amount) ->
            Ok([ChargeCard(order_id: id, amount: amount)])
          OrderApproved(id) -> Ok([ShipOrder(order_id: id)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, event) {
        case event {
          OrderPlaced(id, amt) ->
            OrderPMState(..state, order_id: id, amount: amt)
          OrderApproved(_) -> OrderPMState(..state, approved: True)
          _ -> state
        }
      },
      initial_state: initial_state(),
      dispatch_command: make_capturing_dispatcher(cmd_subject),
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-1", 100))
  process.sleep(50)

  let cmds = collect_messages(cmd_subject, 5, 100)
  should.equal(cmds, [ChargeCard(order_id: "order-1", amount: 100)])
}

// ---------------------------------------------------------------------------
// Test 2: Skip — uninteresting events produce no commands
// ---------------------------------------------------------------------------

pub fn pm_skips_uninteresting_events_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()

  let config =
    process_manager.new(
      name: "skip_pm_2",
      interested: fn(_event) { Skip },
      handle: fn(_state, _event, _recorded) { Ok([]) },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: make_capturing_dispatcher(cmd_subject),
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-2", 100))
  process.sleep(50)

  let cmds = collect_messages(cmd_subject, 5, 50)
  should.equal(cmds, [])
}

// ---------------------------------------------------------------------------
// Test 3: Stop — commands dispatched then instance removed
// ---------------------------------------------------------------------------

pub fn pm_stop_dispatches_and_removes_instance_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()

  let config =
    process_manager.new(
      name: "stop_pm_3",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          OrderCancelled(id) -> Stop(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderCancelled(id) -> Ok([RefundCard(order_id: id)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: make_capturing_dispatcher(cmd_subject),
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-3", 100))
  process.sleep(30)
  append_event(store, OrderCancelled("order-3"))
  process.sleep(50)

  let cmds = collect_messages(cmd_subject, 5, 100)
  should.equal(cmds, [RefundCard(order_id: "order-3")])
}

// ---------------------------------------------------------------------------
// Test 4: StartMany fan-out — one event routes to multiple instances
// ---------------------------------------------------------------------------

pub fn pm_start_many_fan_out_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()

  let config =
    process_manager.new(
      name: "fanout_pm_4",
      interested: fn(event) {
        case event {
          PaymentRequested(order_id, _) ->
            StartMany(["buyer-" <> order_id, "seller-" <> order_id])
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          PaymentRequested(id, amount) ->
            Ok([ChargeCard(order_id: id, amount: amount)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: make_capturing_dispatcher(cmd_subject),
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, PaymentRequested("order-4", 200))
  process.sleep(50)

  // Two instances each dispatch ChargeCard
  let cmds = collect_messages(cmd_subject, 5, 100)
  should.equal(list.length(cmds), 2)
  list.each(cmds, fn(cmd) {
    should.equal(cmd, ChargeCard(order_id: "order-4", amount: 200))
  })
}

// ---------------------------------------------------------------------------
// Test 5: StopMany fan-out
// ---------------------------------------------------------------------------

pub fn pm_stop_many_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()

  let config =
    process_manager.new(
      name: "stopmany_pm_5",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> StartMany(["a-" <> id, "b-" <> id])
          OrderCancelled(id) -> StopMany(["a-" <> id, "b-" <> id])
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderCancelled(id) -> Ok([RefundCard(order_id: id)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: make_capturing_dispatcher(cmd_subject),
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-5", 50))
  process.sleep(30)
  append_event(store, OrderCancelled("order-5"))
  process.sleep(50)

  let cmds = collect_messages(cmd_subject, 5, 100)
  should.equal(list.length(cmds), 2)
  list.each(cmds, fn(cmd) {
    should.equal(cmd, RefundCard(order_id: "order-5"))
  })
}

// ---------------------------------------------------------------------------
// Test 6: StartStrict — error when instance already exists
// ---------------------------------------------------------------------------

pub fn pm_start_strict_error_when_exists_test() {
  let store = setup_store()
  let err_subject: Subject(String) = process.new_subject()

  let config =
    process_manager.new(
      name: "strictstart_pm_6",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          OrderApproved(id) -> StartStrict(id)
          _ -> Skip
        }
      },
      handle: fn(_state, _event, _recorded) { Ok([]) },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: fn(_cmd, _c, _r) { Ok(Nil) },
    )
    |> process_manager.with_event_error_handler(fn(reason, _event, _state) {
      process.send(err_subject, reason)
      error.Skip
    })

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-6", 100))
  process.sleep(30)
  // StartStrict for already-existing instance
  append_event(store, OrderApproved("order-6"))
  process.sleep(50)

  let errors = collect_messages(err_subject, 3, 100)
  should.equal(errors, ["start!: process already started"])
}

// ---------------------------------------------------------------------------
// Test 7: ContinueStrict — error when instance does not exist
// ---------------------------------------------------------------------------

pub fn pm_continue_strict_error_when_missing_test() {
  let store = setup_store()
  let err_subject: Subject(String) = process.new_subject()

  let config =
    process_manager.new(
      name: "strictcontinue_pm_7",
      interested: fn(event) {
        case event {
          OrderApproved(id) -> ContinueStrict(id)
          _ -> Skip
        }
      },
      handle: fn(_state, _event, _recorded) { Ok([]) },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: fn(_cmd, _c, _r) { Ok(Nil) },
    )
    |> process_manager.with_event_error_handler(fn(reason, _event, _state) {
      process.send(err_subject, reason)
      error.Skip
    })

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderApproved("nonexistent-7"))
  process.sleep(50)

  let errors = collect_messages(err_subject, 3, 100)
  should.equal(errors, ["continue!: process not started"])
}

// ---------------------------------------------------------------------------
// Test 8: after_command — AfterStop terminates instance
// ---------------------------------------------------------------------------

pub fn pm_after_command_stops_instance_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()
  let stop_subject: Subject(String) = process.new_subject()

  let config =
    process_manager.new(
      name: "aftercmd_pm_8",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderPlaced(id, amount) ->
            Ok([ChargeCard(order_id: id, amount: amount)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: make_capturing_dispatcher(cmd_subject),
    )
    |> process_manager.with_after_command(fn(cmd, _state) {
      case cmd {
        ChargeCard(id, _) -> {
          process.send(stop_subject, "stopped-" <> id)
          AfterStop
        }
        _ -> AfterContinue
      }
    })

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-8", 150))
  process.sleep(50)

  let cmds = collect_messages(cmd_subject, 3, 100)
  should.equal(cmds, [ChargeCard(order_id: "order-8", amount: 150)])

  let stops = collect_messages(stop_subject, 3, 50)
  should.equal(stops, ["stopped-order-8"])
}

// ---------------------------------------------------------------------------
// Test 9: Event handling error — on_event_error callback invoked
// ---------------------------------------------------------------------------

pub fn pm_event_error_callback_test() {
  let store = setup_store()
  let err_subject: Subject(String) = process.new_subject()

  let config =
    process_manager.new(
      name: "eventerr_pm_9",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderPlaced(_, amount) if amount < 0 ->
            Error("negative amount")
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: fn(_cmd, _c, _r) { Ok(Nil) },
    )
    |> process_manager.with_event_error_handler(fn(reason, _event, _state) {
      process.send(err_subject, reason)
      error.Skip
    })

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-9", -50))
  process.sleep(50)

  let errors = collect_messages(err_subject, 3, 100)
  should.equal(errors, ["negative amount"])
}

// ---------------------------------------------------------------------------
// Test 10: Command dispatch error — CmdSkip continues with pending commands
// ---------------------------------------------------------------------------

pub fn pm_command_error_skip_continues_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()
  let err_subject: Subject(String) = process.new_subject()

  let dispatch = fn(cmd: FulfillmentCommand, _c, _r) {
    case cmd {
      ChargeCard(_, _) -> Error("payment gateway down")
      _ -> {
        process.send(cmd_subject, cmd)
        Ok(Nil)
      }
    }
  }

  let config =
    process_manager.new(
      name: "cmderr_pm_10",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderPlaced(id, amount) ->
            Ok([
              ChargeCard(order_id: id, amount: amount),
              ShipOrder(order_id: id),
            ])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: dispatch,
    )
    |> process_manager.with_command_error_handler(fn(
      reason,
      _failed,
      _pending,
      _event,
      _state,
    ) {
      process.send(err_subject, reason)
      CmdSkip
    })

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-10", 75))
  process.sleep(50)

  // ChargeCard skipped, ShipOrder should succeed
  let cmds = collect_messages(cmd_subject, 3, 100)
  should.equal(cmds, [ShipOrder(order_id: "order-10")])

  let errors = collect_messages(err_subject, 3, 50)
  should.equal(errors, ["payment gateway down"])
}

// ---------------------------------------------------------------------------
// Test 11: Causation ID propagated (Invariant 10)
// ---------------------------------------------------------------------------

pub fn pm_causation_id_propagated_test() {
  let store = setup_store()
  let causation_subject: Subject(option.Option(String)) = process.new_subject()

  let dispatch = fn(_cmd, causation: option.Option(String), _correlation) {
    process.send(causation_subject, causation)
    Ok(Nil)
  }

  let config =
    process_manager.new(
      name: "causation_pm_11",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderPlaced(id, amount) ->
            Ok([ChargeCard(order_id: id, amount: amount)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: dispatch,
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-11", 90))
  process.sleep(50)

  let causations = collect_messages(causation_subject, 3, 100)
  // causation_id should be Some(event_id), not None
  let assert [causation] = causations
  should.not_equal(causation, None)
}

// ---------------------------------------------------------------------------
// Test 12: ContinueMany fan-out
// ---------------------------------------------------------------------------

pub fn pm_continue_many_fan_out_test() {
  let store = setup_store()
  let cmd_subject: Subject(FulfillmentCommand) = process.new_subject()

  let config =
    process_manager.new(
      name: "continuemany_pm_12",
      interested: fn(event) {
        case event {
          OrderApproved(id) ->
            ContinueMany(["audit-" <> id, "fulfill-" <> id])
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderApproved(id) -> Ok([ShipOrder(order_id: id)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: make_capturing_dispatcher(cmd_subject),
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderApproved("order-12"))
  process.sleep(50)

  let cmds = collect_messages(cmd_subject, 5, 100)
  should.equal(list.length(cmds), 2)
  list.each(cmds, fn(cmd) {
    should.equal(cmd, ShipOrder(order_id: "order-12"))
  })
}

// ---------------------------------------------------------------------------
// Test 13: Command dispatch CmdStop stops instance processing
// ---------------------------------------------------------------------------

pub fn pm_command_error_stop_test() {
  let store = setup_store()
  let err_subject: Subject(String) = process.new_subject()

  let config =
    process_manager.new(
      name: "cmdstop_pm_13",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          _ -> Skip
        }
      },
      handle: fn(_state, event, _recorded) {
        case event {
          OrderPlaced(id, amount) ->
            Ok([ChargeCard(order_id: id, amount: amount)])
          _ -> Ok([])
        }
      },
      apply_event: fn(state, _event) { state },
      initial_state: initial_state(),
      dispatch_command: fn(_cmd, _c, _r) { Error("terminal failure") },
    )
    |> process_manager.with_command_error_handler(fn(
      reason,
      _failed,
      _pending,
      _event,
      _state,
    ) {
      process.send(err_subject, reason)
      CmdStop("stopping")
    })

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-13", 300))
  process.sleep(50)

  let errors = collect_messages(err_subject, 3, 100)
  should.equal(errors, ["terminal failure"])
}

// ---------------------------------------------------------------------------
// Test 14: apply_event mutates state correctly
// ---------------------------------------------------------------------------

pub fn pm_apply_event_updates_state_test() {
  let store = setup_store()
  // We'll verify state update via the handle function which receives updated state
  let state_subject: Subject(Int) = process.new_subject()

  let config =
    process_manager.new(
      name: "state_pm_14",
      interested: fn(event) {
        case event {
          OrderPlaced(id, _) -> Start(id)
          OrderApproved(id) -> Continue(id)
          _ -> Skip
        }
      },
      handle: fn(state: OrderPMState, event, _recorded) {
        case event {
          OrderApproved(_) -> {
            // At this point state should reflect applied PlacedEvent
            process.send(state_subject, state.amount)
            Ok([])
          }
          _ -> Ok([])
        }
      },
      apply_event: fn(state, event) {
        case event {
          OrderPlaced(id, amount) ->
            OrderPMState(order_id: id, amount: amount, approved: False)
          OrderApproved(_) -> OrderPMState(..state, approved: True)
          _ -> state
        }
      },
      initial_state: initial_state(),
      dispatch_command: fn(_cmd, _c, _r) { Ok(Nil) },
    )

  let assert Ok(_pm) = process_manager.start(config, store)
  process.sleep(20)

  append_event(store, OrderPlaced("order-14", 250))
  process.sleep(30)
  append_event(store, OrderApproved("order-14"))
  process.sleep(50)

  let amounts = collect_messages(state_subject, 3, 100)
  // State from OrderPlaced should be visible when handling OrderApproved
  should.equal(amounts, [250])
}
