import gleam/dict
import gleam/erlang/process
import gleam/option.{None, Some}
import gleeunit/should
import instructed/aggregate
import instructed/application
import instructed/event
import instructed/event_handler
import instructed/event_store
import instructed/in_memory_event_store
import instructed/middleware
import instructed/router
import instructed/subscriptions

// ---------------------------------------------------------------------------
// Test domain
// ---------------------------------------------------------------------------

type OrderEvent {
  OrderPlaced(id: String)
  OrderShipped(id: String)
}

type OrderCommand {
  PlaceOrder(id: String)
  ShipOrder(id: String)
}

type OrderState {
  OrderState(id: String, placed: Bool, shipped: Bool)
}

fn order_aggregate() {
  aggregate.new(
    empty_state: fn() { OrderState("", False, False) },
    execute: fn(state, cmd) {
      case cmd {
        PlaceOrder(id) ->
          case state.placed {
            False -> Ok([OrderPlaced(id)])
            True -> Error("already placed")
          }
        ShipOrder(id) ->
          case state.placed && !state.shipped {
            True -> Ok([OrderShipped(id)])
            False -> Error("cannot ship")
          }
      }
    },
    apply_event: fn(state, ev) {
      case ev {
        OrderPlaced(id) -> OrderState(id, True, False)
        OrderShipped(_) -> OrderState(..state, shipped: True)
      }
    },
  )
}

fn make_store() {
  let assert Ok(subject) = in_memory_event_store.start()
  in_memory_event_store.to_event_store(subject)
}

fn make_router(store) {
  router.new(
    aggregate: order_aggregate(),
    event_store: store,
    identity: fn(cmd) {
      case cmd {
        PlaceOrder(id) -> id
        ShipOrder(id) -> id
      }
    },
  )
}

// ---------------------------------------------------------------------------
// subscriptions.start/0
// ---------------------------------------------------------------------------

pub fn subscriptions_start_test() {
  subscriptions.start()
  |> should.be_ok
}

// ---------------------------------------------------------------------------
// wait_for with no strong handlers → immediate Ok (Invariant 11 edge case)
// ---------------------------------------------------------------------------

pub fn wait_for_no_handlers_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.wait_for(subs, "stream-1", 1, 200)
  |> should.be_ok
}

pub fn wait_for_eventual_only_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "eventual", middleware.Eventual)
  process.sleep(5)
  subscriptions.wait_for(subs, "stream-1", 1, 200)
  |> should.be_ok
}

// ---------------------------------------------------------------------------
// wait_for with strong handler: ack before wait → immediate Ok
// ---------------------------------------------------------------------------

pub fn wait_for_acked_before_wait_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "h1", middleware.Strong)
  subscriptions.ack_event(subs, "h1", "stream-1", 5)
  process.sleep(10)
  subscriptions.wait_for(subs, "stream-1", 5, 500)
  |> should.be_ok
}

pub fn wait_for_higher_version_satisfies_lower_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "h1", middleware.Strong)
  // Ack v10 before waiting for v5 — should satisfy immediately
  subscriptions.ack_event(subs, "h1", "stream-1", 10)
  process.sleep(10)
  subscriptions.wait_for(subs, "stream-1", 5, 500)
  |> should.be_ok
}

// ---------------------------------------------------------------------------
// wait_for with strong handler: ack arrives after wait → unblocks
// ---------------------------------------------------------------------------

pub fn wait_for_acked_async_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "h1", middleware.Strong)

  let subs_copy = subs
  let _ =
    process.spawn(fn() {
      process.sleep(30)
      subscriptions.ack_event(subs_copy, "h1", "stream-1", 3)
    })

  subscriptions.wait_for(subs, "stream-1", 3, 1000)
  |> should.be_ok
}

// ---------------------------------------------------------------------------
// wait_for times out when handler never acks
// ---------------------------------------------------------------------------

pub fn wait_for_timeout_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "h1", middleware.Strong)
  // No ack → timeout after 50ms
  subscriptions.wait_for(subs, "stream-1", 1, 50)
  |> should.be_error
}

// ---------------------------------------------------------------------------
// Multiple strong handlers — all must ack
// ---------------------------------------------------------------------------

pub fn wait_for_partial_ack_times_out_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "ha", middleware.Strong)
  subscriptions.register(subs, "hb", middleware.Strong)

  // Only ha acks → hb still pending → timeout
  subscriptions.ack_event(subs, "ha", "stream-1", 1)
  process.sleep(10)

  subscriptions.wait_for(subs, "stream-1", 1, 50)
  |> should.be_error
}

pub fn wait_for_all_handlers_acked_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "ha", middleware.Strong)
  subscriptions.register(subs, "hb", middleware.Strong)

  subscriptions.ack_event(subs, "ha", "stream-1", 2)
  subscriptions.ack_event(subs, "hb", "stream-1", 2)
  process.sleep(10)

  subscriptions.wait_for(subs, "stream-1", 2, 500)
  |> should.be_ok
}

// ---------------------------------------------------------------------------
// Stream isolation: ack on stream-A ≠ stream-B
// ---------------------------------------------------------------------------

pub fn stream_isolation_test() {
  let assert Ok(subs) = subscriptions.start()
  subscriptions.register(subs, "h1", middleware.Strong)

  subscriptions.ack_event(subs, "h1", "stream-A", 5)
  process.sleep(10)

  // Waiting on stream-B — different stream, not satisfied
  subscriptions.wait_for(subs, "stream-B", 5, 50)
  |> should.be_error
}

// ---------------------------------------------------------------------------
// Integration: event_handler with Strong consistency sends ack
// ---------------------------------------------------------------------------

pub fn event_handler_acks_subscriptions_test() {
  let store = make_store()
  let assert Ok(subs) = subscriptions.start()

  let processed = process.new_subject()

  let handler_config =
    event_handler.new(
      name: "strong_handler",
      handle_event: fn(ev, _recorded, _state) {
        case ev {
          OrderPlaced(id) -> {
            process.send(processed, id)
            Ok(Nil)
          }
          _ -> Ok(Nil)
        }
      },
      initial_state: Nil,
    )
    |> event_handler.with_consistency(middleware.Strong)
    |> event_handler.with_subscriptions(subs)

  let assert Ok(_) = event_handler.start(handler_config, store)

  // Append an event directly to the store
  let _ =
    store.append_to_stream("order-42", event_store.AnyVersion, [
      event.EventData(
        data: OrderPlaced("ORD-42"),
        event_type: "OrderPlaced",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  // Wait for handler to process the event
  let assert Ok(id) = process.receive(processed, 1000)
  id |> should.equal("ORD-42")

  // Give subscriptions actor time to receive the ack
  process.sleep(30)

  // Now the dispatcher can unblock: handler acked stream_version=1
  subscriptions.wait_for(subs, "order-42", 1, 500)
  |> should.be_ok
}

// ---------------------------------------------------------------------------
// Integration: eventual handler does NOT ack subscriptions
// ---------------------------------------------------------------------------

pub fn eventual_handler_does_not_ack_test() {
  let store = make_store()
  let assert Ok(subs) = subscriptions.start()

  let processed = process.new_subject()

  let handler_config =
    event_handler.new(
      name: "eventual_handler",
      handle_event: fn(ev, _recorded, _state) {
        case ev {
          OrderPlaced(id) -> {
            process.send(processed, id)
            Ok(Nil)
          }
          _ -> Ok(Nil)
        }
      },
      initial_state: Nil,
    )
    |> event_handler.with_consistency(middleware.Eventual)
    |> event_handler.with_subscriptions(subs)

  let assert Ok(_) = event_handler.start(handler_config, store)

  // Also register a strong handler (to ensure wait_for doesn't trivially succeed)
  subscriptions.register(subs, "blocker", middleware.Strong)

  let _ =
    store.append_to_stream("order-99", event_store.AnyVersion, [
      event.EventData(
        data: OrderPlaced("ORD-99"),
        event_type: "OrderPlaced",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  // Wait for eventual handler to process
  let assert Ok(_) = process.receive(processed, 1000)

  // The "blocker" strong handler never acked → still times out
  subscriptions.wait_for(subs, "order-99", 1, 50)
  |> should.be_error
}

// ---------------------------------------------------------------------------
// Integration: router.with_subscriptions wires the actor
// ---------------------------------------------------------------------------

pub fn router_with_subscriptions_test() {
  let store = make_store()
  let assert Ok(subs) = subscriptions.start()

  let r =
    make_router(store)
    |> router.with_subscriptions(subs)

  // Dispatch with Eventual (default) — no blocking regardless of strong handlers
  subscriptions.register(subs, "irrelevant", middleware.Strong)

  let assert Ok(_) = router.dispatch(r, PlaceOrder("ORD-R1"))
  Nil
}

// ---------------------------------------------------------------------------
// Integration: application.with_subscriptions threads to handlers
// ---------------------------------------------------------------------------

pub fn application_subscriptions_field_test() {
  let store = make_store()
  let assert Ok(subs) = subscriptions.start()

  let assert Ok(app) =
    application.new(store)
    |> application.with_subscriptions(subs)
    |> application.start()

  application.get_subscriptions(app)
  |> should.equal(Some(subs))
}

pub fn application_no_subscriptions_test() {
  let store = make_store()
  let assert Ok(app) =
    application.new(store)
    |> application.start()

  application.get_subscriptions(app)
  |> should.equal(None)
}

pub fn application_auto_injects_subscriptions_into_handler_test() {
  let store = make_store()
  let assert Ok(subs) = subscriptions.start()

  let assert Ok(app) =
    application.new(store)
    |> application.with_subscriptions(subs)
    |> application.start()

  let processed = process.new_subject()

  // Handler config has no explicit subscriptions set — app injects it
  let handler_config =
    event_handler.new(
      name: "auto_injected_handler",
      handle_event: fn(ev, _recorded, _state) {
        case ev {
          OrderPlaced(id) -> {
            process.send(processed, id)
            Ok(Nil)
          }
          _ -> Ok(Nil)
        }
      },
      initial_state: Nil,
    )
    |> event_handler.with_consistency(middleware.Strong)

  // subscriptions is None on the config — app.start_event_handler injects it
  let assert Ok(_) = application.start_event_handler(app, handler_config)

  let _ =
    store.append_to_stream("order-inject", event_store.AnyVersion, [
      event.EventData(
        data: OrderPlaced("ORD-INJ"),
        event_type: "OrderPlaced",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  let assert Ok(id) = process.receive(processed, 1000)
  id |> should.equal("ORD-INJ")

  // Handler acked the subscriptions (injected by app) → wait_for unblocks
  process.sleep(30)
  subscriptions.wait_for(subs, "order-inject", 1, 500)
  |> should.be_ok
}
