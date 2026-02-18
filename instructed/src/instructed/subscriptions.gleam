//// Subscriptions tracking for strong consistency guarantees.
////
//// Tracks which strong-consistency handlers have processed which events,
//// and unblocks dispatchers that are waiting for consistency.
////
//// Equivalent to `Commanded.Subscriptions` and `Commanded.Subscriptions.Registry`.
////
//// ## How it works
////
//// 1. Event handlers and process managers register with the subscriptions actor on startup.
//// 2. After successfully processing an event, strong-consistency handlers call `ack_event/4`.
//// 3. After appending events, the router calls `wait_for/4` when dispatched with `Strong` consistency.
//// 4. The subscriptions actor unblocks the waiting dispatcher once all registered
////    strong handlers have acked >= the stream version returned by the dispatch.
////
//// ## Example
////
//// ```gleam
//// // Start the subscriptions actor
//// let assert Ok(subs) = subscriptions.start()
////
//// // Register a strong-consistency handler
//// subscriptions.register(subs, "order_handler", middleware.Strong)
////
//// // In the handler, after processing an event:
//// subscriptions.ack_event(subs, "order_handler", event.stream_id, event.stream_version)
////
//// // After dispatch, the router waits:
//// case subscriptions.wait_for(subs, stream_id, version, 5000) {
////   Ok(Nil) -> // all strong handlers acked
////   Error(Nil) -> // timed out — ConsistencyTimeout
//// }
//// ```

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import instructed/middleware.{type Consistency, Eventual, Strong}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Messages handled by the subscriptions actor.
///
/// Opaque to prevent external code from constructing messages directly —
/// use the helper functions (`register/3`, `ack_event/4`, `wait_for/4`) instead.
pub opaque type SubMessage {
  /// Register a handler's consistency level.
  Register(name: String, consistency: Consistency)
  /// Acknowledge that a handler has processed a specific event version.
  AckEvent(name: String, stream_id: String, stream_version: Int)
  /// Request that the caller be unblocked when all strong handlers have acked.
  WaitFor(stream_id: String, stream_version: Int, reply_to: Subject(Nil))
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

type Waiter {
  Waiter(stream_id: String, stream_version: Int, reply_to: Subject(Nil))
}

type SubState {
  SubState(
    /// Registered handlers: name → consistency
    handlers: Dict(String, Consistency),
    /// Acked versions: (handler_name, stream_id) → max version acked
    acked: Dict(#(String, String), Int),
    /// Pending waiters blocked until their version is acked
    waiters: List(Waiter),
  )
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

/// Start the subscriptions actor.
///
/// Returns the Subject used to send messages to the actor.
/// Returns an error string if the actor fails to start.
pub fn start() -> Result(Subject(SubMessage), String) {
  let state =
    SubState(handlers: dict.new(), acked: dict.new(), waiters: [])

  case
    actor.new(state)
    |> actor.on_message(handle_message)
    |> actor.start
  {
    Ok(started) -> Ok(started.data)
    Error(_) -> Error("Failed to start subscriptions actor")
  }
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

/// Register an event handler or process manager.
///
/// Must be called before any events are dispatched.
/// Only strong-consistency handlers participate in the consistency wait.
///
/// Equivalent to `Commanded.Subscriptions.Registry.register/5`.
pub fn register(
  subject: Subject(SubMessage),
  name: String,
  consistency: Consistency,
) -> Nil {
  process.send(subject, Register(name: name, consistency: consistency))
}

/// Acknowledge that a handler has finished processing an event.
///
/// Should be called after successful event processing when the handler
/// is configured with `consistency: Strong`.
///
/// Equivalent to `Commanded.Subscriptions.ack_event/4`.
pub fn ack_event(
  subject: Subject(SubMessage),
  name: String,
  stream_id: String,
  stream_version: Int,
) -> Nil {
  process.send(
    subject,
    AckEvent(name: name, stream_id: stream_id, stream_version: stream_version),
  )
}

/// Block until all strong-consistency handlers have acked the given stream version.
///
/// Returns `Ok(Nil)` when all strong handlers have processed events up to
/// `stream_version` on the given `stream_id`.
///
/// Returns `Error(Nil)` if the timeout elapses before all handlers ack.
///
/// If no strong-consistency handlers are registered, returns `Ok(Nil)` immediately.
///
/// Equivalent to `Commanded.Subscriptions.wait_for/5`.
pub fn wait_for(
  subject: Subject(SubMessage),
  stream_id: String,
  stream_version: Int,
  timeout_ms: Int,
) -> Result(Nil, Nil) {
  let reply_subject = process.new_subject()
  process.send(
    subject,
    WaitFor(
      stream_id: stream_id,
      stream_version: stream_version,
      reply_to: reply_subject,
    ),
  )
  process.receive(reply_subject, timeout_ms)
}

// ---------------------------------------------------------------------------
// Actor message handler
// ---------------------------------------------------------------------------

fn handle_message(
  state: SubState,
  msg: SubMessage,
) -> actor.Next(SubState, SubMessage) {
  case msg {
    Register(name, consistency) -> {
      let new_handlers = dict.insert(state.handlers, name, consistency)
      actor.continue(SubState(..state, handlers: new_handlers))
    }

    AckEvent(name, stream_id, stream_version) -> {
      // Update max acked version for this handler + stream combo.
      // Guard against out-of-order delivery by keeping only the highest version.
      let key = #(name, stream_id)
      let current = case dict.get(state.acked, key) {
        Ok(v) -> v
        Error(_) -> 0
      }
      let new_acked = case stream_version > current {
        True -> dict.insert(state.acked, key, stream_version)
        False -> state.acked
      }
      let new_state = SubState(..state, acked: new_acked)

      // Satisfy any waiters that are now fully acked
      let #(satisfied, remaining) =
        list.partition(new_state.waiters, fn(w) {
          is_waiter_satisfied(new_state, w)
        })
      list.each(satisfied, fn(w) { process.send(w.reply_to, Nil) })

      actor.continue(SubState(..new_state, waiters: remaining))
    }

    WaitFor(stream_id, stream_version, reply_to) -> {
      let waiter =
        Waiter(
          stream_id: stream_id,
          stream_version: stream_version,
          reply_to: reply_to,
        )
      case is_waiter_satisfied(state, waiter) {
        True -> {
          // Already satisfied — reply immediately
          process.send(reply_to, Nil)
          actor.continue(state)
        }
        False -> {
          // Add to waiters list; will be resolved when acks arrive
          actor.continue(SubState(..state, waiters: [waiter, ..state.waiters]))
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Check if a waiter is satisfied — all strong handlers have acked >= stream_version.
///
/// Special case: if no strong handlers are registered, immediately satisfied.
fn is_waiter_satisfied(state: SubState, waiter: Waiter) -> Bool {
  let strong_names =
    dict.to_list(state.handlers)
    |> list.filter_map(fn(entry) {
      let #(name, consistency) = entry
      case consistency {
        Strong -> Ok(name)
        Eventual -> Error(Nil)
      }
    })

  case strong_names {
    // No strong handlers registered → satisfied immediately
    [] -> True
    names ->
      list.all(names, fn(name) {
        case dict.get(state.acked, #(name, waiter.stream_id)) {
          Ok(version) -> version >= waiter.stream_version
          Error(_) -> False
        }
      })
  }
}
