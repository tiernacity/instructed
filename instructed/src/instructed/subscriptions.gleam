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
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import instructed/middleware.{type Consistency, Eventual, Strong}

/// Monotonic time in nanoseconds for TTL tracking.
@external(erlang, "instructed_subscriptions_ffi", "monotonic_time_ns")
fn monotonic_time_ns() -> Int

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
  /// Register a process (PID) as belonging to a named handler.
  /// Used for automatic dispatcher exclusion from consistency wait.
  RegisterPid(pid: Pid, name: String)
  /// Acknowledge that a handler has processed a specific event version.
  AckEvent(name: String, stream_id: String, stream_version: Int)
  /// Request that the caller be unblocked when all strong handlers have acked.
  /// `caller_pid` is used to auto-exclude the calling handler (prevents deadlock
  /// when a strong handler dispatches a command with strong consistency).
  WaitFor(
    stream_id: String,
    stream_version: Int,
    reply_to: Subject(Nil),
    caller_pid: Option(Pid),
  )
  /// Internal: periodic purge of stale ack entries to prevent memory leaks.
  /// Matches Commanded's 1-hour TTL for ack entries.
  Purge
  /// Internal: store self-reference for scheduling purge messages.
  SetSelf(subject: Subject(SubMessage))
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

type Waiter {
  Waiter(
    stream_id: String,
    stream_version: Int,
    reply_to: Subject(Nil),
    exclude: List(String),
  )
}

/// Ack entry: version and timestamp (monotonic nanoseconds) of last ack.
type AckEntry {
  AckEntry(version: Int, timestamp_ns: Int)
}

/// TTL for ack entries: 1 hour in nanoseconds.
/// Matches Commanded's purge interval.
const ack_ttl_ns = 3_600_000_000_000

/// Interval between purge cycles: 5 minutes in milliseconds.
const purge_interval_ms = 300_000

type SubState {
  SubState(
    /// Registered handlers: name → consistency
    handlers: Dict(String, Consistency),
    /// Acked versions: (handler_name, stream_id) → AckEntry (version + timestamp)
    acked: Dict(#(String, String), AckEntry),
    /// Pending waiters blocked until their version is acked
    waiters: List(Waiter),
    /// PID → handler name mapping for automatic dispatcher exclusion
    pid_to_name: Dict(Pid, String),
    /// Self-reference for scheduling purge messages
    self_subject: Option(Subject(SubMessage)),
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
    SubState(
      handlers: dict.new(),
      acked: dict.new(),
      waiters: [],
      pid_to_name: dict.new(),
      self_subject: None,
    )

  case
    actor.new(state)
    |> actor.on_message(handle_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data
      // Store self-reference and schedule the first purge cycle.
      process.send(subject, SetSelf(subject))
      schedule_purge(subject)
      Ok(subject)
    }
    Error(_) -> Error("Failed to start subscriptions actor")
  }
}

/// Schedule the next purge cycle.
fn schedule_purge(subject: Subject(SubMessage)) -> Nil {
  let _ = process.send_after(subject, purge_interval_ms, Purge)
  Nil
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

/// Register a process (PID) as belonging to a named handler.
///
/// This enables automatic dispatcher exclusion: when a strong-consistency handler
/// dispatches a command that also requires strong consistency, the handler is
/// automatically excluded from the wait, preventing deadlock.
///
/// Should be called by event handlers and process managers during startup.
///
/// Equivalent to Commanded's PID-based dispatcher exclusion in
/// `Commanded.Subscriptions.wait_for/5`.
pub fn register_pid(
  subject: Subject(SubMessage),
  pid: Pid,
  name: String,
) -> Nil {
  process.send(subject, RegisterPid(pid: pid, name: name))
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
  // Capture the caller's PID so the subscriptions actor can auto-exclude
  // the dispatching handler (prevents strong-consistency deadlock).
  let caller_pid = Some(process.self())
  process.send(
    subject,
    WaitFor(
      stream_id: stream_id,
      stream_version: stream_version,
      reply_to: reply_subject,
      caller_pid: caller_pid,
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

    SetSelf(subject) -> {
      actor.continue(SubState(..state, self_subject: Some(subject)))
    }

    RegisterPid(pid, name) -> {
      let new_pid_to_name = dict.insert(state.pid_to_name, pid, name)
      actor.continue(SubState(..state, pid_to_name: new_pid_to_name))
    }

    AckEvent(name, stream_id, stream_version) -> {
      // Update max acked version for this handler + stream combo.
      // Guard against out-of-order delivery by keeping only the highest version.
      let key = #(name, stream_id)
      let now = monotonic_time_ns()
      let current_version = case dict.get(state.acked, key) {
        Ok(entry) -> entry.version
        Error(_) -> 0
      }
      let new_acked = case stream_version > current_version {
        True ->
          dict.insert(
            state.acked,
            key,
            AckEntry(version: stream_version, timestamp_ns: now),
          )
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

    Purge -> {
      // Remove ack entries older than the TTL (1 hour).
      // Prevents unbounded memory growth in long-running systems.
      // Matches Commanded's periodic purge behavior.
      let now = monotonic_time_ns()
      let new_acked =
        dict.filter(state.acked, fn(_key, entry) {
          now - entry.timestamp_ns < ack_ttl_ns
        })
      // Reschedule the next purge. We need our own subject reference.
      // Since we can't easily get it from actor state, we use self() to
      // construct a send_after. The subject is passed via the start function.
      case state.self_subject {
        Some(subj) -> schedule_purge(subj)
        None -> Nil
      }
      actor.continue(SubState(..state, acked: new_acked))
    }

    WaitFor(stream_id, stream_version, reply_to, caller_pid) -> {
      // Auto-exclude the calling handler to prevent deadlock when a
      // strong-consistency handler dispatches with strong consistency.
      let exclude = case caller_pid {
        Some(pid) ->
          case dict.get(state.pid_to_name, pid) {
            Ok(name) -> [name]
            Error(_) -> []
          }
        None -> []
      }
      let waiter =
        Waiter(
          stream_id: stream_id,
          stream_version: stream_version,
          reply_to: reply_to,
          exclude: exclude,
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

/// Check if a waiter is satisfied — all strong handlers (minus excluded)
/// have acked >= stream_version.
///
/// Special case: if no strong handlers are registered, immediately satisfied.
///
/// The `exclude` list is used for automatic dispatcher exclusion: when a
/// strong-consistency handler dispatches a command with strong consistency,
/// it is excluded from the wait to prevent deadlock.
fn is_waiter_satisfied(state: SubState, waiter: Waiter) -> Bool {
  let strong_names =
    dict.to_list(state.handlers)
    |> list.filter_map(fn(entry) {
      let #(name, consistency) = entry
      case consistency {
        Strong ->
          case list.contains(waiter.exclude, name) {
            True -> Error(Nil)
            False -> Ok(name)
          }
        Eventual -> Error(Nil)
      }
    })

  case strong_names {
    // No strong handlers registered (or all excluded) → satisfied immediately
    [] -> True
    names ->
      list.all(names, fn(name) {
        case dict.get(state.acked, #(name, waiter.stream_id)) {
          Ok(entry) -> entry.version >= waiter.stream_version
          Error(_) -> False
        }
      })
  }
}
