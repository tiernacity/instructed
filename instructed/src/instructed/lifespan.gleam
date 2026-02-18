//// Aggregate Lifespan — controls when an aggregate process stops.
////
//// By default, aggregate server processes run forever (until the supervision
//// tree stops them). `Lifespan` lets you control when an aggregate process
//// should stop itself based on events, commands, or errors.
////
//// This is the Gleam equivalent of Commanded's `AggregateLifespan` behaviour.
////
//// ## Available decisions
////
//// - `KeepRunning` — do nothing; the process stays alive (default)
//// - `Stop` — stop the process immediately after the current operation
//// - `StopAfter(ms)` — stop after `ms` milliseconds of inactivity (idle timeout).
////   If a new command arrives before the timeout, the timer resets.
//// - `Hibernate` — keep running (hibernate is not directly supported in
////   Gleam's actor model; this falls back to `KeepRunning`)
////
//// ## Callbacks
////
//// Three callbacks control the decision point:
////
//// - `after_command` — called after a command is successfully processed
////   (i.e., events were appended). Receives the updated aggregate state.
//// - `after_error` — called after a command returns an error.
////   Receives the current aggregate state, the command, and the error reason.
//// - `after_event` — called after an EXTERNAL event is applied to the
////   cached aggregate state (events from outside this process).
////
//// ## Example
////
//// ```gleam
//// import instructed/lifespan
////
//// // Stop aggregate process immediately after processing a "final" command:
//// let my_lifespan = lifespan.Lifespan(
////   after_command: fn(state, _cmd) {
////     case state.status {
////       Closed -> lifespan.Stop
////       _ -> lifespan.KeepRunning
////     }
////   },
////   after_error: fn(_state, _cmd, _reason) { lifespan.KeepRunning },
////   after_event: fn(_state, _event) { lifespan.KeepRunning },
//// )
////
//// // With an idle timeout (stop if no commands within 5 minutes):
//// let idle_lifespan = lifespan.new_idle(300_000)
//// ```

/// What the aggregate server should do after a lifespan callback.
pub type LifespanDecision {
  /// Keep the process running — process the next command as normal.
  /// This is the default behavior.
  KeepRunning
  /// Stop the aggregate process immediately after the current operation.
  /// The next command for this aggregate will start a fresh process.
  Stop
  /// Stop the aggregate process after `ms` milliseconds of inactivity.
  /// If a new command arrives before the timeout, the timer is reset.
  StopAfter(ms: Int)
  /// Hibernate the process to reduce memory usage.
  /// Note: Gleam's actor model does not natively support Erlang-level
  /// process hibernation via the Actor abstraction. This currently falls
  /// back to `KeepRunning`.
  Hibernate
}

/// Lifespan configuration for an aggregate server.
///
/// All three callbacks must be provided. Use `always_running()` if you want
/// the default behavior (never stop).
pub type Lifespan(state, command, event) {
  Lifespan(
    /// Called after a command is successfully executed (events appended).
    /// Receives the NEW aggregate state (post-event application).
    after_command: fn(state, command) -> LifespanDecision,
    /// Called after a command returns an error (no events appended).
    /// Receives the CURRENT aggregate state and the error reason string.
    after_error: fn(state, command, String) -> LifespanDecision,
    /// Called after an externally-originated event is applied.
    /// Receives the NEW aggregate state.
    after_event: fn(state, event) -> LifespanDecision,
  )
}

/// Create a lifespan that keeps the aggregate running forever.
/// This is the default behavior when no lifespan is configured.
pub fn always_running() -> Lifespan(state, command, event) {
  Lifespan(
    after_command: fn(_state, _cmd) { KeepRunning },
    after_error: fn(_state, _cmd, _reason) { KeepRunning },
    after_event: fn(_state, _event) { KeepRunning },
  )
}

/// Create a lifespan that stops the aggregate after `ms` milliseconds of
/// inactivity. If no command arrives within `ms` ms, the process stops.
pub fn new_idle(ms: Int) -> Lifespan(state, command, event) {
  Lifespan(
    after_command: fn(_state, _cmd) { StopAfter(ms) },
    after_error: fn(_state, _cmd, _reason) { StopAfter(ms) },
    after_event: fn(_state, _event) { StopAfter(ms) },
  )
}

/// Create a lifespan that stops the aggregate after the first command succeeds.
/// Useful for one-shot aggregates.
pub fn stop_after_command() -> Lifespan(state, command, event) {
  Lifespan(
    after_command: fn(_state, _cmd) { Stop },
    after_error: fn(_state, _cmd, _reason) { KeepRunning },
    after_event: fn(_state, _event) { KeepRunning },
  )
}
