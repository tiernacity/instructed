//// Error types used throughout the Instructed framework.
////
//// These types provide explicit error handling matching Commanded's error
//// patterns, but using Gleam's type system instead of Erlang's dynamic
//// error tuples.

import gleam/int

/// Errors that can occur during command dispatch.
///
/// Maps to the various `{:error, reason}` returns from Commanded's
/// `Commanded.Commands.Dispatcher` and aggregate execution.
pub type DispatchError {
  /// The aggregate returned a domain error from its execute function.
  /// Equivalent to Commanded's `{:error, reason}` from `execute/2`.
  AggregateError(reason: String)
  /// The command was halted by middleware (before_dispatch or after_dispatch).
  /// Equivalent to Commanded's `%Pipeline{halted: true}` handling.
  Halted
  /// Command execution timed out.
  /// Equivalent to Commanded's `{:error, :aggregate_execution_timeout}`.
  Timeout
  /// Wrong expected version when appending events.
  /// This triggers automatic retry (up to max attempts).
  WrongExpectedVersion
  /// The aggregate process could not be started.
  AggregateStartError(reason: String)
  /// The event store returned an error during append or read.
  EventStoreError(reason: String)
  /// A middleware error occurred during pipeline execution.
  MiddlewareError(reason: String)
  /// Exhausted all retry attempts on version conflict.
  /// Equivalent to Commanded's `{:error, :too_many_attempts}`.
  /// Default max attempts: 10 (matching Commanded).
  TooManyAttempts
  /// Strong consistency wait timed out.
  /// Equivalent to Commanded's `{:error, :consistency_timeout}`.
  /// Default timeout: 5000ms (matching Commanded).
  ConsistencyTimeout
  /// An exception was raised during command execution.
  /// Equivalent to Commanded's `{:error, error}` from caught exceptions.
  ExecutionException(reason: String)
}

/// Errors that can occur in the event store.
///
/// Maps to the various error atoms returned by Commanded's
/// `EventStore.Adapter` callbacks.
pub type EventStoreError {
  /// The expected version didn't match the current stream version.
  /// Equivalent to Commanded's `{:error, :wrong_expected_version}`.
  VersionConflict
  /// The requested stream does not exist.
  /// Equivalent to Commanded's `{:error, :stream_not_found}`.
  StreamNotFound
  /// The stream already exists (returned when NoStream expected version fails).
  /// Equivalent to Commanded's `{:error, :stream_exists}`.
  StreamAlreadyExists
  /// The requested snapshot does not exist.
  /// Equivalent to Commanded's `{:error, :snapshot_not_found}`.
  SnapshotNotFound
  /// A subscription with this name already exists.
  /// Equivalent to Commanded's `{:error, :subscription_already_exists}`.
  SubscriptionAlreadyExists
  /// The requested subscription was not found.
  /// Equivalent to Commanded's `{:error, :subscription_not_found}`.
  SubscriptionNotFound
  /// Too many subscribers for the subscription's concurrency limit.
  /// Equivalent to Commanded's `{:error, :too_many_subscribers}`.
  TooManySubscribers
  /// A storage or I/O error from the underlying adapter.
  StorageError(reason: String)
}

/// Error handling strategy for event handler and process manager failures.
///
/// Equivalent to Commanded's `error/3` callback return values from
/// `Commanded.Event.Handler` and `Commanded.ProcessManagers.ProcessManager`.
pub type ErrorAction(context) {
  /// Retry processing the failed event with updated context.
  /// Equivalent to Commanded's `{:retry, context}`.
  Retry(context: context)
  /// Retry after a delay (milliseconds) with updated context.
  /// Equivalent to Commanded's `{:retry, delay, context}`.
  RetryWithDelay(delay_ms: Int, context: context)
  /// Skip the failed event and continue processing.
  /// Equivalent to Commanded's `:skip`.
  Skip
  /// Stop the handler/process manager with the given reason.
  /// Equivalent to Commanded's `{:stop, reason}`.
  Stop(reason: String)
}

/// Extended error action for process manager command dispatch failures.
///
/// Process managers can fail in two ways: event handling errors and
/// command dispatch errors. Command dispatch errors have additional
/// options for managing pending commands.
///
/// Equivalent to Commanded's process manager `error/3` return values
/// for command dispatch failures.
pub type ProcessManagerErrorAction(context) {
  /// Retry with updated context
  PMRetry(context: context)
  /// Retry after delay
  PMRetryWithDelay(delay_ms: Int, context: context)
  /// Skip the failed event
  PMSkip
  /// Skip but continue dispatching remaining pending commands
  PMSkipContinuePending
  /// Skip and discard all remaining pending commands
  PMSkipDiscardPending
  /// Replace pending commands with new ones and continue
  PMContinue(commands: List(String), context: context)
  /// Stop the process manager
  PMStop(reason: String)
}

/// Context provided when an event handler or process manager fails.
///
/// Equivalent to Commanded's `Commanded.Event.FailureContext` struct.
pub type FailureContext(handler_state) {
  FailureContext(
    /// User-defined context that persists across retries.
    /// Start with an empty map; add retry-specific data as needed.
    context: handler_state,
    /// The handler/PM state at the time of failure
    handler_state: handler_state,
    /// Number of times this event has been retried
    failure_count: Int,
    /// The error that caused the failure
    last_error: String,
    /// Stacktrace if available (may be empty)
    stacktrace: String,
  )
}

// ---------------------------------------------------------------------------
// Built-in exponential backoff
// ---------------------------------------------------------------------------

/// Minimum backoff delay: 1 second (in milliseconds).
const min_backoff_ms = 1000

/// Maximum backoff delay: 24 hours (in milliseconds).
const max_backoff_ms = 86_400_000

/// Calculate exponential backoff delay for a given failure count.
///
/// Formula: `failures² × 1000` milliseconds, clamped to [1s, 24h].
///
/// This matches Commanded's built-in exponential backoff strategy.
/// Commanded also adds random jitter; use `backoff_with_jitter/1` for that.
///
/// ## Examples
///
/// ```gleam
/// backoff(1)  // 1000ms  (1s)
/// backoff(2)  // 4000ms  (4s)
/// backoff(3)  // 9000ms  (9s)
/// backoff(10) // 100000ms (100s)
/// ```
pub fn backoff(failure_count: Int) -> Int {
  let delay = failure_count * failure_count * 1000
  int.clamp(delay, min_backoff_ms, max_backoff_ms)
}

/// Calculate exponential backoff delay with random jitter.
///
/// Formula: `failures² × 1000 + random(0..1000)` ms, clamped to [1s, 24h].
///
/// The jitter prevents thundering herd when multiple handlers retry
/// simultaneously. Matches Commanded's backoff strategy.
///
/// ## Example usage in an error callback
///
/// ```gleam
/// fn on_error(reason, _event, state) {
///   let failure_count = state.failures + 1
///   let delay = error.backoff_with_jitter(failure_count)
///   error.RetryWithDelay(delay, MyState(..state, failures: failure_count))
/// }
/// ```
pub fn backoff_with_jitter(failure_count: Int) -> Int {
  let base = failure_count * failure_count * 1000
  let jitter = random_jitter_ms()
  int.clamp(base + jitter, min_backoff_ms, max_backoff_ms)
}

/// Random jitter between 0 and 1000 milliseconds.
@external(erlang, "instructed_error_ffi", "random_jitter_ms")
fn random_jitter_ms() -> Int
