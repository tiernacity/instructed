//// Telemetry & Observability for Instructed.
////
//// This module provides structured instrumentation events that can be consumed
//// by any telemetry subscriber — most commonly for logging, metrics, and
//// distributed tracing.
////
//// ## Design
////
//// Instructed emits telemetry via the Erlang `:telemetry` library
//// (https://hex.pm/packages/telemetry). When `:telemetry` is not available
//// in the release, all calls are gracefully no-ops — the framework continues
//// to work without errors.
////
//// If you want to consume telemetry events, add `:telemetry` (via Gleam's
//// Erlang dependency mechanism) to your project and attach handlers:
////
//// ```erlang
//// % Erlang / FFI
//// telemetry:attach(<<"my-handler">>,
////   [<<"instructed">>, <<"command">>, <<"dispatch">>, <<"stop">>],
////   fun handle_event/4, nil).
//// ```
////
//// ## Event Catalogue
////
//// All events follow the convention:
//// `[:instructed, <subsystem>, <action>, :start | :stop | :exception]`
////
//// ### Command Dispatch
//// - `[:instructed, :command, :dispatch, :start]`
////   - Measurements: `%{system_time: integer()}`
////   - Metadata: `%{command: term(), command_id: string(), aggregate_stream_id: string()}`
////
//// - `[:instructed, :command, :dispatch, :stop]`
////   - Measurements: `%{duration: integer()}`
////   - Metadata: `%{command: term(), command_id: string(), aggregate_stream_id: string(), result: :ok | :error}`
////
//// - `[:instructed, :command, :dispatch, :exception]`
////   - Measurements: `%{duration: integer()}`
////   - Metadata: `%{command: term(), command_id: string(), aggregate_stream_id: string(), error: term()}`
////
//// ### Aggregate Execution
//// - `[:instructed, :aggregate, :execute, :start]`
////   - Measurements: `%{system_time: integer()}`
////   - Metadata: `%{aggregate_stream_id: string(), command: term()}`
////
//// - `[:instructed, :aggregate, :execute, :stop]`
////   - Measurements: `%{duration: integer()}`
////   - Metadata: `%{aggregate_stream_id: string(), command: term(), event_count: integer()}`
////
//// - `[:instructed, :aggregate, :execute, :exception]`
////   - Measurements: `%{duration: integer()}`
////   - Metadata: `%{aggregate_stream_id: string(), command: term(), error: string()}`
////
//// ### Event Handler Processing
//// - `[:instructed, :event, :handle, :start]`
////   - Measurements: `%{system_time: integer()}`
////   - Metadata: `%{handler_name: string(), event_type: string(), event_number: integer()}`
////
//// - `[:instructed, :event, :handle, :stop]`
////   - Measurements: `%{duration: integer()}`
////   - Metadata: `%{handler_name: string(), event_type: string(), event_number: integer()}`
////
//// - `[:instructed, :event, :handle, :exception]`
////   - Measurements: `%{duration: integer()}`
////   - Metadata: `%{handler_name: string(), event_type: string(), event_number: integer(), error: string()}`
////
//// ### Process Manager Processing
//// - `[:instructed, :process_manager, :handle, :start]`
////   - Measurements: `%{system_time: integer()}`
////   - Metadata: `%{pm_name: string(), event_type: string(), event_number: integer()}`
////
//// - `[:instructed, :process_manager, :handle, :stop]`
////   - Measurements: `%{duration: integer()}`
////   - Metadata: `%{pm_name: string(), event_type: string(), event_number: integer(), commands_dispatched: integer()}`
////
//// ## Gleam-first Telemetry Hook
////
//// If you prefer a pure-Gleam approach without the Erlang `:telemetry`
//// dependency, use `set_handler` to register a Gleam function that receives
//// all events:
////
//// ```gleam
//// import instructed/telemetry
////
//// telemetry.set_handler(fn(event) {
////   io.println("event: " <> string.inspect(event))
//// })
//// ```
////
//// ## Example — Logging Middleware
////
//// ```gleam
//// import instructed/middleware.{type Pipeline}
//// import instructed/telemetry
////
//// pub fn logging_middleware() -> middleware.Middleware(command) {
////   middleware.Middleware(
////     before_dispatch: fn(pipeline) {
////       telemetry.dispatch_start(pipeline.command_id, "unknown")
////       pipeline
////     },
////     after_dispatch: fn(pipeline) {
////       telemetry.dispatch_stop(pipeline.command_id, "unknown", 0)
////       pipeline
////     },
////     after_failure: fn(pipeline) {
////       telemetry.dispatch_exception(pipeline.command_id, "unknown", 0, "failed")
////       pipeline
////     },
////   )
//// }
//// ```

/// A structured telemetry event emitted by Instructed.
pub type TelemetryEvent {
  /// Fired just before a command is dispatched through the router.
  CommandDispatchStart(
    command_id: String,
    aggregate_stream_id: String,
    system_time: Int,
  )
  /// Fired after a command is successfully dispatched (events appended).
  CommandDispatchStop(
    command_id: String,
    aggregate_stream_id: String,
    duration_ns: Int,
    event_count: Int,
  )
  /// Fired when command dispatch returns an error.
  CommandDispatchException(
    command_id: String,
    aggregate_stream_id: String,
    duration_ns: Int,
    error: String,
  )
  /// Fired just before an aggregate executes a command.
  AggregateExecuteStart(aggregate_stream_id: String, system_time: Int)
  /// Fired after an aggregate successfully executes a command.
  AggregateExecuteStop(
    aggregate_stream_id: String,
    duration_ns: Int,
    event_count: Int,
  )
  /// Fired when aggregate command execution returns an error.
  AggregateExecuteException(
    aggregate_stream_id: String,
    duration_ns: Int,
    error: String,
  )
  /// Fired just before an event handler processes an event.
  EventHandleStart(
    handler_name: String,
    event_type: String,
    event_number: Int,
    system_time: Int,
  )
  /// Fired after an event handler successfully processes an event.
  EventHandleStop(
    handler_name: String,
    event_type: String,
    event_number: Int,
    duration_ns: Int,
  )
  /// Fired when an event handler returns an error.
  EventHandleException(
    handler_name: String,
    event_type: String,
    event_number: Int,
    duration_ns: Int,
    error: String,
  )
  /// Fired just before a process manager handles an event.
  ProcessManagerHandleStart(
    pm_name: String,
    event_type: String,
    event_number: Int,
    system_time: Int,
  )
  /// Fired after a process manager successfully handles an event.
  ProcessManagerHandleStop(
    pm_name: String,
    event_type: String,
    event_number: Int,
    duration_ns: Int,
    commands_dispatched: Int,
  )
  /// Fired when a process manager handler returns an error.
  ProcessManagerHandleException(
    pm_name: String,
    event_type: String,
    event_number: Int,
    duration_ns: Int,
    error: String,
  )
}

// --- Global Gleam handler ---

/// Set a Gleam function to receive all `TelemetryEvent` values emitted by
/// Instructed. Only one handler can be registered at a time; calling this
/// again replaces the previous handler.
///
/// This is a lightweight, in-process alternative to Erlang `:telemetry`.
/// It is primarily useful for testing and simple logging scenarios.
///
/// **Note**: The handler runs synchronously in the emitting process — keep
/// it fast to avoid blocking the framework.
@external(erlang, "instructed_telemetry_ffi", "set_handler")
pub fn set_handler(handler: fn(TelemetryEvent) -> Nil) -> Nil

/// Clear the Gleam telemetry handler installed by `set_handler/1`.
@external(erlang, "instructed_telemetry_ffi", "clear_handler")
pub fn clear_handler() -> Nil

// --- Emit ---

/// Emit a `TelemetryEvent`. Called internally by the framework at well-defined
/// instrumentation points.
///
/// This function:
/// 1. Calls the Gleam handler registered via `set_handler/1` (if any).
/// 2. Attempts to emit via Erlang `:telemetry.execute/3` (if available).
///    If `:telemetry` is not in the release, this step is silently skipped.
@external(erlang, "instructed_telemetry_ffi", "emit")
pub fn emit(event: TelemetryEvent) -> Nil

// --- Convenience emit helpers ---

/// Emit `CommandDispatchStart`.
pub fn dispatch_start(command_id: String, stream_id: String) -> Nil {
  emit(CommandDispatchStart(
    command_id: command_id,
    aggregate_stream_id: stream_id,
    system_time: system_time(),
  ))
}

/// Emit `CommandDispatchStop`.
pub fn dispatch_stop(
  command_id: String,
  stream_id: String,
  start_time: Int,
  event_count: Int,
) -> Nil {
  emit(CommandDispatchStop(
    command_id: command_id,
    aggregate_stream_id: stream_id,
    duration_ns: system_time() - start_time,
    event_count: event_count,
  ))
}

/// Emit `CommandDispatchException`.
pub fn dispatch_exception(
  command_id: String,
  stream_id: String,
  start_time: Int,
  error: String,
) -> Nil {
  emit(CommandDispatchException(
    command_id: command_id,
    aggregate_stream_id: stream_id,
    duration_ns: system_time() - start_time,
    error: error,
  ))
}

/// Emit `AggregateExecuteStart`.
pub fn aggregate_start(stream_id: String) -> Int {
  let t = system_time()
  emit(AggregateExecuteStart(aggregate_stream_id: stream_id, system_time: t))
  t
}

/// Emit `AggregateExecuteStop`.
pub fn aggregate_stop(stream_id: String, start_time: Int, event_count: Int) -> Nil {
  emit(AggregateExecuteStop(
    aggregate_stream_id: stream_id,
    duration_ns: system_time() - start_time,
    event_count: event_count,
  ))
}

/// Emit `AggregateExecuteException`.
pub fn aggregate_exception(stream_id: String, start_time: Int, error: String) -> Nil {
  emit(AggregateExecuteException(
    aggregate_stream_id: stream_id,
    duration_ns: system_time() - start_time,
    error: error,
  ))
}

/// Emit `EventHandleStart`. Returns the start timestamp (nanoseconds) for use
/// with `event_handle_stop/4` and `event_handle_exception/5`.
pub fn event_handle_start(
  handler_name: String,
  event_type: String,
  event_number: Int,
) -> Int {
  let t = system_time()
  emit(EventHandleStart(
    handler_name: handler_name,
    event_type: event_type,
    event_number: event_number,
    system_time: t,
  ))
  t
}

/// Emit `EventHandleStop`.
pub fn event_handle_stop(
  handler_name: String,
  event_type: String,
  event_number: Int,
  start_time: Int,
) -> Nil {
  emit(EventHandleStop(
    handler_name: handler_name,
    event_type: event_type,
    event_number: event_number,
    duration_ns: system_time() - start_time,
  ))
}

/// Emit `EventHandleException`.
pub fn event_handle_exception(
  handler_name: String,
  event_type: String,
  event_number: Int,
  start_time: Int,
  error: String,
) -> Nil {
  emit(EventHandleException(
    handler_name: handler_name,
    event_type: event_type,
    event_number: event_number,
    duration_ns: system_time() - start_time,
    error: error,
  ))
}

/// Emit `ProcessManagerHandleStart`.
pub fn pm_handle_start(
  pm_name: String,
  event_type: String,
  event_number: Int,
) -> Int {
  let t = system_time()
  emit(ProcessManagerHandleStart(
    pm_name: pm_name,
    event_type: event_type,
    event_number: event_number,
    system_time: t,
  ))
  t
}

/// Emit `ProcessManagerHandleStop`.
pub fn pm_handle_stop(
  pm_name: String,
  event_type: String,
  event_number: Int,
  start_time: Int,
  commands_dispatched: Int,
) -> Nil {
  emit(ProcessManagerHandleStop(
    pm_name: pm_name,
    event_type: event_type,
    event_number: event_number,
    duration_ns: system_time() - start_time,
    commands_dispatched: commands_dispatched,
  ))
}

/// Emit `ProcessManagerHandleException`.
pub fn pm_handle_exception(
  pm_name: String,
  event_type: String,
  event_number: Int,
  start_time: Int,
  error: String,
) -> Nil {
  emit(ProcessManagerHandleException(
    pm_name: pm_name,
    event_type: event_type,
    event_number: event_number,
    duration_ns: system_time() - start_time,
    error: error,
  ))
}

// --- Time ---

/// Current monotonic time in nanoseconds. Used for duration measurements.
@external(erlang, "instructed_telemetry_ffi", "monotonic_time_ns")
pub fn system_time() -> Int
