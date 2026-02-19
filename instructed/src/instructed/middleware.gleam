//// Middleware for the command processing pipeline.
////
//// Middleware provides an extension point to add functions that you want to be
//// called for every command dispatched through the router.
////
//// Equivalent to Commanded's `Commanded.Middleware` behaviour and
//// `Commanded.Middleware.Pipeline` struct.
////
//// ## Pipeline Stages
////
//// - `before_dispatch` — runs before the command is dispatched. Can halt
////   the pipeline to prevent dispatch.
//// - `after_dispatch` — runs after successful command dispatch.
//// - `after_failure` — runs after a failed dispatch. Always runs through
////   ALL middleware (not stopped by halt).
////
//// ## Halting
////
//// Calling `halt` on a pipeline stops `before_dispatch` and `after_dispatch`
//// from calling further middleware. `after_failure` always runs all middleware.
////
//// ## Example
////
//// ```gleam
//// import instructed/middleware.{type Pipeline}
////
//// fn logging_middleware() -> Middleware(cmd) {
////   middleware.new(
////     before_dispatch: fn(pipeline) {
////       io.println("Dispatching command")
////       pipeline
////     },
////     after_dispatch: fn(pipeline) {
////       io.println("Command dispatched")
////       pipeline
////     },
////     after_failure: fn(pipeline) {
////       io.println("Command failed")
////       pipeline
////     },
////   )
//// }
//// ```

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option}

/// Requested dispatch consistency.
/// Equivalent to Commanded's `:consistency` option.
pub type Consistency {
  /// Events delivered eventually (default).
  Eventual
  /// Dispatch blocks until all strong-consistency handlers have processed events.
  Strong
  /// Dispatch blocks until the named handlers have processed events.
  /// Equivalent to Commanded's `consistency: [MyProjector, "OtherHandler"]`.
  Selective(handlers: List(String))
}

/// The pipeline state that flows through middleware.
///
/// Equivalent to Commanded's `Commanded.Middleware.Pipeline` struct.
pub type Pipeline(command) {
  Pipeline(
    /// The command being dispatched
    command: command,
    /// UUID assigned to this command dispatch
    command_id: String,
    /// Causation ID — becomes causation_id on produced events
    causation_id: Option(String),
    /// Correlation ID — propagated to produced events
    correlation_id: Option(String),
    /// Metadata for events
    metadata: Dict(String, String),
    /// Shared user data passed between middleware stages.
    /// In Commanded this is a map with atom keys and any values.
    /// In Gleam we use Dict(String, String) for type safety.
    assigns: Dict(String, String),
    /// Whether the pipeline has been halted
    halted: Bool,
    /// The response to return to the caller
    response: Option(PipelineResponse),
    /// Requested dispatch consistency
    consistency: Consistency,
    /// The aggregate identity extracted from the command
    identity: Option(String),
    /// Prefix for the aggregate stream ID
    identity_prefix: Option(String),
  )
}

/// Possible pipeline responses.
pub type PipelineResponse {
  PipelineOk
  PipelineError(reason: String)
}

/// A middleware definition with before/after/failure hooks.
///
/// Equivalent to Commanded's `Commanded.Middleware` behaviour with
/// `before_dispatch/1`, `after_dispatch/1`, `after_failure/1` callbacks.
pub type Middleware(command) {
  Middleware(
    before_dispatch: fn(Pipeline(command)) -> Pipeline(command),
    after_dispatch: fn(Pipeline(command)) -> Pipeline(command),
    after_failure: fn(Pipeline(command)) -> Pipeline(command),
  )
}

/// Create a new middleware.
pub fn new(
  before_dispatch before_dispatch: fn(Pipeline(command)) -> Pipeline(command),
  after_dispatch after_dispatch: fn(Pipeline(command)) -> Pipeline(command),
  after_failure after_failure: fn(Pipeline(command)) -> Pipeline(command),
) -> Middleware(command) {
  Middleware(before_dispatch:, after_dispatch:, after_failure:)
}

/// Create a pipeline for a command.
pub fn create_pipeline(
  command: command,
  command_id: String,
  causation_id: Option(String),
  correlation_id: Option(String),
  metadata: Dict(String, String),
) -> Pipeline(command) {
  Pipeline(
    command: command,
    command_id: command_id,
    causation_id: causation_id,
    correlation_id: correlation_id,
    metadata: metadata,
    assigns: dict.new(),
    halted: False,
    response: option.None,
    consistency: Eventual,
    identity: option.None,
    identity_prefix: option.None,
  )
}

/// Put a key-value pair into the assigns map.
pub fn assign(
  pipeline: Pipeline(command),
  key: String,
  value: String,
) -> Pipeline(command) {
  Pipeline(..pipeline, assigns: dict.insert(pipeline.assigns, key, value))
}

/// Put a key-value pair into the metadata map.
/// Equivalent to Commanded's `assign_metadata/3`.
pub fn assign_metadata(
  pipeline: Pipeline(command),
  key: String,
  value: String,
) -> Pipeline(command) {
  Pipeline(..pipeline, metadata: dict.insert(pipeline.metadata, key, value))
}

/// Halt the pipeline, preventing further middleware and command dispatch.
pub fn halt(pipeline: Pipeline(command)) -> Pipeline(command) {
  Pipeline(
    ..pipeline,
    halted: True,
    response: option.Some(PipelineError("halted")),
  )
}

/// Set the response on the pipeline (only if not already set).
pub fn respond(
  pipeline: Pipeline(command),
  response: PipelineResponse,
) -> Pipeline(command) {
  case pipeline.response {
    option.None -> Pipeline(..pipeline, response: option.Some(response))
    option.Some(_) -> pipeline
  }
}

/// Set the consistency on the pipeline.
pub fn with_consistency(
  pipeline: Pipeline(command),
  consistency: Consistency,
) -> Pipeline(command) {
  Pipeline(..pipeline, consistency: consistency)
}

/// Set the identity on the pipeline.
pub fn with_identity(
  pipeline: Pipeline(command),
  identity: String,
) -> Pipeline(command) {
  Pipeline(..pipeline, identity: option.Some(identity))
}

/// Set the identity prefix on the pipeline.
pub fn with_identity_prefix(
  pipeline: Pipeline(command),
  prefix: String,
) -> Pipeline(command) {
  Pipeline(..pipeline, identity_prefix: option.Some(prefix))
}

/// Run the before_dispatch stage through all middleware.
/// Stops if pipeline is halted.
pub fn run_before_dispatch(
  pipeline: Pipeline(command),
  middleware: List(Middleware(command)),
) -> Pipeline(command) {
  list.fold(middleware, pipeline, fn(p, m) {
    case p.halted {
      True -> p
      False -> m.before_dispatch(p)
    }
  })
}

/// Run the after_dispatch stage through all middleware.
/// Stops if pipeline is halted.
pub fn run_after_dispatch(
  pipeline: Pipeline(command),
  middleware: List(Middleware(command)),
) -> Pipeline(command) {
  list.fold(middleware, pipeline, fn(p, m) {
    case p.halted {
      True -> p
      False -> m.after_dispatch(p)
    }
  })
}

/// Run the after_failure stage through all middleware.
/// Always runs through ALL middleware (never stopped by halt).
pub fn run_after_failure(
  pipeline: Pipeline(command),
  middleware: List(Middleware(command)),
) -> Pipeline(command) {
  list.fold(middleware, pipeline, fn(p, m) { m.after_failure(p) })
}
