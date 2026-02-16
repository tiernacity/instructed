//// Middleware for the command processing pipeline.
////
//// Middleware provides an extension point to add functions that you want to be
//// called for every command dispatched through the router.
////
//// ## Example
////
//// ```gleam
//// import instructed/middleware.{type Pipeline}
////
//// fn logging_middleware() -> Middleware(cmd, event) {
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

/// The pipeline state that flows through middleware.
pub type Pipeline(command) {
  Pipeline(
    /// The command being dispatched
    command: command,
    /// UUID assigned to this command dispatch
    command_id: String,
    /// Causation ID
    causation_id: Option(String),
    /// Correlation ID
    correlation_id: Option(String),
    /// Metadata for events
    metadata: Dict(String, String),
    /// Shared user data
    assigns: Dict(String, String),
    /// Whether the pipeline has been halted
    halted: Bool,
    /// The response to return to the caller
    response: Option(PipelineResponse),
  )
}

/// Possible pipeline responses.
pub type PipelineResponse {
  PipelineOk
  PipelineError(reason: String)
}

/// A middleware definition with before/after/failure hooks.
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

/// Halt the pipeline, preventing further middleware and command dispatch.
pub fn halt(pipeline: Pipeline(command)) -> Pipeline(command) {
  Pipeline(
    ..pipeline,
    halted: True,
    response: option.Some(PipelineError("halted")),
  )
}

/// Set the response on the pipeline.
pub fn respond(
  pipeline: Pipeline(command),
  response: PipelineResponse,
) -> Pipeline(command) {
  case pipeline.response {
    option.None -> Pipeline(..pipeline, response: option.Some(response))
    option.Some(_) -> pipeline
  }
}

/// Run the before_dispatch stage through all middleware.
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
pub fn run_after_failure(
  pipeline: Pipeline(command),
  middleware: List(Middleware(command)),
) -> Pipeline(command) {
  list.fold(middleware, pipeline, fn(p, m) { m.after_failure(p) })
}
