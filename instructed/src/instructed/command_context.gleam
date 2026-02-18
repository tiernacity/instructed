//// Command execution context - metadata that accompanies a command dispatch.
////
//// Equivalent to Commanded's `Commanded.Aggregates.ExecutionContext`.
//// In Gleam, we don't need the `handler`, `function`, or `before_execute`
//// fields because we use function records (not behaviours/modules).

import gleam/dict.{type Dict}
import gleam/option.{type Option}

/// What the dispatch should return.
/// Equivalent to Commanded's ExecutionContext `:returning` option.
pub type Returning {
  /// Return Ok/Error only (default, equivalent to Commanded's `false`)
  ReturnNothing
  /// Return the aggregate state after command execution
  ReturnAggregateState
  /// Return the aggregate version after command execution
  ReturnAggregateVersion
  /// Return the events produced by the command
  ReturnEvents
  /// Return the full execution result
  ReturnExecutionResult
}

/// Context for command execution, carrying metadata through the pipeline.
pub type CommandContext(command) {
  CommandContext(
    /// The command being dispatched
    command: command,
    /// UUID assigned to this command dispatch
    command_id: String,
    /// UUID identifying the cause of this command.
    /// Becomes the causation_id of all events produced by this command.
    causation_id: Option(String),
    /// UUID correlating related commands/events.
    /// Propagated to all events produced by this command.
    correlation_id: Option(String),
    /// Metadata to associate with events created by this command
    metadata: Dict(String, String),
    /// Number of retry attempts remaining for version conflicts.
    /// Default: 10 (matches Commanded's default, Invariant 16).
    retry_attempts: Int,
    /// What to return from dispatch.
    /// Default: ReturnNothing (just Ok/Error).
    returning: Returning,
  )
}

/// Create a new command context with default values.
/// Default retry attempts: 10 (matching Commanded, Invariant 16).
pub fn new(command: command, command_id: String) -> CommandContext(command) {
  CommandContext(
    command: command,
    command_id: command_id,
    causation_id: option.None,
    correlation_id: option.None,
    metadata: dict.new(),
    retry_attempts: 10,
    returning: ReturnNothing,
  )
}

/// Set the causation ID.
pub fn with_causation_id(
  ctx: CommandContext(command),
  id: String,
) -> CommandContext(command) {
  CommandContext(..ctx, causation_id: option.Some(id))
}

/// Set the correlation ID.
pub fn with_correlation_id(
  ctx: CommandContext(command),
  id: String,
) -> CommandContext(command) {
  CommandContext(..ctx, correlation_id: option.Some(id))
}

/// Add metadata.
pub fn with_metadata(
  ctx: CommandContext(command),
  key: String,
  value: String,
) -> CommandContext(command) {
  CommandContext(..ctx, metadata: dict.insert(ctx.metadata, key, value))
}

/// Set the number of retry attempts.
pub fn with_retry_attempts(
  ctx: CommandContext(command),
  attempts: Int,
) -> CommandContext(command) {
  CommandContext(..ctx, retry_attempts: attempts)
}

/// Set what to return from dispatch.
pub fn with_returning(
  ctx: CommandContext(command),
  returning: Returning,
) -> CommandContext(command) {
  CommandContext(..ctx, returning: returning)
}

/// Attempt to retry. Decrements the retry counter.
/// Returns Error if no attempts remain.
///
/// Equivalent to Commanded's `ExecutionContext.retry/1`.
pub fn retry(
  ctx: CommandContext(command),
) -> Result(CommandContext(command), Nil) {
  case ctx.retry_attempts > 0 {
    True ->
      Ok(CommandContext(..ctx, retry_attempts: ctx.retry_attempts - 1))
    False -> Error(Nil)
  }
}
