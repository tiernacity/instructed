//// Command execution context - metadata that accompanies a command dispatch.

import gleam/dict.{type Dict}
import gleam/option.{type Option}

/// Context for command execution, carrying metadata through the pipeline.
pub type CommandContext(command) {
  CommandContext(
    /// The command being dispatched
    command: command,
    /// UUID assigned to this command dispatch
    command_id: String,
    /// UUID identifying the cause of this command
    causation_id: Option(String),
    /// UUID correlating related commands/events
    correlation_id: Option(String),
    /// Metadata to associate with events created by this command
    metadata: Dict(String, String),
    /// Number of retry attempts for version conflicts
    retry_attempts: Int,
  )
}

/// Create a new command context with default values.
pub fn new(command: command, command_id: String) -> CommandContext(command) {
  CommandContext(
    command: command,
    command_id: command_id,
    causation_id: option.None,
    correlation_id: option.None,
    metadata: dict.new(),
    retry_attempts: 3,
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
