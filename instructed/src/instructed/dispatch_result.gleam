//// The result of a successful command dispatch.
////
//// Shared between router and aggregate_server to avoid import cycles.

/// The result of a successful command dispatch.
///
/// Equivalent to Commanded's `Commanded.Commands.ExecutionResult`.
pub type DispatchResult(state, event) {
  DispatchResult(
    /// The aggregate state after command execution
    aggregate_state: state,
    /// The aggregate version after command execution
    aggregate_version: Int,
    /// The events produced by the command
    events: List(event),
  )
}
