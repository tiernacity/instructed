//// Command routing - dispatches commands to aggregates.
////
//// The router is the main entry point for dispatching commands in Instructed.
//// It handles:
//// - Looking up or creating aggregate instances
//// - Running middleware pipeline
//// - Executing commands against aggregates
//// - Persisting resulting events to the event store
//// - Retry on version conflicts
////
//// ## Example
////
//// ```gleam
//// import instructed/router
//// import instructed/aggregate
//// import instructed/event_store
////
//// let my_router = router.new(
////   aggregate: bank_aggregate(),
////   event_store: store,
////   identity: fn(cmd) { cmd.account_number },
////   middleware: [],
//// )
////
//// let assert Ok(_) = router.dispatch(my_router, OpenAccount("ACC1", 100))
//// ```

import gleam/dict
import gleam/list
import gleam/option.{type Option}
import instructed/aggregate.{type Aggregate}
import instructed/error.{type DispatchError}
import instructed/event.{EventData}
import instructed/event_store.{type EventStore, type ExpectedVersion, ExactVersion}
import instructed/middleware.{type Middleware, type Pipeline}
import youid/uuid

/// A command router configuration.
pub type Router(state, command, event) {
  Router(
    /// The aggregate definition
    aggregate: Aggregate(state, command, event),
    /// The event store to use
    event_store: EventStore(event),
    /// Function to extract the aggregate identity from a command
    identity: fn(command) -> String,
    /// Optional prefix for aggregate stream IDs
    identity_prefix: String,
    /// Middleware pipeline
    middleware: List(Middleware(command)),
    /// Number of retry attempts on version conflicts
    retry_attempts: Int,
    /// Optional snapshot configuration
    snapshot_every: Option(Int),
  )
}

/// The result of a successful command dispatch.
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

/// Create a new router.
pub fn new(
  aggregate aggregate: Aggregate(state, command, event),
  event_store event_store: EventStore(event),
  identity identity: fn(command) -> String,
) -> Router(state, command, event) {
  Router(
    aggregate: aggregate,
    event_store: event_store,
    identity: identity,
    identity_prefix: "",
    middleware: [],
    retry_attempts: 3,
    snapshot_every: option.None,
  )
}

/// Set the identity prefix for stream IDs.
pub fn with_prefix(
  router: Router(state, command, event),
  prefix: String,
) -> Router(state, command, event) {
  Router(..router, identity_prefix: prefix)
}

/// Add middleware to the router.
pub fn with_middleware(
  router: Router(state, command, event),
  mw: Middleware(command),
) -> Router(state, command, event) {
  Router(..router, middleware: list.append(router.middleware, [mw]))
}

/// Set the number of retry attempts.
pub fn with_retry_attempts(
  router: Router(state, command, event),
  attempts: Int,
) -> Router(state, command, event) {
  Router(..router, retry_attempts: attempts)
}

/// Set snapshot interval.
pub fn with_snapshot_every(
  router: Router(state, command, event),
  every: Int,
) -> Router(state, command, event) {
  Router(..router, snapshot_every: option.Some(every))
}

/// Dispatch a command through the router.
///
/// This is the main entry point for command processing. It:
/// 1. Extracts the aggregate identity from the command
/// 2. Runs before_dispatch middleware
/// 3. Loads the aggregate state from the event store
/// 4. Executes the command against the aggregate
/// 5. Persists any resulting events
/// 6. Runs after_dispatch or after_failure middleware
///
pub fn dispatch(
  router: Router(state, command, event),
  command: command,
) -> Result(DispatchResult(state, event), DispatchError) {
  let command_id = uuid.v4_string()
  dispatch_with_context(router, command, command_id, option.None, option.None, dict.new())
}

/// Dispatch a command with explicit context (causation_id, correlation_id, metadata).
pub fn dispatch_with_context(
  router: Router(state, command, event),
  command: command,
  command_id: String,
  causation_id: Option(String),
  correlation_id: Option(String),
  metadata: dict.Dict(String, String),
) -> Result(DispatchResult(state, event), DispatchError) {
  // Create middleware pipeline
  let pipeline =
    middleware.create_pipeline(
      command,
      command_id,
      causation_id,
      correlation_id,
      metadata,
    )

  // Run before_dispatch middleware
  let pipeline = middleware.run_before_dispatch(pipeline, router.middleware)

  // Check if halted by middleware
  case pipeline.halted {
    True -> Error(error.Halted)
    False ->
      dispatch_to_aggregate(router, pipeline, router.retry_attempts)
  }
}

fn dispatch_to_aggregate(
  router: Router(state, command, event),
  pipeline: Pipeline(command),
  remaining_attempts: Int,
) -> Result(DispatchResult(state, event), DispatchError) {
  let aggregate_id = router.identity(pipeline.command)
  let stream_id = router.identity_prefix <> aggregate_id

  // Load aggregate state from event store
  let load_result = load_aggregate_state(router, stream_id)

  case load_result {
    Error(reason) -> {
      let failed_pipeline =
        middleware.run_after_failure(pipeline, router.middleware)
      let _ = failed_pipeline
      Error(error.EventStoreError(reason))
    }
    Ok(#(state, version)) -> {
      // Execute command against aggregate
      case router.aggregate.execute(state, pipeline.command) {
        Error(reason) -> {
          let failed_pipeline =
            middleware.run_after_failure(pipeline, router.middleware)
          let _ = failed_pipeline
          Error(error.AggregateError(reason))
        }
        Ok([]) -> {
          // No events produced - command accepted but nothing changed
          let success_pipeline =
            middleware.run_after_dispatch(pipeline, router.middleware)
          let _ = success_pipeline
          Ok(DispatchResult(
            aggregate_state: state,
            aggregate_version: version,
            events: [],
          ))
        }
        Ok(events) -> {
          // Convert domain events to event data
          let event_data =
            list.map(events, fn(evt) {
              EventData(
                data: evt,
                causation_id: pipeline.causation_id,
                correlation_id: pipeline.correlation_id,
                metadata: pipeline.metadata,
              )
            })

          // Determine expected version
          let expected_version: ExpectedVersion = ExactVersion(version)

          // Persist events
          case
            router.event_store.append_to_stream(
              stream_id,
              expected_version,
              event_data,
            )
          {
            Error(event_store_err) -> {
              // Check if it's a version conflict and we can retry
              case event_store_err, remaining_attempts > 0 {
                error.VersionConflict, True ->
                  dispatch_to_aggregate(
                    router,
                    pipeline,
                    remaining_attempts - 1,
                  )
                _, _ -> {
                  let failed_pipeline =
                    middleware.run_after_failure(pipeline, router.middleware)
                  let _ = failed_pipeline
                  Error(error.WrongExpectedVersion)
                }
              }
            }
            Ok(new_version) -> {
              // Apply events to get final aggregate state
              let new_state =
                list.fold(events, state, router.aggregate.apply_event)

              let success_pipeline =
                middleware.run_after_dispatch(pipeline, router.middleware)
              let _ = success_pipeline

              Ok(DispatchResult(
                aggregate_state: new_state,
                aggregate_version: new_version,
                events: events,
              ))
            }
          }
        }
      }
    }
  }
}

fn load_aggregate_state(
  router: Router(state, command, event),
  stream_id: String,
) -> Result(#(state, Int), String) {
  // Read all events with large batch size (matches Commanded's 1000 default)
  case router.event_store.read_stream_forward(stream_id, 1, 1000) {
    Ok(recorded_events) -> {
      let events = list.map(recorded_events, fn(e) { e.data })
      let state = aggregate.rebuild_state(router.aggregate, events)
      let version = list.length(recorded_events)
      Ok(#(state, version))
    }
    Error(error.StreamNotFound) -> {
      // New aggregate - return empty state
      Ok(#(router.aggregate.empty_state(), 0))
    }
    Error(err) -> {
      let reason = case err {
        error.VersionConflict -> "version conflict"
        error.StreamNotFound -> "stream not found"
        error.StreamAlreadyExists -> "stream already exists"
        error.SnapshotNotFound -> "snapshot not found"
        error.SubscriptionAlreadyExists -> "subscription already exists"
        error.SubscriptionNotFound -> "subscription not found"
        error.StorageError(r) -> "storage error: " <> r
      }
      Error(reason)
    }
  }
}


