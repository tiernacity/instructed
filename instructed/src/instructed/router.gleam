//// Command routing - dispatches commands to aggregates.
////
//// The router is the main entry point for dispatching commands in Instructed.
//// It dispatches commands THROUGH aggregate server processes, ensuring
//// per-instance command serialization (Invariant 1).
////
//// This is equivalent to Commanded's `Commanded.Commands.Router` and
//// `Commanded.Commands.Dispatcher`.
////
//// ## How Dispatch Works
////
//// 1. Extract aggregate identity from the command
//// 2. Validate identity is non-empty
//// 3. Run before_dispatch middleware
//// 4. Look up or start an aggregate server for this identity
//// 5. Execute command through the aggregate server (serialized)
//// 6. Run after_dispatch or after_failure middleware
//// 7. Return result
////
//// ## Command Serialization (Invariant 1)
////
//// Commands to the SAME aggregate instance are serialized via the
//// aggregate server's actor mailbox. Commands to DIFFERENT aggregates
//// run in parallel (different processes).
////
//// ## Example
////
//// ```gleam
//// let my_router = router.new(
////   aggregate: bank_aggregate(),
////   event_store: store,
////   identity: fn(cmd) { cmd.account_number },
//// )
////
//// let assert Ok(_) = router.dispatch(my_router, OpenAccount("ACC1", 100))
//// ```

import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/string
import instructed/aggregate.{type Aggregate}
import instructed/aggregate_server
import instructed/dispatch_result
import instructed/error.{type DispatchError}
import instructed/event_store.{type EventStore}
import instructed/middleware.{type Middleware, type Pipeline}
import instructed/snapshot
import instructed/subscriptions.{type SubMessage}
import instructed/telemetry
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
    /// Number of retry attempts on version conflicts.
    /// Default: 10 (matches Commanded, Invariant 16).
    retry_attempts: Int,
    /// Optional snapshot configuration
    snapshot_every: Option(Int),
    /// Dispatch timeout in milliseconds.
    /// Default: 5000 (matches Commanded, Invariant 17).
    dispatch_timeout: Int,
    /// Registry for aggregate server processes.
    /// Started automatically when the router is created.
    registry: Option(Subject(RegistryMessage(state, command, event))),
    /// Optional subscriptions actor for strong consistency tracking.
    /// When set, `dispatch` with `consistency: Strong` will block until all
    /// registered strong handlers have acked the events produced by the command.
    subscriptions: Option(Subject(SubMessage)),
  )
}

/// Re-export DispatchResult from the shared module.
pub type DispatchResult(state, event) =
  dispatch_result.DispatchResult(state, event)

/// Create a new router with an aggregate server registry.
///
/// This starts a registry actor that manages aggregate server processes.
/// Each unique aggregate identity gets its own server for command serialization.
pub fn new(
  aggregate aggregate: Aggregate(state, command, event),
  event_store event_store: EventStore(event),
  identity identity: fn(command) -> String,
) -> Router(state, command, event) {
  // Start the aggregate server registry
  let registry = start_registry()

  Router(
    aggregate: aggregate,
    event_store: event_store,
    identity: identity,
    identity_prefix: "",
    middleware: [],
    retry_attempts: 10,
    snapshot_every: None,
    dispatch_timeout: 5000,
    registry: registry,
    subscriptions: None,
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

/// Set the dispatch timeout.
pub fn with_dispatch_timeout(
  router: Router(state, command, event),
  timeout: Int,
) -> Router(state, command, event) {
  Router(..router, dispatch_timeout: timeout)
}

/// Set snapshot interval.
pub fn with_snapshot_every(
  router: Router(state, command, event),
  every: Int,
) -> Router(state, command, event) {
  Router(..router, snapshot_every: Some(every))
}

/// Set the subscriptions actor for strong consistency tracking.
///
/// When set, dispatching with `consistency: Strong` will block until all
/// registered strong-consistency handlers have processed the produced events.
///
/// Equivalent to Commanded's `Subscriptions.wait_for/5` integration in the dispatcher.
pub fn with_subscriptions(
  router: Router(state, command, event),
  subs: Subject(SubMessage),
) -> Router(state, command, event) {
  Router(..router, subscriptions: Some(subs))
}

/// Dispatch a command through the router.
///
/// Automatically generates a command_id and correlation_id.
/// The command_id becomes the causation_id on produced events.
pub fn dispatch(
  router: Router(state, command, event),
  command: command,
) -> Result(DispatchResult(state, event), DispatchError) {
  let command_id = uuid.v4_string()
  let correlation_id = uuid.v4_string()
  dispatch_with_context(
    router,
    command,
    command_id,
    // causation_id = command_id (the command caused the events)
    Some(command_id),
    Some(correlation_id),
    dict.new(),
  )
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
  // Extract and validate identity
  let aggregate_id = router.identity(command)
  case string.is_empty(string.trim(aggregate_id)) {
    True -> Error(error.AggregateError("aggregate identity must not be empty"))
    False -> {
      let stream_id = router.identity_prefix <> aggregate_id

      // Create middleware pipeline
      let pipeline =
        middleware.create_pipeline(
          command,
          command_id,
          causation_id,
          correlation_id,
          metadata,
        )
        |> middleware.with_identity(aggregate_id)
        |> middleware.with_identity_prefix(router.identity_prefix)

      // Run before_dispatch middleware
      let pipeline =
        middleware.run_before_dispatch(pipeline, router.middleware)

      // Check if halted by middleware
      case pipeline.halted {
        True -> Error(error.Halted)
        False -> {
          let t_start = telemetry.system_time()
          telemetry.emit(telemetry.CommandDispatchStart(
            command_id: command_id,
            aggregate_stream_id: stream_id,
            system_time: t_start,
          ))
          let result = dispatch_through_server(router, pipeline, stream_id)
          case result {
            Ok(dr) ->
              telemetry.emit(telemetry.CommandDispatchStop(
                command_id: command_id,
                aggregate_stream_id: stream_id,
                duration_ns: telemetry.system_time() - t_start,
                event_count: list.length(dr.events),
              ))
            Error(err) ->
              telemetry.emit(telemetry.CommandDispatchException(
                command_id: command_id,
                aggregate_stream_id: stream_id,
                duration_ns: telemetry.system_time() - t_start,
                error: string.inspect(err),
              ))
          }
          result
        }
      }
    }
  }
}

/// Dispatch through the aggregate server for command serialization.
fn dispatch_through_server(
  router: Router(state, command, event),
  pipeline: Pipeline(command),
  stream_id: String,
) -> Result(DispatchResult(state, event), DispatchError) {
  // Get or create aggregate server for this stream
  case get_or_start_server(router, stream_id) {
    Error(reason) -> {
      let _ = middleware.run_after_failure(pipeline, router.middleware)
      Error(error.AggregateStartError(reason))
    }
    Ok(server) -> {
      // Execute through aggregate server (serialized, with retry)
      case
        aggregate_server.execute_with_context(
          server,
          pipeline.command,
          pipeline.causation_id,
          pipeline.correlation_id,
          pipeline.metadata,
          router.dispatch_timeout,
        )
      {
        Ok(result) -> {
          let _ = middleware.run_after_dispatch(pipeline, router.middleware)
          // Wait for strong-consistency handlers if requested (Invariant 11)
          case pipeline.consistency, router.subscriptions {
            middleware.Strong, Some(subs) -> {
              case
                subscriptions.wait_for(
                  subs,
                  stream_id,
                  result.aggregate_version,
                  router.dispatch_timeout,
                )
              {
                Ok(Nil) -> Ok(result)
                Error(Nil) -> Error(error.ConsistencyTimeout)
              }
            }
            _, _ -> Ok(result)
          }
        }
        Error(err) -> {
          let _ = middleware.run_after_failure(pipeline, router.middleware)
          Error(err)
        }
      }
    }
  }
}

// --- Aggregate Server Registry ---
// Manages aggregate server processes per stream_id.
// This is a simplified version of Commanded's DynamicSupervisor + Registration.

/// Message type for the registry actor.
pub opaque type RegistryMessage(state, command, event) {
  GetOrStart(
    stream_id: String,
    aggregate: Aggregate(state, command, event),
    event_store: EventStore(event),
    retry_attempts: Int,
    snapshot_every: Option(Int),
    reply: Subject(
      Result(
        Subject(
          aggregate_server.ServerMessage(state, command, event),
        ),
        String,
      ),
    ),
  )
}

type RegistryState(state, command, event) {
  RegistryState(
    servers: dict.Dict(
      String,
      Subject(aggregate_server.ServerMessage(state, command, event)),
    ),
  )
}

fn start_registry() -> Option(
  Subject(RegistryMessage(state, command, event)),
) {
  case
    actor.new(RegistryState(servers: dict.new()))
    |> actor.on_message(handle_registry_message)
    |> actor.start
  {
    Ok(started) -> Some(started.data)
    Error(_) -> None
  }
}

fn handle_registry_message(
  state: RegistryState(state, command, event),
  msg: RegistryMessage(state, command, event),
) -> actor.Next(
  RegistryState(state, command, event),
  RegistryMessage(state, command, event),
) {
  case msg {
    GetOrStart(stream_id, aggregate, event_store, retry_attempts, snapshot_every, reply) -> {
      case dict.get(state.servers, stream_id) {
        Ok(server) -> {
          process.send(reply, Ok(server))
          actor.continue(state)
        }
        Error(_) -> {
          // Start a new aggregate server
          let snap_config = case snapshot_every {
            Some(n) ->
              snapshot.SnapshotConfig(
                snapshot_every: Some(n),
                snapshot_version: 1,
              )
            None -> snapshot.default_config()
          }

          let config =
            aggregate_server.new_config(
              aggregate: aggregate,
              event_store: event_store,
              stream_id: stream_id,
            )
            |> aggregate_server.with_retry_attempts(retry_attempts)
            |> aggregate_server.with_snapshot_config(snap_config)

          case aggregate_server.start(config) {
            Ok(server) -> {
              let new_state =
                RegistryState(
                  servers: dict.insert(state.servers, stream_id, server),
                )
              process.send(reply, Ok(server))
              actor.continue(new_state)
            }
            Error(_) -> {
              process.send(reply, Error("Failed to start aggregate server"))
              actor.continue(state)
            }
          }
        }
      }
    }
  }
}

fn get_or_start_server(
  router: Router(state, command, event),
  stream_id: String,
) -> Result(
  Subject(aggregate_server.ServerMessage(state, command, event)),
  String,
) {
  case router.registry {
    None -> Error("Router registry not started")
    Some(registry) ->
      process.call(registry, 5000, fn(reply) {
        GetOrStart(
          stream_id,
          router.aggregate,
          router.event_store,
          router.retry_attempts,
          router.snapshot_every,
          reply,
        )
      })
  }
}
