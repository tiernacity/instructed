//// Aggregate Server - OTP Actor process for aggregate lifecycle management.
////
//// Each aggregate instance runs as a separate Actor process. The server:
//// - Loads aggregate state from the event store on first command
//// - Caches state in memory for subsequent commands
//// - Serializes command execution (one at a time)
//// - Persists events and updates cached state
//// - Supports configurable lifespan (timeout/shutdown)
////
//// ## Example
////
//// ```gleam
//// import instructed/aggregate_server
////
//// let config = aggregate_server.Config(
////   aggregate: my_aggregate,
////   event_store: store,
////   stream_id: "account-123",
//// )
//// let assert Ok(server) = aggregate_server.start(config)
//// let assert Ok(result) = aggregate_server.execute(server, my_command, 5000)
//// ```

import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import instructed/aggregate.{type Aggregate}
import instructed/error.{type DispatchError}
import instructed/event.{EventData}
import instructed/event_store.{type EventStore, ExactVersion}
import instructed/router.{type DispatchResult, DispatchResult}

/// Configuration for starting an aggregate server.
pub type Config(state, command, event) {
  Config(
    /// The aggregate definition
    aggregate: Aggregate(state, command, event),
    /// The event store to use
    event_store: EventStore(event),
    /// The stream ID for this aggregate instance
    stream_id: String,
  )
}

/// The internal state of an aggregate server.
type ServerState(state, command, event) {
  ServerState(
    config: Config(state, command, event),
    aggregate_state: state,
    aggregate_version: Int,
    loaded: Bool,
  )
}

/// Messages the aggregate server can receive.
pub opaque type ServerMessage(state, command, event) {
  Execute(
    command: command,
    causation_id: Option(String),
    correlation_id: Option(String),
    metadata: dict.Dict(String, String),
    reply: Subject(Result(DispatchResult(state, event), DispatchError)),
  )
}

/// Start an aggregate server actor.
pub fn start(
  config: Config(state, command, event),
) -> Result(Subject(ServerMessage(state, command, event)), actor.StartError) {
  let initial_state =
    ServerState(
      config: config,
      aggregate_state: config.aggregate.empty_state(),
      aggregate_version: 0,
      loaded: False,
    )

  actor.new(initial_state)
  |> actor.on_message(handle_message)
  |> actor.start
  |> map_started
}

fn map_started(
  result: Result(
    actor.Started(Subject(ServerMessage(state, command, event))),
    actor.StartError,
  ),
) -> Result(Subject(ServerMessage(state, command, event)), actor.StartError) {
  case result {
    Ok(started) -> Ok(started.data)
    Error(e) -> Error(e)
  }
}

/// Execute a command against the aggregate server.
pub fn execute(
  server: Subject(ServerMessage(state, command, event)),
  command: command,
  timeout: Int,
) -> Result(DispatchResult(state, event), DispatchError) {
  process.call(server, timeout, fn(reply) {
    Execute(
      command: command,
      causation_id: option.None,
      correlation_id: option.None,
      metadata: dict.new(),
      reply: reply,
    )
  })
}

/// Execute a command with explicit context.
pub fn execute_with_context(
  server: Subject(ServerMessage(state, command, event)),
  command: command,
  causation_id: Option(String),
  correlation_id: Option(String),
  metadata: dict.Dict(String, String),
  timeout: Int,
) -> Result(DispatchResult(state, event), DispatchError) {
  process.call(server, timeout, fn(reply) {
    Execute(
      command: command,
      causation_id: causation_id,
      correlation_id: correlation_id,
      metadata: metadata,
      reply: reply,
    )
  })
}

fn handle_message(
  state: ServerState(state, command, event),
  msg: ServerMessage(state, command, event),
) -> actor.Next(ServerState(state, command, event), ServerMessage(state, command, event)) {
  case msg {
    Execute(command, causation_id, correlation_id, metadata, reply) -> {
      // Ensure aggregate state is loaded
      let state = case state.loaded {
        True -> state
        False -> load_state(state)
      }

      // Execute the command
      case state.config.aggregate.execute(state.aggregate_state, command) {
        Error(reason) -> {
          process.send(reply, Error(error.AggregateError(reason)))
          actor.continue(state)
        }
        Ok([]) -> {
          process.send(
            reply,
            Ok(DispatchResult(
              aggregate_state: state.aggregate_state,
              aggregate_version: state.aggregate_version,
              events: [],
            )),
          )
          actor.continue(state)
        }
        Ok(events) -> {
          // Create event data for persistence
          let event_data =
            list.map(events, fn(evt) {
              EventData(
                data: evt,
                causation_id: causation_id,
                correlation_id: correlation_id,
                metadata: metadata,
              )
            })

          // Persist events
          case
            state.config.event_store.append_to_stream(
              state.config.stream_id,
              ExactVersion(state.aggregate_version),
              event_data,
            )
          {
            Error(_) -> {
              process.send(reply, Error(error.WrongExpectedVersion))
              actor.continue(state)
            }
            Ok(new_version) -> {
              // Apply events to state
              let new_agg_state =
                list.fold(
                  events,
                  state.aggregate_state,
                  state.config.aggregate.apply_event,
                )

              let new_state =
                ServerState(
                  ..state,
                  aggregate_state: new_agg_state,
                  aggregate_version: new_version,
                )

              process.send(
                reply,
                Ok(DispatchResult(
                  aggregate_state: new_agg_state,
                  aggregate_version: new_version,
                  events: events,
                )),
              )
              actor.continue(new_state)
            }
          }
        }
      }
    }
  }
}

fn load_state(
  state: ServerState(state, command, event),
) -> ServerState(state, command, event) {
  case state.config.event_store.read_stream_forward(state.config.stream_id, 1, 1000) {
    Ok(recorded_events) -> {
      let events = list.map(recorded_events, fn(e) { e.data })
      let agg_state =
        aggregate.rebuild_state(state.config.aggregate, events)
      let version = list.length(recorded_events)
      ServerState(
        ..state,
        aggregate_state: agg_state,
        aggregate_version: version,
        loaded: True,
      )
    }
    Error(_) -> {
      ServerState(..state, loaded: True)
    }
  }
}
