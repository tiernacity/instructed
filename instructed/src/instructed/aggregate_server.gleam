//// Aggregate Server - OTP Actor process for aggregate lifecycle management.
////
//// Each aggregate instance runs as a separate Actor process, providing:
//// - **Command serialization**: commands to the same aggregate are processed
////   one at a time via the actor's mailbox (Invariant 1)
//// - **State caching**: aggregate state is loaded once and cached in memory
//// - **Optimistic concurrency with retry**: on version conflict, the server
////   rebuilds state from new events only (not full replay) and retries
////   (Invariant 4), up to max_retry_attempts (default 10, Invariant 16)
//// - **Snapshot integration**: reads snapshot on startup, takes snapshot
////   after configured number of events
//// - **Self-subscription**: subscribes to its own stream to catch externally
////   appended events and stay in sync
////
//// This is equivalent to Commanded's `Commanded.Aggregates.Aggregate`
//// GenServer, which provides the same guarantees.
////
//// ## Example
////
//// ```gleam
//// import instructed/aggregate_server
////
//// let config = aggregate_server.new_config(
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
import gleam/option.{type Option, None}
import gleam/otp/actor
import instructed/aggregate.{type Aggregate}
import instructed/error.{type DispatchError}
import instructed/event.{type RecordedEvent, EventData}
import instructed/event_store.{type EventStore, ExactVersion}
import instructed/router.{type DispatchResult, DispatchResult}
import instructed/snapshot.{type SnapshotConfig}

/// Default retry attempts on version conflict.
/// Matches Commanded's default of 10 (Invariant 16).
pub const default_retry_attempts = 10

/// Default command execution timeout in milliseconds.
/// Matches Commanded's default of 5000ms (Invariant 17).
pub const default_timeout = 5000

/// Configuration for starting an aggregate server.
pub type Config(state, command, event) {
  Config(
    /// The aggregate definition
    aggregate: Aggregate(state, command, event),
    /// The event store to use
    event_store: EventStore(event),
    /// The stream ID for this aggregate instance
    stream_id: String,
    /// Maximum retry attempts on version conflict (default: 10)
    retry_attempts: Int,
    /// Snapshot configuration
    snapshot_config: SnapshotConfig,
  )
}

/// Create a new config with defaults.
pub fn new_config(
  aggregate aggregate: Aggregate(state, command, event),
  event_store event_store: EventStore(event),
  stream_id stream_id: String,
) -> Config(state, command, event) {
  Config(
    aggregate: aggregate,
    event_store: event_store,
    stream_id: stream_id,
    retry_attempts: default_retry_attempts,
    snapshot_config: snapshot.default_config(),
  )
}

/// Set retry attempts on the config.
pub fn with_retry_attempts(
  config: Config(state, command, event),
  attempts: Int,
) -> Config(state, command, event) {
  Config(..config, retry_attempts: attempts)
}

/// Set snapshot configuration.
pub fn with_snapshot_config(
  config: Config(state, command, event),
  snapshot_config: SnapshotConfig,
) -> Config(state, command, event) {
  Config(..config, snapshot_config: snapshot_config)
}

/// The internal state of an aggregate server.
type ServerState(state, command, event) {
  ServerState(
    config: Config(state, command, event),
    aggregate_state: state,
    aggregate_version: Int,
    loaded: Bool,
    /// Number of events since last snapshot
    events_since_snapshot: Int,
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
  /// Internal: received from self-subscription when external events arrive
  ExternalEvent(RecordedEvent(event))
}

/// Start an aggregate server actor.
/// The server loads state from the event store on first command.
pub fn start(
  config: Config(state, command, event),
) -> Result(Subject(ServerMessage(state, command, event)), actor.StartError) {
  let initial_state =
    ServerState(
      config: config,
      aggregate_state: config.aggregate.empty_state(),
      aggregate_version: 0,
      loaded: False,
      events_since_snapshot: 0,
    )

  case
    actor.new(initial_state)
    |> actor.on_message(handle_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data

      // Subscribe to the aggregate's own stream to catch external events.
      // This matches Commanded's handle_continue(:subscribe_to_events).
      let handler = fn(evt: RecordedEvent(event)) {
        process.send(subject, ExternalEvent(evt))
      }
      let _ = config.event_store.subscribe_to_stream(config.stream_id, handler)

      Ok(subject)
    }
    Error(e) -> Error(e)
  }
}

/// Execute a command against the aggregate server.
/// Timeout defaults to 5000ms (matching Commanded's default).
pub fn execute(
  server: Subject(ServerMessage(state, command, event)),
  command: command,
  timeout: Int,
) -> Result(DispatchResult(state, event), DispatchError) {
  process.call(server, timeout, fn(reply) {
    Execute(
      command: command,
      causation_id: None,
      correlation_id: None,
      metadata: dict.new(),
      reply: reply,
    )
  })
}

/// Execute a command with explicit causation/correlation context.
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
) -> actor.Next(
  ServerState(state, command, event),
  ServerMessage(state, command, event),
) {
  case msg {
    Execute(command, causation_id, correlation_id, metadata, reply) -> {
      // Ensure aggregate state is loaded from event store
      let state = case state.loaded {
        True -> state
        False -> load_state(state)
      }

      // Execute command with retry on version conflict
      let #(result, new_state) =
        execute_with_retry(
          state,
          command,
          causation_id,
          correlation_id,
          metadata,
          state.config.retry_attempts,
        )

      process.send(reply, result)
      actor.continue(new_state)
    }

    ExternalEvent(recorded_event) -> {
      // Handle externally appended events (from self-subscription).
      // Only apply events we haven't seen yet.
      case recorded_event.stream_version > state.aggregate_version {
        True -> {
          // Verify it's the expected next version
          case recorded_event.stream_version == state.aggregate_version + 1 {
            True -> {
              let new_agg_state =
                state.config.aggregate.apply_event(
                  state.aggregate_state,
                  recorded_event.data,
                )
              actor.continue(
                ServerState(
                  ..state,
                  aggregate_state: new_agg_state,
                  aggregate_version: recorded_event.stream_version,
                ),
              )
            }
            False -> {
              // Gap in events - reload from event store
              actor.continue(load_state(state))
            }
          }
        }
        False ->
          // Already seen this event, ignore
          actor.continue(state)
      }
    }
  }
}

/// Execute command with automatic retry on version conflict.
/// On conflict, rebuilds state from NEW events only (not full replay)
/// then retries. Matches Commanded's retry behavior (Invariant 4).
fn execute_with_retry(
  state: ServerState(state, command, event),
  command: command,
  causation_id: Option(String),
  correlation_id: Option(String),
  metadata: dict.Dict(String, String),
  remaining_attempts: Int,
) -> #(Result(DispatchResult(state, event), DispatchError), ServerState(state, command, event)) {
  case remaining_attempts <= 0 {
    True -> #(Error(error.TooManyAttempts), state)
    False -> {
      case execute_once(state, command, causation_id, correlation_id, metadata) {
        #(Ok(result), new_state) -> #(Ok(result), new_state)

        #(Error(error.WrongExpectedVersion), _) -> {
          // Version conflict - rebuild state from new events and retry
          let rebuilt_state = rebuild_from_current_version(state)
          execute_with_retry(
            rebuilt_state,
            command,
            causation_id,
            correlation_id,
            metadata,
            remaining_attempts - 1,
          )
        }

        #(Error(err), new_state) -> #(Error(err), new_state)
      }
    }
  }
}

/// Execute the command once (no retry).
fn execute_once(
  state: ServerState(state, command, event),
  command: command,
  causation_id: Option(String),
  correlation_id: Option(String),
  metadata: dict.Dict(String, String),
) -> #(Result(DispatchResult(state, event), DispatchError), ServerState(state, command, event)) {
  case state.config.aggregate.execute(state.aggregate_state, command) {
    Error(reason) -> #(Error(error.AggregateError(reason)), state)

    Ok([]) ->
      #(
        Ok(DispatchResult(
          aggregate_state: state.aggregate_state,
          aggregate_version: state.aggregate_version,
          events: [],
        )),
        state,
      )

    Ok(events) -> {
      let event_data =
        list.map(events, fn(evt) {
          EventData(
            data: evt,
            event_type: "",
            causation_id: causation_id,
            correlation_id: correlation_id,
            metadata: metadata,
          )
        })

      case
        state.config.event_store.append_to_stream(
          state.config.stream_id,
          ExactVersion(state.aggregate_version),
          event_data,
        )
      {
        Error(error.VersionConflict) ->
          #(Error(error.WrongExpectedVersion), state)

        Error(error.StreamAlreadyExists) ->
          #(Error(error.WrongExpectedVersion), state)

        Error(err) -> {
          let reason = case err {
            error.StreamNotFound -> "stream not found"
            error.SnapshotNotFound -> "snapshot not found"
            error.SubscriptionAlreadyExists -> "subscription already exists"
            error.SubscriptionNotFound -> "subscription not found"
            error.TooManySubscribers -> "too many subscribers"
            error.StorageError(r) -> "storage error: " <> r
            _ -> "unknown error"
          }
          #(Error(error.EventStoreError(reason)), state)
        }

        Ok(new_version) -> {
          // Apply events to cached state
          let new_agg_state =
            list.fold(
              events,
              state.aggregate_state,
              state.config.aggregate.apply_event,
            )

          let events_since =
            state.events_since_snapshot + list.length(events)

          let new_state =
            ServerState(
              ..state,
              aggregate_state: new_agg_state,
              aggregate_version: new_version,
              events_since_snapshot: events_since,
            )

          // Maybe take snapshot
          let new_state = maybe_take_snapshot(new_state)

          #(
            Ok(DispatchResult(
              aggregate_state: new_agg_state,
              aggregate_version: new_version,
              events: events,
            )),
            new_state,
          )
        }
      }
    }
  }
}

/// Rebuild state from the current version (incremental, not full replay).
/// This reads only events after the current version.
fn rebuild_from_current_version(
  state: ServerState(state, command, event),
) -> ServerState(state, command, event) {
  case
    aggregate.rebuild_from_version(
      state.config.aggregate,
      state.config.event_store,
      state.config.stream_id,
      state.aggregate_state,
      state.aggregate_version,
    )
  {
    Ok(populated) ->
      ServerState(
        ..state,
        aggregate_state: populated.state,
        aggregate_version: populated.version,
      )
    Error(_) -> state
  }
}

/// Load aggregate state from the event store (full load with snapshot support).
fn load_state(
  state: ServerState(state, command, event),
) -> ServerState(state, command, event) {
  case
    aggregate.populate_from_event_store(
      state.config.aggregate,
      state.config.event_store,
      state.config.stream_id,
      state.config.snapshot_config,
    )
  {
    Ok(populated) ->
      ServerState(
        ..state,
        aggregate_state: populated.state,
        aggregate_version: populated.version,
        loaded: True,
        events_since_snapshot: 0,
      )
    Error(_) ->
      ServerState(..state, loaded: True)
  }
}

/// Take a snapshot if configured and threshold reached.
/// Note: Snapshot type integration is completed in Module 6.
/// The EventStore snapshot operations use the event type parameter,
/// but aggregate snapshots store state. Module 6 adds proper
/// serialization support to bridge this gap.
fn maybe_take_snapshot(
  state: ServerState(state, command, event),
) -> ServerState(state, command, event) {
  // TODO: Enable after Module 6 adds snapshot serialization support
  // For now, just track the count for when snapshots are enabled
  state
}


