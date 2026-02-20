//// Projections - build read models from event streams.
////
//// A projection subscribes to domain events and builds a queryable read model.
//// This is the "Query" side of CQRS.
////
//// In Commanded, projections ARE event handlers — there's no separate
//// projection module in core. The `commanded_ecto_projections` library adds
//// database-backed projections. Instructed provides in-memory projections
//// with the same handler guarantees (ack, idempotency, error handling).
////
//// ## Features
////
//// - In-memory state queryable via get_state
//// - Event acknowledgment for backpressure
//// - Idempotency via last_seen_event tracking
//// - Error handling via on_error callback
//// - start_from semantics (only applies on first subscription)
////
//// ## Example
////
//// ```gleam
//// let proj = projection.new(
////   name: "account_balances",
////   initial_state: dict.new(),
////   handle_event: fn(event, _recorded, state) {
////     case event {
////       AccountOpened(num, balance) ->
////         Ok(dict.insert(state, num, balance))
////       _ -> Ok(state)
////     }
////   },
//// )
////
//// let assert Ok(proj_actor) = projection.start(proj, event_store)
//// let state = projection.get_state(proj_actor, 5000)
//// ```

import gleam/erlang/process.{type Subject}
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import instructed/error
import instructed/event.{type RecordedEvent}
import instructed/event_store.{
  type EventStore, type StartFrom, type Subscription, Origin,
}
import instructed/middleware.{type Consistency, Eventual}
import instructed/subscriptions.{type SubMessage}

/// Configuration for a projection.
pub type ProjectionConfig(event, projection_state) {
  ProjectionConfig(
    /// Unique name for this projection (must be stable across restarts)
    name: String,
    /// Initial state for the projection
    initial_state: projection_state,
    /// Function to handle each event and update projection state
    handle_event: fn(event, RecordedEvent(event), projection_state) ->
      Result(projection_state, String),
    /// Where to start from on first subscription creation
    start_from: StartFrom,
    /// Optional error callback
    on_error: Option(
      fn(String, RecordedEvent(event), projection_state) ->
        error.ErrorAction(projection_state),
    ),
    /// Consistency level for this projection.
    /// When Strong, dispatchers using strong consistency will wait for this
    /// projection to process events before returning.
    /// Equivalent to Commanded's projections with strong consistency for
    /// POST/Redirect/GET patterns.
    consistency: Consistency,
    /// Optional subscriptions actor for strong consistency tracking.
    subscriptions: Option(Subject(SubMessage)),
  )
}

/// Create a new projection configuration.
pub fn new(
  name name: String,
  initial_state initial_state: projection_state,
  handle_event handle_event: fn(event, RecordedEvent(event), projection_state) ->
    Result(projection_state, String),
) -> ProjectionConfig(event, projection_state) {
  ProjectionConfig(
    name: name,
    initial_state: initial_state,
    handle_event: handle_event,
    start_from: Origin,
    on_error: None,
    consistency: Eventual,
    subscriptions: None,
  )
}

/// Set where to start from (only applies on first subscription creation).
pub fn with_start_from(
  config: ProjectionConfig(event, projection_state),
  start_from: StartFrom,
) -> ProjectionConfig(event, projection_state) {
  ProjectionConfig(..config, start_from: start_from)
}

/// Set the consistency level and subscriptions actor.
///
/// When set to `Strong` (or `Selective`), the projection registers with the
/// subscriptions actor and acks events after processing. This allows
/// dispatchers using strong consistency to wait for this projection.
///
/// Typical use: POST/Redirect/GET patterns where a command dispatch must
/// guarantee the read model is updated before redirecting.
pub fn with_consistency(
  config: ProjectionConfig(event, projection_state),
  consistency: Consistency,
  subs: Subject(SubMessage),
) -> ProjectionConfig(event, projection_state) {
  ProjectionConfig(..config, consistency: consistency, subscriptions: Some(subs))
}

/// Set the error callback.
pub fn with_error_handler(
  config: ProjectionConfig(event, projection_state),
  on_error: fn(String, RecordedEvent(event), projection_state) ->
    error.ErrorAction(projection_state),
) -> ProjectionConfig(event, projection_state) {
  ProjectionConfig(..config, on_error: Some(on_error))
}

/// Internal state of the projection actor.
type ProjectionActorState(event, projection_state) {
  ProjectionActorState(
    config: ProjectionConfig(event, projection_state),
    state: projection_state,
    event_store: Option(EventStore(event)),
    subscription: Option(Subscription),
    last_seen_event: Option(Int),
    subscriptions_actor: Option(Subject(SubMessage)),
  )
}

/// Messages for the projection actor.
pub opaque type ProjectionMessage(event, projection_state) {
  ProjectionHandleEvent(RecordedEvent(event))
  GetState(Subject(projection_state))
  SetSubscriptionInfo(EventStore(event), Subscription)
}

/// Start a projection, subscribing to the event store.
pub fn start(
  config: ProjectionConfig(event, projection_state),
  event_store: EventStore(event),
) -> Result(Subject(ProjectionMessage(event, projection_state)), String) {
  let actor_state =
    ProjectionActorState(
      config: config,
      state: config.initial_state,
      event_store: None,
      subscription: None,
      last_seen_event: None,
      subscriptions_actor: config.subscriptions,
    )

  case
    actor.new(actor_state)
    |> actor.on_message(handle_projection_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data

      // Register with the subscriptions actor so strong-consistency waiters
      // know about this projection. Also register PID for auto-exclusion.
      case config.subscriptions {
        Some(subs) -> {
          subscriptions.register(subs, config.name, config.consistency)
          subscriptions.register_pid(subs, process.self(), config.name)
        }
        None -> Nil
      }

      let handler = fn(event: RecordedEvent(event)) {
        process.send(subject, ProjectionHandleEvent(event))
      }

      // Try to subscribe (don't delete existing - resume from last position)
      // Subscribe persistently — idempotent (Fix 3).
      // If subscription already exists, the adapter reconnects with the
      // existing checkpoint position, preserving the projection's progress.
      case
        event_store.subscribe_persistent(
          "$all",
          config.name,
          config.start_from,
          handler,
        )
      {
        Ok(subscription) -> {
          process.send(
            subject,
            SetSubscriptionInfo(event_store, subscription),
          )
          Ok(subject)
        }
        Error(_) -> Error("Failed to create subscription")
      }
    }
    Error(_) -> Error("Failed to start projection")
  }
}

/// Query the current state of a projection.
pub fn get_state(
  projection: Subject(ProjectionMessage(event, projection_state)),
  timeout: Int,
) -> projection_state {
  process.call(projection, timeout, fn(reply) { GetState(reply) })
}

fn handle_projection_message(
  state: ProjectionActorState(event, projection_state),
  msg: ProjectionMessage(event, projection_state),
) -> actor.Next(
  ProjectionActorState(event, projection_state),
  ProjectionMessage(event, projection_state),
) {
  case msg {
    SetSubscriptionInfo(es, sub) -> {
      actor.continue(
        ProjectionActorState(
          ..state,
          event_store: Some(es),
          subscription: Some(sub),
        ),
      )
    }

    ProjectionHandleEvent(recorded_event) -> {
      // Idempotency guard
      case state.last_seen_event {
        Some(last) if recorded_event.event_number <= last -> {
          ack_event(state, recorded_event)
          actor.continue(state)
        }
        _ -> {
          case
            state.config.handle_event(
              recorded_event.data,
              recorded_event,
              state.state,
            )
          {
            Ok(new_state) -> {
              ack_event(state, recorded_event)
              actor.continue(
                ProjectionActorState(
                  ..state,
                  state: new_state,
                  last_seen_event: Some(recorded_event.event_number),
                ),
              )
            }
            Error(reason) -> {
              // Error handling: delegate to handle_projection_error
              handle_projection_error(state, reason, recorded_event)
            }
          }
        }
      }
    }

    GetState(reply) -> {
      process.send(reply, state.state)
      actor.continue(state)
    }
  }
}

/// Handle a projection event processing error using the configured error callback.
/// Matches the same semantics as event_handler: stop by default, recursive retry.
fn handle_projection_error(
  state: ProjectionActorState(event, projection_state),
  reason: String,
  recorded_event: RecordedEvent(event),
) -> actor.Next(
  ProjectionActorState(event, projection_state),
  ProjectionMessage(event, projection_state),
) {
  case state.config.on_error {
    None -> {
      // Default: stop on error (matching Commanded's default behavior).
      // Do NOT ack the event so it can be redelivered on restart.
      actor.stop()
    }
    Some(error_fn) -> {
      case error_fn(reason, recorded_event, state.state) {
        error.Skip -> {
          ack_event(state, recorded_event)
          actor.continue(
            ProjectionActorState(
              ..state,
              last_seen_event: Some(recorded_event.event_number),
            ),
          )
        }
        error.Retry(new_state) -> {
          // Retry with updated state. If retry also fails, call on_error
          // again recursively (matching Commanded's recursive retry).
          let updated_state =
            ProjectionActorState(..state, state: new_state)
          case
            state.config.handle_event(
              recorded_event.data,
              recorded_event,
              new_state,
            )
          {
            Ok(final_state) -> {
              ack_event(state, recorded_event)
              actor.continue(
                ProjectionActorState(
                  ..state,
                  state: final_state,
                  last_seen_event: Some(recorded_event.event_number),
                ),
              )
            }
            Error(retry_reason) -> {
              handle_projection_error(
                updated_state,
                retry_reason,
                recorded_event,
              )
            }
          }
        }
        error.RetryWithDelay(delay_ms, new_state) -> {
          process.sleep(delay_ms)
          let updated_state =
            ProjectionActorState(..state, state: new_state)
          case
            state.config.handle_event(
              recorded_event.data,
              recorded_event,
              new_state,
            )
          {
            Ok(final_state) -> {
              ack_event(state, recorded_event)
              actor.continue(
                ProjectionActorState(
                  ..state,
                  state: final_state,
                  last_seen_event: Some(recorded_event.event_number),
                ),
              )
            }
            Error(retry_reason) -> {
              handle_projection_error(
                updated_state,
                retry_reason,
                recorded_event,
              )
            }
          }
        }
        error.Stop(_) -> {
          ack_event(state, recorded_event)
          actor.stop()
        }
      }
    }
  }
}

fn ack_event(
  state: ProjectionActorState(event, projection_state),
  event: RecordedEvent(event),
) -> Nil {
  case state.event_store, state.subscription {
    Some(es), Some(sub) -> {
      let _ = es.ack_event(sub, event)
      Nil
    }
    _, _ -> Nil
  }
  // Ack the subscriptions actor for strong consistency tracking.
  // This unblocks any dispatchers waiting for this projection.
  case state.subscriptions_actor {
    Some(subs) ->
      subscriptions.ack_event(
        subs,
        state.config.name,
        event.stream_id,
        event.stream_version,
      )
    None -> Nil
  }
}
