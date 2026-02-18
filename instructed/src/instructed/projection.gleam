//// Projections - build read models from event streams.
////
//// A projection subscribes to domain events and builds a queryable read model.
//// This is the "Query" side of CQRS.
////
//// After processing each event, the projection acknowledges it to the event
//// store, enabling backpressure (next event won't be delivered until ack).
////
//// ## Example
////
//// ```gleam
//// import instructed/projection
////
//// let proj = projection.new(
////   name: "account_balances",
////   initial_state: dict.new(),
////   handle_event: fn(event, _recorded, state) {
////     case event {
////       AccountOpened(num, balance) ->
////         Ok(dict.insert(state, num, AccountBalance(num, balance)))
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
import instructed/event.{type RecordedEvent}
import instructed/event_store.{
  type EventStore, type Subscription, Origin,
}

/// Configuration for a projection.
pub type ProjectionConfig(event, projection_state) {
  ProjectionConfig(
    /// Unique name for this projection
    name: String,
    /// Initial state for the projection
    initial_state: projection_state,
    /// Function to handle each event and update projection state
    handle_event: fn(event, RecordedEvent(event), projection_state) ->
      Result(projection_state, String),
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
  )
}

/// Internal state of the projection actor.
type ProjectionActorState(event, projection_state) {
  ProjectionActorState(
    config: ProjectionConfig(event, projection_state),
    state: projection_state,
    /// Event store reference for acknowledging events
    event_store: Option(EventStore(event)),
    /// Subscription reference for acknowledging events
    subscription: Option(Subscription),
  )
}

/// Messages for the projection actor.
pub opaque type ProjectionMessage(event, projection_state) {
  ProjectionHandleEvent(RecordedEvent(event))
  GetState(Subject(projection_state))
  /// Internal: set subscription info for event acknowledgment
  SetSubscriptionInfo(EventStore(event), Subscription)
}

/// Start a projection, subscribing to the event store.
/// Events are acknowledged after processing to enable backpressure.
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
    )

  case
    actor.new(actor_state)
    |> actor.on_message(handle_projection_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data

      // Non-blocking handler: sends event to actor's mailbox
      let handler = fn(event: RecordedEvent(event)) {
        process.send(subject, ProjectionHandleEvent(event))
      }

      // Delete any existing subscription first (for restarts)
      let _ = event_store.delete_subscription("$all", config.name)

      case
        event_store.subscribe_persistent(
          "$all",
          config.name,
          Origin,
          handler,
        )
      {
        Ok(subscription) -> {
          // Send subscription info to actor so it can ack events
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
      case
        state.config.handle_event(
          recorded_event.data,
          recorded_event,
          state.state,
        )
      {
        Ok(new_state) -> {
          // Acknowledge the event for backpressure
          ack_event(state, recorded_event)
          actor.continue(ProjectionActorState(..state, state: new_state))
        }
        Error(_) -> {
          // Still ack on error to prevent blocking the subscription
          // (proper error handling will be added in Module 10)
          ack_event(state, recorded_event)
          actor.continue(state)
        }
      }
    }

    GetState(reply) -> {
      process.send(reply, state.state)
      actor.continue(state)
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
}
