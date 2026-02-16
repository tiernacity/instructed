//// Projections - build read models from event streams.
////
//// A projection subscribes to domain events and builds a queryable read model.
//// This is the "Query" side of CQRS.
////
//// ## Example
////
//// ```gleam
//// import instructed/projection
////
//// type AccountBalance {
////   AccountBalance(account_number: String, balance: Int)
//// }
////
//// type AccountList = Dict(String, AccountBalance)
////
//// let proj = projection.new(
////   name: "account_balances",
////   initial_state: dict.new(),
////   handle_event: fn(event, _recorded, state) {
////     case event {
////       AccountOpened(num, balance) ->
////         Ok(dict.insert(state, num, AccountBalance(num, balance)))
////       MoneyDeposited(num, amount) -> {
////         let assert Ok(account) = dict.get(state, num)
////         Ok(dict.insert(state, num, AccountBalance(..account, balance: account.balance + amount)))
////       }
////       _ -> Ok(state)
////     }
////   },
//// )
////
//// let assert Ok(proj_actor) = projection.start(proj, event_store)
//// // Query the projection state
//// let state = projection.get_state(proj_actor, 5000)
//// ```

import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import instructed/event.{type RecordedEvent}
import instructed/event_store.{type EventStore, Origin}

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
  )
}

/// Messages for the projection actor.
pub opaque type ProjectionMessage(event, projection_state) {
  ProjectionHandleEvent(RecordedEvent(event))
  GetState(Subject(projection_state))
}

/// Start a projection, subscribing to the event store.
pub fn start(
  config: ProjectionConfig(event, projection_state),
  event_store: EventStore(event),
) -> Result(Subject(ProjectionMessage(event, projection_state)), String) {
  let actor_state =
    ProjectionActorState(config: config, state: config.initial_state)

  case
    actor.new(actor_state)
    |> actor.on_message(handle_projection_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data

      // Subscribe to all events
      let handler = fn(event: RecordedEvent(event)) {
        process.send(subject, ProjectionHandleEvent(event))
      }

      let _ =
        event_store.subscribe_persistent(
          "$all",
          config.name,
          Origin,
          handler,
        )

      Ok(subject)
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
    ProjectionHandleEvent(recorded_event) -> {
      case
        state.config.handle_event(
          recorded_event.data,
          recorded_event,
          state.state,
        )
      {
        Ok(new_state) -> {
          actor.continue(ProjectionActorState(..state, state: new_state))
        }
        Error(_) -> actor.continue(state)
      }
    }

    GetState(reply) -> {
      process.send(reply, state.state)
      actor.continue(state)
    }
  }
}
