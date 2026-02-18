//// Application module - top-level supervisor for Instructed.
////
//// The Application module provides a convenient way to start and manage
//// all the processes needed for a CQRS/ES system: event store, aggregate
//// servers, event handlers, process managers, and projections.
////
//// ## Example
////
//// ```gleam
//// import instructed/application as app
//// import instructed/in_memory_event_store
////
//// pub fn main() {
////   // Start the event store
////   let assert Ok(store_subject) = in_memory_event_store.start()
////   let store = in_memory_event_store.to_event_store(store_subject)
////
////   // Create and start the application
////   let config = app.new(store)
////   let assert Ok(application) = app.start(config)
////
////   // Dispatch commands through the application
////   let assert Ok(_) = app.dispatch(application, my_router, my_command)
//// }
//// ```

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/option
import gleam/otp/actor
import instructed/event.{type RecordedEvent}
import instructed/event_store.{type EventStore}
import instructed/projection.{type ProjectionConfig, type ProjectionMessage}
import instructed/router.{type DispatchResult, type Router}

/// Application configuration.
pub type AppConfig(event) {
  AppConfig(
    /// The event store to use
    event_store: EventStore(event),
  )
}

/// A running application instance.
pub type Application(event) {
  Application(
    /// The event store
    event_store: EventStore(event),
    /// Actor subject for managing state
    subject: Subject(AppMessage(event)),
  )
}

/// Internal app state tracking started components.
type AppState(event) {
  AppState(
    event_store: EventStore(event),
    projection_names: List(String),
  )
}

/// Messages for the application actor.
pub opaque type AppMessage(event) {
  GetEventStore(Subject(EventStore(event)))
}

/// Create a new application configuration.
pub fn new(event_store: EventStore(event)) -> AppConfig(event) {
  AppConfig(event_store: event_store)
}

/// Start the application supervisor.
pub fn start(
  config: AppConfig(event),
) -> Result(Application(event), String) {
  let app_state =
    AppState(
      event_store: config.event_store,
      projection_names: [],
    )

  case
    actor.new(app_state)
    |> actor.on_message(handle_app_message)
    |> actor.start
  {
    Ok(started) -> {
      Ok(Application(
        event_store: config.event_store,
        subject: started.data,
      ))
    }
    Error(_) -> Error("Failed to start application")
  }
}

fn handle_app_message(
  state: AppState(event),
  msg: AppMessage(event),
) -> actor.Next(AppState(event), AppMessage(event)) {
  case msg {
    GetEventStore(reply) -> {
      process.send(reply, state.event_store)
      actor.continue(state)
    }
  }
}

/// Dispatch a command through a router.
pub fn dispatch(
  app: Application(event),
  router: Router(state, command, event),
  command: command,
) -> Result(DispatchResult(state, event), error.DispatchError) {
  // Ensure the router uses the app's event store
  let router = router.Router(..router, event_store: app.event_store)
  router.dispatch(router, command)
}

/// Dispatch a command with explicit context.
pub fn dispatch_with_context(
  app: Application(event),
  router: Router(state, command, event),
  command: command,
  command_id: String,
  causation_id: option.Option(String),
  correlation_id: option.Option(String),
  metadata: Dict(String, String),
) -> Result(DispatchResult(state, event), error.DispatchError) {
  let router = router.Router(..router, event_store: app.event_store)
  router.dispatch_with_context(
    router,
    command,
    command_id,
    causation_id,
    correlation_id,
    metadata,
  )
}

/// Start a projection within the application.
pub fn start_projection(
  app: Application(event),
  config: ProjectionConfig(event, projection_state),
) -> Result(Subject(ProjectionMessage(event, projection_state)), String) {
  projection.start(config, app.event_store)
}

/// Read events from a stream.
pub fn read_stream(
  app: Application(event),
  stream_id: String,
) -> Result(List(RecordedEvent(event)), error.EventStoreError) {
  app.event_store.read_stream_forward(stream_id, 1, 1000)
}

import instructed/error

