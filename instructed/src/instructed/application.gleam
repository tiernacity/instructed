//// Application module - top-level coordinator for Instructed.
////
//// An Instructed application is a lightweight struct that groups together:
//// - An event store (shared by all components)
//// - An optional command router (for aggregate dispatch)
////
//// The application provides convenience functions for:
//// - Dispatching commands (delegates to the router)
//// - Starting event handlers, projections, and process managers
////   (wiring them to the application's event store)
//// - Reading aggregate events for state reconstruction
////
//// ## Key differences from Commanded
////
//// Commanded's Application is an Elixir module that acts as a named
//// OTP Supervisor, starting all infrastructure components (event store,
//// pubsub, registry, aggregate supervisor, subscriptions) under a
//// supervision tree.
////
//// Instructed's Application is a plain struct. Supervision is handled
//// by the user using `gleam/otp/static_supervisor`:
////
//// ```gleam
//// import gleam/otp/static_supervisor as sup
////
//// pub fn main() {
////   // The event store starts as a supervisor child
////   let assert Ok(store_subject) = in_memory_event_store.start()
////   let store = in_memory_event_store.to_event_store(store_subject)
////
////   let app = application.new(store) |> application.with_router(my_router)
////   let assert Ok(application) = application.start(app)
////
////   // Start event handlers as supervisor children
////   let assert Ok(_) = application.start_event_handler(application, my_handler_config)
////   let assert Ok(_) = application.start_process_manager(application, my_pm_config)
////   let assert Ok(_) = application.start_projection(application, my_proj_config)
//// }
//// ```
////
//// ## Named applications (multi-tenancy)
////
//// Start multiple applications with separate event stores for isolation:
////
//// ```gleam
//// for tenant <- [tenant1, tenant2] {
////   let assert Ok(store) = start_tenant_store(tenant)
////   let app = application.new(store) |> application.with_router(router)
////   let assert Ok(_) = application.start(app)
//// }
//// ```

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/option.{type Option, None, Some}
import instructed/dispatch_result.{type DispatchResult}
import instructed/error.{type DispatchError, type EventStoreError}
import instructed/event.{type RecordedEvent}
import instructed/event_handler.{type EventHandlerConfig, type HandlerMessage}
import instructed/event_store.{type EventStore}
import instructed/process_manager.{
  type PMMessage, type ProcessManagerConfig,
}
import instructed/projection.{type ProjectionConfig, type ProjectionMessage}
import instructed/router.{type Router}
import instructed/subscriptions.{type SubMessage}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Configuration for building an application.
pub type AppConfig(event, command, aggregate_state) {
  AppConfig(
    /// The event store all components will share
    event_store: EventStore(event),
    /// Optional command router for aggregate dispatch
    router: Option(Router(aggregate_state, command, event)),
    /// Optional subscriptions actor for strong consistency tracking.
    /// When set, it is automatically injected into the router and all
    /// event handlers and process managers started via the application.
    subscriptions: Option(Subject(SubMessage)),
  )
}

/// A running application instance.
///
/// Holds references to the event store and command router.
/// This is a plain struct — all process management happens externally
/// (start_event_handler, start_projection, start_process_manager return
/// Subjects that the caller can supervise).
pub type Application(event, command, aggregate_state) {
  Application(
    /// The event store shared by all components
    event_store: EventStore(event),
    /// Optional command router
    router: Option(Router(aggregate_state, command, event)),
    /// Optional subscriptions actor for strong consistency tracking.
    subscriptions: Option(Subject(SubMessage)),
  )
}

// ---------------------------------------------------------------------------
// Configuration builder
// ---------------------------------------------------------------------------

/// Create a new application configuration with an event store.
pub fn new(
  event_store: EventStore(event),
) -> AppConfig(event, command, aggregate_state) {
  AppConfig(event_store: event_store, router: None, subscriptions: None)
}

/// Set the command router for this application.
pub fn with_router(
  config: AppConfig(event, command, aggregate_state),
  router: Router(aggregate_state, command, event),
) -> AppConfig(event, command, aggregate_state) {
  AppConfig(..config, router: Some(router))
}

/// Attach a subscriptions actor to the application.
///
/// When set, the subscriptions actor is automatically injected into:
/// - The router (so `dispatch` with `consistency: Strong` can block)
/// - All event handlers started via `start_event_handler/2`
/// - All process managers started via `start_process_manager/2`
///
/// Call `subscriptions.start()` first to obtain the Subject, then:
/// ```gleam
/// let assert Ok(subs) = subscriptions.start()
/// let config = application.new(store) |> application.with_subscriptions(subs)
/// ```
pub fn with_subscriptions(
  config: AppConfig(event, command, aggregate_state),
  subs: Subject(SubMessage),
) -> AppConfig(event, command, aggregate_state) {
  AppConfig(..config, subscriptions: Some(subs))
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

/// Start the application.
///
/// Returns an `Application` struct that components can use to access
/// the shared event store and dispatch commands.
///
/// The application itself is not an OTP process — it is a lightweight
/// struct. The caller is responsible for starting individual components
/// (event handlers, projections, process managers) and supervising them.
pub fn start(
  config: AppConfig(event, command, aggregate_state),
) -> Result(Application(event, command, aggregate_state), String) {
  // If a subscriptions actor is configured, inject it into the router so
  // strong-consistency dispatch can block until handlers have processed events.
  let router = case config.router, config.subscriptions {
    Some(r), Some(subs) -> Some(router.with_subscriptions(r, subs))
    r, _ -> r
  }
  Ok(Application(
    event_store: config.event_store,
    router: router,
    subscriptions: config.subscriptions,
  ))
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

/// Dispatch a command through the application's router.
///
/// Returns an error if no router is configured.
///
/// Equivalent to Commanded's `MyApp.dispatch(command)`.
pub fn dispatch(
  app: Application(event, command, aggregate_state),
  command: command,
) -> Result(DispatchResult(aggregate_state, event), DispatchError) {
  case app.router {
    None -> Error(error.AggregateStartError("No router configured"))
    Some(r) -> {
      let r = router.Router(..r, event_store: app.event_store)
      router.dispatch(r, command)
    }
  }
}

/// Dispatch a command with explicit causation/correlation metadata.
///
/// Equivalent to Commanded's `MyApp.dispatch(command, opts)` with
/// `causation_id`, `correlation_id`, and `metadata` options.
pub fn dispatch_with_context(
  app: Application(event, command, aggregate_state),
  command: command,
  command_id: String,
  causation_id: Option(String),
  correlation_id: Option(String),
  metadata: Dict(String, String),
) -> Result(DispatchResult(aggregate_state, event), DispatchError) {
  case app.router {
    None -> Error(error.AggregateStartError("No router configured"))
    Some(r) -> {
      let r = router.Router(..r, event_store: app.event_store)
      router.dispatch_with_context(
        r,
        command,
        command_id,
        causation_id,
        correlation_id,
        metadata,
      )
    }
  }
}

// ---------------------------------------------------------------------------
// Component start helpers
// ---------------------------------------------------------------------------

/// Start an event handler wired to the application's event store.
///
/// If the application has a subscriptions actor configured, it is automatically
/// injected into the handler. The handler will ack the subscriptions actor after
/// each successfully processed event when `consistency: Strong` is set.
///
/// Equivalent to Commanded's `EventHandler.start_link(application: MyApp)`.
pub fn start_event_handler(
  app: Application(event, command, aggregate_state),
  config: EventHandlerConfig(event, handler_state),
) -> Result(Subject(HandlerMessage(event)), String) {
  let config = case app.subscriptions, config.subscriptions {
    Some(subs), None -> event_handler.with_subscriptions(config, subs)
    _, _ -> config
  }
  event_handler.start(config, app.event_store)
}

/// Start a projection wired to the application's event store.
///
/// Returns the projection's Subject which can be supervised externally.
pub fn start_projection(
  app: Application(event, command, aggregate_state),
  config: ProjectionConfig(event, projection_state),
) -> Result(Subject(ProjectionMessage(event, projection_state)), String) {
  projection.start(config, app.event_store)
}

/// Start a process manager wired to the application's event store.
///
/// If the application has a subscriptions actor configured, it is automatically
/// injected into the process manager. The PM will ack the subscriptions actor after
/// each event when `consistency: Strong` is set.
///
/// Note: `pm_command` is the process manager's command type, which may differ
/// from the router's command type. PMs dispatch commands through their own
/// `dispatch_command` function (configured in `ProcessManagerConfig`).
///
/// Equivalent to Commanded's `ProcessManager.start_link(application: MyApp)`.
pub fn start_process_manager(
  app: Application(event, command, aggregate_state),
  config: ProcessManagerConfig(event, pm_command, pm_state),
) -> Result(Subject(PMMessage(event)), String) {
  let config = case app.subscriptions, config.subscriptions {
    Some(subs), None -> process_manager.with_subscriptions(config, subs)
    _, _ -> config
  }
  process_manager.start(config, app.event_store)
}

// ---------------------------------------------------------------------------
// Event store access
// ---------------------------------------------------------------------------

/// Read all events from a stream.
///
/// Equivalent to Commanded's `Commanded.EventStore.stream_forward/3`.
pub fn read_stream(
  app: Application(event, command, aggregate_state),
  stream_id: String,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  app.event_store.read_stream_forward(stream_id, 1, 1000)
}

/// Read a page of events from a stream starting at a given version.
///
/// Useful for pagination and incremental reads.
pub fn read_stream_from(
  app: Application(event, command, aggregate_state),
  stream_id: String,
  start_version: Int,
  count: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  app.event_store.read_stream_forward(stream_id, start_version, count)
}

/// Get the event store from the application.
///
/// Allows direct event store access when needed (e.g., for testing,
/// or for custom read model queries).
pub fn event_store(
  app: Application(event, command, aggregate_state),
) -> EventStore(event) {
  app.event_store
}

/// Get the subscriptions actor from the application, if configured.
///
/// Useful for manually registering handlers or sending acks outside the
/// standard event handler / process manager lifecycle.
pub fn get_subscriptions(
  app: Application(event, command, aggregate_state),
) -> Option(Subject(SubMessage)) {
  app.subscriptions
}
