//// Event Handler - subscribes to domain events and processes them.
////
//// Event handlers are used for side effects, read model projections,
//// and process manager triggering. Each handler runs as an OTP Actor.
////
//// ## Example
////
//// ```gleam
//// import instructed/event_handler
////
//// let handler = event_handler.new(
////   name: "email_notifier",
////   handle_event: fn(event, _metadata, state) {
////     case event {
////       AccountOpened(email, ..) -> {
////         send_welcome_email(email)
////         Ok(state)
////       }
////       _ -> Ok(state)
////     }
////   },
////   initial_state: Nil,
//// )
////
//// let assert Ok(pid) = event_handler.start(handler, event_store)
//// ```

import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import instructed/event.{type RecordedEvent}
import instructed/event_store.{type EventStore, type Subscription, Origin}

/// Configuration for an event handler.
pub type EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(
    /// Unique name for this event handler
    name: String,
    /// Function to handle each event. Receives the event data,
    /// the recorded event metadata, and the current handler state.
    /// Returns Ok(new_state) or Error(reason).
    handle_event: fn(event, RecordedEvent(event), handler_state) ->
      Result(handler_state, String),
    /// Initial state for the handler
    initial_state: handler_state,
    /// Optional: subscribe to a specific stream (None = all streams)
    stream_id: StreamSelection,
  )
}

/// Which stream(s) to subscribe to.
pub type StreamSelection {
  /// Subscribe to all events across all streams
  AllStreams
  /// Subscribe to a specific stream
  SpecificStream(String)
}

/// Create a new event handler configuration.
pub fn new(
  name name: String,
  handle_event handle_event: fn(event, RecordedEvent(event), handler_state) ->
    Result(handler_state, String),
  initial_state initial_state: handler_state,
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(
    name: name,
    handle_event: handle_event,
    initial_state: initial_state,
    stream_id: AllStreams,
  )
}

/// Set the handler to subscribe to a specific stream.
pub fn for_stream(
  config: EventHandlerConfig(event, handler_state),
  stream_id: String,
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(..config, stream_id: SpecificStream(stream_id))
}

/// Internal state of the event handler actor.
type HandlerActorState(event, handler_state) {
  HandlerActorState(
    config: EventHandlerConfig(event, handler_state),
    handler_state: handler_state,
    subscription: Subscription,
  )
}

/// Messages the event handler actor receives.
pub opaque type HandlerMessage(event) {
  HandleEvent(RecordedEvent(event))
}

/// Start an event handler, subscribing to the event store.
pub fn start(
  config: EventHandlerConfig(event, handler_state),
  event_store: EventStore(event),
) -> Result(Subject(HandlerMessage(event)), String) {
  // Create a persistent subscription
  let subscribe_result = case config.stream_id {
    AllStreams ->
      event_store.subscribe_persistent(
        "$all",
        config.name,
        Origin,
        fn(_event) { Nil },
      )
    SpecificStream(stream_id) ->
      event_store.subscribe_persistent(
        stream_id,
        config.name,
        Origin,
        fn(_event) { Nil },
      )
  }

  case subscribe_result {
    Error(_) -> Error("Failed to create subscription")
    Ok(subscription) -> {
      let actor_state =
        HandlerActorState(
          config: config,
          handler_state: config.initial_state,
          subscription: subscription,
        )

      // Start the actor
      case
        actor.new(actor_state)
        |> actor.on_message(handle_actor_message)
        |> actor.start
      {
        Ok(started) -> {
          let subject = started.data

          // Now set up the real subscription handler that sends to the actor
          let handler = fn(event: RecordedEvent(event)) {
            process.send(subject, HandleEvent(event))
          }

          // Unsubscribe the placeholder and create real subscription
          let _ = event_store.unsubscribe(subscription)
          let _ = case config.stream_id {
            AllStreams ->
              event_store.subscribe_persistent(
                "$all",
                config.name <> "_active",
                Origin,
                handler,
              )
            SpecificStream(stream_id) ->
              event_store.subscribe_persistent(
                stream_id,
                config.name <> "_active",
                Origin,
                handler,
              )
          }

          Ok(subject)
        }
        Error(_) -> Error("Failed to start event handler actor")
      }
    }
  }
}

fn handle_actor_message(
  state: HandlerActorState(event, handler_state),
  msg: HandlerMessage(event),
) -> actor.Next(
  HandlerActorState(event, handler_state),
  HandlerMessage(event),
) {
  case msg {
    HandleEvent(recorded_event) -> {
      case
        state.config.handle_event(
          recorded_event.data,
          recorded_event,
          state.handler_state,
        )
      {
        Ok(new_handler_state) -> {
          actor.continue(
            HandlerActorState(..state, handler_state: new_handler_state),
          )
        }
        Error(_reason) -> {
          // Log error and continue with existing state
          actor.continue(state)
        }
      }
    }
  }
}
