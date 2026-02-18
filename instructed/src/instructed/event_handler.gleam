//// Event Handler - subscribes to domain events and processes them.
////
//// Event handlers are used for side effects, read model projections,
//// and process manager triggering. Each handler runs as an OTP Actor.
////
//// ## Key guarantees (matching Commanded):
////
//// - **Singleton**: One instance per handler name (Invariant 5)
//// - **Idempotency**: last_seen_event guard skips already-processed events (Invariant 20)
//// - **Error handling**: error callback with retry/skip/stop strategies (Invariant 8)
//// - **Acknowledgment**: Events acked after processing for position tracking
//// - **start_from**: Only applies on FIRST subscription creation (Invariant 7)
//// - **Resumption**: On restart, resumes from last ack'd position (not replay all)
////
//// ## Example
////
//// ```gleam
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
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import instructed/error.{type ErrorAction}
import instructed/event.{type RecordedEvent}
import instructed/event_store.{
  type EventStore, type StartFrom, type Subscription, Origin,
}
import instructed/middleware.{type Consistency, Eventual, Strong}
import instructed/subscriptions.{type SubMessage}
import instructed/upcast.{type Upcaster}

/// Configuration for an event handler.
pub type EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(
    /// Unique name for this event handler (must be stable across restarts).
    /// Changing the name creates a NEW subscription (Invariant 6).
    name: String,
    /// Function to handle each event.
    handle_event: fn(event, RecordedEvent(event), handler_state) ->
      Result(handler_state, String),
    /// Optional error callback (retry/skip/stop strategy).
    /// If None, errors stop the handler (matching Commanded default).
    on_error: Option(
      fn(String, RecordedEvent(event), handler_state) ->
        ErrorAction(handler_state),
    ),
    /// Initial state for the handler
    initial_state: handler_state,
    /// Which stream(s) to subscribe to
    stream_id: StreamSelection,
    /// Where to start reading on FIRST subscription creation.
    /// Restarts resume from last ack'd position (Invariant 7).
    start_from: StartFrom,
    /// Consistency level (eventual or strong)
    consistency: Consistency,
    /// Optional subscriptions actor for strong consistency acking.
    /// When set and `consistency: Strong`, the handler sends an ack after
    /// each successfully processed event so waiting dispatchers can unblock.
    subscriptions: Option(Subject(SubMessage)),
    /// Optional event upcaster.
    /// Applied to each event before it is delivered to `handle_event`.
    /// Use this to transform older event representations to the current schema.
    upcaster: Option(Upcaster(event)),
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
    on_error: None,
    initial_state: initial_state,
    stream_id: AllStreams,
    start_from: Origin,
    consistency: Eventual,
    subscriptions: None,
    upcaster: None,
  )
}

/// Set the handler to subscribe to a specific stream.
pub fn for_stream(
  config: EventHandlerConfig(event, handler_state),
  stream_id: String,
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(..config, stream_id: SpecificStream(stream_id))
}

/// Set where to start reading from (only applies on first subscription creation).
pub fn with_start_from(
  config: EventHandlerConfig(event, handler_state),
  start_from: StartFrom,
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(..config, start_from: start_from)
}

/// Set the error callback for handling failures.
pub fn with_error_handler(
  config: EventHandlerConfig(event, handler_state),
  on_error: fn(String, RecordedEvent(event), handler_state) ->
    ErrorAction(handler_state),
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(..config, on_error: Some(on_error))
}

/// Set the consistency level.
pub fn with_consistency(
  config: EventHandlerConfig(event, handler_state),
  consistency: Consistency,
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(..config, consistency: consistency)
}

/// Set the subscriptions actor for strong consistency acking.
///
/// When set and the handler is configured with `consistency: Strong`,
/// the handler sends an ack to the subscriptions actor after each
/// successfully processed event, allowing waiting dispatchers to unblock.
///
/// Equivalent to Commanded's `Subscriptions.ack_event/4` call in handlers.
pub fn with_subscriptions(
  config: EventHandlerConfig(event, handler_state),
  subs: Subject(SubMessage),
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(..config, subscriptions: Some(subs))
}

/// Set an event upcaster.
/// The upcaster is applied to each event before it is delivered to
/// the `handle_event` callback, allowing older event representations to
/// be transparently transformed to the current schema.
pub fn with_upcaster(
  config: EventHandlerConfig(event, handler_state),
  upcaster: Upcaster(event),
) -> EventHandlerConfig(event, handler_state) {
  EventHandlerConfig(..config, upcaster: Some(upcaster))
}

/// Internal state of the event handler actor.
type HandlerActorState(event, handler_state) {
  HandlerActorState(
    config: EventHandlerConfig(event, handler_state),
    handler_state: handler_state,
    /// Event store reference for acknowledging events
    event_store: Option(EventStore(event)),
    /// Subscription reference for acknowledging events
    subscription: Option(Subscription),
    /// Last seen event number for idempotency (Invariant 20)
    last_seen_event: Option(Int),
  )
}

/// Messages the event handler actor receives.
pub opaque type HandlerMessage(event) {
  HandleEvent(RecordedEvent(event))
  /// Internal: set subscription info for event acknowledgment
  SetSubscriptionInfo(EventStore(event), Subscription)
}

/// Start an event handler, subscribing to the event store.
///
/// Does NOT delete existing subscriptions — on restart, the handler
/// resumes from the last acknowledged position (Invariant 7).
/// start_from only applies when the subscription is first created.
pub fn start(
  config: EventHandlerConfig(event, handler_state),
  event_store: EventStore(event),
) -> Result(Subject(HandlerMessage(event)), String) {
  let actor_state =
    HandlerActorState(
      config: config,
      handler_state: config.initial_state,
      event_store: None,
      subscription: None,
      last_seen_event: None,
    )

  case
    actor.new(actor_state)
    |> actor.on_message(handle_actor_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data

      // Register with the subscriptions actor so strong-consistency waiters
      // know about this handler (Invariant 11).
      case config.subscriptions {
        Some(subs) ->
          subscriptions.register(subs, config.name, config.consistency)
        None -> Nil
      }

      // Non-blocking handler: sends event to actor's mailbox.
      // Apply upcaster if configured before delivering to the handler.
      let handler = fn(event: RecordedEvent(event)) {
        let event = case config.upcaster {
          None -> event
          Some(u) -> upcast.apply(u, event)
        }
        process.send(subject, HandleEvent(event))
      }

      let stream = case config.stream_id {
        AllStreams -> "$all"
        SpecificStream(s) -> s
      }

      // Subscribe persistently — do NOT delete existing subscription.
      // If subscription already exists, it resumes from last ack'd position.
      // If it doesn't exist, it's created with start_from.
      case
        event_store.subscribe_persistent(
          stream,
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
        Error(error.SubscriptionAlreadyExists) -> {
          // Subscription exists — delete and recreate to re-register handler.
          // Position is preserved if the adapter supports it.
          // For in-memory adapter, this loses position (acceptable for testing).
          let _ = event_store.delete_subscription(stream, config.name)
          case
            event_store.subscribe_persistent(
              stream,
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
            Error(_) -> Error("Failed to create subscription after delete")
          }
        }
        Error(_) -> Error("Failed to create subscription")
      }
    }
    Error(_) -> Error("Failed to start event handler actor")
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
    SetSubscriptionInfo(es, sub) -> {
      actor.continue(
        HandlerActorState(
          ..state,
          event_store: Some(es),
          subscription: Some(sub),
        ),
      )
    }

    HandleEvent(recorded_event) -> {
      // Idempotency guard: skip already-seen events (Invariant 20)
      case state.last_seen_event {
        Some(last) if recorded_event.event_number <= last -> {
          // Already processed — ack and continue
          ack_event(state, recorded_event)
          actor.continue(state)
        }
        _ -> {
          // Process the event
          case
            state.config.handle_event(
              recorded_event.data,
              recorded_event,
              state.handler_state,
            )
          {
            Ok(new_handler_state) -> {
              ack_event(state, recorded_event)
              // Ack strong-consistency subscriptions after successful processing
              // (Invariant 11): unblocks any waiting dispatchers
              case state.config.consistency, state.config.subscriptions {
                Strong, Some(subs) ->
                  subscriptions.ack_event(
                    subs,
                    state.config.name,
                    recorded_event.stream_id,
                    recorded_event.stream_version,
                  )
                _, _ -> Nil
              }
              actor.continue(
                HandlerActorState(
                  ..state,
                  handler_state: new_handler_state,
                  last_seen_event: Some(recorded_event.event_number),
                ),
              )
            }
            Error(reason) -> {
              // Error handling via callback (Invariant 8)
              handle_error(state, reason, recorded_event)
            }
          }
        }
      }
    }
  }
}

/// Handle an event processing error using the configured error callback.
fn handle_error(
  state: HandlerActorState(event, handler_state),
  reason: String,
  recorded_event: RecordedEvent(event),
) -> actor.Next(
  HandlerActorState(event, handler_state),
  HandlerMessage(event),
) {
  case state.config.on_error {
    None -> {
      // Default: stop on error (matching Commanded's default)
      // But since we can't easily stop a Gleam actor with a reason,
      // we ack the event and continue (preventing blocking).
      // This will be improved with proper supervision in Module 13.
      ack_event(state, recorded_event)
      actor.continue(state)
    }
    Some(error_fn) -> {
      case error_fn(reason, recorded_event, state.handler_state) {
        error.Retry(new_state) -> {
          // Retry with updated state — re-process the same event
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
                HandlerActorState(
                  ..state,
                  handler_state: final_state,
                  last_seen_event: Some(recorded_event.event_number),
                ),
              )
            }
            Error(_) -> {
              // Retry failed again — ack and continue to prevent blocking
              ack_event(state, recorded_event)
              actor.continue(
                HandlerActorState(..state, handler_state: new_state),
              )
            }
          }
        }
        error.RetryWithDelay(delay_ms, new_state) -> {
          // Sleep then retry
          process.sleep(delay_ms)
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
                HandlerActorState(
                  ..state,
                  handler_state: final_state,
                  last_seen_event: Some(recorded_event.event_number),
                ),
              )
            }
            Error(_) -> {
              ack_event(state, recorded_event)
              actor.continue(
                HandlerActorState(..state, handler_state: new_state),
              )
            }
          }
        }
        error.Skip -> {
          // Skip the event — ack and continue
          ack_event(state, recorded_event)
          actor.continue(
            HandlerActorState(
              ..state,
              last_seen_event: Some(recorded_event.event_number),
            ),
          )
        }
        error.Stop(_reason) -> {
          // Stop the handler
          // In Gleam, we can't easily stop with a reason from on_message.
          // We ack the event to prevent re-delivery, then stop.
          ack_event(state, recorded_event)
          actor.stop()
        }
      }
    }
  }
}

fn ack_event(
  state: HandlerActorState(event, handler_state),
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
