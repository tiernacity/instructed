//// Event store behaviour (trait) definition.
////
//// The event store is responsible for persisting and reading domain events.
//// Instructed ships with an in-memory event store for testing.
//// Use `instructed_postgres` for production persistence.
////
//// ## Event Store Functions
////
//// An event store is represented as a record of functions, allowing different
//// backends to be used interchangeably.
////
//// ## Subscription Model
////
//// Persistent subscriptions deliver events one at a time to a subscriber
//// process via message passing (Subject). The subscriber must acknowledge
//// each event before the next one is delivered, providing backpressure.
//// This matches Commanded's subscription model where events are sent as
//// messages to a subscriber PID.
////
//// Transient subscriptions use callback functions for fire-and-forget
//// notifications. Callbacks MUST be non-blocking (e.g., just send a message
//// to another process).

import gleam/option.{type Option}
import instructed/error.{type EventStoreError}
import instructed/event.{type EventData, type RecordedEvent}
import instructed/snapshot.{type SnapshotData}

/// Expected version when appending events to a stream.
pub type ExpectedVersion {
  /// Accept any version - no optimistic concurrency check
  AnyVersion
  /// Stream must not exist yet
  NoStream
  /// Stream must already exist
  StreamExists
  /// Stream must be at exactly this version
  ExactVersion(Int)
}

/// Where to start reading events from.
pub type StartFrom {
  /// Start from the beginning
  Origin
  /// Start from only new events
  Current
  /// Start from a specific event number
  FromEventNumber(Int)
}

/// A subscription reference.
pub type Subscription {
  Subscription(id: String)
}

/// The event store interface.
/// All functions operate on typed events.
///
/// This is Gleam's equivalent of Commanded's `EventStore.Adapter` behaviour.
/// Instead of a behaviour with callbacks, we use a record of functions,
/// which is idiomatic Gleam.
pub type EventStore(event) {
  EventStore(
    /// Append events to a stream atomically.
    /// Returns the new stream version on success.
    /// Checks expected_version for optimistic concurrency control.
    append_to_stream: fn(
      String,
      ExpectedVersion,
      List(EventData(event)),
    ) ->
      Result(Int, EventStoreError),
    /// Read events from a stream starting from a version, with batch size limit.
    /// The third parameter is the read batch size (max events to return).
    /// Use 1000 as the default batch size (matches Commanded's default).
    /// Returns events in stream_version order.
    read_stream_forward: fn(String, Int, Int) ->
      Result(List(RecordedEvent(event)), EventStoreError),
    /// Subscribe to all events (transient - no persistence of position).
    /// The callback MUST be non-blocking (e.g., send a message to another process).
    /// Events are delivered as they are appended.
    subscribe: fn(fn(RecordedEvent(event)) -> Nil) ->
      Result(Subscription, EventStoreError),
    /// Subscribe to a specific stream (transient).
    /// The callback MUST be non-blocking.
    subscribe_to_stream: fn(String, fn(RecordedEvent(event)) -> Nil) ->
      Result(Subscription, EventStoreError),
    /// Create a persistent subscription with durable position tracking.
    ///
    /// Events are delivered one at a time via the handler callback.
    /// The handler callback MUST be non-blocking (e.g., send a message
    /// to another process). It runs in the event store's process context.
    ///
    /// The subscriber MUST call ack_event after processing each event
    /// before the next event will be delivered (backpressure).
    ///
    /// Parameters:
    /// - stream: "$all" for all streams, or a specific stream ID
    /// - name: unique subscription name (must be stable across restarts)
    /// - start_from: where to start reading (Origin/Current/FromEventNumber)
    /// - handler: callback invoked with each event (MUST be non-blocking)
    subscribe_persistent: fn(
      String,
      String,
      StartFrom,
      fn(RecordedEvent(event)) -> Nil,
    ) ->
      Result(Subscription, EventStoreError),
    /// Acknowledge receipt and successful processing of an event.
    /// This updates the subscription's durable position.
    /// After acknowledgment, the next event will be delivered to the subscriber.
    ack_event: fn(Subscription, RecordedEvent(event)) ->
      Result(Nil, EventStoreError),
    /// Unsubscribe from event notifications.
    /// For persistent subscriptions, this pauses delivery but preserves position.
    unsubscribe: fn(Subscription) -> Result(Nil, EventStoreError),
    /// Delete a persistent subscription entirely, including its position.
    delete_subscription: fn(String, String) ->
      Result(Nil, EventStoreError),
    /// Read a snapshot for a given source.
    read_snapshot: fn(String) ->
      Result(SnapshotData(event), EventStoreError),
    /// Record a snapshot (upsert - replaces existing snapshot for same source).
    record_snapshot: fn(SnapshotData(event)) ->
      Result(Nil, EventStoreError),
    /// Delete a snapshot for a given source.
    delete_snapshot: fn(String) -> Result(Nil, EventStoreError),
    /// Reset the event store (for testing).
    reset: fn() -> Result(Nil, EventStoreError),
    /// Get all events across all streams starting from a global event number.
    read_all_forward: fn(Int) ->
      Result(List(RecordedEvent(event)), EventStoreError),
    /// Get the current global event number (None if no events).
    get_latest_event_number: fn() -> Result(Option(Int), EventStoreError),
  )
}
