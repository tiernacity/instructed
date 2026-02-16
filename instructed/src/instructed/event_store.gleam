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
pub type EventStore(event) {
  EventStore(
    /// Append events to a stream atomically.
    append_to_stream: fn(
      String,
      ExpectedVersion,
      List(EventData(event)),
    ) ->
      Result(Int, EventStoreError),
    /// Read all events from a stream starting from a version.
    read_stream_forward: fn(String, Int) ->
      Result(List(RecordedEvent(event)), EventStoreError),
    /// Subscribe to all events (transient - no persistence of position).
    subscribe: fn(fn(RecordedEvent(event)) -> Nil) ->
      Result(Subscription, EventStoreError),
    /// Subscribe to a specific stream.
    subscribe_to_stream: fn(String, fn(RecordedEvent(event)) -> Nil) ->
      Result(Subscription, EventStoreError),
    /// Create a persistent subscription (with name, position tracking).
    subscribe_persistent: fn(
      String,
      String,
      StartFrom,
      fn(RecordedEvent(event)) -> Nil,
    ) ->
      Result(Subscription, EventStoreError),
    /// Acknowledge receipt of an event from a persistent subscription.
    ack_event: fn(Subscription, RecordedEvent(event)) ->
      Result(Nil, EventStoreError),
    /// Unsubscribe from event notifications.
    unsubscribe: fn(Subscription) -> Result(Nil, EventStoreError),
    /// Delete a persistent subscription.
    delete_subscription: fn(String, String) ->
      Result(Nil, EventStoreError),
    /// Read a snapshot for a given source.
    read_snapshot: fn(String) ->
      Result(SnapshotData(event), EventStoreError),
    /// Record a snapshot.
    record_snapshot: fn(SnapshotData(event)) ->
      Result(Nil, EventStoreError),
    /// Delete a snapshot for a given source.
    delete_snapshot: fn(String) -> Result(Nil, EventStoreError),
    /// Reset the event store (for testing).
    reset: fn() -> Result(Nil, EventStoreError),
    /// Get all events across all streams (for subscriptions).
    read_all_forward: fn(Int) ->
      Result(List(RecordedEvent(event)), EventStoreError),
    /// Get the current global event number.
    get_latest_event_number: fn() -> Result(Option(Int), EventStoreError),
  )
}
