//// Types for domain events and recorded events.
////
//// In Instructed, events are strongly typed using Gleam's type system.
//// The `event` type parameter represents your domain event type.
////
//// ## Example
////
//// ```gleam
//// type BankEvent {
////   AccountOpened(account_number: String, initial_balance: Int)
////   MoneyDeposited(account_number: String, amount: Int)
////   MoneyWithdrawn(account_number: String, amount: Int)
//// }
//// ```

import gleam/dict.{type Dict}
import gleam/option.{type Option}

/// An event before it is persisted to the event store.
/// The `event` type parameter is your domain event type.
pub type EventData(event) {
  EventData(
    /// The domain event data
    data: event,
    /// An optional UUID used to identify the cause of this event
    causation_id: Option(String),
    /// An optional UUID used to correlate related events
    correlation_id: Option(String),
    /// Metadata associated with the event
    metadata: Dict(String, String),
  )
}

/// A persisted event read from the event store.
/// Contains the domain event data plus storage metadata.
pub type RecordedEvent(event) {
  RecordedEvent(
    /// A globally unique identifier for this event
    event_id: String,
    /// A globally unique, monotonically incrementing number
    event_number: Int,
    /// The stream this event belongs to
    stream_id: String,
    /// The version of the stream at this event
    stream_version: Int,
    /// UUID identifying the cause of this event
    causation_id: Option(String),
    /// UUID correlating related events
    correlation_id: Option(String),
    /// The domain event data
    data: event,
    /// Metadata associated with the event
    metadata: Dict(String, String),
    /// When the event was created (milliseconds since epoch)
    created_at: Int,
  )
}

/// Enriched metadata combining recorded event fields with custom metadata.
pub type EventMetadata {
  EventMetadata(
    event_id: String,
    event_number: Int,
    stream_id: String,
    stream_version: Int,
    causation_id: Option(String),
    correlation_id: Option(String),
    created_at: Int,
    custom: Dict(String, String),
  )
}

/// Extract enriched metadata from a recorded event.
pub fn enrich_metadata(event: RecordedEvent(a)) -> EventMetadata {
  EventMetadata(
    event_id: event.event_id,
    event_number: event.event_number,
    stream_id: event.stream_id,
    stream_version: event.stream_version,
    causation_id: event.causation_id,
    correlation_id: event.correlation_id,
    created_at: event.created_at,
    custom: event.metadata,
  )
}
