//// Shared test event type and helpers for the conformance test suite.
////
//// All event store adapters use these types and helpers to run
//// the same conformance tests, ensuring consistent behaviour.

import gleam/dict
import gleam/option.{type Option, None}
import instructed/event.{type EventData, EventData}

/// Shared test event type used across all conformance tests.
pub type TestEvent {
  Created(name: String)
  Updated(name: String)
  Deleted
}

/// Create a simple EventData with no metadata.
pub fn evt(data: TestEvent) -> EventData(TestEvent) {
  EventData(
    data: data,
    event_type: event_type_name(data),
    causation_id: None,
    correlation_id: None,
    metadata: dict.new(),
  )
}

/// Create an EventData with causation, correlation, and metadata.
pub fn evt_with_metadata(
  data: TestEvent,
  causation_id: Option(String),
  correlation_id: Option(String),
  metadata: dict.Dict(String, String),
) -> EventData(TestEvent) {
  EventData(
    data: data,
    event_type: event_type_name(data),
    causation_id: causation_id,
    correlation_id: correlation_id,
    metadata: metadata,
  )
}

/// Get the event type name for a test event.
pub fn event_type_name(event: TestEvent) -> String {
  case event {
    Created(_) -> "Created"
    Updated(_) -> "Updated"
    Deleted -> "Deleted"
  }
}
