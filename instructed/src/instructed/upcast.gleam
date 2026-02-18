//// Event Upcasting — transforms older event representations to the current schema.
////
//// When a domain event's structure changes (new fields, renamed fields, merged
//// events, etc.), old events stored in the event store must be transformed
//// before they are processed by the current application code.
////
//// `Upcaster(event)` is a record containing a single `upcast` function that
//// takes a `RecordedEvent` and returns a (possibly modified) `RecordedEvent`.
//// The upcaster can inspect the `event_type` field and/or metadata to decide
//// what transformation to apply.
////
//// ## How it works
////
//// - The aggregate server applies the upcaster when **reading events** from
////   the store (during state rebuild and incremental updates after retries).
//// - Event handlers and process managers apply the upcaster when events are
////   **delivered** to their callbacks.
//// - New events written by the current version are never upcast — they are
////   already in the current schema.
////
//// ## Example
////
//// Suppose `UserRegistered` was renamed to `AccountCreated` and a new
//// `plan` field with a default value of `"free"` was added:
////
//// ```gleam
//// import instructed/upcast
//// import instructed/event.{type RecordedEvent}
////
//// type MyEvent {
////   AccountCreated(email: String, plan: String)
////   // Old version no longer defined — handled by upcaster
//// }
////
//// fn my_upcaster() -> upcast.Upcaster(MyEvent) {
////   upcast.Upcaster(upcast: fn(recorded) {
////     // Inspect the event_type from the raw record to transform old events
////     case recorded.event_type {
////       "UserRegistered" ->
////         // Upcast old UserRegistered → AccountCreated with default plan
////         case recorded.data {
////           UserRegistered(email) ->
////             event.RecordedEvent(..recorded,
////               data: AccountCreated(email, "free"),
////               event_type: "AccountCreated",
////             )
////           _ -> recorded
////         }
////       _ -> recorded
////     }
////   })
//// }
////
//// // Wire into the aggregate server:
//// let config =
////   aggregate_server.new_config(aggregate: my_aggregate, ...)
////   |> aggregate_server.with_upcaster(my_upcaster())
//// ```
////
//// ## Chaining upcasters
////
//// Use `chain/2` to apply multiple upcasters in sequence. Each upcaster
//// processes the output of the previous one:
////
//// ```gleam
//// upcast.chain(v1_to_v2_upcaster(), v2_to_v3_upcaster())
//// ```

import gleam/list
import instructed/event.{type RecordedEvent}

/// An upcaster for domain events.
///
/// The `upcast` function receives a `RecordedEvent` as stored and returns
/// a (possibly modified) `RecordedEvent` in the current schema.
///
/// To leave an event unchanged, return it as-is.
pub type Upcaster(event) {
  Upcaster(upcast: fn(RecordedEvent(event)) -> RecordedEvent(event))
}

/// An identity upcaster — returns all events unchanged.
/// Use this as a placeholder when you don't need upcasting yet.
pub fn identity() -> Upcaster(event) {
  Upcaster(upcast: fn(recorded) { recorded })
}

/// Apply the upcaster to a single recorded event.
pub fn apply(
  upcaster: Upcaster(event),
  recorded: RecordedEvent(event),
) -> RecordedEvent(event) {
  upcaster.upcast(recorded)
}

/// Apply the upcaster to a list of recorded events.
pub fn apply_all(
  upcaster: Upcaster(event),
  events: List(RecordedEvent(event)),
) -> List(RecordedEvent(event)) {
  list.map(events, upcaster.upcast)
}

/// Chain two upcasters, applying `first` then `second`.
///
/// Use this to compose multiple versioned upcasters:
/// ```gleam
/// upcast.chain(v1_to_v2(), v2_to_v3())
/// ```
pub fn chain(
  first: Upcaster(event),
  second: Upcaster(event),
) -> Upcaster(event) {
  Upcaster(upcast: fn(recorded) { second.upcast(first.upcast(recorded)) })
}

/// Chain a list of upcasters, applying them left-to-right.
/// If the list is empty, returns the identity upcaster.
pub fn chain_all(upcasters: List(Upcaster(event))) -> Upcaster(event) {
  list.fold(upcasters, identity(), chain)
}
