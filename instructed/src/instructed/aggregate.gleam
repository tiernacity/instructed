//// Aggregate behaviour for event-sourced domain entities.
////
//// An aggregate is the core building block in CQRS/ES. It receives commands,
//// validates them against current state, and produces domain events.
//// State is rebuilt by replaying events through the `apply_event` function.
////
//// ## Design
////
//// In Gleam, we use records of functions instead of behaviours/traits.
//// This is Gleam's equivalent of Commanded's aggregate callbacks
//// (`execute/2` and `apply/2`).
////
//// The `Aggregate` type takes three type parameters:
//// - `state`: The aggregate's state type
//// - `command`: The command type the aggregate handles
//// - `event`: The domain event type the aggregate produces
////
//// ## State Rebuilding
////
//// State is rebuilt from events using `rebuild_state` (pure fold) or
//// `populate_from_event_store` (with snapshot support and batched reading).
//// The latter matches Commanded's `AggregateStateBuilder.populate/1`.
////
//// ## Invariants
////
//// - `apply_event` must never fail — events are facts that already occurred
//// - `execute` may return an empty list (no events) which is a valid "no-op"
//// - State is rebuilt deterministically from the same events
////
//// ## Example
////
//// ```gleam
//// import instructed/aggregate.{type Aggregate}
////
//// type BankAccount {
////   BankAccount(account_number: String, balance: Int)
//// }
////
//// type BankCommand {
////   OpenAccount(account_number: String, initial_balance: Int)
////   DepositMoney(amount: Int)
//// }
////
//// type BankEvent {
////   AccountOpened(account_number: String, initial_balance: Int)
////   MoneyDeposited(amount: Int, new_balance: Int)
//// }
////
//// fn bank_aggregate() -> Aggregate(BankAccount, BankCommand, BankEvent) {
////   aggregate.new(
////     empty_state: fn() { BankAccount("", 0) },
////     execute: fn(state, cmd) {
////       case cmd {
////         OpenAccount(num, balance) if balance > 0 ->
////           Ok([AccountOpened(num, balance)])
////         OpenAccount(_, _) ->
////           Error("Initial balance must be positive")
////         DepositMoney(amount) ->
////           Ok([MoneyDeposited(amount, state.balance + amount)])
////       }
////     },
////     apply_event: fn(state, event) {
////       case event {
////         AccountOpened(num, balance) ->
////           BankAccount(num, balance)
////         MoneyDeposited(_, new_balance) ->
////           BankAccount(..state, balance: new_balance)
////       }
////     },
////   )
//// }
//// ```

import gleam/list
import instructed/error
import instructed/event_store.{type EventStore}
import gleam/option.{None, Some}
import instructed/snapshot.{type SnapshotConfig}

/// An aggregate definition with strongly-typed state, commands, and events.
///
/// Equivalent to Commanded's aggregate module with `execute/2` and `apply/2`
/// callbacks, but expressed as a record of functions (idiomatic Gleam).
pub type Aggregate(state, command, event) {
  Aggregate(
    /// Function to create the initial empty state.
    /// Called when no events exist for the aggregate.
    empty_state: fn() -> state,
    /// Function to execute a command against state, returning events or an error.
    /// Equivalent to Commanded's `execute/2` callback.
    ///
    /// Valid return values:
    /// - `Ok([])` — command succeeded but produced no events (no-op)
    /// - `Ok(events)` — command succeeded with events to persist
    /// - `Error(reason)` — command rejected
    execute: fn(state, command) -> Result(List(event), String),
    /// Function to apply an event to state, returning new state.
    /// This must never fail — events are facts that have already occurred.
    /// Equivalent to Commanded's `apply/2` callback.
    apply_event: fn(state, event) -> state,
    /// Function to derive a type name string from an event value.
    /// Used when persisting events to the event store (Fix 5).
    /// Returns a human-readable, stable string identifying the event type.
    event_type: fn(event) -> String,
  )
}

/// The result of populating aggregate state from the event store.
/// Contains the rebuilt state and current version number.
pub type PopulatedState(state) {
  PopulatedState(state: state, version: Int)
}

/// Default batch size for reading events during state rebuild.
/// Matches Commanded's `@read_event_batch_size` of 1,000.
pub const default_read_batch_size = 1000

/// Create a new aggregate definition.
pub fn new(
  empty_state empty_state: fn() -> state,
  execute execute: fn(state, command) -> Result(List(event), String),
  apply_event apply_event: fn(state, event) -> state,
) -> Aggregate(state, command, event) {
  Aggregate(empty_state:, execute:, apply_event:, event_type: fn(_) { "" })
}

/// Create a new aggregate definition with a custom event_type function.
/// The event_type function derives a type name string from each event,
/// used when persisting events to the event store.
pub fn new_with_event_type(
  empty_state empty_state: fn() -> state,
  execute execute: fn(state, command) -> Result(List(event), String),
  apply_event apply_event: fn(state, event) -> state,
  event_type event_type: fn(event) -> String,
) -> Aggregate(state, command, event) {
  Aggregate(empty_state:, execute:, apply_event:, event_type:)
}

/// Set the event_type function on an existing aggregate.
pub fn with_event_type(
  aggregate: Aggregate(state, command, event),
  event_type: fn(event) -> String,
) -> Aggregate(state, command, event) {
  Aggregate(..aggregate, event_type: event_type)
}

/// Rebuild aggregate state from a list of events (pure function).
/// This is a simple fold — use `populate_from_event_store` for
/// production use with snapshots and batched reading.
pub fn rebuild_state(
  aggregate: Aggregate(state, command, event),
  events: List(event),
) -> state {
  list.fold(events, aggregate.empty_state(), aggregate.apply_event)
}

/// Populate aggregate state from the event store with snapshot support.
///
/// This is equivalent to Commanded's `AggregateStateBuilder.populate/1`:
/// 1. Attempt to read a snapshot for the aggregate
/// 2. If snapshot exists and version matches, use it as initial state
/// 3. Read events after the snapshot version (or from start if no snapshot)
/// 4. Read events in batches (default 1000, matching Commanded)
/// 5. Apply events to rebuild current state
///
/// Returns the populated state and current version.
pub fn populate_from_event_store(
  aggregate: Aggregate(state, command, event),
  event_store: EventStore(event),
  stream_id: String,
  snapshot_config: SnapshotConfig,
) -> Result(PopulatedState(state), String) {
  // Step 1: Try to read snapshot, checking version compatibility
  let #(initial_state, initial_version) = case
    snapshot_config.snapshot_every
  {
    None ->
      // Snapshots disabled
      #(aggregate.empty_state(), 0)
    Some(_) ->
      case event_store.read_snapshot(stream_id) {
        Ok(snapshot_data) -> {
          // Check snapshot version matches config (Invariant 14).
          // Old snapshots with wrong version are ignored, forcing full replay.
          let version_ok = case
            snapshot.decode_snapshot_version(snapshot_data.source_type)
          {
            option.Some(v) -> v == snapshot_config.snapshot_version
            // Legacy snapshot without version encoding — accept it
            option.None -> True
          }
          case version_ok {
            True -> {
              // Coerce SnapshotData(event) back to SnapshotData(state)
              let state_snapshot: snapshot.SnapshotData(state) =
                snapshot.coerce(snapshot_data)
              // Use snapshot as starting point, replay events from snapshot version
              #(state_snapshot.data, state_snapshot.source_version)
            }
            False ->
              // Version mismatch — ignore snapshot, full replay
              #(aggregate.empty_state(), 0)
          }
        }
        Error(_) ->
          // No snapshot, start from beginning
          #(aggregate.empty_state(), 0)
      }
  }

  // Step 2: Read events in batches and rebuild state
  rebuild_from_event_stream(
    aggregate,
    event_store,
    stream_id,
    initial_state,
    initial_version,
    default_read_batch_size,
  )
}

/// Populate aggregate state from the event store without snapshot support.
/// Reads all events in batches and applies them to rebuild state.
pub fn populate_from_event_store_simple(
  aggregate: Aggregate(state, command, event),
  event_store: EventStore(event),
  stream_id: String,
) -> Result(PopulatedState(state), String) {
  rebuild_from_event_stream(
    aggregate,
    event_store,
    stream_id,
    aggregate.empty_state(),
    0,
    default_read_batch_size,
  )
}

/// Rebuild state from events after a known version.
/// Used during retry after version conflict — reads only new events,
/// not the entire stream. This matches Commanded's behavior where
/// the aggregate process only reads events since its last known version.
pub fn rebuild_from_version(
  aggregate: Aggregate(state, command, event),
  event_store: EventStore(event),
  stream_id: String,
  current_state: state,
  current_version: Int,
) -> Result(PopulatedState(state), String) {
  rebuild_from_event_stream(
    aggregate,
    event_store,
    stream_id,
    current_state,
    current_version,
    default_read_batch_size,
  )
}

/// Internal: read events in batches and apply them.
fn rebuild_from_event_stream(
  aggregate: Aggregate(state, command, event),
  event_store: EventStore(event),
  stream_id: String,
  state: state,
  version: Int,
  batch_size: Int,
) -> Result(PopulatedState(state), String) {
  case
    event_store.read_stream_forward(stream_id, version + 1, batch_size)
  {
    Error(error.StreamNotFound) ->
      // Aggregate doesn't exist yet — return empty state
      Ok(PopulatedState(state: state, version: version))

    Error(err) -> {
      let reason = case err {
        error.VersionConflict -> "version conflict"
        error.StreamNotFound -> "stream not found"
        error.StreamAlreadyExists -> "stream already exists"
        error.SnapshotNotFound -> "snapshot not found"
        error.SubscriptionAlreadyExists -> "subscription already exists"
        error.SubscriptionNotFound -> "subscription not found"
        error.TooManySubscribers -> "too many subscribers"
        error.StorageError(r) -> "storage error: " <> r
      }
      Error("Failed to read events: " <> reason)
    }

    Ok(events) -> {
      // Apply events to state
      let #(new_state, new_version) =
        list.fold(events, #(state, version), fn(acc, recorded_event) {
          let #(s, _v) = acc
          let updated = aggregate.apply_event(s, recorded_event.data)
          #(updated, recorded_event.stream_version)
        })

      case list.length(events) < batch_size {
        True ->
          // Last batch — all events consumed
          Ok(PopulatedState(state: new_state, version: new_version))
        False ->
          // More events may exist — read next batch
          rebuild_from_event_stream(
            aggregate,
            event_store,
            stream_id,
            new_state,
            new_version,
            batch_size,
          )
      }
    }
  }
}
