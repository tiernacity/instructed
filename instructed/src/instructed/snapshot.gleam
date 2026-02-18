//// Snapshot types for aggregate and process manager state persistence.
////
//// Snapshots allow aggregate state to be cached at a point in time,
//// avoiding the need to replay all events from the beginning.
////
//// In Commanded, snapshots go through a serializer before storage,
//// making them effectively untyped. In Gleam, we use `Dynamic` for
//// the snapshot data field, and callers cast to their expected type
//// using `dynamic.from` and `dynamic.unsafe_coerce`.
////
//// ## Snapshot Versioning (Invariant 14)
////
//// The `snapshot_version` in SnapshotConfig should be incremented when
//// the aggregate's state type changes. Old snapshots with a different
//// version are ignored during state rebuild.
////
//// ## Usage
////
//// ```gleam
//// import gleam/dynamic
//// import instructed/snapshot
////
//// // Store a snapshot
//// let snap = snapshot.new_snapshot(
////   source_uuid: "account-123",
////   source_version: 42,
////   source_type: "BankAccount",
////   data: dynamic.from(my_state),
//// )
//// event_store.record_snapshot(snap)
////
//// // Read a snapshot
//// let assert Ok(snap) = event_store.read_snapshot("account-123")
//// let state: MyState = dynamic.unsafe_coerce(snap.data)
//// ```

import gleam/option.{type Option}

/// Configuration for aggregate snapshotting.
///
/// Equivalent to Commanded's per-aggregate snapshot configuration:
/// `snapshot_every: N, snapshot_version: N`
pub type SnapshotConfig {
  SnapshotConfig(
    /// Take a snapshot every N events. None disables snapshotting.
    /// Equivalent to Commanded's `snapshot_every` option.
    snapshot_every: Option(Int),
    /// Version of the snapshot format. Incrementing this invalidates
    /// all earlier recorded snapshots, forcing a full event replay.
    /// Equivalent to Commanded's `snapshot_version` option.
    snapshot_version: Int,
  )
}

/// Snapshot data stored in the event store.
///
/// The type parameter represents the stored state type. At the adapter
/// level, snapshots may use serialization to handle type erasure.
/// The in-memory adapter stores values directly.
///
/// For aggregate snapshots, the type parameter is the aggregate state type.
/// For process manager snapshots, it's the PM state type.
///
/// Equivalent to Commanded's `Commanded.EventStore.SnapshotData` struct.
pub type SnapshotData(data) {
  SnapshotData(
    /// The aggregate/PM instance identifier
    source_uuid: String,
    /// The aggregate/PM version at snapshot time
    source_version: Int,
    /// The type identifier (e.g., "BankAccount", "OrderProcess")
    /// Used with snapshot_version for invalidation
    source_type: String,
    /// The stored state data
    data: data,
    /// When the snapshot was created (milliseconds since epoch)
    created_at: Int,
  )
}

/// Default snapshot configuration (snapshotting disabled).
pub fn default_config() -> SnapshotConfig {
  SnapshotConfig(snapshot_every: option.None, snapshot_version: 1)
}

/// Create a new snapshot data record.
pub fn new_snapshot(
  source_uuid source_uuid: String,
  source_version source_version: Int,
  source_type source_type: String,
  data data: data,
) -> SnapshotData(data) {
  SnapshotData(
    source_uuid: source_uuid,
    source_version: source_version,
    source_type: source_type,
    data: data,
    created_at: now_ms(),
  )
}

/// Check if a snapshot should be taken based on config and events count.
pub fn snapshot_required(config: SnapshotConfig, events_since_snapshot: Int) -> Bool {
  case config.snapshot_every {
    option.None -> False
    option.Some(n) -> events_since_snapshot >= n
  }
}

/// Coerce snapshot data from one type to another.
/// This is safe at the Erlang level because the data is the same
/// binary/term. Used when the event store stores snapshots with a
/// different type parameter than the caller expects.
///
/// Example: store aggregate state as SnapshotData(state) but the
/// event store expects SnapshotData(event) — coerce between them.
/// Unsafe type coercion - the Erlang runtime doesn't check types,
/// so this is a no-op at runtime. Use carefully.
@external(erlang, "gleam_stdlib", "identity")
fn unsafe_coerce(a: a) -> b

pub fn coerce(snapshot: SnapshotData(a)) -> SnapshotData(b) {
  // At the Erlang level, SnapshotData is a tuple and the data field
  // is just a term. Reinterpreting the type parameter is safe.
  let data = unsafe_coerce(snapshot.data)
  SnapshotData(
    source_uuid: snapshot.source_uuid,
    source_version: snapshot.source_version,
    source_type: snapshot.source_type,
    data: data,
    created_at: snapshot.created_at,
  )
}

@external(erlang, "os", "system_time")
fn system_time_native() -> Int

fn now_ms() -> Int {
  system_time_native() / 1_000_000
}
