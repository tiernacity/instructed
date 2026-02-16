//// Snapshot types for aggregate state persistence.
////
//// Snapshots allow aggregate state to be cached at a point in time,
//// avoiding the need to replay all events from the beginning.

import gleam/option.{type Option}

/// Configuration for aggregate snapshotting.
pub type SnapshotConfig {
  SnapshotConfig(
    /// Take a snapshot every N events. None disables snapshotting.
    snapshot_every: Option(Int),
    /// Version of the snapshot format. Incrementing this forces
    /// earlier snapshots to be ignored.
    snapshot_version: Int,
  )
}

/// Snapshot data stored in the event store.
pub type SnapshotData(state) {
  SnapshotData(
    /// The aggregate instance identifier
    source_uuid: String,
    /// The aggregate version at snapshot time
    source_version: Int,
    /// The snapshot format version
    source_type: String,
    /// The aggregate state at snapshot time
    data: state,
    /// When the snapshot was created (milliseconds since epoch)
    created_at: Int,
  )
}

/// Default snapshot configuration (snapshotting disabled).
pub fn default_config() -> SnapshotConfig {
  SnapshotConfig(snapshot_every: option.None, snapshot_version: 1)
}
