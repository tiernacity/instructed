//// Snapshot conformance tests.
////
//// Tests that an EventStore correctly implements snapshot
//// read, record, delete, and upsert operations.

import instructed/conformance/test_event.{type TestEvent, Created}
import instructed/conformance/assertions as should
import instructed/error
import instructed/event_store.{type EventStore}
import instructed/snapshot.{SnapshotData}

/// Run all snapshot conformance tests.
pub fn run_all(factory: fn() -> EventStore(TestEvent)) -> Nil {
  test_read_nonexistent_snapshot(factory)
  test_record_and_read_snapshot(factory)
  test_delete_snapshot(factory)
  test_overwrite_snapshot(factory)
}

fn test_read_nonexistent_snapshot(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()
  let result = store.read_snapshot("nonexistent")
  let assert Error(error.SnapshotNotFound) = result
  Nil
}

fn test_record_and_read_snapshot(
  factory: fn() -> EventStore(TestEvent),
) -> Nil {
  let store = factory()
  let snap =
    SnapshotData(
      source_uuid: "src-1",
      source_version: 5,
      source_type: "test_aggregate",
      data: Created("snapshot-data"),
      created_at: 12_345,
    )
  let assert Ok(Nil) = store.record_snapshot(snap)

  let assert Ok(read_snap) = store.read_snapshot("src-1")
  should.equal(read_snap.source_uuid, "src-1")
  should.equal(read_snap.source_version, 5)
  should.equal(read_snap.source_type, "test_aggregate")
  should.equal(read_snap.data, Created("snapshot-data"))
  Nil
}

fn test_delete_snapshot(factory: fn() -> EventStore(TestEvent)) -> Nil {
  let store = factory()
  let snap =
    SnapshotData(
      source_uuid: "src-del",
      source_version: 3,
      source_type: "test",
      data: Created("to-delete"),
      created_at: 100,
    )
  let assert Ok(Nil) = store.record_snapshot(snap)
  let assert Ok(_) = store.read_snapshot("src-del")

  let assert Ok(Nil) = store.delete_snapshot("src-del")
  let assert Error(error.SnapshotNotFound) = store.read_snapshot("src-del")
  Nil
}

fn test_overwrite_snapshot(factory: fn() -> EventStore(TestEvent)) -> Nil {
  let store = factory()
  let snap1 =
    SnapshotData(
      source_uuid: "src-ow",
      source_version: 1,
      source_type: "test",
      data: Created("v1"),
      created_at: 100,
    )
  let assert Ok(Nil) = store.record_snapshot(snap1)

  let snap2 =
    SnapshotData(
      source_uuid: "src-ow",
      source_version: 5,
      source_type: "test",
      data: Created("v5"),
      created_at: 200,
    )
  let assert Ok(Nil) = store.record_snapshot(snap2)

  let assert Ok(read_snap) = store.read_snapshot("src-ow")
  should.equal(read_snap.source_version, 5)
  should.equal(read_snap.data, Created("v5"))
  Nil
}
