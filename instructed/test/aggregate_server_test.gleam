import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import instructed/aggregate
import instructed/aggregate_server
import instructed/in_memory_event_store
import instructed/snapshot

// --- Test domain ---

type Task {
  Task(id: String, title: String, done: Bool)
}

type TaskCommand {
  CreateTask(id: String, title: String)
  CompleteTask(id: String)
  RenameTask(id: String, title: String)
}

type TaskEvent {
  TaskCreated(id: String, title: String)
  TaskCompleted(id: String)
  TaskRenamed(id: String, title: String)
}

fn task_aggregate() -> aggregate.Aggregate(Task, TaskCommand, TaskEvent) {
  aggregate.new(
    empty_state: fn() { Task("", "", False) },
    execute: fn(state, cmd) {
      case cmd {
        CreateTask(id, title) ->
          case state.id == "" {
            True -> Ok([TaskCreated(id, title)])
            False -> Error("Task already exists")
          }
        CompleteTask(_id) ->
          case state.id != "" && !state.done {
            True -> Ok([TaskCompleted(state.id)])
            False -> Error("Cannot complete task")
          }
        RenameTask(_id, title) ->
          case state.id != "" {
            True -> Ok([TaskRenamed(state.id, title)])
            False -> Error("Task not found")
          }
      }
    },
    apply_event: fn(state, event) {
      case event {
        TaskCreated(id, title) -> Task(id, title, False)
        TaskCompleted(_) -> Task(..state, done: True)
        TaskRenamed(_, title) -> Task(..state, title: title)
      }
    },
  )
}

fn start_server(stream_id: String) {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)
  let config =
    aggregate_server.new_config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: stream_id,
    )
  let assert Ok(server) = aggregate_server.start(config)
  #(server, store)
}

// --- Tests ---

pub fn execute_creates_aggregate_test() {
  let #(server, _store) = start_server("task-1")

  let assert Ok(result) =
    aggregate_server.execute(server, CreateTask("1", "Buy milk"), 5000)
  should.equal(result.aggregate_state, Task("1", "Buy milk", False))
  should.equal(result.aggregate_version, 1)
  should.equal(result.events, [TaskCreated("1", "Buy milk")])
}

pub fn sequential_commands_test() {
  let #(server, _store) = start_server("task-2")

  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("2", "Do homework"), 5000)
  let assert Ok(result) =
    aggregate_server.execute(server, RenameTask("2", "Do all homework"), 5000)

  should.equal(result.aggregate_state.title, "Do all homework")
  should.equal(result.aggregate_version, 2)
}

pub fn domain_error_test() {
  let #(server, _store) = start_server("task-3")

  // Try to complete a task that doesn't exist
  let result = aggregate_server.execute(server, CompleteTask("3"), 5000)
  should.be_error(result)
}

pub fn persists_events_to_store_test() {
  let #(server, store) = start_server("task-4")
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("4", "Persist test"), 5000)

  // Read events from the store directly
  let assert Ok(events) = store.read_stream_forward("task-4", 1, 1000)
  should.equal(list.length(events), 1)
  let assert [first] = events
  should.equal(first.data, TaskCreated("4", "Persist test"))
}

pub fn complete_then_fail_test() {
  let #(server, _store) = start_server("task-5")
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("5", "Complete me"), 5000)
  let assert Ok(_) =
    aggregate_server.execute(server, CompleteTask("5"), 5000)

  // Trying to complete again should fail
  let result = aggregate_server.execute(server, CompleteTask("5"), 5000)
  should.be_error(result)
}

pub fn state_cached_across_commands_test() {
  let #(server, _store) = start_server("task-6")

  // Execute multiple commands - state should be cached
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("6", "Task A"), 5000)
  let assert Ok(_) =
    aggregate_server.execute(server, RenameTask("6", "Task B"), 5000)
  let assert Ok(result) =
    aggregate_server.execute(server, RenameTask("6", "Task C"), 5000)

  should.equal(result.aggregate_state.title, "Task C")
  should.equal(result.aggregate_version, 3)
}

pub fn multiple_events_per_command_test() {
  // This tests that no-op (Ok([])) works correctly
  let #(server, _store) = start_server("task-7")

  let assert Ok(result) =
    aggregate_server.execute(server, CreateTask("7", "Multi"), 5000)
  should.equal(list.length(result.events), 1)
}

pub fn snapshot_taken_after_n_events_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let snap_config =
    snapshot.SnapshotConfig(snapshot_every: Some(2), snapshot_version: 1)

  let config =
    aggregate_server.new_config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-snap",
    )
    |> aggregate_server.with_snapshot_config(snap_config)

  let assert Ok(server) = aggregate_server.start(config)

  // First command (1 event) - no snapshot yet
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("s", "Snap test"), 5000)
  should.be_error(store.read_snapshot("task-snap"))

  // Second command (2 events total) - snapshot should be taken
  let assert Ok(_) =
    aggregate_server.execute(server, RenameTask("s", "Snap test 2"), 5000)

  // Snapshot should exist now
  let assert Ok(snap) = store.read_snapshot("task-snap")
  should.equal(snap.source_uuid, "task-snap")
  should.equal(snap.source_version, 2)
  // Coerce back to verify state
  let state_snap: snapshot.SnapshotData(Task) = snapshot.coerce(snap)
  should.equal(state_snap.data.title, "Snap test 2")
}

// ---------------------------------------------------------------------------
// Fix 4: Snapshot version checking - matching version uses snapshot
// ---------------------------------------------------------------------------

pub fn snapshot_version_match_uses_snapshot_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let snap_config =
    snapshot.SnapshotConfig(snapshot_every: Some(2), snapshot_version: 1)

  let config =
    aggregate_server.new_config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-vcheck-1",
    )
    |> aggregate_server.with_snapshot_config(snap_config)

  let assert Ok(server) = aggregate_server.start(config)

  // Create and rename to trigger snapshot (2 events)
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("v1", "Version test"), 5000)
  let assert Ok(_) =
    aggregate_server.execute(server, RenameTask("v1", "Renamed"), 5000)

  // Verify snapshot was taken with version encoding
  let assert Ok(snap) = store.read_snapshot("task-vcheck-1")
  should.equal(snap.source_type, "aggregate:v1")

  // Now rebuild from the same config (version 1) — should use snapshot
  let result =
    aggregate.populate_from_event_store(
      task_aggregate(),
      store,
      "task-vcheck-1",
      snap_config,
    )
  let assert Ok(populated) = result
  should.equal(populated.state.title, "Renamed")
  should.equal(populated.version, 2)
}

// ---------------------------------------------------------------------------
// Fix 4: Snapshot version mismatch forces full replay
// ---------------------------------------------------------------------------

pub fn snapshot_version_mismatch_forces_replay_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  // Write snapshot with version 1
  let snap_config_v1 =
    snapshot.SnapshotConfig(snapshot_every: Some(2), snapshot_version: 1)

  let config =
    aggregate_server.new_config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-vcheck-2",
    )
    |> aggregate_server.with_snapshot_config(snap_config_v1)

  let assert Ok(server) = aggregate_server.start(config)
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("v2", "First title"), 5000)
  let assert Ok(_) =
    aggregate_server.execute(server, RenameTask("v2", "Second title"), 5000)

  // Snapshot exists at version 1
  let assert Ok(_snap) = store.read_snapshot("task-vcheck-2")

  // Now try to load with version 2 — snapshot should be IGNORED
  let snap_config_v2 =
    snapshot.SnapshotConfig(snapshot_every: Some(2), snapshot_version: 2)

  let result =
    aggregate.populate_from_event_store(
      task_aggregate(),
      store,
      "task-vcheck-2",
      snap_config_v2,
    )
  let assert Ok(populated) = result
  // State should still be correct (full replay from events)
  should.equal(populated.state.title, "Second title")
  should.equal(populated.version, 2)
}

// ---------------------------------------------------------------------------
// Fix 4: Snapshot encode/decode version
// ---------------------------------------------------------------------------

pub fn snapshot_encode_decode_version_test() {
  should.equal(
    snapshot.encode_snapshot_type("aggregate", 1),
    "aggregate:v1",
  )
  should.equal(
    snapshot.encode_snapshot_type("aggregate", 42),
    "aggregate:v42",
  )
  should.equal(
    snapshot.decode_snapshot_version("aggregate:v1"),
    Some(1),
  )
  should.equal(
    snapshot.decode_snapshot_version("aggregate:v42"),
    Some(42),
  )
  // Legacy format without version
  should.equal(
    snapshot.decode_snapshot_version("aggregate"),
    None,
  )
}
