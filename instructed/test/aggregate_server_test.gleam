import gleeunit/should
import instructed/aggregate
import instructed/aggregate_server
import instructed/in_memory_event_store

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

// --- Tests ---

pub fn aggregate_server_execute_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let config =
    aggregate_server.Config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-1",
    )

  let assert Ok(server) = aggregate_server.start(config)

  // Execute create command
  let assert Ok(result) =
    aggregate_server.execute(server, CreateTask("1", "Buy milk"), 5000)
  should.equal(result.aggregate_state, Task("1", "Buy milk", False))
  should.equal(result.aggregate_version, 1)
  should.equal(result.events, [TaskCreated("1", "Buy milk")])
}

pub fn aggregate_server_sequential_commands_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let config =
    aggregate_server.Config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-2",
    )

  let assert Ok(server) = aggregate_server.start(config)

  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("2", "Do homework"), 5000)
  let assert Ok(result) =
    aggregate_server.execute(server, RenameTask("2", "Do all homework"), 5000)

  should.equal(result.aggregate_state.title, "Do all homework")
  should.equal(result.aggregate_version, 2)
}

pub fn aggregate_server_error_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let config =
    aggregate_server.Config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-3",
    )

  let assert Ok(server) = aggregate_server.start(config)

  // Try to complete a task that doesn't exist
  let result = aggregate_server.execute(server, CompleteTask("3"), 5000)
  should.be_error(result)
}

pub fn aggregate_server_persists_events_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let config =
    aggregate_server.Config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-4",
    )

  let assert Ok(server) = aggregate_server.start(config)
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("4", "Persist test"), 5000)

  // Read events from the store directly
  let assert Ok(events) = store.read_stream_forward("task-4", 1, 1000)
  should.equal(list.length(events), 1)
  let assert [first] = events
  should.equal(first.data, TaskCreated("4", "Persist test"))
}

pub fn aggregate_server_complete_then_fail_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let config =
    aggregate_server.Config(
      aggregate: task_aggregate(),
      event_store: store,
      stream_id: "task-5",
    )

  let assert Ok(server) = aggregate_server.start(config)
  let assert Ok(_) =
    aggregate_server.execute(server, CreateTask("5", "Complete me"), 5000)
  let assert Ok(_) =
    aggregate_server.execute(server, CompleteTask("5"), 5000)

  // Trying to complete again should fail
  let result = aggregate_server.execute(server, CompleteTask("5"), 5000)
  should.be_error(result)
}

import gleam/list
