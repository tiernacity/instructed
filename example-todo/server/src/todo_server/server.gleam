//// Todo Server - starts the CQRS infrastructure with SQLite persistence

import gleam/erlang/process.{type Subject}
import gleam/result
import instructed/error
import instructed/event_store.{type EventStore}
import instructed/projection.{type ProjectionMessage}
import instructed/router.{type DispatchResult, type Router}
import instructed_sqlite
import todo_server/aggregate
import todo_server/projections.{type AllTodosState}
import todo_server/serialization
import todo_shared/domain.{type TodoCommand, type TodoEvent}
import todo_shared/views.{type ByPriorityState, type TodoView}

/// Server state holding all references
pub type TodoServer {
  TodoServer(
    router: Router(domain.Todo, TodoCommand, TodoEvent),
    event_store: EventStore(TodoEvent),
    active_todos: Subject(ProjectionMessage(TodoEvent, AllTodosState)),
    completed_todos: Subject(ProjectionMessage(TodoEvent, AllTodosState)),
    all_todos: Subject(ProjectionMessage(TodoEvent, AllTodosState)),
    overdue_todos: Subject(ProjectionMessage(TodoEvent, AllTodosState)),
    by_priority: Subject(ProjectionMessage(TodoEvent, ByPriorityState)),
    by_due_date: Subject(ProjectionMessage(TodoEvent, List(TodoView))),
  )
}

/// Start the todo server with SQLite persistence
pub fn start(db_path: String) -> Result(TodoServer, String) {
  let sqlite_config =
    instructed_sqlite.SqliteConfig(
      db_path: db_path,
      serialize: serialization.serialize_event,
      deserialize: serialization.deserialize_event,
      event_type: serialization.event_type_name,
    )

  let assert Ok(sqlite_actor) = instructed_sqlite.start(sqlite_config)
  let store = instructed_sqlite.to_event_store(sqlite_actor)

  let todo_router =
    router.new(
      aggregate: aggregate.todo_aggregate(),
      event_store: store,
      identity: fn(cmd) {
        case cmd {
          domain.CreateTodo(id, ..) -> id
          domain.UpdateDescription(id, ..) -> id
          domain.UpdatePriority(id, ..) -> id
          domain.UpdateDueDate(id, ..) -> id
          domain.CompleteTodo(id) -> id
          domain.ReopenTodo(id) -> id
          domain.DeleteTodo(id) -> id
        }
      },
    )
    |> router.with_prefix("todo-")

  let today = get_today()

  use active <- result.try(
    projection.start(projections.active_todos_projection(), store),
  )
  use completed <- result.try(
    projection.start(projections.completed_todos_projection(), store),
  )
  use all <- result.try(
    projection.start(projections.all_todos_projection(), store),
  )
  use overdue <- result.try(
    projection.start(projections.overdue_todos_projection(today), store),
  )
  use by_pri <- result.try(
    projection.start(projections.by_priority_projection(), store),
  )
  use by_due <- result.try(
    projection.start(projections.by_due_date_projection(), store),
  )

  Ok(TodoServer(
    router: todo_router,
    event_store: store,
    active_todos: active,
    completed_todos: completed,
    all_todos: all,
    overdue_todos: overdue,
    by_priority: by_pri,
    by_due_date: by_due,
  ))
}

/// Dispatch a command to the todo server
pub fn dispatch(
  server: TodoServer,
  command: TodoCommand,
) -> Result(DispatchResult(domain.Todo, TodoEvent), String) {
  case router.dispatch(server.router, command) {
    Ok(result) -> Ok(result)
    Error(err) ->
      case err {
        error.AggregateError(reason) -> Error(reason)
        error.Halted -> Error("Command halted by middleware")
        error.WrongExpectedVersion -> Error("Concurrency conflict")
        error.EventStoreError(reason) -> Error("Storage error: " <> reason)
        _ -> Error("Command dispatch failed")
      }
  }
}

/// Get active todos
pub fn get_active_todos(server: TodoServer) -> AllTodosState {
  projection.get_state(server.active_todos, 5000)
}

/// Get completed todos
pub fn get_completed_todos(server: TodoServer) -> AllTodosState {
  projection.get_state(server.completed_todos, 5000)
}

/// Get all todos
pub fn get_all_todos(server: TodoServer) -> AllTodosState {
  projection.get_state(server.all_todos, 5000)
}

/// Get overdue todos
pub fn get_overdue_todos(server: TodoServer) -> AllTodosState {
  projection.get_state(server.overdue_todos, 5000)
}

/// Get todos by priority
pub fn get_by_priority(server: TodoServer) -> ByPriorityState {
  projection.get_state(server.by_priority, 5000)
}

/// Get todos sorted by due date
pub fn get_by_due_date(server: TodoServer) -> List(TodoView) {
  projection.get_state(server.by_due_date, 5000)
}

fn get_today() -> String {
  "2026-02-17"
}
