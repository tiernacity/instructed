//// Todo Server - starts the CQRS infrastructure with PostgreSQL persistence

import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/result
import instructed/event_store.{type EventStore}
import instructed/projection.{type ProjectionMessage}
import instructed/router.{type DispatchResult, type Router}
import instructed_postgres
import pog
import app/aggregate
import app/domain.{type TodoCommand, type TodoEvent}
import app/projections.{type AllTodosState, type ByPriorityState, type TodoView}
import app/serialization

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

/// Start the todo server with PostgreSQL persistence
pub fn start(db_url: String) -> Result(TodoServer, String) {
  // Connect to database
  let pool_name = process.new_name(prefix: "todo_pool")
  let db_config = case pog.url_config(pool_name, db_url) {
    Ok(config) -> config
    Error(_) -> {
      io.println("Failed to parse database URL")
      panic as "Invalid database URL"
    }
  }

  let db = case pog.start(db_config) {
    Ok(started) -> started.data
    Error(_) -> {
      io.println("Failed to start database pool")
      panic as "Database connection failed"
    }
  }

  // Create schema
  let assert Ok(Nil) = instructed_postgres.create_schema(db)

  // Create event store
  let pg_config =
    instructed_postgres.PgConfig(
      db: db,
      serialize: serialization.serialize_event,
      deserialize: serialization.deserialize_event,
      event_type: serialization.event_type_name,
    )

  let store = instructed_postgres.new(pg_config)

  // Create router
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

  // Start projections
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
  // Simple date string - in production, use proper date library
  "2026-02-16"
}

import instructed/error

// Suppress warnings
