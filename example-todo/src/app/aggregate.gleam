//// Todo aggregate definition

import gleam/option.{None, Some}
import instructed/aggregate.{type Aggregate}
import app/domain.{
  type Todo, type TodoCommand, type TodoEvent, Active, Completed,
  CompleteTodo, CreateTodo, Deleted, DeleteTodo, DescriptionUpdated,
  DueDateUpdated, PriorityUpdated, ReopenTodo, Todo,
  TodoCompleted, TodoCreated, TodoDeleted, TodoReopened,
  UpdateDescription, UpdateDueDate, UpdatePriority,
}

/// Create the todo aggregate definition
pub fn todo_aggregate() -> Aggregate(Todo, TodoCommand, TodoEvent) {
  aggregate.new(
    empty_state: fn() {
      Todo(
        id: "",
        description: "",
        priority: domain.Low,
        due_date: "",
        status: Active,
        created_at: "",
        completed_at: None,
      )
    },
    execute: execute_command,
    apply_event: apply_event,
  )
}

fn execute_command(
  state: Todo,
  cmd: TodoCommand,
) -> Result(List(TodoEvent), String) {
  case cmd {
    CreateTodo(id, description, priority, due_date) ->
      case state.id == "" {
        True -> {
          case description == "" {
            True -> Error("Description cannot be empty")
            False ->
              Ok([TodoCreated(id, description, priority, due_date, "now")])
          }
        }
        False -> Error("Todo already exists")
      }

    UpdateDescription(_, description) ->
      case state.id != "" && state.status != Deleted {
        True ->
          case description == "" {
            True -> Error("Description cannot be empty")
            False -> Ok([DescriptionUpdated(state.id, description)])
          }
        False -> Error("Todo not found or deleted")
      }

    UpdatePriority(_, priority) ->
      case state.id != "" && state.status != Deleted {
        True -> Ok([PriorityUpdated(state.id, priority)])
        False -> Error("Todo not found or deleted")
      }

    UpdateDueDate(_, due_date) ->
      case state.id != "" && state.status != Deleted {
        True -> Ok([DueDateUpdated(state.id, due_date)])
        False -> Error("Todo not found or deleted")
      }

    CompleteTodo(_) ->
      case state.id != "" {
        True ->
          case state.status {
            Active -> Ok([TodoCompleted(state.id, "now")])
            Completed -> Error("Todo is already completed")
            Deleted -> Error("Cannot complete a deleted todo")
          }
        False -> Error("Todo not found")
      }

    ReopenTodo(_) ->
      case state.id != "" {
        True ->
          case state.status {
            Completed -> Ok([TodoReopened(state.id)])
            Active -> Error("Todo is already active")
            Deleted -> Error("Cannot reopen a deleted todo")
          }
        False -> Error("Todo not found")
      }

    DeleteTodo(_) ->
      case state.id != "" {
        True ->
          case state.status {
            Deleted -> Error("Todo is already deleted")
            _ -> Ok([TodoDeleted(state.id)])
          }
        False -> Error("Todo not found")
      }
  }
}

fn apply_event(state: Todo, event: TodoEvent) -> Todo {
  case event {
    TodoCreated(id, description, priority, due_date, created_at) ->
      Todo(
        id: id,
        description: description,
        priority: priority,
        due_date: due_date,
        status: Active,
        created_at: created_at,
        completed_at: None,
      )

    DescriptionUpdated(_, description) ->
      Todo(..state, description: description)

    PriorityUpdated(_, priority) -> Todo(..state, priority: priority)

    DueDateUpdated(_, due_date) -> Todo(..state, due_date: due_date)

    TodoCompleted(_, completed_at) ->
      Todo(..state, status: Completed, completed_at: Some(completed_at))

    TodoReopened(_) ->
      Todo(..state, status: Active, completed_at: None)

    TodoDeleted(_) -> Todo(..state, status: Deleted)
  }
}
