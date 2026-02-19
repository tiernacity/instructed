//// Domain types for the Todo application.
//// Defines all commands, events, and aggregate state.

import gleam/option.{type Option}

/// Priority levels for todos
pub type Priority {
  Low
  Medium
  High
  Critical
}

/// Status of a todo item
pub type Status {
  Active
  Completed
  Deleted
}

/// The aggregate state for a todo item
pub type Todo {
  Todo(
    id: String,
    description: String,
    priority: Priority,
    due_date: String,
    status: Status,
    created_at: String,
    completed_at: Option(String),
  )
}

/// Commands for the todo aggregate
pub type TodoCommand {
  CreateTodo(
    id: String,
    description: String,
    priority: Priority,
    due_date: String,
  )
  UpdateDescription(id: String, description: String)
  UpdatePriority(id: String, priority: Priority)
  UpdateDueDate(id: String, due_date: String)
  CompleteTodo(id: String)
  ReopenTodo(id: String)
  DeleteTodo(id: String)
}

/// Domain events produced by the todo aggregate
pub type TodoEvent {
  TodoCreated(
    id: String,
    description: String,
    priority: Priority,
    due_date: String,
    created_at: String,
  )
  DescriptionUpdated(id: String, description: String)
  PriorityUpdated(id: String, priority: Priority)
  DueDateUpdated(id: String, due_date: String)
  TodoCompleted(id: String, completed_at: String)
  TodoReopened(id: String)
  TodoDeleted(id: String)
}

/// Convert priority to string
pub fn priority_to_string(p: Priority) -> String {
  case p {
    Low -> "low"
    Medium -> "medium"
    High -> "high"
    Critical -> "critical"
  }
}

/// Parse priority from string
pub fn priority_from_string(s: String) -> Result(Priority, String) {
  case s {
    "low" -> Ok(Low)
    "medium" -> Ok(Medium)
    "high" -> Ok(High)
    "critical" -> Ok(Critical)
    _ -> Error("Invalid priority: " <> s)
  }
}

/// Convert status to string
pub fn status_to_string(s: Status) -> String {
  case s {
    Active -> "active"
    Completed -> "completed"
    Deleted -> "deleted"
  }
}

/// Parse status from string
pub fn status_from_string(s: String) -> Result(Status, String) {
  case s {
    "active" -> Ok(Active)
    "completed" -> Ok(Completed)
    "deleted" -> Ok(Deleted)
    _ -> Error("Invalid status: " <> s)
  }
}
